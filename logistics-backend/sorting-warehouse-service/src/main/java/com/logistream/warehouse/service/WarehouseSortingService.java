package com.logistream.warehouse.service;

import com.logistream.common.event.PackageSortingEvent;
import com.logistream.common.model.PackageState;
import com.logistream.warehouse.config.StateMachineConfig;
import com.logistream.warehouse.model.PackageEntity;
import com.logistream.warehouse.model.PackageEvent;
import com.logistream.warehouse.repository.PackageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseSortingService {

    private final PackageRepository packageRepository;
    private final StateMachineFactory<PackageState, PackageEvent> stateMachineFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // Dedicated Virtual Thread Executor for simulations
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public PackageEntity createPackage(PackageEntity pkg) {
        pkg.setState(PackageState.MANIFESTED);
        pkg.setLastUpdatedByThread(Thread.currentThread().toString());
        PackageEntity saved = packageRepository.save(pkg);
        publishStateEvent(saved, null, PackageState.MANIFESTED, "Package registered in system");
        return saved;
    }

    public synchronized PackageEntity transitionState(String packageId, PackageEvent event, String notes) {
        PackageEntity pkg = packageRepository.findById(packageId)
                .orElseThrow(() -> new IllegalArgumentException("Package not found: " + packageId));

        PackageState originalState = pkg.getState();

        // Initialize state machine restore context
        StateMachine<PackageState, PackageEvent> stateMachine = stateMachineFactory.getStateMachine(packageId);
        stateMachine.stopReactively().block();
        stateMachine.getStateMachineAccessor().doWithAllRegions(accessor -> {
            accessor.resetStateMachineReactively(new DefaultStateMachineContext<>(
                    originalState, null, null, null)).block();
        });
        stateMachine.startReactively().block();

        // Send event to trigger transition
        boolean success = stateMachine.sendEvent(event);
        if (!success) {
            log.warn("Failed to transition package {} with event {} from state {}", packageId, event, originalState);
            return pkg;
        }

        PackageState newState = stateMachine.getState().getId();
        pkg.setState(newState);
        pkg.setLastUpdatedByThread(Thread.currentThread().toString());
        pkg.setNotes(notes);
        
        PackageEntity updatedPkg = packageRepository.save(pkg);
        publishStateEvent(updatedPkg, originalState, newState, notes);

        return updatedPkg;
    }

    public List<PackageEntity> getAllPackages() {
        return packageRepository.findAll();
    }

    public void simulateSortingBatch(int batchSize) {
        log.info("Starting virtual thread batch simulation for {} packages", batchSize);
        Random random = new Random();

        for (int i = 0; i < batchSize; i++) {
            final String packageId = "PKG-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            final int index = i;
            
            // Spawn a task per package using virtual thread
            virtualThreadExecutor.submit(() -> {
                try {
                    // Step 1: Create package in MANIFESTED state
                    PackageEntity pkg = PackageEntity.builder()
                            .packageId(packageId)
                            .recipientName("Recipient " + (index + 1))
                            .destinationAddress("Address " + (100 + index) + " St, Jakarta")
                            .latitude(-6.2088 + (random.nextDouble() - 0.5) * 0.05)
                            .longitude(106.8456 + (random.nextDouble() - 0.5) * 0.05)
                            .notes("Simulated Package")
                            .build();
                    createPackage(pkg);
                    
                    // Sleep random time before sorting starts (1 to 3 seconds)
                    Thread.sleep(1000 + random.nextInt(2000));

                    // Step 2: Transition MANIFESTED -> HUB_SORTING
                    transitionState(packageId, PackageEvent.START_SORTING, "Sorting started in Hub Jakarta");

                    // Sleep random time for sorting (2 to 4 seconds)
                    Thread.sleep(2000 + random.nextInt(2000));

                    // Step 3: Transition HUB_SORTING -> IN_TRANSIT
                    transitionState(packageId, PackageEvent.FINISH_SORTING, "Loaded onto delivery truck");

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Simulation thread interrupted for package {}", packageId, e);
                } catch (Exception e) {
                    log.error("Error in simulation for package {}", packageId, e);
                }
            });
        }
    }

    private void publishStateEvent(PackageEntity pkg, PackageState fromState, PackageState toState, String notes) {
        String eventId = UUID.randomUUID().toString();
        PackageSortingEvent event = new PackageSortingEvent(
                pkg.getPackageId(),
                eventId,
                fromState,
                toState,
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                notes,
                pkg.getLastUpdatedByThread()
        );
        try {
            kafkaTemplate.send("warehouse-sorting-events", pkg.getPackageId(), event);
            log.info("Published PackageSortingEvent to Kafka: {}", event);
        } catch (Exception e) {
            log.error("Failed to send Kafka event for package {}: {}", pkg.getPackageId(), e.getMessage());
        }
    }
}
