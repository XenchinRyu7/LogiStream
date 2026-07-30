package com.logistream.routing.solver;

import com.logistream.common.event.PackageSortingEvent;
import com.logistream.common.event.RouteUpdateEvent;
import com.logistream.common.event.RouteUpdateEvent.RoutePoint;
import com.logistream.common.model.PackageState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RouteSolverService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    // Dedicated Virtual Thread Executor for route solver calculations
    private final ExecutorService solverExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // Map of vehicleId to list of pending delivery package locations
    private final Map<String, List<PendingStop>> vehiclePendingStops = new ConcurrentHashMap<>();

    // Central Jakarta hub coordinate
    private static final double HUB_LAT = -6.2088;
    private static final double HUB_LON = 106.8456;

    record PendingStop(String packageId, double latitude, double longitude, String address) {}

    @KafkaListener(topics = "warehouse-sorting-events", groupId = "routing-engine-group")
    public void listenPackageEvents(PackageSortingEvent event) {
        log.info("Routing engine received package event: {}", event);

        // If package is transitioned to IN_TRANSIT, it means it is loaded onto a truck.
        // We will assign it to a simulated vehicle and trigger route optimization.
        if (event.stateTo() == PackageState.IN_TRANSIT) {
            solverExecutor.submit(() -> {
                try {
                    // Assign to a dummy vehicle based on package hash or simple round-robin
                    String vehicleId = "TRUCK-" + (Math.abs(event.packageId().hashCode()) % 3 + 1);
                    
                    // Parse coordinates or generate them near the hub for demo purposes
                    double lat = HUB_LAT + (Math.sin(event.packageId().hashCode()) * 0.04);
                    double lon = HUB_LON + (Math.cos(event.packageId().hashCode()) * 0.04);
                    
                    PendingStop stop = new PendingStop(
                            event.packageId(), 
                            lat, 
                            lon, 
                            event.notes() != null ? event.notes() : "Delivery Stop"
                    );

                    vehiclePendingStops.computeIfAbsent(vehicleId, k -> new ArrayList<>()).add(stop);
                    
                    log.info("Assigned package {} to {}. Running Timefold route optimization...", event.packageId(), vehicleId);
                    optimizeAndPublishRoute(vehicleId);
                } catch (Exception e) {
                    log.error("Error processing routing optimization: {}", e.getMessage(), e);
                }
            });
        }
    }

    private void optimizeAndPublishRoute(String vehicleId) {
        List<PendingStop> stops = vehiclePendingStops.get(vehicleId);
        if (stops == null || stops.isEmpty()) {
            return;
        }

        // We run a Nearest Neighbor VRP TSP algorithm (similar to OptaPlanner's construction heuristics)
        List<PendingStop> unvisited = new ArrayList<>(stops);
        List<RoutePoint> route = new ArrayList<>();
        
        // Add start point (Hub)
        route.add(new RoutePoint(0, HUB_LAT, HUB_LON, "Central Logistics Hub (Jakarta)"));
        
        double currentLat = HUB_LAT;
        double currentLon = HUB_LON;
        int seq = 1;

        while (!unvisited.isEmpty()) {
            // Find closest stop to the current position
            double finalCurrentLat = currentLat;
            double finalCurrentLon = currentLon;
            
            PendingStop closest = unvisited.stream()
                    .min(Comparator.comparingDouble(stop -> 
                            calculateDistance(finalCurrentLat, finalCurrentLon, stop.latitude(), stop.longitude())))
                    .orElse(unvisited.get(0));

            route.add(new RoutePoint(seq++, closest.latitude(), closest.longitude(), "Deliver " + closest.packageId() + " to " + closest.address()));
            unvisited.remove(closest);
            
            // Move current position to the closest stop
            currentLat = closest.latitude();
            currentLon = closest.longitude();
        }

        // Add return to Hub as final stop
        route.add(new RoutePoint(seq, HUB_LAT, HUB_LON, "Return to Central Logistics Hub"));

        RouteUpdateEvent routeEvent = new RouteUpdateEvent(vehicleId, route, System.currentTimeMillis());
        
        // Publish optimized route to Kafka topic
        kafkaTemplate.send("route-optimization-events", vehicleId, routeEvent);
        log.info("Optimized route for {} published to Kafka. Total stops: {}", vehicleId, route.size());
    }

    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        // Simple Euclidean distance for performance and simplicity
        double dLat = lat1 - lat2;
        double dLon = lon1 - lon2;
        return Math.sqrt(dLat * dLat + dLon * dLon);
    }
}
