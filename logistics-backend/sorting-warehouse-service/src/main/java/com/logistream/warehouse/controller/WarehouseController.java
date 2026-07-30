package com.logistream.warehouse.controller;

import com.logistream.warehouse.model.PackageEntity;
import com.logistream.warehouse.model.PackageEvent;
import com.logistream.warehouse.service.WarehouseSortingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/packages")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class WarehouseController {

    private final WarehouseSortingService warehouseSortingService;

    @GetMapping
    public ResponseEntity<List<PackageEntity>> getPackages() {
        return ResponseEntity.ok(warehouseSortingService.getAllPackages());
    }

    @PostMapping("/simulate")
    public ResponseEntity<Map<String, String>> triggerSimulation(@RequestParam(defaultValue = "10") int batchSize) {
        warehouseSortingService.simulateSortingBatch(batchSize);
        return ResponseEntity.ok(Map.of("message", "Triggered simulation of " + batchSize + " packages in background virtual threads"));
    }

    @PostMapping("/{id}/event")
    public ResponseEntity<PackageEntity> sendEvent(
            @PathVariable String id,
            @RequestParam PackageEvent event,
            @RequestParam(required = false, defaultValue = "") String notes) {
        try {
            PackageEntity updated = warehouseSortingService.transitionState(id, event, notes);
            return ResponseEntity.ok(updated);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }
}
