package com.logistream.tracking.controller;

import com.logistream.common.event.TelemetryEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@Controller
@RequestMapping("/api/telemetry")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class TelemetryController {

    private final RedisTemplate<String, Object> redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final JdbcTemplate jdbcTemplate;

    private static final String REDIS_SESSIONS_KEY = "logistream:active_sessions";
    private static final String REDIS_VEHICLES_KEY = "logistream:vehicle:latest";
    private static final String REDIS_HISTORY_LIST = "logistream:telemetry:history:pending";
    
    // In-memory counter for consecutive temperature anomalies (> 5.0) per vehicle
    private final Map<String, Integer> tempAnomalyCounters = new ConcurrentHashMap<>();

    @PostConstruct
    public void initDatabaseSchema() {
        try {
            log.info("Initializing PostGIS extension and database tables...");
            jdbcTemplate.execute("CREATE EXTENSION IF NOT EXISTS postgis;");
            
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_history (
                    id BIGSERIAL PRIMARY KEY,
                    vehicle_id VARCHAR(50) NOT NULL,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,
                    speed DOUBLE PRECISION NOT NULL,
                    temperature DOUBLE PRECISION NOT NULL,
                    timestamp BIGINT NOT NULL,
                    geom GEOMETRY(Point, 4326)
                );
            """);
            
            // Create index on geom for spatial queries
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS telemetry_geom_idx ON telemetry_history USING gist(geom);");
            log.info("Database schema initialized successfully.");
        } catch (Exception e) {
            log.error("Failed to initialize database schema: {}", e.getMessage());
        }
    }

    // Handles WebSocket Incoming Telemetry
    @MessageMapping("/telemetry")
    public void receiveTelemetry(TelemetryEvent event) {
        log.debug("Received telemetry event: {}", event);

        // 1. Write latest to Redis Hash
        redisTemplate.opsForHash().put(REDIS_VEHICLES_KEY, event.vehicleId(), event);

        // 2. Push to Redis List (For batch persistence)
        redisTemplate.opsForList().rightPush(REDIS_HISTORY_LIST, event);

        // 3. Broadcast to subscribing frontend maps clients
        messagingTemplate.convertAndSend("/topic/telemetry", event);

        // 4. Anomaly detection: Temperature > 5.0 C
        if (event.temperature() > 5.0) {
            int count = tempAnomalyCounters.merge(event.vehicleId(), 1, Integer::sum);
            log.warn("Vehicle {} temperature anomaly tick: {} (Temp: {} C)", event.vehicleId(), count, event.temperature());
            if (count >= 3) {
                // Trigger Alarm Event
                Map<String, Object> alarm = Map.of(
                        "vehicleId", event.vehicleId(),
                        "temperature", event.temperature(),
                        "latitude", event.latitude(),
                        "longitude", event.longitude(),
                        "message", "CRITICAL TEMPERATURE ALARM! Temperature exceeded 5°C consecutively: " + event.temperature() + "°C",
                        "timestamp", System.currentTimeMillis()
                );
                messagingTemplate.convertAndSend("/topic/alerts", alarm);
                log.error("Sent temperature anomaly alert for vehicle {}", event.vehicleId());
            }
        } else {
            // Reset counter on acceptable temperature reading
            tempAnomalyCounters.put(event.vehicleId(), 0);
        }
    }

    // WebSocket Connection Events to track active sessions
    @EventListener
    public void handleSessionConnect(SessionConnectEvent event) {
        Long count = redisTemplate.opsForValue().increment(REDIS_SESSIONS_KEY);
        log.info("Client connected. Active sessions: {}", count);
        messagingTemplate.convertAndSend("/topic/sessions", Map.of("activeSessions", count));
    }

    @EventListener
    public void handleSessionDisconnect(SessionDisconnectEvent event) {
        Long count = redisTemplate.opsForValue().decrement(REDIS_SESSIONS_KEY);
        if (count != null && count < 0) {
            redisTemplate.opsForValue().set(REDIS_SESSIONS_KEY, 0);
            count = 0L;
        }
        log.info("Client disconnected. Active sessions: {}", count);
        messagingTemplate.convertAndSend("/topic/sessions", Map.of("activeSessions", count));
    }

    // REST: Get Active Session Count
    @GetMapping("/sessions")
    public ResponseEntity<Map<String, Object>> getActiveSessions() {
        Object countVal = redisTemplate.opsForValue().get(REDIS_SESSIONS_KEY);
        long count = countVal != null ? Long.parseLong(countVal.toString()) : 0L;
        return ResponseEntity.ok(Map.of("activeSessions", count));
    }

    // REST: Get all latest vehicle locations
    @GetMapping("/vehicles")
    public ResponseEntity<Map<Object, Object>> getLatestVehicles() {
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(REDIS_VEHICLES_KEY);
        return ResponseEntity.ok(entries);
    }

    // Batch persistence scheduler (runs every 15 seconds to flush Redis logs into PostgreSQL)
    @Scheduled(fixedRate = 15000)
    public void flushTelemetryToDatabase() {
        Long size = redisTemplate.opsForList().size(REDIS_HISTORY_LIST);
        if (size == null || size == 0) {
            return;
        }

        log.info("Scheduled batch flush: Found {} telemetry records pending in Redis", size);
        List<TelemetryEvent> eventsToPersist = new ArrayList<>();
        
        // Pop all events from Redis list
        for (int i = 0; i < size; i++) {
            TelemetryEvent event = (TelemetryEvent) redisTemplate.opsForList().leftPop(REDIS_HISTORY_LIST);
            if (event != null) {
                eventsToPersist.add(event);
            }
        }

        if (eventsToPersist.isEmpty()) {
            return;
        }

        // Batch insert using JdbcTemplate
        String sql = """
            INSERT INTO telemetry_history (vehicle_id, latitude, longitude, speed, temperature, timestamp, geom)
            VALUES (?, ?, ?, ?, ?, ?, ST_SetSRID(ST_MakePoint(?, ?), 4326))
        """;

        try {
            jdbcTemplate.batchUpdate(sql, eventsToPersist, 100, (ps, event) -> {
                ps.setString(1, event.vehicleId());
                ps.setDouble(2, event.latitude());
                ps.setDouble(3, event.longitude());
                ps.setDouble(4, event.speed());
                ps.setDouble(5, event.temperature());
                ps.setLong(6, event.timestamp());
                ps.setDouble(7, event.longitude()); // ST_MakePoint(longitude, latitude)
                ps.setDouble(8, event.latitude());
            });
            log.info("Batch persistence completed. Successfully persisted {} records to PostgreSQL", eventsToPersist.size());
        } catch (Exception e) {
            log.error("Failed to execute batch persistence: {}", e.getMessage(), e);
            // Put events back in Redis to prevent data loss
            for (TelemetryEvent event : eventsToPersist) {
                redisTemplate.opsForList().rightPush(REDIS_HISTORY_LIST, event);
            }
        }
    }
}
