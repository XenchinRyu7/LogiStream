package com.logistream.tracking.consumer;

import com.logistream.common.event.RouteUpdateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RouteEventListener {

    private final SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "route-optimization-events", groupId = "websocket-tracking-group")
    public void listenRouteUpdates(RouteUpdateEvent event) {
        log.info("WebSocket Service received RouteUpdateEvent from Kafka: {}", event);
        // Forward to WebSocket clients on topic /topic/routes
        messagingTemplate.convertAndSend("/topic/routes", event);
        log.info("Broadcasted RouteUpdateEvent to /topic/routes for vehicle {}", event.vehicleId());
    }
}
