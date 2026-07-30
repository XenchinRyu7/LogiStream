package com.logistream.common.event;

import java.util.List;

public record RouteUpdateEvent(
    String vehicleId,
    List<RoutePoint> routePoints,
    long timestamp
) {
    public record RoutePoint(
        int sequence,
        double latitude,
        double longitude,
        String stopName
    ) {}
}
