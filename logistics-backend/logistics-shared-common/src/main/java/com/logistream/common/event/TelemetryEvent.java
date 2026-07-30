package com.logistream.common.event;

public record TelemetryEvent(
    String vehicleId,
    double latitude,
    double longitude,
    double speed,
    double temperature,
    long timestamp
) {}
