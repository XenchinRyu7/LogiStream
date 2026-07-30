package com.logistream.common.event;

import com.logistream.common.model.PackageState;
import java.time.LocalDateTime;

public record PackageSortingEvent(
    String packageId,
    String eventId,
    PackageState stateFrom,
    PackageState stateTo,
    String timestamp,
    String notes,
    String activeThreadName
) {}
