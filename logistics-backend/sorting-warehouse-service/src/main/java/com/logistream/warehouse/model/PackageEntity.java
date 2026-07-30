package com.logistream.warehouse.model;

import com.logistream.common.model.PackageState;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "packages")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PackageEntity {

    @Id
    private String packageId;

    @Enumerated(EnumType.STRING)
    private PackageState state;

    private String recipientName;
    private String destinationAddress;
    private double latitude;
    private double longitude;

    private String lastUpdatedByThread;
    private String notes;
}
