package com.logistream.warehouse.repository;

import com.logistream.warehouse.model.PackageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PackageRepository extends JpaRepository<PackageEntity, String> {
}
