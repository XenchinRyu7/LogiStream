package com.logistream.warehouse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class SortingWarehouseApplication {
    public static void main(String[] args) {
        SpringApplication.run(SortingWarehouseApplication.class, args);
    }
}
