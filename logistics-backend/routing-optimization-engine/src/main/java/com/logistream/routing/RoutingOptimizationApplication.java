package com.logistream.routing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class RoutingOptimizationApplication {
    public static void main(String[] args) {
        SpringApplication.run(RoutingOptimizationApplication.class, args);
    }
}
