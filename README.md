# LogiStream: Enterprise Intelligent Logistics Hub

LogiStream is a high-concurrency, real-time supply chain and telemetry control tower platform. The system simulates end-to-end logistics routing, package sorting state-machines, and real-time fleet GPS tracking.

## Technical Architecture

The platform uses a **Monorepo** structure built with:
- **Angular (v18+)** + **RxJS** + **Leaflet Maps**: High-performance UI utilizing RxJS event throttling to keep DOM renders fluid (55-60 FPS) under massive telemetry throughput.
- **Spring Boot 3.3.1 (Java 21 Virtual Threads)**: Low-latency asynchronous microservices.
- **PostgreSQL + PostGIS**: Acid persistence and geospatial queries.
- **Redis Cache**: Sub-millisecond hot cache for live coordinates tracking and session management.
- **Apache Kafka (KRaft mode)**: High-performance message broker.

---

## Directory Structure

```
enterprise-logistics-hub/               <-- Root Monorepo Workspace
├── logistics-frontend/                 <-- Angular Frontend Application
│   ├── Dockerfile                      <-- Multi-stage Nginx-ready Docker build
│   └── src/app/
│       ├── core/                       <-- RxJS WebSocket (STOMP) connections
│       ├── features/                   <-- Control Tower, Warehouse, & Driver Panel
│       └── shared/                     <-- Core types and widgets
├── logistics-backend/                  <-- Multi-Module Spring Boot Backend
│   ├── pom.xml                         <-- Parent Maven config
│   ├── logistics-shared-common/        <-- Shared Java Records & DTOs
│   ├── sorting-warehouse-service/      <-- Package State transitions (Spring Statemachine)
│   ├── fleet-tracking-websocket/       <-- STOMP Websocket & Redis cache manager
│   └── routing-optimization-engine/    <-- VRP Route optimization engine (Timefold)
├── docker-compose.yml                  <-- Local Infrastructure Orchestration
└── README.md                           <-- Main Project Guide
```

---

## Execution Instructions

To spin up the entire ecosystem (databases, broker, backend microservices, and frontend webapp), execute a single command from the project root:

```bash
docker-compose up --build
```

### Access Points
- **Angular Frontend UI**: [http://localhost](http://localhost)
- **Warehouse Sorting REST API**: [http://localhost:8081](http://localhost:8081)
- **Fleet WebSocket STOMP Engine**: `ws://localhost:8082/ws-fleet`
- **Routing Solver API**: [http://localhost:8083](http://localhost:8083)

---

## Optimization Highlight: RxJS Telemetry Throttling
To prevent CPU/RAM overhead on the dashboard client map from hundreds of vehicle events:
1. The simulated vehicles transmit coordinates every `500ms` via WebSockets.
2. The Angular app captures the WebSocket stream via STOMP subscription.
3. The stream passes through a `.pipe(throttleTime(250))` RxJS operator.
4. Leaflet map elements are re-rendered **at most 4 times per second**, preserving the browser's 60 FPS animation loop while using less than 15% CPU.
