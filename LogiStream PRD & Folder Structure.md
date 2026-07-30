# **Product Requirement Document (PRD)**

**Project Name:** Enterprise Intelligent Logistics Hub (LogiStream)  
**Author:** Saeful Rohman  
**Stack Architecture:** Monorepo (Angular Enterprise Frontend \+ Multi-Module Spring Boot Backend)  
**Target Capabilities:** High Concurrency, Real-Time Telemetry Stream Throttling, Event-Driven Architecture

## ---

**1\. Project Overview & Objectives**

LogiStream adalah ekosistem platform logistik skala *multinasional* untuk manajemen supply chain hulu-ke-hilir (end-to-end). Sistem ini didesain untuk menangani *high-throughput event streaming* dari pergerakan armada (IoT) dan inventaris gudang secara asinkronus.  
Alih-alih menggunakan React/Next.js yang rentan *performance drop* pada manipulasi data real-time masif, proyek ini menggunakan **Angular dengan RxJS engine** di frontend untuk melakukan manajemen *data stream throttling* berlatensi rendah guna menjamin stabilitas browser pada level *Control Tower Dashboard*.

### **Key Goals:**

> * **High Throughput & Concurrency:** Memproses ribuan transisi status paket per detik tanpa *blocking* di backend menggunakan Java 21 Virtual Threads.  
> * **Strict Structural Uniformity:** Menggunakan arsitektur *opinionated* baik di backend (Spring Boot) maupun frontend (Angular) untuk mensimulasikan standarisasi kode tim enterprise skala besar.  
> * **Optimized Real-Time Stream:** Membatasi tekanan data (*backpressure*) dari WebSocket di frontend menggunakan operator RxJS agar performa UI tetap mulus di bawah 60 FPS.

## ---

**2\. System Architecture & Component Mapping**

Proyek ini diatur dalam satu repositori (**Monorepo**) yang terbagi menjadi komponen berikut:

### **A. Frontend (logistics-frontend)**

> * **Tech Stack:** Angular (v17/v18+) / TypeScript / RxJS (Reactive Extensions) / TailwindCSS / Mapbox GL JS (atau Leaflet).  
> * **Scope:** Dashboard pemantauan utama (Control Tower), panel simulasi kurir/IoT, dan sistem manajemen status gudang.

### **B. Backend (logistics-backend)**

Menggunakan Maven/Gradle Multi-Module Project:

> 1. **logistics-shared-common**: Modul library internal berisi Java record untuk skema Event (Kafka Topics payloads) dan DTO global untuk menjamin kontrak tipe data yang *strict* dengan frontend.  
> 2. **sorting-warehouse-service**: Core business logic yang mengelola siklus hidup paket menggunakan **Spring Statemachine** dan PostgreSQL.  
> 3. **fleet-tracking-websocket**: Service *high-concurrency* berbasis Spring Boot khusus untuk me-manage ribuan koneksi WebSocket masuk (dari kurir) dan menyebarkannya keluar (ke Angular operator).  
> 4. **routing-optimization-engine**: Service komputasi berat menggunakan **Timefold / OptaPlanner** untuk kalkulasi VRP (Vehicle Routing Problem).

## ---

**3\. Epics & User Stories**

### **Epic 1: High-Concurrency Package Sorting Simulator**

> * **User Story:** Sebagai Manager Gudang, saya ingin sistem dapat memproses penyortiran batch paket berskala besar secara bersamaan agar status logistik berpindah secara *real-time* tanpa ada data yang hilang/korup.  
> * **Functional Requirements:**  
  * Backend memproses transisi state paket (MANIFESTED → HUB\_SORTING\` → IN\_TRANSIT) secara asinkronus menggunakan **Java 21 Virtual Threads (Project Loom)**.  
  * Setiap transisi status sukses wajib melempar event ke Apache Kafka topic warehouse-sorting-events.  
  * Angular frontend mengonsumsi data log sorting ini untuk diupdate ke tabel manifest gudang.

### **Epic 2: Live IoT Telemetry Control Tower dengan RxJS Throttling**

> * **User Story:** Sebagai Operator Pusat Multinasional, saya ingin melihat pergerakan ratusan truk kurir di peta secara real-time tanpa membuat browser saya macet (*freeze*).  
> * **Functional Requirements:**  
  * **Inbound Stream:** Aplikasi kurir/IoT mengirim koordinat GPS dan telemetri suhu setiap 1-2 detik via WebSocket ke service fleet-tracking-websocket.  
  * **Outbound Stream:** Service backend mem-broadcast data tersebut langsung ke Angular via WebSocket protokol STOMP.  
  * **RxJS Backpressure Handling (Frontend):** Angular Service wajib menangkap data stream tersebut menggunakan RxJS dan menerapkan operator throttleTime(250) atau sampleTime() sebelum dilempar ke Mapbox komponen. Ini memastikan peta hanya melakukan re-render maksimal 4 kali sedetik demi menjaga efisiensi RAM/CPU browser.  
  * **Anomaly Alarm:** Jika sensor membaca suhu kontainer pendingin \> 5°C berturut-turut, sistem langsung memicu alarm suara dan visual pada Angular UI.

### **Epic 3: Dynamic Vehicle Routing Optimization (VRP)**

> * **User Story:** Sebagai Driver/Kurir, saya ingin mendapatkan urutan alamat pengiriman yang sudah dioptimasi rutenya oleh sistem pusat agar efisien.  
> * **Functional Requirements:**  
  * Service routing-optimization-engine mengambil data titik koordinat paket aktif dan menjalankannya di background menggunakan algoritma **Timefold/OptaPlanner**.  
  * Urutan rute terbaik dikirim balik ke Kafka, lalu diteruskan via WebSocket untuk memperbarui rute pada maps di sisi Angular kurir secara otomatis.

## ---

**4\. Technical & Non-Functional Requirements (NFR)**

### **Technical Constraints:**

> * **Java & Spring Version:** Wajib Java 21 dan Spring Boot 3.x (Enterprise Standard).  
> * **Frontend Engine:** Angular dengan Strict Type Checking diaktifkan (strict: true di tsconfig).  
> * **Messaging Layer:** Apache Kafka sebagai event broker antar service backend.  
> * **Database Layers:** PostgreSQL (Core Data) \+ Redis (Session/WebSocket Tracking state).

### **Performance Standards:**

> * **UI Fluidity:** Dashboard Angular harus mempertahankan performa render minimal 55-60 FPS meskipun menerima ratusan event koordinat per detik (berkat optimasi RxJS).  
> * **Server Concurrency:** Service WebSocket Spring Boot harus dikonfigurasi menggunakan non-blocking I/O agar kuat menahan ribuan koneksi konkuren.

## ---

**5\. Project Directory Structure (Monorepo)**

enterprise-logistics-hub/               \<-- Root Monorepo Workspace  
│  
├── logistics-frontend/                 \<-- Angular Enterprise Application  
│   ├── src/  
│   │   ├── app/  
│   │   │   ├── core/                   \<-- Services, Interceptors, Guards (RxJS WebSocket Engine)  
│   │   │   ├── features/               \<-- Modules: control-tower, warehouse, driver-panel  
│   │   │   └── shared/                 \<-- Shared components, pipes, directives  
│   │   ├── assets/  
│   │   └── index.html  
│   ├── angular.json  
│   ├── package.json  
│   └── tsconfig.json  
│  
├── logistics-backend/                  \<-- Multi-Module Java Spring Boot 3.x Project  
│   ├── pom.xml                         \<-- Parent Maven XML (Defines all sub-modules)  
│   │  
│   ├── logistics-shared-common/        \<-- Module 1: Shared Models, Java Records, Global DTOs  
│   │   ├── pom.xml  
│   │   └── src/main/java/com/logistream/common/  
│   │  
│   ├── sorting-warehouse-service/      \<-- Module 2: Core Processing & Spring Statemachine  
│   │   ├── pom.xml  
│   │   └── src/main/java/com/logistream/warehouse/  
│   │  
│   ├── fleet-tracking-websocket/       \<-- Module 3: High-Concurrency WebSockets Engine  
│   │   ├── pom.xml  
│   │   └── src/main/java/com/logistream/tracking/  
│   │  
│   └── routing-optimization-engine/    \<-- Module 4: Computational Heavy Route Solver (Timefold)  
│       ├── pom.xml  
│       └── src/main/java/com/logistream/routing/  
│  
├── docker-compose.yml                  \<-- Local Infrastructure & Services Orchestration  
└── README.md                           \<-- Main Project Guide

## ---

**6\. Deployment & Infrastructure Requirement**

File docker-compose.yml di folder root harus mengorkestrasi:

> 1. Container Angular Frontend (logistics-frontend).  
> 2. 3 Container Service Spring Boot (sorting-service, websocket-service, routing-engine).  
> 3. 1 Instance Apache Kafka \+ Kraft mode.  
> 4. 1 Instance PostgreSQL & 1 Instance Redis.

### 4.3 Data Architecture & Polyglot Persistence Strategy (New)
Sistem menerapkan strategi *Polyglot Persistence* untuk memisahkan beban kerja data transaksional (ACID) dengan data telemetri berlatensi rendah (In-Memory).

*   **Primary Relational Database (PostgreSQL + PostGIS):**
    *   **Fungsi:** Menyimpan data master (akun, manifest gudang, detail order) dan memproses transisi status paket secara *idempotent* melalui modul `sorting-warehouse-service`.
    *   **Geospatial Capabilities:** Wajib mengaktifkan ekstensi **PostGIS** pada PostgreSQL untuk menangani query spasial radius rute (misal: mencari kurir terdekat dari titik koordinat gudang) langsung di level database, bukan di memori aplikasi.
*   **High-Speed In-Memory Cache (Redis):**
    *   **Fungsi:** Bertindak sebagai *hot data storage* berlatensi sub-milidetik untuk me-manage status sesi WebSocket aktif dan data koordinat kurir paling mutakhir (*latest vehicle state*).
    *   **Data Ingestion Pipeline:** Setiap detak koordinat (per 1-2 detik) yang masuk ke `fleet-tracking-websocket` hanya boleh ditulis ke Redis untuk kebutuhan *live streaming* ke Angular Frontend. Data koordinat mentah dilarang melakukan *direct-write* ke PostgreSQL guna menghindari IOPS *bottleneck*.
    *   **Batch Persistence:** Histori perjalanan armada akan di-dump dari Redis ke PostgreSQL secara berkala (batch processing per 5 menit) untuk kebutuhan audit dan laporan performa.

## ---

**7\. Acceptance Criteria (Definisi Sukses Proyek)**

> 1. **Single-Command Up:** Reviewer cukup melakukan docker-compose up \--build di root folder, dan seluruh sistem langsung aktif dan saling terhubung.  
> 2. **RxJS Efficiency Validation:** Saat simulator IoT diaktifkan dan mengirim ribuan data koordinat, penggunaan CPU browser pada tab Angular Dashboard tidak boleh melonjak melebihe 25%.  
> 3. **Strict State Transition:** Status paket berjalan mulus melewati siklus hidup *State Machine* di backend tanpa terjadi tumpang tindih data.