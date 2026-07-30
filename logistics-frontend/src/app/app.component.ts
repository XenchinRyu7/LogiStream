import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { WebsocketService, TelemetryEvent, AlarmEvent, RouteUpdateEvent } from './core/services/websocket.service';
import { SortingService, PackageEntity } from './core/services/sorting.service';
import { Subscription } from 'rxjs';
import * as L from 'leaflet';

interface MarkerInterpolation {
  marker: L.Marker;
  startLat: number;
  startLon: number;
  targetLat: number;
  targetLon: number;
  startTime: number;
  duration: number;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit, OnDestroy {
  // Connection states
  wsConnected = false;
  activeSessions = 0;

  // Simulator configurations
  simRunning = false;
  tempAnomalyTriggered = false;
  simulationBatchSize = 10;
  private simulatorIntervalId: any = null;

  // Data lists
  packages: PackageEntity[] = [];
  activeAlarms: AlarmEvent[] = [];
  private activeVehicles = new Map<string, TelemetryEvent>();
  logisticsLogs: string[] = [];

  // Stream Performance statistics
  rawTelemetryRate = 0;
  throttledTelemetryRate = 0;
  private rawEventsCount = 0;
  private throttledEventsCount = 0;
  private performanceIntervalId: any = null;

  // Leaflet map objects
  private map!: L.Map;
  private hubMarker!: L.Marker;
  private vehicleMarkers = new Map<string, L.Marker>();
  private routeLines = new Map<string, L.Polyline>();

  // Marker Interpolation Engine
  private interpolations = new Map<string, MarkerInterpolation>();
  private animationFrameId: any = null;

  // Subscriptions
  private subs: Subscription = new Subscription();

  // Central Jakarta hub coordinate
  private readonly HUB_LAT = -6.2088;
  private readonly HUB_LON = 106.8456;

  // Simulated GPS route paths around Jakarta
  private readonly routesData: Record<string, [number, number][]> = {
    'TRUCK-1': [
      [-6.2088, 106.8456], [-6.195, 106.845], [-6.182, 106.846], [-6.170, 106.852],
      [-6.160, 106.840], [-6.155, 106.825], [-6.168, 106.815], [-6.185, 106.822],
      [-6.198, 106.830], [-6.205, 106.840], [-6.2088, 106.8456]
    ],
    'TRUCK-2': [
      [-6.2088, 106.8456], [-6.218, 106.858], [-6.230, 106.870], [-6.245, 106.882],
      [-6.260, 106.875], [-6.255, 106.858], [-6.240, 106.845], [-6.228, 106.838],
      [-6.218, 106.840], [-6.2088, 106.8456]
    ],
    'TRUCK-3': [
      [-6.2088, 106.8456], [-6.202, 106.825], [-6.205, 106.810], [-6.212, 106.795],
      [-6.225, 106.780], [-6.238, 106.772], [-6.250, 106.790], [-6.242, 106.810],
      [-6.225, 106.828], [-6.215, 106.838], [-6.2088, 106.8456]
    ]
  };

  private routeIndices: Record<string, number> = {
    'TRUCK-1': 0,
    'TRUCK-2': 0,
    'TRUCK-3': 0
  };

  constructor(
    private websocketService: WebsocketService,
    private sortingService: SortingService
  ) {}

  ngOnInit() {
    this.initMap();
    this.loadPackages();
    this.setupWebSocketStreams();
    this.startPerformanceMonitoring();
    this.startInterpolationEngine();
  }

  ngOnDestroy() {
    this.stopTelemetrySimulator();
    if (this.performanceIntervalId) clearInterval(this.performanceIntervalId);
    if (this.animationFrameId) cancelAnimationFrame(this.animationFrameId);
    this.subs.unsubscribe();
  }

  // Pushes clean log message to the logistics logs terminal queue
  addLog(prefix: string, message: string) {
    const time = new Date().toLocaleTimeString();
    const formatted = `[${prefix}] ${time} - ${message}`;
    this.logisticsLogs.unshift(formatted);
    if (this.logisticsLogs.length > 30) {
      this.logisticsLogs.pop();
    }
  }

  // Load packages from database via REST API
  loadPackages() {
    this.sortingService.getPackages().subscribe({
      next: (data) => {
        this.packages = data.sort((a, b) => b.packageId.localeCompare(a.packageId));
        this.addLog('DATABASE', 'Synced latest hub package registry logs');
      },
      error: (err) => console.error('Failed to load packages:', err)
    });
  }

  // Setup Streams with STOMP WebSockets
  private setupWebSocketStreams() {
    // 1. Connection status
    this.subs.add(
      this.websocketService.getConnectionStatus().subscribe(status => {
        this.wsConnected = status;
        if (status) {
          this.fetchInitialSessionsCount();
          this.addLog('SYSTEM', 'Telemetry ingestion pipeline connection established');
        } else {
          this.addLog('SYSTEM', 'Telemetry ingestion pipeline connection disconnected');
        }
      })
    );

    // 2. Active Session counter
    this.subs.add(
      this.websocketService.getActiveSessionsCount().subscribe(count => {
        this.activeSessions = count;
      })
    );

    // 3. Raw Telemetry metrics collector (for bandwidth calculation)
    this.subs.add(
      this.websocketService.getRawTelemetry().subscribe(() => {
        this.rawEventsCount++;
      })
    );

    // 4. Throttled Telemetry stream (used to update Leaflet Map DOM)
    this.subs.add(
      this.websocketService.getThrottledTelemetry().subscribe(event => {
        this.throttledEventsCount++;
        this.updateVehicleMarker(event);
      })
    );

    // 5. Thermal Alerts (Alarms queue)
    this.subs.add(
      this.websocketService.getAlerts().subscribe(alarm => {
        if (!this.activeAlarms.some(a => a.vehicleId === alarm.vehicleId)) {
          this.activeAlarms.unshift(alarm);
          this.playAlarmBuzzer();
          this.addLog('CRITICAL', `Vehicle ${alarm.vehicleId.replace('TRUCK-', 'TR-')} cold chain containment breach: ${alarm.temperature}°C`);
          
          // Re-render marker instantly with alarm status
          const activeEvent = this.activeVehicles.get(alarm.vehicleId);
          if (activeEvent) {
            this.updateVehicleMarker(activeEvent);
          }
        }
      })
    );

    // 6. Route updates solved by routing engine
    this.subs.add(
      this.websocketService.getRouteUpdates().subscribe(routeEvent => {
        this.drawRoutePath(routeEvent);
        this.addLog('ROUTING', `Dynamic VRP route updated for Fleet ${routeEvent.vehicleId.replace('TRUCK-', 'TR-')} (${routeEvent.routePoints.length} points)`);
      })
    );
  }

  private fetchInitialSessionsCount() {
    this.sortingService.getLiveSessionsCount().subscribe({
      next: (res) => this.activeSessions = res.activeSessions,
      error: (err) => console.warn('Failed to fetch REST sessions count:', err)
    });
  }

  // Map Initialization
  private initMap() {
    // AWS charcoal-grey/dark tile layer
    const darkTileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19,
      attribution: '© OpenStreetMap contributors, CartoDB'
    });

    this.map = L.map('map', {
      center: [this.HUB_LAT, this.HUB_LON],
      zoom: 12,
      layers: [darkTileLayer]
    });

    // Create solid AWS-like hub marker
    const hubIcon = L.divIcon({
      className: 'hub-marker-icon',
      html: this.getHubSvg(),
      iconSize: [28, 28],
      iconAnchor: [14, 14]
    });

    this.hubMarker = L.marker([this.HUB_LAT, this.HUB_LON], { icon: hubIcon })
      .addTo(this.map)
      .bindPopup('<strong>Central Logistics Hub</strong><br>Primary dispatch and sorting facility.');
  }

  // Start Marker Position Interpolation Loop (60 FPS Glide)
  private startInterpolationEngine() {
    const tick = () => {
      const now = Date.now();
      
      this.interpolations.forEach((item, id) => {
        const elapsed = now - item.startTime;
        const t = Math.min(1, elapsed / item.duration);
        
        // Linear Interpolation (lerp) equation
        const lat = item.startLat + (item.targetLat - item.startLat) * t;
        const lon = item.startLon + (item.targetLon - item.startLon) * t;
        
        item.marker.setLatLng([lat, lon]);
        
        if (t >= 1) {
          this.interpolations.delete(id);
        }
      });
      
      this.animationFrameId = requestAnimationFrame(tick);
    };
    
    this.animationFrameId = requestAnimationFrame(tick);
  }

  // Update or render vehicle location on Leaflet Map
  private updateVehicleMarker(event: TelemetryEvent) {
    this.activeVehicles.set(event.vehicleId, event);
    const hasAnomaly = this.activeAlarms.some(a => a.vehicleId === event.vehicleId);
    
    const displayId = event.vehicleId.replace('TRUCK-', 'TR-');
    
    // Custom DIV icon for the truck (using custom clean SVGs)
    const icon = L.divIcon({
      className: 'vehicle-marker-icon',
      html: hasAnomaly ? this.getTruckSvgAnomaly(event.vehicleId) : this.getTruckSvgNormal(event.vehicleId),
      iconSize: [32, 36],
      iconAnchor: [16, 18]
    });

    if (this.vehicleMarkers.has(event.vehicleId)) {
      const marker = this.vehicleMarkers.get(event.vehicleId)!;
      marker.setIcon(icon);
      
      // Update popup content
      marker.getPopup()?.setContent(`
        <strong style="color:var(--action-blue);">Fleet Unit: ${displayId}</strong><br>
        Speed: ${event.speed.toFixed(1)} km/h<br>
        Cargo Temp: <span style="font-weight:bold; color:${event.temperature > 5.0 ? 'var(--danger)' : 'var(--success)'}">${event.temperature.toFixed(2)}°C</span>
      `);

      // Trigger/Refresh interpolation coordinates Glide
      const currentLatLng = marker.getLatLng();
      this.interpolations.set(event.vehicleId, {
        marker,
        startLat: currentLatLng.lat,
        startLon: currentLatLng.lng,
        targetLat: event.latitude,
        targetLon: event.longitude,
        startTime: Date.now(),
        duration: 500 // Interpolate over 500ms matching coordinates step emission rate
      });
      
    } else {
      // Create new marker on map (First frame, instantly placed)
      const marker = L.marker([event.latitude, event.longitude], { icon })
        .addTo(this.map)
        .bindPopup(`<strong>Fleet Unit ${displayId}</strong>`);
      this.vehicleMarkers.set(event.vehicleId, marker);
    }
  }

  // Draw optimized VRP route path
  private drawRoutePath(routeEvent: RouteUpdateEvent) {
    const latLngs = routeEvent.routePoints.map(p => L.latLng(p.latitude, p.longitude));
    
    // Clean solid colors matching AWS console widgets
    const colors: Record<string, string> = {
      'TRUCK-1': '#54b2ff', // AWS Action Blue
      'TRUCK-2': '#a78bfa', // Purple
      'TRUCK-3': '#03a84e'  // AWS Success Green
    };
    
    const color = colors[routeEvent.vehicleId] || '#f1a80a';

    if (this.routeLines.has(routeEvent.vehicleId)) {
      const polyline = this.routeLines.get(routeEvent.vehicleId)!;
      polyline.setLatLngs(latLngs);
    } else {
      const polyline = L.polyline(latLngs, {
        color: color,
        weight: 2,
        opacity: 0.8,
        dashArray: '3, 6',
        lineJoin: 'round'
      }).addTo(this.map);
      this.routeLines.set(routeEvent.vehicleId, polyline);
    }
  }

  // Audio Alarm Synth: plays an alert sound when high temp is triggered
  private playAlarmBuzzer() {
    try {
      const audioCtx = new (window.AudioContext || (window as any).webkitAudioContext)();
      const oscillator = audioCtx.createOscillator();
      const gainNode = audioCtx.createGain();
      
      oscillator.connect(gainNode);
      gainNode.connect(audioCtx.destination);
      
      oscillator.type = 'sawtooth';
      oscillator.frequency.setValueAtTime(987.77, audioCtx.currentTime); // B5 note
      gainNode.gain.setValueAtTime(0.08, audioCtx.currentTime);
      
      oscillator.start();
      oscillator.stop(audioCtx.currentTime + 0.25); // Sound lasts 250ms
    } catch (e) {
      console.warn('Audio Context error (User interaction required):', e);
    }
  }

  // Simulation: Trigger sorting batch on Virtual Threads
  runSortingSimulation() {
    this.addLog('MANIFEST', `Ingesting new batch manifest simulation (${this.simulationBatchSize} packages)`);
    this.sortingService.triggerSimulation(this.simulationBatchSize).subscribe({
      next: (res) => {
        this.addLog('ENGINE', `Dispatched batch sorting manifest to processing nodes`);
        // Refresh packages list after a small delay to see new entries
        setTimeout(() => this.loadPackages(), 500);
        // Poll periodically for state transitions
        let pollCount = 0;
        const interval = setInterval(() => {
          this.loadPackages();
          if (++pollCount >= 10) clearInterval(interval);
        }, 1500);
      },
      error: (err) => console.error('Failed to trigger package sorting simulation:', err)
    });
  }

  // Inbound Telemetry Simulator: Starts mock truck movements
  toggleTelemetrySimulator() {
    if (this.simRunning) {
      this.stopTelemetrySimulator();
    } else {
      this.startTelemetrySimulator();
    }
  }

  private startTelemetrySimulator() {
    this.simRunning = true;
    const vehicles = ['TRUCK-1', 'TRUCK-2', 'TRUCK-3'];
    this.addLog('SIMULATOR', 'Initiated fleet simulation telemetry stream');
    
    this.simulatorIntervalId = setInterval(() => {
      vehicles.forEach(vehicleId => {
        const route = this.routesData[vehicleId];
        let index = this.routeIndices[vehicleId];
        
        index = (index + 1) % route.length;
        this.routeIndices[vehicleId] = index;
        
        const coords = route[index];
        const speed = 40 + Math.random() * 25; // 40-65 km/h
        
        let temperature = 2.0 + Math.random() * 2.0; // Normal: 2 - 4 C
        if (vehicleId === 'TRUCK-2' && this.tempAnomalyTriggered) {
          temperature = 6.2 + Math.random() * 1.5; // Trigger anomaly temp: > 5 C
        }

        const telemetry: TelemetryEvent = {
          vehicleId,
          latitude: coords[0],
          longitude: coords[1],
          speed,
          temperature,
          timestamp: Date.now()
        };

        this.websocketService.sendTelemetry(telemetry);
      });
    }, 500); // Emits every 500ms
  }

  private stopTelemetrySimulator() {
    this.simRunning = false;
    this.addLog('SIMULATOR', 'Terminated fleet simulation telemetry stream');
    if (this.simulatorIntervalId) {
      clearInterval(this.simulatorIntervalId);
      this.simulatorIntervalId = null;
    }
  }

  // Anomaly trigger button
  triggerTempAnomaly() {
    this.tempAnomalyTriggered = !this.tempAnomalyTriggered;
    if (this.tempAnomalyTriggered) {
      this.addLog('SIMULATOR', 'Injected thermal overheat anomaly payload on Fleet Unit TR-02');
    } else {
      this.activeAlarms = [];
      this.addLog('SIMULATOR', 'Cleared thermal alarms. Cold chain containment normal.');
      this.activeVehicles.forEach((event, id) => {
        this.updateVehicleMarker(event);
      });
    }
  }

  // Calculate telemetry rate every 1s
  private startPerformanceMonitoring() {
    this.performanceIntervalId = setInterval(() => {
      this.rawTelemetryRate = this.rawEventsCount;
      this.throttledTelemetryRate = this.throttledEventsCount;
      
      this.rawEventsCount = 0;
      this.throttledEventsCount = 0;
    }, 1000);
  }

  // Formats the raw messages rate into a realistic operational telemetry rate (events/min)
  getIngestionRateFormatted(): string {
    if (this.rawTelemetryRate === 0) return '0 events/min';
    const rate = this.rawTelemetryRate * 180; // Scale up to realistic operational density
    return rate >= 1000 ? `${(rate / 1000).toFixed(1)}k events/min` : `${rate} events/min`;
  }

  get activeVehiclesCount(): number {
    return this.activeVehicles.size;
  }

  calculateBandwidthSaving(): number {
    if (this.rawTelemetryRate === 0) return 0;
    const saving = ((this.rawTelemetryRate - this.throttledTelemetryRate) / this.rawTelemetryRate) * 100;
    return Math.max(0, Math.round(saving));
  }

  // String helpers for templates (AWS Indicator Colors)
  getStateColor(state: string): string {
    switch (state) {
      case 'MANIFESTED': return '#8795a5'; // AWS Neutral Grey
      case 'HUB_SORTING': return 'var(--warning)'; // AWS yellow
      case 'IN_TRANSIT': return 'var(--action-blue)'; // AWS blue
      case 'OUT_FOR_DELIVERY': return '#8b5cf6'; // Purple
      case 'DELIVERED': return 'var(--success)'; // AWS green
      default: return '#8795a5';
    }
  }

  cleanThreadName(threadStr: string): string {
    if (!threadStr) return 'system-node';
    // Clean to "Dispatcher #xx" format
    const match = threadStr.match(/#(\d+)/);
    if (match) {
      return `Dispatcher #${match[1]}`;
    }
    return `Dispatcher Node`;
  }

  // Solid vector SVGs for AWS console rendering
  private getHubSvg(): string {
    return `
      <div style="position: relative; display: flex; align-items: center; justify-content: center; width: 28px; height: 28px; background: #1c222e; border: 2px solid var(--action-blue); border-radius: 4px; box-shadow: 0 1px 2px rgba(0,0,0,0.5);">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="var(--action-blue)" width="16" height="16">
          <path d="M12 2L2 7v13c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V7L12 2zm0 3.3l7 3.5v9.2H5V8.8l7-3.5zm-3 7.7h2v4H9v-4zm4 0h2v4h-2v-4z"/>
        </svg>
      </div>
    `;
  }

  private getTruckSvgNormal(id: string): string {
    const displayId = id.replace('TRUCK-', 'TR-');
    return `
      <div style="position: relative; display: flex; flex-direction: column; align-items: center; width: 32px;">
        <div style="display: flex; align-items: center; justify-content: center; width: 26px; height: 26px; background: #1c222e; border: 2px solid var(--success); border-radius: 4px; box-shadow: 0 1px 2px rgba(0,0,0,0.5);">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="var(--success)" width="15" height="15">
            <path d="M20 8h-3V4H3c-1.1 0-2 .9-2 2v11h2c0 1.66 1.34 3 3 3s3-1.34 3-3h6c0 1.66 1.34 3 3 3s3-1.34 3-3h2v-5l-3-4zM6 18.5c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm12 0c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm1.5-7l2.25 3H17v-3h2.5z"/>
          </svg>
        </div>
        <span style="font-family: monospace; font-size: 8px; color: var(--text-primary); background: #242f3e; padding: 1px 3px; border-radius: 2px; margin-top: 2px; border: 1px solid var(--panel-border); font-weight: bold;">${displayId}</span>
      </div>
    `;
  }

  private getTruckSvgAnomaly(id: string): string {
    const displayId = id.replace('TRUCK-', 'TR-');
    return `
      <div style="position: relative; display: flex; flex-direction: column; align-items: center; width: 32px;">
        <!-- Flat solid alarm siren dot -->
        <div style="position: absolute; top:-3px; right:-3px; width: 8px; height: 8px; background: var(--danger); border-radius: 50%; border: 1px solid #fff; animation: siren-flash-flat 0.4s infinite alternate;"></div>
        <div style="display: flex; align-items: center; justify-content: center; width: 26px; height: 26px; background: #1c222e; border: 2px solid var(--danger); border-radius: 4px; box-shadow: 0 1px 2px rgba(0,0,0,0.5);">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="var(--danger)" width="15" height="15">
            <path d="M20 8h-3V4H3c-1.1 0-2 .9-2 2v11h2c0 1.66 1.34 3 3 3s3-1.34 3-3h6c0 1.66 1.34 3 3 3s3-1.34 3-3h2v-5l-3-4zM6 18.5c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm12 0c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm1.5-7l2.25 3H17v-3h2.5z"/>
          </svg>
        </div>
        <span style="font-family: monospace; font-size: 8px; color: #fff; background: var(--danger); padding: 1px 3px; border-radius: 2px; margin-top: 2px; border: 1px solid #fff; font-weight: bold;">${displayId}</span>
      </div>
    `;
  }
}
