import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { groupBy, mergeMap, throttleTime } from 'rxjs/operators';
import { Client } from '@stomp/stompjs';
import SockJS from 'sockjs-client';

export interface TelemetryEvent {
  vehicleId: string;
  latitude: number;
  longitude: number;
  speed: number;
  temperature: number;
  timestamp: number;
}

export interface AlarmEvent {
  vehicleId: string;
  temperature: number;
  latitude: number;
  longitude: number;
  message: string;
  timestamp: number;
}

export interface RoutePoint {
  sequence: number;
  latitude: number;
  longitude: number;
  stopName: string;
}

export interface RouteUpdateEvent {
  vehicleId: string;
  routePoints: RoutePoint[];
  timestamp: number;
}

@Injectable({
  providedIn: 'root'
})
export class WebsocketService {
  private client: Client | null = null;
  
  private telemetrySubject = new Subject<TelemetryEvent>();
  private alertsSubject = new Subject<AlarmEvent>();
  private sessionsSubject = new Subject<number>();
  private routesSubject = new Subject<RouteUpdateEvent>();
  private connectionStatusSubject = new Subject<boolean>();

  constructor() {
    this.initWebSocket();
  }

  private initWebSocket() {
    // Connect to WebSocket service using SockJS factory with Stomp
    const host = window.location.hostname;
    const wsUrl = `http://${host}:8082/ws-fleet`;

    this.client = new Client({
      webSocketFactory: () => new SockJS(wsUrl),
      reconnectDelay: 5000,
      debug: (str) => {
        console.log('STOMP: ' + str);
      },
      onConnect: () => {
        this.connectionStatusSubject.next(true);
        console.log('Connected to LogiStream STOMP broker.');

        // Subscribe to Telemetry Topic
        this.client?.subscribe('/topic/telemetry', (message) => {
          if (message.body) {
            const data: TelemetryEvent = JSON.parse(message.body);
            this.telemetrySubject.next(data);
          }
        });

        // Subscribe to Alerts Topic
        this.client?.subscribe('/topic/alerts', (message) => {
          if (message.body) {
            const data: AlarmEvent = JSON.parse(message.body);
            this.alertsSubject.next(data);
          }
        });

        // Subscribe to Session Count Topic
        this.client?.subscribe('/topic/sessions', (message) => {
          if (message.body) {
            const data = JSON.parse(message.body);
            this.sessionsSubject.next(data.activeSessions);
          }
        });

        // Subscribe to Route Updates Topic
        this.client?.subscribe('/topic/routes', (message) => {
          if (message.body) {
            const data: RouteUpdateEvent = JSON.parse(message.body);
            this.routesSubject.next(data);
          }
        });
      },
      onDisconnect: () => {
        this.connectionStatusSubject.next(false);
        console.log('Disconnected from STOMP broker.');
      },
      onStompError: (frame) => {
        console.error('Broker reported error: ' + frame.headers['message']);
        console.error('Additional details: ' + frame.body);
      }
    });

    this.client.activate();
  }

  // Stream of connection states
  public getConnectionStatus(): Observable<boolean> {
    return this.connectionStatusSubject.asObservable();
  }

  // High throughput telemetry, throttled per vehicle to max 4 updates/sec (250ms)
  public getThrottledTelemetry(): Observable<TelemetryEvent> {
    return this.telemetrySubject.asObservable().pipe(
      groupBy(event => event.vehicleId),
      mergeMap(group$ => group$.pipe(throttleTime(250)))
    );
  }

  // Get raw telemetry (for testing/comparison)
  public getRawTelemetry(): Observable<TelemetryEvent> {
    return this.telemetrySubject.asObservable();
  }

  // Alert events (unthrottled, must trigger instantly)
  public getAlerts(): Observable<AlarmEvent> {
    return this.alertsSubject.asObservable();
  }

  // Active WebSocket clients count
  public getActiveSessionsCount(): Observable<number> {
    return this.sessionsSubject.asObservable();
  }

  // Route updates
  public getRouteUpdates(): Observable<RouteUpdateEvent> {
    return this.routesSubject.asObservable();
  }

  // Simulator helper: Send coordinates to backend via STOMP
  public sendTelemetry(telemetry: TelemetryEvent) {
    if (this.client && this.client.connected) {
      this.client.publish({
        destination: '/app/telemetry',
        body: JSON.stringify(telemetry)
      });
    }
  }
}
