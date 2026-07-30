import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface PackageEntity {
  packageId: string;
  state: string;
  recipientName: string;
  destinationAddress: string;
  latitude: number;
  longitude: number;
  lastUpdatedByThread: string;
  notes: string;
}

@Injectable({
  providedIn: 'root'
})
export class SortingService {
  private warehouseUrl = '';
  private websocketUrl = '';

  constructor(private http: HttpClient) {
    const host = window.location.hostname;
    this.warehouseUrl = `http://${host}:8081/api/packages`;
    this.websocketUrl = `http://${host}:8082/api/telemetry`;
  }

  // Get packages list
  public getPackages(): Observable<PackageEntity[]> {
    return this.http.get<PackageEntity[]>(this.warehouseUrl);
  }

  // Trigger virtual thread batch sorting simulation
  public triggerSimulation(batchSize: number): Observable<{ message: string }> {
    const params = new HttpParams().set('batchSize', batchSize.toString());
    return this.http.post<{ message: string }>(`${this.warehouseUrl}/simulate`, {}, { params });
  }

  // Trigger manual package state transition
  public transitionState(packageId: string, event: string, notes: string): Observable<PackageEntity> {
    const params = new HttpParams()
      .set('event', event)
      .set('notes', notes);
    return this.http.post<PackageEntity>(`${this.warehouseUrl}/${packageId}/event`, {}, { params });
  }

  // REST fallback: Get live sessions count
  public getLiveSessionsCount(): Observable<{ activeSessions: number }> {
    return this.http.get<{ activeSessions: number }>(`${this.websocketUrl}/sessions`);
  }

  // REST fallback: Get latest vehicle locations
  public getLatestVehicles(): Observable<Record<string, any>> {
    return this.http.get<Record<string, any>>(`${this.websocketUrl}/vehicles`);
  }
}
