import type { components, paths } from "./schema";

type Schemas = components["schemas"];
type AlertsQuery = NonNullable<paths["/api/v1/alerts"]["get"]["parameters"]["query"]>;

export type DeviceSummary = Schemas["DeviceSummary"];
export type DeviceDetail = Schemas["DeviceDetail"];
export type DeviceStatus = DeviceSummary["status"];
export type StatusCounts = Schemas["StatusCounts"];
export type FleetSummary = Schemas["FleetSummary"];
export type Alert = Schemas["Alert"];
export type AlertList = Schemas["AlertList"];
export type Severity = Alert["severity"];
export type AlertState = NonNullable<AlertsQuery["state"]>;
export type ServiceStatus = Schemas["ServiceStatus"];
export type LatestReading = Schemas["LatestReading"];
export type MetricSeries = Schemas["MetricSeries"];
export type MetricPoint = Schemas["MetricPoint"];
export type MetricName = MetricSeries["metric"];
