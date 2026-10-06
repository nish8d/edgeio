import type { LatestReading, ServiceStatus } from "../api/types";

interface Props {
  services: ServiceStatus[];
  latest?: LatestReading | null;
}

export function ServicesPanel({ services, latest }: Props) {
  return (
    <section className="panel">
      <h2>Services</h2>
      {services.length === 0 ? (
        <p className="empty">No services reported.</p>
      ) : (
        <ul className="service-list">
          {services.map((service) => {
            const running = service.state === "running";
            return (
              <li key={service.name}>
                <span className="mono">{service.name}</span>
                <span className={`badge badge-${running ? "healthy" : "critical"}`}>
                  <span aria-hidden="true" className="badge-icon">
                    {running ? "✓" : "✕"}
                  </span>
                  {service.state}
                </span>
              </li>
            );
          })}
        </ul>
      )}
      {latest && (
        <p className="containers">
          Containers: <strong>{latest.containers_running}</strong> running,{" "}
          <strong>{latest.containers_stopped}</strong> stopped
        </p>
      )}
    </section>
  );
}
