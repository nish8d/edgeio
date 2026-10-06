import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { AlertsPage } from "./pages/AlertsPage";
import { DeviceDetail } from "./pages/DeviceDetail";
import { FleetOverview } from "./pages/FleetOverview";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<FleetOverview />} />
        <Route path="devices/:deviceId" element={<DeviceDetail />} />
        <Route path="alerts" element={<AlertsPage />} />
        <Route path="*" element={<p className="empty">Page not found.</p>} />
      </Route>
    </Routes>
  );
}
