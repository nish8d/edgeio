import { NavLink, Outlet } from "react-router";

export function Layout() {
  return (
    <div className="app">
      <header className="app-header">
        <span className="brand">edgeio</span>
        <nav>
          <NavLink to="/" end>
            Fleet
          </NavLink>
          <NavLink to="/alerts">Alerts</NavLink>
        </nav>
      </header>
      <main className="app-main">
        <Outlet />
      </main>
    </div>
  );
}
