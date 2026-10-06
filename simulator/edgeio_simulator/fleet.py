"""Deterministic generation of the simulated device fleet."""

import random
from dataclasses import dataclass
from ipaddress import IPv4Address

from edgeio_contracts.models import TAILSCALE_NETWORK

SERVICES: tuple[str, ...] = ("docker", "postgresql", "edge_streamer")

# Knuth's multiplicative hash constant. It is prime, so it is coprime with the number of
# usable addresses and (index * multiplier) mod usable is a bijection: no IP collisions.
_HASH_MULTIPLIER = 2654435761


@dataclass(frozen=True)
class DeviceProfile:
    index: int
    device_id: IPv4Address
    hostname: str
    os: str
    cpu_cores: int
    ram_total_mb: int
    disk_total_gb: int
    interface: str
    base_cpu_percent: float
    base_ram_fraction: float
    initial_disk_fraction: float
    container_count: int


def tailscale_ip(index: int, seed: int) -> IPv4Address:
    """A random-looking but unique address in 100.64.0.0/10 for each device index."""
    usable = TAILSCALE_NETWORK.num_addresses - 2  # skip network and broadcast addresses
    offset = (index * _HASH_MULTIPLIER + seed) % usable
    return IPv4Address(int(TAILSCALE_NETWORK.network_address) + 1 + offset)


def make_profile(index: int, seed: int) -> DeviceProfile:
    rng = random.Random(f"{seed}:{index}")
    return DeviceProfile(
        index=index,
        device_id=tailscale_ip(index, seed),
        hostname=f"edge-{index + 1:03d}",
        os=rng.choices(["Ubuntu 24.04", "Ubuntu 22.04"], weights=[4, 1])[0],
        cpu_cores=rng.choice([4, 8]),
        ram_total_mb=rng.choice([4096, 8192, 16384, 32768]),
        disk_total_gb=rng.choice([128, 256, 476, 953]),
        interface=rng.choices(["eth0", "wlan0"], weights=[6, 1])[0],
        base_cpu_percent=round(rng.uniform(10, 45), 1),
        base_ram_fraction=rng.uniform(0.3, 0.6),
        initial_disk_fraction=rng.uniform(0.3, 0.7),
        container_count=rng.randint(3, 8),
    )


def generate_fleet(seed: int, count: int, offset: int = 0) -> list[DeviceProfile]:
    return [make_profile(offset + i, seed) for i in range(count)]
