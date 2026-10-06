from edgeio_contracts.models import TAILSCALE_NETWORK
from edgeio_simulator.fleet import generate_fleet, tailscale_ip


def test_fleet_is_deterministic_for_a_seed() -> None:
    assert generate_fleet(seed=42, count=50) == generate_fleet(seed=42, count=50)


def test_different_seeds_give_different_addresses() -> None:
    a = {p.device_id for p in generate_fleet(seed=1, count=50)}
    b = {p.device_id for p in generate_fleet(seed=2, count=50)}
    assert a != b


def test_addresses_are_unique_tailscale_ips() -> None:
    ips = [tailscale_ip(i, seed=42) for i in range(2000)]
    assert len(set(ips)) == 2000
    assert all(ip in TAILSCALE_NETWORK for ip in ips)
    assert TAILSCALE_NETWORK.network_address not in ips
    assert TAILSCALE_NETWORK.broadcast_address not in ips


def test_hostnames_are_numbered_from_one() -> None:
    fleet = generate_fleet(seed=42, count=50)
    assert fleet[0].hostname == "edge-001"
    assert fleet[49].hostname == "edge-050"


def test_offset_shards_the_same_fleet() -> None:
    shard = generate_fleet(seed=42, count=3, offset=10)
    full = generate_fleet(seed=42, count=13)
    assert shard == full[10:13]
    assert shard[0].hostname == "edge-011"


def test_profiles_have_sane_hardware() -> None:
    for p in generate_fleet(seed=42, count=50):
        assert p.ram_total_mb in (4096, 8192, 16384, 32768)
        assert p.disk_total_gb in (128, 256, 476, 953)
        assert p.os in ("Ubuntu 24.04", "Ubuntu 22.04")
        assert 0.3 <= p.initial_disk_fraction <= 0.7
        assert 3 <= p.container_count <= 8
