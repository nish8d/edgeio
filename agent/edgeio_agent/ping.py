"""Internet packet loss: ping several public targets and keep the best result."""

import re
import subprocess
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor

PingRunner = Callable[[str, int], str]
_LOSS = re.compile(r"([\d.]+)% packet loss")


def parse_loss(output: str) -> float:
    """Loss % from iputils ping output; anything unparseable (no route, no DNS) is 100."""
    match = _LOSS.search(output)
    return min(100.0, float(match.group(1))) if match else 100.0


def run_ping(target: str, count: int) -> str:
    try:
        result = subprocess.run(
            ["ping", "-n", "-q", "-c", str(count), "-i", "0.2", "-W", "1", target],
            capture_output=True,
            text=True,
            timeout=count + 10,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return ""
    return result.stdout + result.stderr


def internet_packet_loss(
    targets: Sequence[str], count: int, runner: PingRunner = run_ping
) -> float:
    """Minimum loss across targets: one provider dropping ICMP isn't an internet outage."""
    if not targets:
        raise ValueError("at least one ping target is required")

    def probe(target: str) -> float:
        return parse_loss(runner(target, count))

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        return min(pool.map(probe, targets))
