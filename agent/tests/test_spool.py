from datetime import UTC, datetime
from pathlib import Path

from edgeio_agent.spool import Spool


def at(minute: int) -> datetime:
    return datetime(2026, 10, 7, 9, minute, tzinfo=UTC)


def test_add_lists_oldest_first_and_leaves_no_temp_files(tmp_path: Path) -> None:
    spool = Spool(tmp_path, max_files=10)
    spool.add(at(5), b"second")
    spool.add(at(0), b"first")
    assert [path.read_bytes() for path in spool.pending()] == [b"first", b"second"]
    assert not list(tmp_path.glob(".*"))


def test_add_drops_the_oldest_beyond_the_cap(tmp_path: Path) -> None:
    spool = Spool(tmp_path, max_files=2)
    dropped = [spool.add(at(minute), str(minute).encode()) for minute in (0, 5, 10)]
    assert dropped == [0, 0, 1]
    assert [path.read_bytes() for path in spool.pending()] == [b"5", b"10"]


def test_spool_creates_its_directory(tmp_path: Path) -> None:
    Spool(tmp_path / "a/b", max_files=1).add(at(0), b"x")
    assert (tmp_path / "a/b").is_dir()
