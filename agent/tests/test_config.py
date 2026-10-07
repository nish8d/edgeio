import pytest

from edgeio_agent.config import AgentSettings


def test_list_settings_split_on_commas() -> None:
    settings = AgentSettings.model_validate(
        {"agent_host_services": "docker, tailscaled,", "agent_ping_targets": "1.1.1.1"}
    )
    assert settings.host_services == ["docker", "tailscaled"]
    assert settings.ping_targets == ["1.1.1.1"]


def test_service_renames_parse_pairs() -> None:
    settings = AgentSettings.model_validate(
        {"agent_service_renames": "streamer-app=edge_streamer, db-1=postgresql"}
    )
    assert settings.service_renames == {"streamer-app": "edge_streamer", "db-1": "postgresql"}


def test_malformed_rename_is_rejected() -> None:
    settings = AgentSettings.model_validate({"agent_service_renames": "streamer-app"})
    with pytest.raises(ValueError, match="name=new_name"):
        _ = settings.service_renames
