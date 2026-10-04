from copy import deepcopy

from scripts.check_compose_security import validate


def compose_config() -> dict[str, object]:
    return {
        "services": {
            "api": {
                "ports": [{"host_ip": "127.0.0.1"}],
                "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
            },
            "processor": {"cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"]},
            "simulator": {"cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"]},
            "web": {"cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"]},
            "postgres": {"ports": [{"host_ip": "127.0.0.1"}]},
            "grafana": {
                "environment": {
                    "GF_USERS_ALLOW_SIGN_UP": "false",
                    "GF_AUTH_ANONYMOUS_ENABLED": "false",
                }
            },
        }
    }


def test_compose_security_contract_accepts_local_hardened_config() -> None:
    assert validate(compose_config()) == []


def test_compose_security_contract_rejects_public_or_unhardened_config() -> None:
    config = deepcopy(compose_config())
    config["services"]["api"]["ports"][0]["host_ip"] = "0.0.0.0"  # type: ignore[index]
    config["services"]["processor"]["cap_drop"] = []  # type: ignore[index]
    config["services"]["simulator"]["security_opt"] = []  # type: ignore[index]
    config["services"]["grafana"]["environment"]["GF_USERS_ALLOW_SIGN_UP"] = "true"  # type: ignore[index]

    failures = validate(config)

    assert any("api: every published port" in failure for failure in failures)
    assert any("processor: cap_drop" in failure for failure in failures)
    assert any("simulator: no-new-privileges" in failure for failure in failures)
    assert any("grafana: anonymous signup" in failure for failure in failures)
