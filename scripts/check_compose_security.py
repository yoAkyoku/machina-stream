from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

APPLICATION_SERVICES = ("api", "processor", "simulator", "web")


def load_compose_config() -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["docker", "compose", "config", "--format", "json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("docker compose config timed out") from error
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker compose config failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("docker compose config returned invalid JSON") from error


def validate(config: dict[str, Any]) -> list[str]:
    services = config.get("services")
    if not isinstance(services, dict):
        return ["Compose config has no services map"]

    failures: list[str] = []
    for service_name, service in services.items():
        if not isinstance(service, dict):
            failures.append(f"{service_name}: service definition is not an object")
            continue
        for binding in service.get("ports", []):
            if not isinstance(binding, dict) or binding.get("host_ip") != "127.0.0.1":
                failures.append(f"{service_name}: every published port must bind 127.0.0.1")
        if service_name in APPLICATION_SERVICES:
            if "ALL" not in service.get("cap_drop", []):
                failures.append(f"{service_name}: cap_drop must include ALL")
            if "no-new-privileges:true" not in service.get("security_opt", []):
                failures.append(f"{service_name}: no-new-privileges must be enabled")

    grafana = services.get("grafana", {})
    environment = grafana.get("environment", {}) if isinstance(grafana, dict) else {}
    if str(environment.get("GF_USERS_ALLOW_SIGN_UP", "")).lower() != "false":
        failures.append("grafana: anonymous signup must be disabled")
    if str(environment.get("GF_AUTH_ANONYMOUS_ENABLED", "")).lower() != "false":
        failures.append("grafana: anonymous access must be disabled")
    return failures


def main() -> int:
    try:
        failures = validate(load_compose_config())
    except RuntimeError as error:
        print(f"Compose security contract failed: {error}", file=sys.stderr)
        return 1
    if failures:
        print("Compose security contract failed:", file=sys.stderr)
        print("\n".join(f"- {failure}" for failure in failures), file=sys.stderr)
        return 1
    print("Compose security contract passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
