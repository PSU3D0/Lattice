#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

DEFAULT_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
DEFAULT_TOKEN_URL = "https://oauth2.googleapis.com/token"
DEFAULT_ENDPOINT_BASE_URL = "https://sheets.googleapis.com"
DEFAULT_EXAMPLE = "connector_google_sheets_local_flow"


def find_codebase_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / "Cargo.toml").exists() and (candidate / "crates" / "cli").exists():
            return candidate
    raise SystemExit("could not locate codebase root from script path")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a live-ready bindings.lock.json for the Google Sheets local-flow example."
        )
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output bindings.lock.json path",
    )
    parser.add_argument(
        "--generated-at",
        default=dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        help="RFC3339 timestamp for generated_at",
    )
    parser.add_argument(
        "--service-account-email-ref",
        default="google_sheets_sa_email",
        help="Secret ref/env name holding the service-account email",
    )
    parser.add_argument(
        "--private-key-ref",
        default="google_sheets_sa_private_key",
        help="Secret ref/env name holding the service-account private key PEM",
    )
    parser.add_argument(
        "--token-url",
        default=DEFAULT_TOKEN_URL,
        help="OAuth token endpoint for the service-account exchange",
    )
    parser.add_argument(
        "--endpoint-base-url",
        default=DEFAULT_ENDPOINT_BASE_URL,
        help="Sheets API base URL",
    )
    parser.add_argument(
        "--scope",
        dest="scopes",
        action="append",
        default=[],
        help=(
            "OAuth scope to request; may be repeated. Defaults to the Sheets scope when omitted."
        ),
    )
    parser.add_argument(
        "--connection-name",
        default="google_sheets_live",
        help="Connector connection instance name",
    )
    parser.add_argument(
        "--auth-handle-name",
        default="auth.google_sheets_sa",
        help="Connector handle name for the service-account auth provider",
    )
    parser.add_argument(
        "--endpoint-handle-name",
        default="endpoint.google_sheets_default",
        help="Connector handle name for the static endpoint profile",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the JSON output (default true)",
        default=True,
    )
    return parser.parse_args()


def run_base_generator(codebase_root: Path, out_path: Path, generated_at: str) -> dict[str, Any]:
    command = [
        "cargo",
        "run",
        "-p",
        "flows-cli",
        "--",
        "bindings",
        "lock",
        "generate",
        "--example",
        DEFAULT_EXAMPLE,
        "--generated-at",
        generated_at,
        "--out",
        str(out_path),
        "--bind",
        "resource::http=reqwest",
    ]
    subprocess.run(command, cwd=codebase_root, check=True)
    return json.loads(out_path.read_text())


def build_connector_sections(
    lock: dict[str, Any],
    *,
    scopes: list[str],
    token_url: str,
    endpoint_base_url: str,
    service_account_email_ref: str,
    private_key_ref: str,
    connection_name: str,
    auth_handle_name: str,
    endpoint_handle_name: str,
) -> None:
    flow_ids = list(lock.get("flows", {}).keys())
    if len(flow_ids) != 1:
        raise SystemExit(
            f"expected exactly one flow in generated lock, found {len(flow_ids)}"
        )
    flow_id = flow_ids[0]

    lock["connector_handles"] = {
        auth_handle_name: {
            "provider_kind": "auth.service_account_jwt",
            "handle_kind": "http.bearer",
            "connect": {
                "service_account_email_ref": service_account_email_ref,
                "private_key_ref": private_key_ref,
            },
            "config": {
                "token_url": token_url,
                "scopes": scopes,
            },
            "grants": {},
        },
        endpoint_handle_name: {
            "provider_kind": "endpoint.profile.static",
            "handle_kind": "endpoint.profile",
            "connect": {},
            "config": {
                "base_url": endpoint_base_url,
                "default_headers": {
                    "Accept": "application/json",
                },
            },
            "grants": {},
        },
    }

    lock["connector_connections"] = {
        connection_name: {
            "connector_id": "connector.google.sheets",
            "roles": {
                "outbound_auth.google_workspace_auth": auth_handle_name,
                "endpoint_profile.google_sheets_default": endpoint_handle_name,
            },
        }
    }

    lock["connector_bindings"] = {
        flow_id: {
            "defaults": {
                "connector.google.sheets": connection_name,
            },
            "nodes": {},
        }
    }


def canonical_json(value: Any) -> str:
    if value is None or isinstance(value, (bool, int, float, str)):
        return json.dumps(value, separators=(",", ":"), sort_keys=False)
    if isinstance(value, list):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    if isinstance(value, dict):
        items = []
        for key in sorted(value.keys()):
            items.append(json.dumps(key, separators=(",", ":")) + ":" + canonical_json(value[key]))
        return "{" + ",".join(items) + "}"
    raise TypeError(f"unsupported JSON type: {type(value)!r}")


def apply_content_hash(lock: dict[str, Any]) -> None:
    payload = dict(lock)
    payload.pop("content_hash", None)
    digest = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
    lock["content_hash"] = digest


def main() -> None:
    args = parse_args()
    scopes = args.scopes or [DEFAULT_SCOPE]
    script_path = Path(__file__).resolve()
    codebase_root = find_codebase_root(script_path.parent)

    with tempfile.TemporaryDirectory(prefix="google-sheets-bindings-") as tmp_dir:
        tmp_path = Path(tmp_dir) / "base.bindings.lock.json"
        lock = run_base_generator(codebase_root, tmp_path, args.generated_at)

    build_connector_sections(
        lock,
        scopes=scopes,
        token_url=args.token_url,
        endpoint_base_url=args.endpoint_base_url,
        service_account_email_ref=args.service_account_email_ref,
        private_key_ref=args.private_key_ref,
        connection_name=args.connection_name,
        auth_handle_name=args.auth_handle_name,
        endpoint_handle_name=args.endpoint_handle_name,
    )
    apply_content_hash(lock)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.pretty:
        args.out.write_text(json.dumps(lock, indent=2) + "\n")
    else:
        args.out.write_text(json.dumps(lock, separators=(",", ":")) + "\n")

    flow_id = next(iter(lock["flows"].keys()))
    print(f"Wrote {args.out}")
    print(f"Flow id: {flow_id}")
    print("Secret refs/env names expected:")
    print(f"  - {args.service_account_email_ref}")
    print(f"  - {args.private_key_ref}")
    print("Next steps:")
    print(f"  export {args.service_account_email_ref}=<service-account-email>")
    print(f"  export {args.private_key_ref}=<private-key-pem>")
    print("  # or load the private key from a service-account JSON file before running")
    print(
        "  cargo run -p flows-cli -- run serve --example "
        f"{DEFAULT_EXAMPLE} --bindings-lock {args.out} --addr 127.0.0.1:8080"
    )


if __name__ == "__main__":
    main()
