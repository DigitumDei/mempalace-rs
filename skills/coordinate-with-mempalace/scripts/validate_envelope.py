#!/usr/bin/env python3
"""Validate the structural contract of a Phase 0 coordination envelope."""

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

VERSION = "mempalace.coordination/v1alpha1"
KINDS = {"task", "handoff", "result", "artifact"}
COMMON = {
    "api_version",
    "kind",
    "coordination_id",
    "message_id",
    "created_at",
    "producer",
    "idempotency_key",
    "payload",
}
REQUIRED = {
    "task": {"task_id", "objective", "acceptance_criteria", "constraints"},
    "handoff": {"task_id", "from", "to", "reason", "task_ref"},
    "result": {"task_id", "status", "summary", "task_ref", "artifact_refs"},
    "artifact": {"artifact_id", "media_type", "content", "sha256"},
}


def validate_ref(value: Any, field: str) -> list[str]:
    if not isinstance(value, dict):
        return [f"{field} must be an object"]
    missing = {"drawer_id", "wing", "room", "origin"} - value.keys()
    return [f"{field} missing {name}" for name in sorted(missing)]


def validate(document: Any) -> list[str]:
    if not isinstance(document, dict):
        return ["envelope must be a JSON object"]
    errors = [f"missing {name}" for name in sorted(COMMON - document.keys())]
    if errors:
        return errors
    if document["api_version"] != VERSION:
        errors.append(f"api_version must be {VERSION}")
    kind = document["kind"]
    if kind not in KINDS:
        errors.append(f"kind must be one of {', '.join(sorted(KINDS))}")
        return errors
    for field in ("coordination_id", "message_id", "producer", "idempotency_key"):
        if not isinstance(document[field], str) or not document[field].strip():
            errors.append(f"{field} must be a non-empty string")
    try:
        datetime.fromisoformat(document["created_at"].replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        errors.append("created_at must be RFC 3339 compatible")
    payload = document["payload"]
    if not isinstance(payload, dict):
        errors.append("payload must be an object")
        return errors
    errors.extend(f"payload missing {name}" for name in sorted(REQUIRED[kind] - payload.keys()))
    if kind == "handoff" and "task_ref" in payload:
        errors.extend(validate_ref(payload["task_ref"], "payload.task_ref"))
    if kind == "result":
        if payload.get("status") not in {"completed", "blocked", "failed"}:
            errors.append("payload.status must be completed, blocked, or failed")
        if "task_ref" in payload:
            errors.extend(validate_ref(payload["task_ref"], "payload.task_ref"))
        for index, ref in enumerate(payload.get("artifact_refs", [])):
            errors.extend(validate_ref(ref, f"payload.artifact_refs[{index}]"))
    if kind == "artifact" and {"content", "sha256"} <= payload.keys():
        content = payload["content"]
        if not isinstance(content, str):
            errors.append("payload.content must be a string")
        elif hashlib.sha256(content.encode()).hexdigest() != payload["sha256"]:
            errors.append("payload.sha256 does not match payload.content")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    failed = False
    for filename in args.files:
        try:
            document = json.loads(Path(filename).read_text(encoding="utf-8"))
            errors = validate(document)
        except (OSError, json.JSONDecodeError) as error:
            errors = [str(error)]
        if errors:
            failed = True
            for error in errors:
                print(f"{filename}: {error}")
        else:
            print(f"{filename}: valid")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
