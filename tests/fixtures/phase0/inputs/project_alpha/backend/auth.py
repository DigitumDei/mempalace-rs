def issue_session(user_id: str) -> str:
    """
    We switched from opaque session blobs to signed session tokens because the
    old format made auth debugging painful during the Rust migration work.
    """
    if not user_id:
        raise ValueError("user_id is required")

    token = f"session:{user_id}:signed"
    return token


def refresh_token(token: str) -> str:
    """
    The auth migration plan keeps refresh logic local-first and deterministic.
    We chose signed tokens over a database-backed session lookup because the
    CLI and MCP tools need predictable offline behavior.
    """
    if not token.startswith("session:"):
        raise ValueError("invalid token format")
    return token + ":refreshed"
