"""Shared state/helpers for tg-mcp MCP servers."""

from __future__ import annotations

import asyncio
import glob
import logging
import os
from typing import Any
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from tganalytics.domain.groups import GroupManager
from tganalytics.infra.tele_client import get_client, get_client_for_session

logger = logging.getLogger(__name__)

# Release the Telegram connection (and the sqlite session file) after this many
# seconds without tool calls; the next tool call reconnects transparently.
# 0 disables idle release (old behaviour: hold the session file for process lifetime).
try:
    IDLE_DISCONNECT_SEC = float(os.environ.get("TG_IDLE_DISCONNECT_SEC", "120"))
except ValueError:
    IDLE_DISCONNECT_SEC = 120.0


def _expected_username() -> str:
    raw = os.environ.get("TG_EXPECTED_USERNAME", "").strip().lstrip("@")
    return raw.lower()


def _build_session_mismatch_error(expected_username: str, actual_username: str | None, account_id: int | None) -> str:
    expected = f"@{expected_username}"
    actual_clean = (actual_username or "").strip()
    actual = f"@{actual_clean}" if actual_clean else "<no_username>"
    return (
        f"Session mismatch: expected account {expected}, got {actual} (id={account_id}). "
        "Set TG_SESSION_PATH to the correct session and restart MCP."
    )


def _validate_expected_account(me: Any) -> str | None:
    expected = _expected_username()
    if not expected:
        return None

    actual_username = (getattr(me, "username", None) or "").strip().lower()
    actual_id = getattr(me, "id", None)
    if actual_username != expected:
        return _build_session_mismatch_error(expected, getattr(me, "username", None), actual_id)
    return None


class MCPServerContext:
    """Shared runtime state for MCP servers.

    Keeps one active Telegram client/session per server process. The client is
    created and connected lazily on the first tool call, and released again after
    ``idle_disconnect_sec`` without calls so that idle MCP processes (one pair per
    Claude session) do not keep the shared sqlite session file open.
    """

    def __init__(
        self,
        sessions_dir: str | None = None,
        allow_session_switch: bool = True,
        idle_disconnect_sec: float | None = None,
    ):
        self.sessions_dir = sessions_dir or os.environ.get("TG_SESSIONS_DIR", "data/sessions")
        self.allow_session_switch = allow_session_switch
        self.idle_disconnect_sec = (
            IDLE_DISCONNECT_SEC if idle_disconnect_sec is None else float(idle_disconnect_sec)
        )

        self._client = None
        self._manager: GroupManager | None = None
        self._current_session: str | None = None
        self._idle_task: asyncio.Task | None = None

    @property
    def current_session(self) -> str | None:
        return self._current_session

    @property
    def client(self) -> Any:
        return self._client

    async def _connect_client(self, client, session_name: str) -> GroupManager:
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError(
                f"Session '{session_name}' is not authorized. "
                "Run create_telegram_session.py (or scripts/create_session_qr.py) to re-authenticate. "
                "Telegram login code usually arrives in-app (SentCodeTypeApp), not SMS."
            )

        me = await client.get_me()
        mismatch_error = _validate_expected_account(me)
        if mismatch_error:
            await client.disconnect()
            raise RuntimeError(mismatch_error)

        self._client = client
        self._current_session = session_name
        self._manager = GroupManager(client)
        self._start_idle_watchdog(client)
        return self._manager

    async def get_manager(self) -> GroupManager:
        """Lazy-init manager and connect on the first call; reconnect after idle release."""
        if self._manager is None:
            session_path = os.environ.get("TG_SESSION_PATH", "").strip()
            if session_path:
                session_name = os.path.basename(session_path).replace(".session", "")
                client = get_client_for_session(session_path)
            else:
                session_name = os.environ.get("SESSION_NAME", "default")
                client = get_client()

            await self._connect_client(client, session_name)
        else:
            await self._reconnect_if_released()

        return self._manager

    async def _reconnect_if_released(self) -> None:
        """Re-open the connection if the idle watchdog released it."""
        client = self._client
        if client is None:
            return
        ensure_connected = getattr(client, "ensure_connected", None)
        if ensure_connected is not None:
            await ensure_connected()
            return
        is_connected = getattr(client, "is_connected", None)
        if is_connected is not None and not is_connected():
            await client.connect()

    # --- idle release ---------------------------------------------------------
    def _start_idle_watchdog(self, client: Any) -> None:
        if self.idle_disconnect_sec <= 0:
            return
        if getattr(client, "disconnect_if_idle", None) is None:
            return
        if self._idle_task is not None and not self._idle_task.done():
            return
        self._idle_task = asyncio.get_running_loop().create_task(self._idle_watchdog())

    async def _idle_watchdog(self) -> None:
        # Poll a few times per idle window; cheap, and keeps the release latency bounded.
        interval = max(0.05, min(self.idle_disconnect_sec / 4.0, 15.0))
        while True:
            await asyncio.sleep(interval)
            client = self._client
            if client is None:
                continue
            try:
                await self.release_if_idle(client)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Idle release of Telegram session failed: %s", exc)

    async def release_if_idle(self, client: Any | None = None) -> bool:
        """Disconnect the client (closing the sqlite session file) if idle long enough."""
        client = client if client is not None else self._client
        if client is None or self.idle_disconnect_sec <= 0:
            return False
        disconnect_if_idle = getattr(client, "disconnect_if_idle", None)
        if disconnect_if_idle is None:
            return False
        released = await disconnect_if_idle(self.idle_disconnect_sec)
        if released:
            logger.info(
                "Telegram session '%s' released after %.0fs idle; will reconnect on next call",
                self._current_session,
                self.idle_disconnect_sec,
            )
        return released

    async def list_sessions(self) -> dict[str, Any]:
        sessions = [
            os.path.basename(path).replace(".session", "")
            for path in glob.glob(os.path.join(self.sessions_dir, "*.session"))
        ]
        return {"sessions": sorted(sessions), "current": self._current_session}

    async def use_session(self, session_name: str) -> dict[str, Any]:
        if not self.allow_session_switch:
            return {
                "error": "Session switching is disabled. "
                "Set TG_ALLOW_SESSION_SWITCH=1 to enable tg_use_session."
            }

        path = os.path.join(self.sessions_dir, f"{session_name}.session")
        if not os.path.exists(path):
            return {"error": f"Session '{session_name}' not found"}

        if self._client is not None:
            await self._client.disconnect()

        try:
            client = get_client_for_session(path)
            await self._connect_client(client, session_name)
            me = await self._client.get_me()
            return {"switched_to": session_name, "account": me.username or me.first_name}
        except RuntimeError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": f"Failed to switch session: {exc}"}

    async def auth_status(self) -> dict[str, Any]:
        """Return authorization status for current/default Telegram session."""
        session_path = os.environ.get("TG_SESSION_PATH", "").strip()
        if session_path:
            resolved_path = str(Path(session_path).expanduser().resolve())
            session_name = Path(session_path).name.replace(".session", "")
            client = self._client or get_client_for_session(session_path)
            is_transient = self._client is None
        else:
            resolved_path = ""
            session_name = os.environ.get("SESSION_NAME", "default")
            client = self._client or get_client()
            is_transient = self._client is None

        try:
            await client.connect()
            authorized = await client.is_user_authorized()
            payload: dict[str, Any] = {
                "authorized": bool(authorized),
                "session_name": session_name,
                "session_path": resolved_path or None,
            }
            if authorized:
                me = await client.get_me()
                payload["account"] = {
                    "id": getattr(me, "id", None),
                    "username": getattr(me, "username", None),
                    "first_name": getattr(me, "first_name", None),
                }
                mismatch_error = _validate_expected_account(me)
                if mismatch_error:
                    payload["authorized"] = False
                    payload["error"] = mismatch_error
            return payload
        except Exception as exc:
            return {
                "authorized": False,
                "session_name": session_name,
                "session_path": resolved_path or None,
                "error": str(exc),
            }
        finally:
            if is_transient:
                try:
                    await client.disconnect()
                except Exception:
                    pass
