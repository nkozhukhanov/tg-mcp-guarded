"""Idle release of the Telegram session file + transparent reconnect.

Several MCP processes (one read+actions pair per Claude session) share one sqlite
.session file. A process must hold it only while it is actually talking to Telegram.
"""

import asyncio
import os
from types import SimpleNamespace

import pytest
from telethon import TelegramClient
from telethon.sessions import MemorySession

os.environ.setdefault("TG_API_ID", "1")
os.environ.setdefault("TG_API_HASH", "testhash")

from tganalytics.infra import tele_client  # noqa: E402
import mcp_server_common as common  # noqa: E402


# --- GuardedSQLiteSession ---------------------------------------------------------

def test_sqlite_session_applies_wal_and_busy_timeout_on_every_open(tmp_path, monkeypatch):
    monkeypatch.setattr(tele_client, "SESSION_WAL_ENABLED", True)
    monkeypatch.setattr(tele_client, "SESSION_BUSY_TIMEOUT_MS", 4321)

    session = tele_client.GuardedSQLiteSession(str(tmp_path / "s"))
    conn = session._conn
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 4321

    # Telethon closes the connection on client.disconnect() and reopens it lazily;
    # busy_timeout is per-connection, so it must be re-applied on reopen.
    session.close()
    assert session._conn is None
    session._cursor()
    assert session._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 4321
    assert session._conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    session.close()


def test_sqlite_session_wal_can_be_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(tele_client, "SESSION_WAL_ENABLED", False)
    session = tele_client.GuardedSQLiteSession(str(tmp_path / "s"))
    assert session._conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    session.close()


# --- GuardedTelegramClient --------------------------------------------------------

def _make_client(monkeypatch):
    client = tele_client.GuardedTelegramClient(MemorySession(), 1, "hash", receive_updates=False)
    state = {"connected": False, "connects": 0, "disconnects": 0}

    async def connect():
        state["connected"] = True
        state["connects"] += 1

    async def disconnect():
        state["connected"] = False
        state["disconnects"] += 1

    monkeypatch.setattr(client, "is_connected", lambda: state["connected"])
    monkeypatch.setattr(client, "connect", connect)
    monkeypatch.setattr(client, "disconnect", disconnect)
    return client, state


def _patch_super_call(monkeypatch, seen_inflight):
    async def fake_call(self, request, *args, **kwargs):
        seen_inflight.append(self.inflight)
        return "ok"

    monkeypatch.setattr(TelegramClient, "__call__", fake_call)


@pytest.mark.asyncio
async def test_call_connects_lazily_and_tracks_inflight(monkeypatch):
    client, state = _make_client(monkeypatch)
    seen = []
    _patch_super_call(monkeypatch, seen)

    assert client.inflight == 0
    assert await client(object()) == "ok"

    assert state["connects"] == 1
    assert seen == [1]  # request was counted as in flight while running
    assert client.inflight == 0
    assert client.idle_seconds() < 1.0

    await client(object())
    assert state["connects"] == 1  # already connected -> no reconnect


@pytest.mark.asyncio
async def test_disconnect_if_idle_respects_inflight_and_threshold(monkeypatch):
    client, state = _make_client(monkeypatch)

    assert await client.disconnect_if_idle(0) is False  # not connected -> nothing to release

    await client.ensure_connected()
    assert state["connects"] == 1
    assert await client.disconnect_if_idle(3600) is False  # not idle long enough

    client._guard_last_activity -= 10
    client._guard_inflight = 1
    assert await client.disconnect_if_idle(5) is False  # request in flight -> keep
    client._guard_inflight = 0
    assert await client.disconnect_if_idle(5) is True
    assert state["disconnects"] == 1 and state["connected"] is False

    # next request reconnects transparently
    _patch_super_call(monkeypatch, [])
    await client(object())
    assert state["connects"] == 2


# --- MCPServerContext idle watchdog -----------------------------------------------

class IdleDummyClient:
    def __init__(self):
        self.connected = False
        self.connects = 0
        self.disconnects = 0
        self.idle = 0.0

    async def connect(self):
        self.connected = True
        self.connects += 1

    async def disconnect(self):
        self.connected = False
        self.disconnects += 1

    def is_connected(self):
        return self.connected

    async def is_user_authorized(self):
        return True

    async def get_me(self):
        return SimpleNamespace(id=1, username="user", first_name="User")

    async def ensure_connected(self):
        if not self.connected:
            await self.connect()

    async def disconnect_if_idle(self, idle_sec):
        if self.connected and self.idle >= idle_sec:
            await self.disconnect()
            return True
        return False


@pytest.mark.asyncio
async def test_context_releases_idle_client_and_reconnects_on_next_call(monkeypatch):
    monkeypatch.delenv("TG_EXPECTED_USERNAME", raising=False)
    ctx = common.MCPServerContext(allow_session_switch=False, idle_disconnect_sec=0.2)
    client = IdleDummyClient()

    manager = await ctx._connect_client(client, "s")
    assert client.connected is True
    assert ctx._idle_task is not None and not ctx._idle_task.done()

    try:
        client.idle = 5.0
        await asyncio.sleep(0.3)  # watchdog polls every idle/4 = 0.05s
        assert client.disconnects == 1
        assert client.connected is False

        assert await ctx.get_manager() is manager
        assert client.connected is True
        assert client.connects == 2
    finally:
        ctx._idle_task.cancel()


@pytest.mark.asyncio
async def test_context_idle_release_disabled_with_zero(monkeypatch):
    monkeypatch.delenv("TG_EXPECTED_USERNAME", raising=False)
    ctx = common.MCPServerContext(allow_session_switch=False, idle_disconnect_sec=0)
    client = IdleDummyClient()
    await ctx._connect_client(client, "s")

    assert ctx._idle_task is None
    client.idle = 999.0
    assert await ctx.release_if_idle() is False
    assert client.connected is True


# --- IPv6 toggle ------------------------------------------------------------------

def test_use_ipv6_flag_is_passed_to_client(tmp_path, monkeypatch):
    monkeypatch.setattr(tele_client, "USE_IPV6", True)
    monkeypatch.setattr(tele_client, "SESSION_WAL_ENABLED", False)
    client = tele_client.get_client_for_session(str(tmp_path / "ipv6_probe.session"))
    assert client._use_ipv6 is True
    client.session.close()
