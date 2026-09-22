"""Тесты для GroupManager.get_post_comments и сериализации сообщений."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from telethon.tl.types import PeerChannel, PeerUser

from tganalytics.domain.groups import GroupManager, _message_to_dict, _peer_id


def _fake_message(msg_id, text, from_id, reply_to_id=None, replies=None):
    return SimpleNamespace(
        id=msg_id,
        date=datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc),
        from_id=from_id,
        message=text,
        fwd_from=None,
        forward=None,
        reply_to=SimpleNamespace(reply_to_msg_id=reply_to_id) if reply_to_id else None,
        views=None,
        forwards=None,
        replies=SimpleNamespace(replies=replies) if replies is not None else None,
        is_pinned=False,
        media=None,
    )


def _async_gen(items):
    async def gen(*_args, **_kwargs):
        for item in items:
            yield item
    return gen


def test_peer_id_handles_user_channel_and_none():
    assert _peer_id(PeerUser(user_id=42)) == 42
    assert _peer_id(PeerChannel(channel_id=777)) == 777
    assert _peer_id(None) is None


def test_message_to_dict_channel_author_and_replies_count():
    msg = _fake_message(12, "post", PeerChannel(channel_id=4399143807), replies=3)
    data = _message_to_dict(msg)
    assert data["from_id"] == 4399143807
    assert data["replies_count"] == 3
    assert data["is_reply"] is False
    assert data["reply_to_msg_id"] is None


@pytest.mark.asyncio
async def test_get_post_comments_passes_reply_to_and_serializes(mock_telegram_client, mock_channel):
    mock_telegram_client.get_entity.return_value = mock_channel
    comments = [
        _fake_message(18, "1 и 2 ок", PeerUser(user_id=542812438), reply_to_id=5),
        _fake_message(13, "можем не менять тексты?", PeerUser(user_id=8763546211), reply_to_id=11),
        _fake_message(19, "", None),  # служебное — должно быть отфильтровано
    ]
    mock_telegram_client.iter_messages = MagicMock(side_effect=_async_gen(comments))

    manager = GroupManager(mock_telegram_client)
    result = await manager.get_post_comments("-100123456789", post_id=7, limit=50, min_id=0)

    assert [c["id"] for c in result] == [18, 13]
    assert result[0]["from_id"] == 542812438
    assert result[0]["reply_to_msg_id"] == 5
    assert result[0]["replies_count"] is None

    kwargs = mock_telegram_client.iter_messages.call_args.kwargs
    assert kwargs["reply_to"] == 7
    assert kwargs["min_id"] == 0
    assert kwargs["limit"] == 50


@pytest.mark.asyncio
async def test_get_post_comments_returns_empty_on_error(mock_telegram_client):
    mock_telegram_client.get_entity.side_effect = Exception("no access")
    manager = GroupManager(mock_telegram_client)
    assert await manager.get_post_comments("-100123456789", post_id=7) == []


@pytest.mark.asyncio
async def test_get_post_comments_rejects_invalid_identifier(mock_telegram_client):
    manager = GroupManager(mock_telegram_client)
    assert await manager.get_post_comments("", post_id=7) == []
    mock_telegram_client.get_entity.assert_not_called()
