"""Read comments under channel posts via GroupManager.get_post_comments (read-only smoke test).

Usage: venv/bin/python3 scripts/fetch_post_comments.py <channel_id> <post_id> [<post_id> ...]
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
os.chdir(REPO_ROOT)
sys.path.insert(0, str(REPO_ROOT / "tganalytics"))
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("SESSION_DIR", str(REPO_ROOT / "tganalytics/data/sessions"))
os.environ.setdefault("SESSION_NAME", "read_only_session")

from tganalytics.infra.tele_client import get_client  # noqa: E402
from tganalytics.domain.groups import GroupManager  # noqa: E402


async def main(channel: str, post_ids: list[int]) -> None:
    client = get_client()
    await client.connect()
    manager = GroupManager(client)
    for post_id in post_ids:
        comments = await manager.get_post_comments(channel, post_id)
        print(f"=== post {post_id}: {len(comments)} comments")
        for c in comments:
            print(json.dumps(c, ensure_ascii=False))
    await client.disconnect()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    asyncio.run(main(sys.argv[1], [int(x) for x in sys.argv[2:]]))
