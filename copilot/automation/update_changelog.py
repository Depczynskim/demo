from __future__ import annotations

"""Update site_updates.md when new blog posts are detected.

Usage: python update_changelog.py
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # repo root
BLOG_METADATA_PATH = BASE_DIR / "copilot" / "content" / "blog_metadata.json"
SNAPSHOT_PATH = BASE_DIR / "copilot" / "changes" / "blog_metadata_snapshot.json"
CHANGELOG_PATH = BASE_DIR / "copilot" / "docs" / "site_updates.md"


def load_json(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)


def save_json(data: List[Dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def append_changelog_entry(post: Dict):
    posted_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    CHANGELOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new_file = not CHANGELOG_PATH.exists()
    with open(CHANGELOG_PATH, "a") as f:
        if is_new_file:
            f.write("# Site Updates Changelog\n\n")
        f.write("---\n")
        f.write("type: blog_post\n")
        f.write(f"title: {post['title']}\n")
        f.write(f"url: {post['url']}\n")
        if post.get("publish_date"):
            f.write(f"published_at: {post['publish_date']}\n")
        f.write(f"logged_at: {posted_at}\n")
        f.write("---\n\n")


def main():
    if not BLOG_METADATA_PATH.exists():
        logger.error("Current blog metadata not found; run crawler first.")
        return

    current_posts = load_json(BLOG_METADATA_PATH)
    prev_posts = load_json(SNAPSHOT_PATH)
    prev_urls = {p["url"] for p in prev_posts}

    new_posts = [p for p in current_posts if p["url"] not in prev_urls]
    if not new_posts:
        logger.info("No new blog posts detected since last snapshot.")
    else:
        logger.info(f"Detected {len(new_posts)} new blog posts. Appending to changelog…")
        for post in new_posts:
            append_changelog_entry(post)

    # Save snapshot
    save_json(current_posts, SNAPSHOT_PATH)
    logger.info("Snapshot updated.")


if __name__ == "__main__":
    main() 