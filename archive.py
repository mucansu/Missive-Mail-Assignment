#!/usr/bin/env python3
"""
missive_auto_archive.py – Daily housekeeping script for Missive
---------------------------------------------------------------
Archives (closes) conversations that **no active teammate owns** and whose
`last_activity_at` is older than a given number of days.  “Un-owned” means:

* `assignees` list is empty **or**
* every assignee ID belongs to a deactivated (disabled) user.

Bu sayede işten çıkarılan çalışanın kapalı hesabı üzerindeki eski
konuşmalar Team Inbox’ta kalmıyor.

Version 2.2 · 2025-06-26
~~~~~~~~~~~~~~~~~~~~~~~~
* NEW `get_active_user_ids()` – çekirdek ekipte hâlâ aktif olan kullanıcı
  ID’lerini alıyor.
* `list_old_unassigned()` artık “assignee yok **veya** tüm assignee’ler
  inaktif” koşuluyla çalışıyor.
* Parametre `open=true` → sadece **açık** (Inbox/Team Inbox) konuşmalar
  taranıyor; kapalı arşiv taraması yok.
"""
from __future__ import annotations
import argparse
import time
from datetime import datetime, timezone, timedelta
import logging
from logging.handlers import RotatingFileHandler
import requests

# ---------------------------------------------------------------------------
# CONFIG – sabitler
# ---------------------------------------------------------------------------

API_KEY  = "missive_pat-o2ylEV6WSMiMEr1NvFMWWIzYh9RGukzVn_rs4jMavbaFsn8ox7Sjfxufw1rWgpGxRuYqtw"
TEAM_ID  = "e3aa36e4-d631-488d-8002-35f8e85bb824"   # boş → auto-discover
ORG_ID   = "f50f2ccf-e588-4b56-bb15-672a515e0e1e"   # boş → auto-discover
ARCHIVE_LABEL_ID = ""      # opsiyonel
DEFAULT_DAYS_OLD = 30
RATE_LIMIT_SLEEP = 1

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logger = logging.getLogger("missive_archive")
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(console)
fh = RotatingFileHandler("missive_archive.log", maxBytes=1_000_000, backupCount=3)
fh.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(fh)

HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def discover_team_and_org() -> tuple[str, str]:
    r = requests.get("https://public.missiveapp.com/v1/teams", headers=HEADERS, timeout=30)
    r.raise_for_status()
    first = r.json()["teams"][0]
    logger.info("Auto‑discovered TEAM_ID=%s ORG_ID=%s", first["id"], first["organization"])
    return first["id"], first["organization"]


def get_active_user_ids(org_id: str) -> set[str]:
    """Return IDs of organization users whose account is NOT deactivated."""
    url = "https://public.missiveapp.com/v1/users"
    r = requests.get(url, headers=HEADERS, params={"organization": org_id}, timeout=30)
    if r.status_code == 404:
        # fallback for older API path
        url = f"https://public.missiveapp.com/v1/organizations/{org_id}/users"
        r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    users = r.json().get("users", [])
    active = {u["id"] for u in users if not u.get("is_deactivated", False)}
    logger.info("Fetched %d active users", len(active))
    return active


# ---------------------------------------------------------------------------
# CORE
# ---------------------------------------------------------------------------

def list_unowned_convos(team_id: str, cutoff_ts: int, active_ids: set[str]):
    """Yield IDs of open conversations older than cutoff that have *no active* assignee."""
    url = "https://public.missiveapp.com/v1/conversations"
    params = {"team_all": team_id, "limit": 50, "open": True}
    scanned = yielded = 0
    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        convos = r.json().get("conversations", [])
        if not convos:
            break
        for c in convos:
            scanned += 1
            if c.get("state") == "closed" or c.get("is_done") or c.get("closed"):
                continue
            if c.get("last_activity_at", 0) > cutoff_ts:
                continue
            ids = [(a["id"] if isinstance(a, dict) else a) for a in c.get("assignees", [])]
            if not ids or not any(i in active_ids for i in ids):
                yielded += 1
                yield c["id"]
        params["until"] = convos[-1]["last_activity_at"]
        if len(convos) < params["limit"]:
            break
        time.sleep(RATE_LIMIT_SLEEP)
    logger.info("Scanned %d convos, %d are un‑owned", scanned, yielded)

def close_conversation(cid: str, org_id: str, label_id: str | None) -> bool:
    post = {
        "conversation": cid,
        "organization": org_id,
        "close": True,               # ✓ kapat
        "reopen": False,             # ✓ yeniden açılmasın
        "add_to_inbox": False,
        "markdown": "_Auto-archive_",# ← Missive ‘markdown’ gördüğü için doğrulama geçer
        "notification": {
            "title": "Auto-archive",
            "body":  "Conversation closed by housekeeping script."
        }
    }
    if label_id:
        post["add_shared_labels"] = [label_id]

    payload = {"posts": post}        # ‘posts’ tekil nesne olmalı, liste değil
    r = requests.post(
        "https://public.missiveapp.com/v1/posts",
        json=payload,
        headers=HEADERS,
        timeout=30
    )
    if r.ok:
        logger.info("Closed %s", cid)
        return True

    logger.error("Failed to close %s – %s", cid, r.text)
    return False

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Archive Missive conversations with no active assignee")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS_OLD, help="Older than N days (default %(default)s)")
    p.add_argument("--team", help="Override team inbox ID")
    p.add_argument("--label", default=ARCHIVE_LABEL_ID, help="Shared‑label to apply (optional)")
    args = p.parse_args()

    global TEAM_ID, ORG_ID
    if args.team:
        TEAM_ID = args.team
    if not TEAM_ID or not ORG_ID:
        TEAM_ID, ORG_ID = discover_team_and_org()

    active_ids = get_active_user_ids(ORG_ID)
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp())

    archived = 0
    for cid in list_unowned_convos(TEAM_ID, cutoff_ts, active_ids):
        if close_conversation(cid, ORG_ID, args.label or None):
            archived += 1
        time.sleep(RATE_LIMIT_SLEEP)

    logger.info("Done – %d conversation(s) archived (>=%d days, team %s)", archived, args.days, TEAM_ID)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user – exiting.")
