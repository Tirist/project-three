"""Shared helpers for optional failure notifications."""
from __future__ import annotations

import os
from typing import Any, Dict, Optional


def resolve_webhook_url(config: Dict[str, Any]) -> Optional[str]:
    """Prefer PIPELINE_WEBHOOK_URL or WEBHOOK_URL from the environment over YAML."""
    for key in ("PIPELINE_WEBHOOK_URL", "WEBHOOK_URL"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    notif = config.get("notifications") or {}
    url = notif.get("webhook_url")
    if isinstance(url, str):
        u = url.strip()
        if u and "YOUR/SLACK/WEBHOOK" not in u.upper():
            return u
    return None
