from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


class JsonCache:
    """Minimal JSON file cache used for schema snapshots.

    The cache is intentionally simple: it stores a single JSON-serializable
    payload at the target path. Consumers decide when to refresh by passing
    ``use_cache=False`` on read paths.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self) -> Optional[Dict[str, Any]]:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def write(self, payload: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
