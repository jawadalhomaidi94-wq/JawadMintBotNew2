import json
from pathlib import Path
from typing import Any


class JsonState:
    def __init__(self, path: str | Path, default: dict[str, Any] | None = None):
        self.path = Path(path)
        self.data = default or {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def set_values(self, key: str) -> set[str]:
        values = self.data.setdefault(key, [])
        return set(values)

    def add_unique(self, key: str, value: str) -> None:
        values = self.data.setdefault(key, [])
        if value not in values:
            values.append(value)
            self.save()
