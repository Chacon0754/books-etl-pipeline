import json
from pathlib import Path
from typing import Any

def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    
    return data

def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)