from pathlib import Path 

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
RAW_DIRECTORY: Path = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIRECTORY: Path = PROJECT_ROOT / "data" / "processed"
