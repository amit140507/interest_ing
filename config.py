import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SOURCES_PATH = BASE_DIR / "sources.json"
DATABASE_PATH = Path(os.environ.get("FD_DB_PATH", str(DATA_DIR / "fd_rates.db")))

REQUEST_TIMEOUT = 45
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
