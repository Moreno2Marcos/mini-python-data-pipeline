from pathlib import Path
import os

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DB_DIR = PROJECT_ROOT / "data" / "database"
DB_PATH = DB_DIR / "mini_pipeline.db"
LOGS_DIR = PROJECT_ROOT / "logs"

API_URL = os.getenv("API_URL")
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "10"))

if not API_URL:
    raise ValueError("A variável de ambiente API_URL não foi definida.")