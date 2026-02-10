import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    load_dotenv()


@dataclass
class Settings:
    bot_token: str
    admin_id: int
    db_path: str


def get_settings() -> Settings:
    # Agar .env da BOT_TOKEN bo'lmasa, foydalanuvchining bergan tokenidan foydalanamiz
    token = os.getenv("BOT_TOKEN", "").strip() or "8497619783:AAFQ7DxprgmbsNJiTPe2ib8K9W5kkW5HvTw"

    admin_raw = os.getenv("ADMIN_ID", "0").strip()
    try:
        admin_id = int(admin_raw) if admin_raw else 0
    except ValueError:
        admin_id = 0

    db_path = os.getenv("DB_PATH", str(BASE_DIR / "bot.db"))

    return Settings(
        bot_token=token,
        admin_id=admin_id,
        db_path=db_path,
    )


settings = get_settings()

