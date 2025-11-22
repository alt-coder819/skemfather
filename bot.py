import asyncio
import json
import os
import re
import logging
from typing import Optional

import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CSV_PATH = "1.5k_ASNB.csv"
CONFIG_PATH = "config.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(path: str) -> dict:
    default = {"whitelist_user_ids": [], "allow_groups": True}
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {**default, **data}
    except Exception as e:
        logger.warning("Failed to load config.json: %s", e)
        return default

def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found at {path}")
    df = pd.read_csv(path, dtype=str, header=None, keep_default_na=False)
    df = df.fillna("")
    return df

def normalize_number(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\D", "", s)

def find_row_by_number(df: pd.DataFrame, phone: str) -> Optional[pd.Series]:
    phone_norm = normalize_number(phone)
    if phone_norm == "":
        return None
    mask = df.apply(lambda col: col.astype(str).str.replace(r"\D", "", regex=True).str.contains(phone_norm, na=False))
    rows = df[mask.any(axis=1)]
    if rows.empty:
        return None
    return rows.iloc[0]

def pretty_format_row(row: pd.Series) -> str:
    parts = []
    for cell in row:
        text = str(cell).strip()
        if not text:
            continue
        tokens = re.findall(r'([A-Za-z0-9_\\-]+):"([^"]*)"', text)
        if tokens:
            for key, val in tokens:
                parts.append(f"{key}: {val}")
        else:
            parts.append(text)
    if not parts:
        return "(geen gegevens gevonden)"
    return "\n".join(parts)

if TELEGRAM_TOKEN is None:
    raise SystemExit("TELEGRAM_TOKEN niet ingesteld!")

config = load_config(CONFIG_PATH)
allow_groups = config.get("allow_groups", True)

df = load_csv(CSV_PATH)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.reply("Stuur een telefoonnummer en ik zoek de gegevens erbij.")

@dp.message_handler()
async def handle_message(message: types.Message):

    if message.chat.type != "private" and not allow_groups:
        return

    text = message.text.strip()
    num = re.search(r"[+]?\\d[\\d \\-()+]{4,}", text)
    if not num:
        return

    phone_raw = num.group(0)
    row = find_row_by_number(df, phone_raw)
    if row is None:
        await message.reply(f"Geen gegevens gevonden voor: {phone_raw}")
        return

    formatted = pretty_format_row(row)
    await message.reply(formatted)

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
