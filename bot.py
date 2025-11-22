import asyncio
import json
import os
import re
import logging
from typing import Optional

import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# --------------------------------------
# CONFIG
# --------------------------------------

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CSV_PATH = "1.5k_ASNB.csv"
CONFIG_PATH = "config.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------------------------
# LOAD CONFIG / CSV
# --------------------------------------

def load_config(path: str) -> dict:
    default = {"allow_groups": True}
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {**default, **data}
    except:
        return default


def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path, dtype=str, header=None, keep_default_na=False)
    return df


# --------------------------------------
# PHONE SEARCH LOGIC
# --------------------------------------

def normalize_number(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def find_row_by_number(df: pd.DataFrame, phone: str) -> Optional[pd.Series]:
    """
    Zoekt in ALLE kolommen, niet alleen kolom 0,
    zodat we ALLE data van die regel vinden.
    """
    phone_norm = normalize_number(phone)
    if phone_norm == "":
        return None

    mask = df.apply(
        lambda col: col.astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.contains(phone_norm, na=False)
    )

    rows = df[mask.any(axis=1)]
    if rows.empty:
        return None

    return rows.iloc[0]


# --------------------------------------
# FORMAT RESULT
# --------------------------------------

def pretty_format_row(row: pd.Series) -> str:
    """
    Combineer alle kolommen → haal ALLE key:"value" velden eruit.
    """
    text = " ".join([str(x) for x in row if str(x).strip()])

    # Zoek alle key:"value" velden
    pairs = re.findall(r'([A-Za-z0-9_]+):"([^"]*)"', text)

    if not pairs:
        return text

    out = []
    for key, val in pairs:
        if val.strip():
            out.append(f"{key}: {val}")

    return "\n".join(out)


# --------------------------------------
# TELEGRAM BOT
# --------------------------------------

if TELEGRAM_TOKEN is None:
    raise SystemExit("TELEGRAM_TOKEN niet ingesteld!")

config = load_config(CONFIG_PATH)
allow_groups = config.get("allow_groups", True)

df = load_csv(CSV_PATH)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.reply("Stuur een telefoonnummer en ik haal de gegevens op.")


@dp.message_handler()
async def handle_message(message: types.Message):

    if message.chat.type != "private" and not allow_groups:
        return

    text = message.text.strip()

    # Zoek telefoonnummer
    num = re.search(r"[+]?\d[\d \-()+]{4,}", text)
    if not num:
        return

    phone_raw = num.group(0)

    # Zoek in CSV
    row = find_row_by_number(df, phone_raw)
    if row is None:
        await message.reply(f"Geen gegevens gevonden voor: {phone_raw}")
        return

    formatted = pretty_format_row(row)
    await message.reply(formatted)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
