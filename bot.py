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
    df[0] = df[0].astype(str)
    return df


# --------------------------------------
# PHONE NUMBER & SEARCH LOGIC
# --------------------------------------

def normalize_number(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def find_row_by_number(df: pd.DataFrame, phone: str) -> Optional[str]:
    phone_norm = normalize_number(phone)
    if not phone_norm:
        return None

    # Zoek in de volledige regel
    mask = df[0].str.replace(r"\D", "", regex=True).str.contains(phone_norm)
    rows = df[mask]

    if rows.empty:
        return None

    return rows.iloc[0, 0]  # volledige regel string


def pretty_format_row(line: str) -> str:
    """
    Extract ALLE key:"value" velden — precies zoals je CSV werkt.
    """
    pairs = re.findall(r'([A-Za-z0-9_]+):"([^"]*)"', line)

    if not pairs:
        return "(geen details gevonden)"

    out = []
    for key, val in pairs:
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
    await message.reply("Stuur een telefoonnummer en ik zoek de gegevens erbij.")


@dp.message_handler()
async def handle_message(message: types.Message):

    # Als groepen verboden zijn (kan via config)
    if message.chat.type != "private" and not allow_groups:
        return

    text = message.text.strip()

    # Telefonummer zoeken
    num = re.search(r"[+]?\d[\d \-()+]{4,}", text)
    if not num:
        return

    phone_raw = num.group(0)

    # CSV zoeken
    line = find_row_by_number(df, phone_raw)
    if line is None:
        await message.reply(f"Geen gegevens gevonden voor: {phone_raw}")
        return

    # Netjes formatteren
    formatted = pretty_format_row(line)
    await message.reply(formatted)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
