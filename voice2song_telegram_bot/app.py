# -*- coding: utf-8 -*-
"""
Voice2Song Persian Telegram Bot
ربات تلگرام فارسی برای تبدیل وویس/آدیو به MIDI و آهنگ MP3 + پنل ادمین ریسپانسیو.

Run:
  pip install -r requirements.txt
  cp .env.example .env
  python app.py
"""
from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import os
import random
import re
import shutil
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import requests
from dotenv import load_dotenv
from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template_string,
    request,
    send_from_directory,
    session,
    url_for,
)
from scipy.io import wavfile
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from werkzeug.security import check_password_hash, generate_password_hash

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
load_dotenv()

APP_NAME = os.getenv("APP_NAME", "Voice2Song")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin12345")
SECRET_KEY = os.getenv("SECRET_KEY", "change-this-secret-key")
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8080"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")
DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
TIMEZONE = ZoneInfo(os.getenv("APP_TZ", "Asia/Tehran"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))
DEFAULT_STYLE = os.getenv("DEFAULT_STYLE", "random").strip() or "random"
SOUNDFONT_PATH = os.getenv("SOUNDFONT_PATH", "").strip()
MAX_AUDIO_SECONDS = int(os.getenv("MAX_AUDIO_SECONDS", "30"))
MIN_MELODY_NOTES = int(os.getenv("MIN_MELODY_NOTES", "5"))
SEND_VARIATION = os.getenv("SEND_VARIATION", "true").lower() in {"1", "true", "yes", "on"}
RENDER_QUALITY = os.getenv("RENDER_QUALITY", "standard").strip()
RAW_VOCAL_MIX = os.getenv("RAW_VOCAL_MIX", "false").lower() in {"1", "true", "yes", "on"}

DB_PATH = DATA_DIR / "voice2song.sqlite3"
UPLOADS_DIR = DATA_DIR / "uploads"
OUTPUTS_DIR = DATA_DIR / "outputs"
LOGS_DIR = DATA_DIR / "logs"

for d in [DATA_DIR, UPLOADS_DIR, OUTPUTS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / "app.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("voice2song")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

if not BOT_TOKEN:
    logger.warning("TELEGRAM_BOT_TOKEN is empty. Set it in .env before production run.")

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def now_dt() -> datetime:
    return datetime.now(TIMEZONE)


def now_iso() -> str:
    return now_dt().isoformat(timespec="seconds")


def today_key() -> str:
    return now_dt().strftime("%Y-%m-%d")


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def fmt_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(value)


def fmt_num(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except Exception:
        return str(value)


def safe_filename(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name or "file")
    return name[:120]


def run_cmd(cmd: List[str], timeout: int = 180) -> subprocess.CompletedProcess:
    logger.info("RUN: %s", " ".join(cmd))
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)


def telegram_api(method: str, payload: Dict[str, Any]) -> Optional[dict]:
    if not BOT_TOKEN:
        return None
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", data=payload, timeout=20)
        try:
            return r.json()
        except Exception:
            return {"ok": r.ok, "text": r.text}
    except Exception as exc:
        logger.warning("Telegram API error %s: %s", method, exc)
        return None


def send_telegram_message(chat_id: Any, text: str, parse_mode: Optional[str] = None) -> Optional[dict]:
    payload: Dict[str, Any] = {"chat_id": str(chat_id), "text": text[:3900]}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    return telegram_api("sendMessage", payload)

# -----------------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------------
class DB:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.init_schema()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self.lock:
            cur = self.conn.execute(sql, tuple(params))
            self.conn.commit()
            return cur

    def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> sqlite3.Cursor:
        with self.lock:
            cur = self.conn.executemany(sql, seq)
            self.conn.commit()
            return cur

    def one(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, tuple(params)).fetchone()

    def all(self, sql: str, params: Iterable[Any] = ()) -> List[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, tuple(params)).fetchall()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS plans (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                price_toman INTEGER NOT NULL DEFAULT 0,
                daily_limit INTEGER NOT NULL DEFAULT 3,
                max_seconds INTEGER NOT NULL DEFAULT 30,
                valid_days INTEGER NOT NULL DEFAULT 30,
                payment_link TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                plan_id TEXT NOT NULL DEFAULT 'free',
                plan_expires_at TEXT,
                selected_style TEXT NOT NULL DEFAULT 'random',
                used_today INTEGER NOT NULL DEFAULT 0,
                usage_date TEXT NOT NULL DEFAULT '',
                total_conversions INTEGER NOT NULL DEFAULT 0,
                is_blocked INTEGER NOT NULL DEFAULT 0,
                pending_plan_id TEXT,
                admin_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                FOREIGN KEY(plan_id) REFERENCES plans(id)
            );
            CREATE TABLE IF NOT EXISTS conversions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                input_kind TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                duration_seconds INTEGER,
                input_path TEXT,
                wav_path TEXT,
                raw_midi_path TEXT,
                arranged_midi_path TEXT,
                mp3_path TEXT,
                variation_mp3_path TEXT,
                style TEXT NOT NULL DEFAULT 'random',
                bpm REAL,
                detected_key TEXT,
                melody_note_count INTEGER DEFAULT 0,
                confidence REAL,
                processing_time REAL,
                debug_json TEXT NOT NULL DEFAULT '',
                error TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                FOREIGN KEY(telegram_id) REFERENCES users(telegram_id)
            );
            CREATE TABLE IF NOT EXISTS payment_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                plan_id TEXT NOT NULL,
                amount_toman INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                receipt_file_id TEXT,
                message TEXT NOT NULL DEFAULT '',
                admin_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(telegram_id) REFERENCES users(telegram_id),
                FOREIGN KEY(plan_id) REFERENCES plans(id)
            );
            CREATE TABLE IF NOT EXISTS admin_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            """
        )
        self.conn.commit()
        self.migrate_schema()
        self.seed_defaults()

    def migrate_schema(self) -> None:
        def add_col(table: str, name: str, ddl: str) -> None:
            cols = [r[1] for r in self.conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if name not in cols:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        add_col("users", "selected_style", "selected_style TEXT NOT NULL DEFAULT 'random'")
        for name, ddl in [
            ("variation_mp3_path", "variation_mp3_path TEXT"),
            ("style", "style TEXT NOT NULL DEFAULT 'random'"),
            ("bpm", "bpm REAL"),
            ("detected_key", "detected_key TEXT"),
            ("melody_note_count", "melody_note_count INTEGER DEFAULT 0"),
            ("confidence", "confidence REAL"),
            ("processing_time", "processing_time REAL"),
            ("debug_json", "debug_json TEXT NOT NULL DEFAULT ''"),
        ]:
            add_col("conversions", name, ddl)
        self.conn.commit()

    def seed_defaults(self) -> None:
        defaults = {
            "bot_title": "ربات تبدیل وویس به آهنگ",
            "welcome_text": (
                "سلام! من وویس، آواز یا فایل صوتی کوتاهت رو به MIDI و یک آهنگ MP3 ساده تبدیل می‌کنم.\n\n"
                "برای شروع یک وویس واضح بفرست یا از منوی پایین گزینه تبدیل را بزن."
            ),
            "support_text": "برای پشتیبانی به آیدی @your_support پیام بدهید.",
            "admin_telegram_id": os.getenv("ADMIN_TELEGRAM_ID", ""),
            "admin_username": ADMIN_USERNAME,
            "admin_password_hash": generate_password_hash(ADMIN_PASSWORD),
            "processing_note": "کیفیت خروجی به واضح بودن وویس و تک‌صدایی بودن ملودی بستگی دارد.",
        }
        for k, v in defaults.items():
            if not self.one("SELECT key FROM settings WHERE key=?", [k]):
                self.execute("INSERT INTO settings(key,value) VALUES(?,?)", [k, v])

        plan_rows = [
            (
                "free",
                "رایگان",
                0,
                3,
                30,
                3650,
                "",
                "روزانه ۳ تبدیل کوتاه تا ۳۰ ثانیه؛ مناسب تست اولیه.",
                1,
                0,
            ),
            (
                "mini",
                "مینی",
                290000,
                20,
                60,
                30,
                "https://example.com/pay/mini",
                "۲۰ تبدیل در روز، هر وویس تا ۶۰ ثانیه؛ مناسب تولید محتوای سبک.",
                1,
                1,
            ),
            (
                "pro",
                "پرو",
                690000,
                80,
                120,
                30,
                "https://example.com/pay/pro",
                "۸۰ تبدیل در روز، هر وویس تا ۲ دقیقه؛ مناسب بیت‌سازها.",
                1,
                2,
            ),
            (
                "studio",
                "استودیو",
                1490000,
                250,
                240,
                30,
                "https://example.com/pay/studio",
                "۲۵۰ تبدیل در روز، هر فایل تا ۴ دقیقه؛ مناسب تیم‌ها و استفاده جدی.",
                1,
                3,
            ),
        ]
        for row in plan_rows:
            if not self.one("SELECT id FROM plans WHERE id=?", [row[0]]):
                self.execute(
                    """INSERT INTO plans
                    (id,title,price_toman,daily_limit,max_seconds,valid_days,payment_link,description,is_active,sort_order)
                    VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    row,
                )

    def setting(self, key: str, default: str = "") -> str:
        row = self.one("SELECT value FROM settings WHERE key=?", [key])
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        self.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [key, value],
        )

    def log_admin(self, actor: str, action: str, details: str = "") -> None:
        self.execute(
            "INSERT INTO admin_events(actor,action,details,created_at) VALUES(?,?,?,?)",
            [actor, action, details, now_iso()],
        )

    def ensure_user(self, tg_user: Any) -> sqlite3.Row:
        telegram_id = int(tg_user.id)
        row = self.one("SELECT * FROM users WHERE telegram_id=?", [telegram_id])
        if row:
            self.execute(
                "UPDATE users SET username=?, first_name=?, last_name=?, last_seen_at=? WHERE telegram_id=?",
                [tg_user.username, tg_user.first_name, tg_user.last_name, now_iso(), telegram_id],
            )
        else:
            self.execute(
                """INSERT INTO users(telegram_id,username,first_name,last_name,created_at,last_seen_at,usage_date)
                VALUES(?,?,?,?,?,?,?)""",
                [telegram_id, tg_user.username, tg_user.first_name, tg_user.last_name, now_iso(), now_iso(), today_key()],
            )
        self.reset_daily_if_needed(telegram_id)
        self.downgrade_if_expired(telegram_id)
        return self.one("SELECT * FROM users WHERE telegram_id=?", [telegram_id])  # type: ignore[return-value]

    def reset_daily_if_needed(self, telegram_id: int) -> None:
        row = self.one("SELECT usage_date FROM users WHERE telegram_id=?", [telegram_id])
        if row and row["usage_date"] != today_key():
            self.execute("UPDATE users SET used_today=0, usage_date=? WHERE telegram_id=?", [today_key(), telegram_id])

    def downgrade_if_expired(self, telegram_id: int) -> None:
        row = self.one("SELECT plan_id, plan_expires_at FROM users WHERE telegram_id=?", [telegram_id])
        if not row or row["plan_id"] == "free" or not row["plan_expires_at"]:
            return
        exp = parse_iso(row["plan_expires_at"])
        if exp and exp < now_dt():
            self.execute("UPDATE users SET plan_id='free', plan_expires_at=NULL WHERE telegram_id=?", [telegram_id])

    def get_user_with_plan(self, telegram_id: int) -> Optional[sqlite3.Row]:
        self.reset_daily_if_needed(telegram_id)
        self.downgrade_if_expired(telegram_id)
        return self.one(
            """SELECT u.*, p.title AS plan_title, p.daily_limit, p.max_seconds, p.valid_days, p.price_toman
            FROM users u JOIN plans p ON p.id=u.plan_id WHERE u.telegram_id=?""",
            [telegram_id],
        )

    def get_plans(self, active_only: bool = False) -> List[sqlite3.Row]:
        if active_only:
            return self.all("SELECT * FROM plans WHERE is_active=1 ORDER BY sort_order, price_toman")
        return self.all("SELECT * FROM plans ORDER BY sort_order, price_toman")

    def get_plan(self, plan_id: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM plans WHERE id=?", [plan_id])

    def can_convert(self, telegram_id: int, duration: Optional[int]) -> Tuple[bool, str]:
        row = self.get_user_with_plan(telegram_id)
        if not row:
            return False, "کاربر پیدا نشد. /start را بزنید."
        if row["is_blocked"]:
            return False, "حساب شما توسط ادمین مسدود شده است."
        if duration and duration > int(row["max_seconds"]):
            return False, f"حداکثر طول فایل در پلن شما {row['max_seconds']} ثانیه است. پلن فعلی: {row['plan_title']}"
        if int(row["used_today"]) >= int(row["daily_limit"]):
            return False, f"سهمیه امروز شما تمام شده است. پلن فعلی: {row['plan_title']}"
        return True, ""

    def increment_usage(self, telegram_id: int) -> None:
        self.reset_daily_if_needed(telegram_id)
        self.execute(
            "UPDATE users SET used_today=used_today+1,total_conversions=total_conversions+1,last_seen_at=? WHERE telegram_id=?",
            [now_iso(), telegram_id],
        )

    def set_user_plan(self, telegram_id: int, plan_id: str, days: Optional[int] = None) -> None:
        plan = self.get_plan(plan_id)
        if not plan:
            raise ValueError("Plan not found")
        if plan_id == "free":
            exp = None
        else:
            valid_days = int(days or plan["valid_days"] or 30)
            exp = (now_dt() + timedelta(days=valid_days)).isoformat(timespec="seconds")
        self.execute("UPDATE users SET plan_id=?, plan_expires_at=? WHERE telegram_id=?", [plan_id, exp, telegram_id])


db = DB(DB_PATH)

# -----------------------------------------------------------------------------
# Audio Processing Engine
# -----------------------------------------------------------------------------
BASIC_PITCH_MODEL = None
BASIC_PITCH_LOCK = threading.RLock()
PROCESS_SEMAPHORE = threading.Semaphore(MAX_WORKERS)

PERSIAN_WEAK_MELODY_ERROR = "❌ ملودی واضحی پیدا نکردم. لطفاً ۵ تا ۱۵ ثانیه فقط با صدای خودت زمزمه یا بخون، بدون موزیک پس‌زمینه."

STYLE_PRESETS: Dict[str, Dict[str, Any]] = {
    "lofi": {"fa": "🎧 لوفای", "bpm": (72, 92), "lead": "Electric Piano 1", "pad": "Pad 2 (warm)", "bass": "Acoustic Bass", "fx": "lofi"},
    "trap": {"fa": "🔥 ترپ", "bpm": (130, 155), "lead": "Lead 2 (sawtooth)", "pad": "Pad 8 (sweep)", "bass": "Synth Bass 2", "fx": "trap"},
    "dark_pop": {"fa": "🌙 دارک پاپ", "bpm": (86, 116), "lead": "Lead 1 (square)", "pad": "Pad 4 (choir)", "bass": "Synth Bass 1", "fx": "dark"},
    "electronic": {"fa": "⚡ الکترونیک", "bpm": (118, 132), "lead": "Lead 2 (sawtooth)", "pad": "Pad 3 (polysynth)", "bass": "Synth Bass 1", "fx": "bright"},
    "piano": {"fa": "🎹 پیانو احساسی", "bpm": (64, 88), "lead": "Acoustic Grand Piano", "pad": "String Ensemble 1", "bass": "Cello", "fx": "soft"},
    "random": {"fa": "🎲 سورپرایزم کن", "bpm": (80, 128), "lead": "Lead 2 (sawtooth)", "pad": "Pad 2 (warm)", "bass": "Synth Bass 1", "fx": "balanced"},
}
STYLE_ORDER = ["lofi", "trap", "dark_pop", "electronic", "piano", "random"]
NOTE_NAMES = ["C", "C#", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"]

@dataclass
class MelodyNote:
    pitch: int
    start: float
    end: float
    velocity: int

@dataclass
class MelodyData:
    wav_path: Path
    raw_midi_path: Path
    notes: List[MelodyNote]
    duration: float
    melody_note_count: int
    pitch_range: int
    average_velocity: float
    density: float
    confidence_like_score: float

@dataclass
class AnalysisData:
    root_pc: int
    key_name: str
    mode: str
    bpm: float
    mood_fa: str
    bar_seconds: float
    duration: float
    stretch: float

@dataclass
class ArrangementResult:
    midi_path: Path
    raw_midi_path: Path
    style: str
    analysis: AnalysisData
    melody_data: MelodyData
    lead_note_count: int
    chord_note_count: int
    bass_note_count: int
    drum_note_count: int
    duration: float
    debug: Dict[str, Any]

@dataclass
class RenderResult:
    mp3_path: Path
    wav_path: Path
    variation_mp3_path: Optional[Path] = None

@dataclass
class ProcessResult:
    raw_midi_path: Path
    arranged_midi_path: Path
    mp3_path: Path
    duration_seconds: int
    notes_count: int
    style: str
    style_fa: str
    bpm: float
    key_name: str
    mood_fa: str
    confidence: float
    variation_mp3_path: Optional[Path]
    debug: Dict[str, Any]


def convert_to_wav(input_path: Path, wav_path: Path) -> int:
    wav_path.parent.mkdir(parents=True, exist_ok=True)
    proc = run_cmd(["ffmpeg", "-y", "-i", str(input_path), "-t", str(MAX_AUDIO_SECONDS), "-ac", "1", "-ar", "22050", "-vn", str(wav_path)], timeout=240)
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg convert failed: " + proc.stderr[-1000:])
    return probe_duration_seconds(wav_path)


def probe_duration_seconds(path: Path) -> int:
    proc = run_cmd(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(path)], timeout=30)
    if proc.returncode != 0:
        return 0
    try: return int(math.ceil(float(proc.stdout.strip())))
    except Exception: return 0


def basic_pitch_to_midi(wav_path: Path, midi_path: Path) -> int:
    global BASIC_PITCH_MODEL
    midi_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        from basic_pitch import ICASSP_2022_MODEL_PATH
        from basic_pitch.inference import Model, predict
    except Exception as exc:
        raise RuntimeError("Basic Pitch نصب یا قابل اجرا نیست. requirements.txt را نصب کنید. خطا: " + str(exc)) from exc
    with BASIC_PITCH_LOCK:
        if BASIC_PITCH_MODEL is None:
            logger.info("Loading Basic Pitch model from packaged path")
            BASIC_PITCH_MODEL = Model(ICASSP_2022_MODEL_PATH)
        _model_output, midi_data, note_events = predict(str(wav_path), BASIC_PITCH_MODEL)
    midi_data.write(str(midi_path))
    return len(note_events or [])


def clamp_pitch(pitch: int, lo: int = 24, hi: int = 96) -> int:
    while pitch < lo: pitch += 12
    while pitch > hi: pitch -= 12
    return int(pitch)


def _pm_notes(midi_path: Path) -> List[MelodyNote]:
    import pretty_midi
    pm = pretty_midi.PrettyMIDI(str(midi_path))
    notes = []
    for inst in pm.instruments:
        if not inst.is_drum:
            for n in inst.notes:
                if n.end > n.start:
                    notes.append(MelodyNote(int(n.pitch), float(n.start), float(n.end), int(n.velocity)))
    return sorted(notes, key=lambda n: (n.start, n.pitch))


def clean_melody_notes(notes: List[MelodyNote], duration: float) -> List[MelodyNote]:
    if not notes: return []
    min_len = 0.07 if duration < 12 else 0.09
    notes = [n for n in notes if (n.end - n.start) >= min_len and 35 <= n.pitch <= 92 and n.velocity >= 8]
    notes.sort(key=lambda n: (n.start, -n.velocity))
    mono: List[MelodyNote] = []
    for n in notes:
        if mono and n.start < mono[-1].end - 0.025:
            if n.velocity > mono[-1].velocity or (n.end - n.start) > (mono[-1].end - mono[-1].start) * 1.3:
                mono[-1] = n
            continue
        mono.append(n)
    merged: List[MelodyNote] = []
    for n in mono:
        if merged and abs(n.pitch - merged[-1].pitch) <= 1 and n.start - merged[-1].end <= 0.12:
            prev = merged[-1]
            prev.end = max(prev.end, n.end)
            prev.velocity = int((prev.velocity + n.velocity) / 2)
        else:
            merged.append(MelodyNote(n.pitch, n.start, n.end, n.velocity))
    for i in range(1, len(merged)):
        diff = merged[i].pitch - merged[i-1].pitch
        if abs(diff) >= 12 and abs(diff) % 12 <= 2:
            merged[i].pitch -= 12 * round(diff / 12)
            merged[i].pitch = clamp_pitch(merged[i].pitch, 40, 84)
    return merged


def extract_melody(input_wav: Path) -> MelodyData:
    raw_midi = input_wav.parent / "melody_raw.mid"
    basic_pitch_to_midi(input_wav, raw_midi)
    duration = max(probe_duration_seconds(input_wav), 1)
    notes = clean_melody_notes(_pm_notes(raw_midi), duration)
    if not notes:
        raise ValueError(PERSIAN_WEAK_MELODY_ERROR)
    pitch_range = max(n.pitch for n in notes) - min(n.pitch for n in notes)
    avg_vel = float(np.mean([n.velocity for n in notes]))
    density = len(notes) / max(duration, 1.0)
    coverage = sum(n.end - n.start for n in notes) / max(duration, 1.0)
    confidence = max(0.0, min(1.0, (len(notes) / 18) * 0.35 + min(pitch_range / 12, 1) * 0.25 + min(coverage, 0.7) * 0.4))
    if len(notes) < MIN_MELODY_NOTES or pitch_range < 3 or density > 9 or coverage < 0.08 or confidence < 0.22:
        raise ValueError(PERSIAN_WEAK_MELODY_ERROR)
    return MelodyData(input_wav, raw_midi, notes, float(duration), len(notes), pitch_range, avg_vel, density, confidence)


def analyze_melody(melody_data: MelodyData, style: str = "random") -> AnalysisData:
    pcs = np.zeros(12)
    for n in melody_data.notes:
        pcs[n.pitch % 12] += (n.end - n.start) * max(n.velocity, 1)
    root = int(np.argmax(pcs))
    major_score = pcs[(root+4)%12] + .6*pcs[(root+7)%12] + .35*pcs[(root+11)%12]
    minor_score = pcs[(root+3)%12] + .6*pcs[(root+7)%12] + .35*pcs[(root+10)%12]
    mode = "minor" if minor_score > major_score else "major"
    preset = STYLE_PRESETS.get(style, STYLE_PRESETS["random"])
    lo, hi = preset["bpm"]
    gaps = [melody_data.notes[i+1].start - melody_data.notes[i].start for i in range(len(melody_data.notes)-1) if 0.12 <= melody_data.notes[i+1].start - melody_data.notes[i].start <= 2.0]
    base = 60.0 / (float(np.median(gaps)) if gaps else 0.55)
    while base < lo: base *= 2
    while base > hi: base /= 2
    bpm = float(max(lo, min(hi, base)))
    mood = "مینور / احساسی" if mode == "minor" else "ماژور / روشن"
    return AnalysisData(root, f"{NOTE_NAMES[root]} {'minor' if mode=='minor' else 'major'}", mode, bpm, mood, 240.0/bpm, melody_data.duration, 1.0)


def _program(name: str) -> int:
    import pretty_midi
    try: return pretty_midi.instrument_name_to_program(name)
    except Exception: return 0


def _chord_for_phrase(notes: List[MelodyNote], analysis: AnalysisData) -> Tuple[int, List[int]]:
    if not notes:
        root = analysis.root_pc
    else:
        weights = np.zeros(12)
        for n in notes: weights[n.pitch % 12] += n.end - n.start
        root = int(np.argmax(weights))
    pcs = {n.pitch % 12 for n in notes}
    minor = ((root+3)%12 in pcs) or (analysis.mode == "minor" and (root+4)%12 not in pcs)
    third = 3 if minor else 4
    tones = [root, (root+third)%12, (root+7)%12]
    if (root+2)%12 in pcs: tones.append((root+14)%12)  # add9
    return root, tones


def arrange_song(melody_data: MelodyData, analysis_data: AnalysisData, style: str = "random", out_path: Optional[Path] = None) -> ArrangementResult:
    import pretty_midi
    if style == "random":
        style = random.choice([s for s in STYLE_ORDER if s != "random"])
    preset = STYLE_PRESETS.get(style, STYLE_PRESETS["lofi"])
    out_path = out_path or (melody_data.wav_path.parent / "song_arranged.mid")
    pm = pretty_midi.PrettyMIDI(initial_tempo=analysis_data.bpm)
    lead = pretty_midi.Instrument(program=_program(preset["lead"]), name="lead_cleaned_user_melody")
    harmony = pretty_midi.Instrument(program=_program(preset["pad"]), name="soft_melody_aware_chords")
    bass = pretty_midi.Instrument(program=_program(preset["bass"]), name="bass_from_chord_roots")
    drums = pretty_midi.Instrument(program=0, is_drum=True, name="style_drums")
    bar = analysis_data.bar_seconds
    target_min = 12.0 if melody_data.duration < 10 else melody_data.duration
    loops = max(1, int(math.ceil(target_min / max(melody_data.duration, 1))))
    duration = min(max(melody_data.duration * loops, target_min), float(MAX_AUDIO_SECONDS) + 4.0)
    # lead preserves original starts/lengths, repeated if needed
    for loop in range(loops):
        offset = loop * melody_data.duration
        if offset >= duration: break
        for n in melody_data.notes:
            st, en = n.start + offset, min(n.end + offset, duration)
            if st >= duration: continue
            lead.notes.append(pretty_midi.Note(velocity=int(min(118, max(50, n.velocity+10))), pitch=clamp_pitch(n.pitch, 48, 88), start=st, end=max(st+.06, en)))
    phrase = bar * 2
    prev_mid = 60
    t = 0.0
    roots: List[int] = []
    while t < duration:
        segment = [n for n in melody_data.notes if t % melody_data.duration <= n.start < min((t % melody_data.duration)+phrase, melody_data.duration)]
        root_pc, tones = _chord_for_phrase(segment, analysis_data)
        roots.append(root_pc)
        chord_pitches = []
        for pc in tones[:4]:
            p = 48 + pc
            while p - prev_mid > 6: p -= 12
            while prev_mid - p > 8: p += 12
            chord_pitches.append(clamp_pitch(p, 45, 74))
        prev_mid = int(np.mean(chord_pitches)) if chord_pitches else prev_mid
        for pch in chord_pitches:
            harmony.notes.append(pretty_midi.Note(velocity=34 if style!='piano' else 48, pitch=pch, start=t, end=min(t+phrase*.92, duration)))
        br = clamp_pitch(36 + root_pc, 28, 48)
        if style == "trap":
            bass.notes.append(pretty_midi.Note(velocity=92, pitch=clamp_pitch(br-12, 24, 42), start=t, end=min(t+phrase*.85, duration)))
        elif style == "electronic":
            step = bar/4
            x=t
            while x < min(t+phrase, duration):
                bass.notes.append(pretty_midi.Note(velocity=76, pitch=br, start=x, end=min(x+step*.65, duration))); x += step
        else:
            bass.notes.append(pretty_midi.Note(velocity=66, pitch=br, start=t, end=min(t+bar*.9, duration)))
            if t+bar < duration: bass.notes.append(pretty_midi.Note(velocity=58, pitch=clamp_pitch(br+7,28,52), start=t+bar, end=min(t+phrase*.82,duration)))
        t += phrase
    if style != "piano":
        step = bar/4
        i = 0; t = 0.0
        while t < duration:
            beat = i % 16
            if style == "electronic":
                if beat % 4 == 0: drums.notes.append(pretty_midi.Note(velocity=100, pitch=36, start=t, end=t+.08))
                if beat % 4 == 2: drums.notes.append(pretty_midi.Note(velocity=54, pitch=42, start=t, end=t+.04))
            elif style == "trap":
                if beat in (0, 6, 10): drums.notes.append(pretty_midi.Note(velocity=102, pitch=36, start=t, end=t+.08))
                if beat in (4, 12): drums.notes.append(pretty_midi.Note(velocity=92, pitch=39, start=t, end=t+.08))
                drums.notes.append(pretty_midi.Note(velocity=42 + (beat%3)*8, pitch=42, start=t, end=t+.035))
                if beat in (7, 15):
                    for r in range(3): drums.notes.append(pretty_midi.Note(velocity=34, pitch=42, start=t+r*step/3, end=t+r*step/3+.025))
            else:
                if beat in (0, 8): drums.notes.append(pretty_midi.Note(velocity=78 if style=='lofi' else 94, pitch=36, start=t, end=t+.08))
                if beat in (4, 12): drums.notes.append(pretty_midi.Note(velocity=70 if style=='lofi' else 90, pitch=38, start=t, end=t+.08))
                if beat % 2 == 0: drums.notes.append(pretty_midi.Note(velocity=36 if style=='lofi' else 48, pitch=42, start=t+(0.02 if style=='lofi' else 0), end=t+.04))
            t += step; i += 1
    else:
        # very light cymbal swells only as MIDI hats
        for t in np.arange(bar, duration, bar*2): drums.notes.append(pretty_midi.Note(velocity=25, pitch=49, start=float(t), end=float(t)+.2))
    pm.instruments.extend([lead, harmony, bass, drums])
    pm.write(str(out_path))
    debug = {"style": style, "bpm": analysis_data.bpm, "key": analysis_data.key_name, "roots": roots, "lead_notes": len(lead.notes), "chord_notes": len(harmony.notes), "bass_notes": len(bass.notes), "drum_notes": len(drums.notes)}
    if len(lead.notes) < int(len(melody_data.notes) * 0.85):
        raise RuntimeError("quality guard: arranged lead lost too many detected melody notes")
    return ArrangementResult(out_path, melody_data.raw_midi_path, style, analysis_data, melody_data, len(lead.notes), len(harmony.notes), len(bass.notes), len(drums.notes), duration, debug)


def _soundfont() -> Optional[str]:
    candidates = [SOUNDFONT_PATH, "/usr/share/sounds/sf2/FluidR3_GM.sf2", "/usr/share/soundfonts/FluidR3_GM.sf2"]
    return next((c for c in candidates if c and Path(c).exists()), None)


def render_song(arrangement_result: ArrangementResult, mp3_path: Path, variation: bool = False) -> RenderResult:
    wav_path = mp3_path.with_suffix(".wav")
    sf = _soundfont()
    if sf:
        proc = run_cmd(["fluidsynth", "-ni", sf, str(arrangement_result.midi_path), "-F", str(wav_path), "-r", "44100"], timeout=240)
        if proc.returncode != 0:
            logger.warning("FluidSynth failed, falling back to pretty_midi synth: %s", proc.stderr[-500:])
            synthesize_midi_to_mp3(arrangement_result.midi_path, wav_path, mp3_path)
            return RenderResult(mp3_path, wav_path)
    else:
        synthesize_midi_to_mp3(arrangement_result.midi_path, wav_path, mp3_path)
        return RenderResult(mp3_path, wav_path)
    fx = STYLE_PRESETS.get(arrangement_result.style, STYLE_PRESETS["random"])["fx"]
    af = "highpass=f=35,lowpass=f=17500,acompressor=threshold=-18dB:ratio=2.5:attack=20:release=180,dynaudnorm=f=150:g=12,alimiter=limit=0.92,afade=t=in:st=0:d=0.04"
    if fx in {"lofi", "dark", "soft"}: af += ",aecho=0.6:0.25:45:0.12"
    if variation: af += ",asetrate=44100*1.003,aresample=44100"
    proc = run_cmd(["ffmpeg", "-y", "-i", str(wav_path), "-af", af, "-codec:a", "libmp3lame", "-b:a", "256k" if RENDER_QUALITY == "high" else "192k", str(mp3_path)], timeout=240)
    if proc.returncode != 0: raise RuntimeError("ffmpeg mastering failed: " + proc.stderr[-1000:])
    return RenderResult(mp3_path, wav_path)

# fallback internal synth kept for machines without a SoundFont
def adsr_envelope(length: int, sr: int, attack: float = 0.01, release: float = 0.08) -> np.ndarray:
    env = np.ones(length, dtype=np.float32); a=min(length,int(sr*attack)); r=min(length,int(sr*release))
    if a>1: env[:a]=np.linspace(0,1,a)
    if r>1: env[-r:]*=np.linspace(1,0,r)
    return env

def add_tone(audio: np.ndarray, sr: int, start: float, end: float, pitch: int, velocity: int, kind: str = "lead") -> None:
    s=max(0,int(start*sr)); e=min(len(audio),int(end*sr))
    if e<=s: return
    t=np.arange(e-s,dtype=np.float32)/sr; freq=440.0*(2.0**((pitch-69)/12.0)); amp=(velocity/127.0)*0.14
    wave=np.sin(2*np.pi*freq*t)
    if kind=="bass": wave += .35*np.sin(2*np.pi*freq*2*t); amp*=.8
    elif kind=="pad": wave += .22*np.sin(2*np.pi*(freq*1.005)*t); amp*=.28
    else: wave += .25*np.sin(2*np.pi*2*freq*t)+.08*np.sin(2*np.pi*3*freq*t)
    audio[s:e]+=(wave*adsr_envelope(e-s,sr,.015 if kind!='pad' else .08,.06 if kind!='pad' else .18)*amp).astype(np.float32)

def add_kick(audio: np.ndarray, sr: int, start: float, velocity: int) -> None:
    s=int(start*sr); e=min(len(audio),s+int(.18*sr))
    if e<=s: return
    t=np.arange(e-s,dtype=np.float32)/sr; freq=90*np.exp(-18*t)+38; phase=2*np.pi*np.cumsum(freq)/sr
    audio[s:e]+=np.sin(phase)*np.exp(-12*t)*(velocity/127.0)*.7

def add_snare(audio: np.ndarray, sr: int, start: float, velocity: int) -> None:
    s=int(start*sr); e=min(len(audio),s+int(.16*sr))
    if e<=s: return
    rng=np.random.default_rng(1234+s); t=np.arange(e-s,dtype=np.float32)/sr
    audio[s:e]+=(rng.normal(0,1,e-s).astype(np.float32)*.28+np.sin(2*np.pi*190*t)*.25)*np.exp(-18*t)*(velocity/127.0)

def add_hat(audio: np.ndarray, sr: int, start: float, velocity: int) -> None:
    s=int(start*sr); e=min(len(audio),s+int(.055*sr))
    if e<=s: return
    rng=np.random.default_rng(4321+s); t=np.arange(e-s,dtype=np.float32)/sr
    audio[s:e]+=rng.normal(0,1,e-s).astype(np.float32)*np.exp(-60*t)*(velocity/127.0)*.16

def synthesize_midi_to_mp3(midi_path: Path, wav_path: Path, mp3_path: Path) -> None:
    import pretty_midi
    pm=pretty_midi.PrettyMIDI(str(midi_path)); sr=44100; duration=max(pm.get_end_time()+1.0,4.0); audio=np.zeros(int(duration*sr),dtype=np.float32)
    for inst in pm.instruments:
        if inst.is_drum:
            for n in inst.notes:
                (add_kick if n.pitch==36 else add_snare if n.pitch in (38,39,40) else add_hat)(audio,sr,n.start,n.velocity)
        else:
            name=(inst.name or '').lower(); kind='bass' if 'bass' in name else 'pad' if 'chord' in name or 'pad' in name else 'lead'
            for n in inst.notes: add_tone(audio,sr,n.start,n.end,n.pitch,n.velocity,kind)
    peak=float(np.max(np.abs(audio))) if len(audio) else 0.0
    if peak>0: audio=audio/max(peak,1.0)*.92 if peak>1 else audio*.92
    wavfile.write(str(wav_path),sr,np.int16(np.clip(audio,-1,1)*32767))
    proc=run_cmd(["ffmpeg","-y","-i",str(wav_path),"-af","highpass=f=35,acompressor,dynaudnorm,alimiter=limit=0.92","-codec:a","libmp3lame","-b:a","192k",str(mp3_path)],timeout=240)
    if proc.returncode!=0: raise RuntimeError("ffmpeg mp3 failed: "+proc.stderr[-1000:])


def process_audio_to_song(input_path: Path, conversion_id: int, style: str = "random") -> ProcessResult:
    if RAW_VOCAL_MIX:
        logger.warning("RAW_VOCAL_MIX is true but raw vocal mixing is intentionally not implemented/enabled in this pipeline")
    started = time.monotonic()
    with PROCESS_SEMAPHORE:
        outdir = OUTPUTS_DIR / str(conversion_id); outdir.mkdir(parents=True, exist_ok=True)
        wav_path = outdir / "input.wav"; arranged_midi_path = outdir / "song_arranged.mid"; mp3_path = outdir / "song.mp3"
        duration = convert_to_wav(input_path, wav_path)
        melody = extract_melody(wav_path)
        selected = style if style in STYLE_PRESETS else DEFAULT_STYLE
        actual_style = selected if selected != "random" else random.choice([s for s in STYLE_ORDER if s != "random"])
        analysis = analyze_melody(melody, actual_style)
        arrangement = arrange_song(melody, analysis, actual_style, arranged_midi_path)
        render = render_song(arrangement, mp3_path)
        variation_path = None
        if SEND_VARIATION and actual_style != "piano":
            variation_path = outdir / "song_variation.mp3"
            render_song(arrangement, variation_path, variation=True)
        elapsed = time.monotonic() - started
        debug = {**arrangement.debug, "duration": duration, "melody_note_count": melody.melody_note_count, "pitch_range": melody.pitch_range, "density": melody.density, "confidence": melody.confidence_like_score, "processing_time": round(elapsed,2)}
        logger.info("conversion_metrics id=%s %s", conversion_id, json.dumps(debug, ensure_ascii=False))
        return ProcessResult(melody.raw_midi_path, arrangement.midi_path, render.mp3_path, duration, melody.melody_note_count, actual_style, STYLE_PRESETS[actual_style]["fa"], analysis.bpm, analysis.key_name, analysis.mood_fa, melody.confidence_like_score, variation_path, debug)

# -----------------------------------------------------------------------------
# Telegram Bot
# -----------------------------------------------------------------------------
BTN_CONVERT = "🎙 ساخت آهنگ با وویس"
BTN_STYLE = "🎛 انتخاب سبک"
BTN_PLANS = "💳 خرید اشتراک"
BTN_STATUS = "👤 حساب من"
BTN_HELP = "ℹ️ راهنما"
BTN_SUPPORT = "☎️ پشتیبانی"


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[BTN_CONVERT, BTN_STYLE], [BTN_PLANS, BTN_STATUS], [BTN_HELP, BTN_SUPPORT]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="وویس یا فایل صوتی بفرست…",
    )


def style_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(STYLE_PRESETS[key]["fa"], callback_data=f"style:{key}")] for key in STYLE_ORDER]
    return InlineKeyboardMarkup(rows)


async def show_style_message(update: Update) -> None:
    row = db.one("SELECT selected_style FROM users WHERE telegram_id=?", [update.effective_user.id])
    current = (row["selected_style"] if row else DEFAULT_STYLE) or "random"
    current_fa = STYLE_PRESETS.get(current, STYLE_PRESETS["random"])["fa"]
    await update.message.reply_text(
        f"🎛 انتخاب سبک تنظیم آهنگ\n\nسبک فعلی شما: {current_fa}\nیکی از سبک‌ها را انتخاب کن؛ اگر انتخاب نکنی، «سورپرایزم کن» استفاده می‌شود.",
        reply_markup=style_keyboard(),
    )


def plan_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for p in db.get_plans(active_only=True):
        if p["id"] == "free":
            continue
        rows.append([InlineKeyboardButton(f"{p['title']} — {fmt_num(p['price_toman'])} تومان", callback_data=f"buy:{p['id']}")])
    rows.append([InlineKeyboardButton("🔄 بروزرسانی وضعیت من", callback_data="status")])
    return InlineKeyboardMarkup(rows)


def status_text(telegram_id: int) -> str:
    u = db.get_user_with_plan(telegram_id)
    if not u:
        return "کاربر پیدا نشد."
    exp = fmt_dt(u["plan_expires_at"]) if u["plan_expires_at"] else "بدون تاریخ انقضا"
    return (
        f"📊 وضعیت حساب شما\n\n"
        f"پلن فعلی: {u['plan_title']}\n"
        f"استفاده امروز: {u['used_today']} از {u['daily_limit']}\n"
        f"حداکثر طول هر فایل: {u['max_seconds']} ثانیه\n"
        f"تعداد کل تبدیل‌ها: {u['total_conversions']}\n"
        f"سبک انتخابی: {STYLE_PRESETS.get(u['selected_style'] or 'random', STYLE_PRESETS['random'])['fa']}\n"
        f"انقضای پلن: {exp}"
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = db.ensure_user(update.effective_user)
    text = db.setting("welcome_text")
    await update.message.reply_text(text, reply_markup=main_keyboard())


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_id = db.setting("admin_telegram_id")
    if admin_id and str(update.effective_user.id) == str(admin_id):
        base = PUBLIC_BASE_URL or f"http://{WEB_HOST}:{WEB_PORT}"
        await update.message.reply_text(f"پنل ادمین: {base}/admin")
    else:
        await update.message.reply_text("این دستور فقط برای ادمین فعال است.")


async def show_help(update: Update) -> None:
    msg = (
        "❓ راهنما\n\n"
        "1) یک وویس واضح، آواز کوتاه، زمزمه ملودی یا فایل صوتی بفرست.\n"
        "2) ربات صدای تو را به MIDI تبدیل می‌کند.\n"
        "3) روی MIDI یک تنظیم ساده شامل لید، بیس، پد و درام ساخته می‌شود.\n"
        "4) خروجی MP3 و فایل MIDI برایت ارسال می‌شود.\n\n"
        "نکته: بهترین نتیجه وقتی است که ملودی تک‌صدایی، بدون نویز و با ریتم مشخص باشد."
    )
    await update.message.reply_text(msg, reply_markup=main_keyboard())


async def show_plans_message(update: Update) -> None:
    txt = "💳 پلن‌ها\n\n"
    for p in db.get_plans(active_only=True):
        txt += f"▪️ {p['title']}: {fmt_num(p['price_toman'])} تومان\n{p['description']}\n\n"
    txt += "برای خرید، پلن مورد نظر را انتخاب کن. بعد از پرداخت، رسید را همین‌جا بفرست تا ادمین فعال کند."
    await update.message.reply_text(txt, reply_markup=plan_keyboard())


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db.ensure_user(update.effective_user)
    text = (update.message.text or "").strip()
    menu_texts = {BTN_CONVERT, BTN_STYLE, BTN_PLANS, BTN_STATUS, BTN_HELP, BTN_SUPPORT}
    if text not in menu_texts and await handle_receipt(update, context):
        return
    if text == BTN_CONVERT:
        await update.message.reply_text(
            "🎙 عالی! حالا ۵ تا ۱۵ ثانیه فقط با صدای خودت زمزمه یا بخون؛ بدون موزیک پس‌زمینه و تا حد ممکن واضح.",
            reply_markup=main_keyboard(),
        )
    elif text == BTN_STYLE:
        await show_style_message(update)
    elif text == BTN_PLANS:
        await show_plans_message(update)
    elif text == BTN_STATUS:
        await update.message.reply_text(status_text(update.effective_user.id), reply_markup=main_keyboard())
    elif text == BTN_HELP:
        await show_help(update)
    elif text == BTN_SUPPORT:
        await update.message.reply_text(db.setting("support_text"), reply_markup=main_keyboard())
    else:
        await update.message.reply_text(
            "متوجه نشدم. از دکمه‌های منو استفاده کن یا یک وویس/فایل صوتی بفرست.",
            reply_markup=main_keyboard(),
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    db.ensure_user(q.from_user)
    data = q.data or ""
    if data == "status":
        await q.edit_message_text(status_text(q.from_user.id))
        return
    if data.startswith("style:"):
        style = data.split(":", 1)[1]
        if style not in STYLE_PRESETS:
            await q.edit_message_text("سبک پیدا نشد.")
            return
        db.execute("UPDATE users SET selected_style=? WHERE telegram_id=?", [style, q.from_user.id])
        await q.edit_message_text(f"✅ سبک شما روی {STYLE_PRESETS[style]['fa']} تنظیم شد. حالا یک وویس واضح بفرست.")
        return
    if data.startswith("buy:"):
        plan_id = data.split(":", 1)[1]
        plan = db.get_plan(plan_id)
        if not plan:
            await q.edit_message_text("پلن پیدا نشد.")
            return
        db.execute("UPDATE users SET pending_plan_id=? WHERE telegram_id=?", [plan_id, q.from_user.id])
        pay = plan["payment_link"] or "لینک پرداخت هنوز توسط ادمین تنظیم نشده است."
        text = (
            f"💳 خرید پلن {plan['title']}\n\n"
            f"قیمت: {fmt_num(plan['price_toman'])} تومان\n"
            f"اعتبار: {plan['valid_days']} روز\n"
            f"سهمیه روزانه: {plan['daily_limit']} تبدیل\n"
            f"حداکثر طول هر فایل: {plan['max_seconds']} ثانیه\n\n"
            f"لینک پرداخت:\n{pay}\n\n"
            "بعد از پرداخت، عکس رسید یا شماره پیگیری را همینجا ارسال کن."
        )
        await q.edit_message_text(text)
        return


async def handle_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True if message was treated as a payment receipt."""
    tg_user = update.effective_user
    u = db.ensure_user(tg_user)
    pending = u["pending_plan_id"]
    if not pending:
        return False

    file_id = ""
    msg_text = update.message.caption or update.message.text or ""
    if update.message.photo:
        file_id = update.message.photo[-1].file_id
    elif update.message.document and not (update.message.document.mime_type or "").startswith("audio"):
        file_id = update.message.document.file_id
    elif update.message.text:
        # text tracking code is accepted as receipt too.
        file_id = "text-only"
    else:
        return False

    plan = db.get_plan(pending)
    amount = int(plan["price_toman"]) if plan else 0
    db.execute(
        """INSERT INTO payment_receipts(telegram_id,plan_id,amount_toman,status,receipt_file_id,message,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?)""",
        [tg_user.id, pending, amount, "pending", file_id, msg_text, now_iso(), now_iso()],
    )
    db.execute("UPDATE users SET pending_plan_id=NULL WHERE telegram_id=?", [tg_user.id])
    await update.message.reply_text("✅ رسید شما ثبت شد و منتظر تایید ادمین است.", reply_markup=main_keyboard())

    admin_id = db.setting("admin_telegram_id")
    if admin_id:
        send_telegram_message(
            admin_id,
            f"رسید جدید برای تایید\nکاربر: {tg_user.id} @{tg_user.username or '-'}\nپلن: {pending}\nمبلغ: {fmt_num(amount)} تومان\nپنل: {(PUBLIC_BASE_URL or '')}/admin/payments",
        )
    return True


def message_audio_meta(update: Update) -> Tuple[Optional[str], str, Optional[int], str]:
    """Return file_id, kind, duration, extension."""
    m = update.message
    if m.voice:
        return m.voice.file_id, "voice", m.voice.duration, ".ogg"
    if m.audio:
        name = safe_filename(m.audio.file_name or "audio.mp3")
        ext = Path(name).suffix or ".mp3"
        return m.audio.file_id, "audio", m.audio.duration, ext
    if m.document and (m.document.mime_type or "").startswith("audio"):
        name = safe_filename(m.document.file_name or "audio")
        ext = Path(name).suffix or ".bin"
        return m.document.file_id, "document-audio", None, ext
    return None, "", None, ""


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await handle_receipt(update, context):
        return
    tg_user = update.effective_user
    db.ensure_user(tg_user)
    file_id, kind, duration, ext = message_audio_meta(update)
    if not file_id:
        return

    ok, reason = db.can_convert(tg_user.id, duration)
    if not ok:
        await update.message.reply_text("⚠️ " + reason + "\nبرای افزایش سقف، بخش خرید اشتراک را ببین.", reply_markup=main_keyboard())
        return

    selected_style = (db.one("SELECT selected_style FROM users WHERE telegram_id=?", [tg_user.id])["selected_style"] or DEFAULT_STYLE)
    conv_id = db.execute(
        """INSERT INTO conversions(telegram_id,input_kind,status,duration_seconds,style,created_at)
        VALUES(?,?,?,?,?,?)""",
        [tg_user.id, kind, "queued", duration or 0, selected_style, now_iso()],
    ).lastrowid
    input_path = UPLOADS_DIR / f"{conv_id}{ext}"
    db.execute("UPDATE conversions SET input_path=? WHERE id=?", [str(input_path), conv_id])

    await update.message.reply_text(
        "⏳ وویس دریافت شد. دارم ملودی رو درمیارم، تنظیم می‌کنم و خروجی حرفه‌ای می‌سازم…",
        reply_markup=main_keyboard(),
    )

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
        tg_file = await context.bot.get_file(file_id)
        await tg_file.download_to_drive(custom_path=str(input_path))

        actual_duration = duration or await asyncio.to_thread(probe_duration_seconds, input_path)
        ok, reason = db.can_convert(tg_user.id, actual_duration)
        if not ok:
            db.execute("UPDATE conversions SET status='failed', error=?, completed_at=?, duration_seconds=? WHERE id=?", [reason, now_iso(), actual_duration or 0, conv_id])
            await update.message.reply_text("⚠️ " + reason + "\nبرای افزایش سقف، بخش خرید اشتراک را ببین.", reply_markup=main_keyboard())
            return

        db.execute("UPDATE conversions SET status='processing', started_at=?, duration_seconds=? WHERE id=?", [now_iso(), actual_duration or 0, conv_id])

        result: ProcessResult = await asyncio.to_thread(process_audio_to_song, input_path, conv_id, selected_style)
        db.execute(
            """UPDATE conversions SET status='done', completed_at=?, wav_path=?, raw_midi_path=?, arranged_midi_path=?, mp3_path=?, variation_mp3_path=?, duration_seconds=?, style=?, bpm=?, detected_key=?, melody_note_count=?, confidence=?, processing_time=?, debug_json=? WHERE id=?""",
            [
                now_iso(),
                str(OUTPUTS_DIR / str(conv_id) / "input.wav"),
                str(result.raw_midi_path),
                str(result.arranged_midi_path),
                str(result.mp3_path),
                str(result.variation_mp3_path) if result.variation_mp3_path else None,
                result.duration_seconds, result.style, result.bpm, result.key_name, result.notes_count, result.confidence, result.debug.get("processing_time"), json.dumps(result.debug, ensure_ascii=False), conv_id,
            ],
        )
        db.increment_usage(tg_user.id)

        caption = (
            "✅ آهنگت آماده شد!\n"
            f"سبک: {result.style_fa}\n"
            f"گام/حال‌وهوا: {result.key_name} — {result.mood_fa}\n"
            f"تمپو: {result.bpm:.0f} BPM\n"
            f"زمان فایل: {result.duration_seconds} ثانیه\n"
            f"نت‌های ملودی: {result.notes_count}\n\n"
            "می‌تونی یک وویس دیگه بفرستی یا از منو سبک رو عوض کنی."
        )
        await update.message.reply_audio(audio=open(result.mp3_path, "rb"), filename="voice2song.mp3", caption=caption)
        await update.message.reply_document(document=open(result.arranged_midi_path, "rb"), filename="voice2song_arranged.mid")
        if result.variation_mp3_path and result.variation_mp3_path.exists():
            await update.message.reply_audio(audio=open(result.variation_mp3_path, "rb"), filename="voice2song_version2.mp3", caption="🎧 نسخه دوم با رنگ صدایی کمی متفاوت")
    except Exception as exc:
        logger.exception("Conversion failed id=%s", conv_id)
        db.execute(
            "UPDATE conversions SET status='failed', error=?, completed_at=? WHERE id=?",
            [str(exc)[:1800], now_iso(), conv_id],
        )
        await update.message.reply_text(
            "❌ تبدیل انجام نشد. لطفاً یک وویس واضح‌تر، کوتاه‌تر و تک‌ملودی بفرست.\n"
            f"کد پیگیری: {conv_id}",
            reply_markup=main_keyboard(),
        )
        admin_id = db.setting("admin_telegram_id")
        if admin_id:
            send_telegram_message(admin_id, f"خطای تبدیل #{conv_id}\nکاربر: {tg_user.id}\n{str(exc)[:1000]}")


async def handle_any(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await handle_receipt(update, context):
        return
    await update.message.reply_text("برای شروع یک وویس یا فایل صوتی بفرست، یا از منو استفاده کن.", reply_markup=main_keyboard())


def build_bot_app() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(False).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("admin", cmd_admin))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(MessageHandler(filters.VOICE | filters.AUDIO | filters.Document.AUDIO, handle_audio))
    application.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, handle_any))
    return application

# -----------------------------------------------------------------------------
# Admin Web Panel
# -----------------------------------------------------------------------------
flask_app = Flask(__name__)
flask_app.secret_key = SECRET_KEY

BASE_HTML = r"""
<!doctype html>
<html lang="fa" dir="rtl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }} | {{ app_name }}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.rtl.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
  <style>
    :root{--bg:#0f172a;--card:#111827;--soft:#1f2937;--text:#e5e7eb;--muted:#9ca3af;--brand:#8b5cf6;--brand2:#06b6d4;}
    body{font-family:Tahoma,Arial,sans-serif;background:#f4f7fb;color:#172033;}
    .sidebar{background:linear-gradient(180deg,#111827,#1e1b4b);min-height:100vh;color:white;position:sticky;top:0;}
    .brand{font-weight:900;font-size:1.2rem;letter-spacing:-.5px;}
    .nav-link{color:#d1d5db;border-radius:14px;margin:.18rem 0;padding:.75rem 1rem;}
    .nav-link:hover,.nav-link.active{background:rgba(255,255,255,.12);color:white;}
    .stat-card{border:0;border-radius:22px;box-shadow:0 10px 30px rgba(15,23,42,.08);overflow:hidden;}
    .stat-card .icon{width:48px;height:48px;border-radius:16px;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,var(--brand),var(--brand2));color:white;font-size:1.35rem;}
    .card{border:0;border-radius:20px;box-shadow:0 10px 28px rgba(15,23,42,.07);}
    .table{vertical-align:middle;}
    .badge-soft{background:#eef2ff;color:#4338ca;}
    .btn{border-radius:12px;}
    .form-control,.form-select{border-radius:12px;}
    .topbar{backdrop-filter:blur(8px);background:rgba(255,255,255,.85);position:sticky;top:0;z-index:5;border-bottom:1px solid #e5e7eb;}
    .login-wrap{min-height:100vh;background:radial-gradient(circle at top right,#8b5cf6 0,#0f172a 45%,#020617 100%);}
    .login-card{max-width:430px;border-radius:28px;background:rgba(255,255,255,.92);box-shadow:0 25px 60px rgba(0,0,0,.25);}
    @media(max-width:991px){.sidebar{min-height:auto;position:relative}.content{padding:1rem!important}.hide-mobile{display:none}}
  </style>
</head>
<body>
{% if login_page %}
{{ content|safe }}
{% else %}
<div class="container-fluid">
  <div class="row">
    <aside class="col-lg-2 p-3 sidebar">
      <div class="d-flex align-items-center gap-2 mb-4">
        <div class="icon rounded-4 p-2" style="background:linear-gradient(135deg,#8b5cf6,#06b6d4)"><i class="bi bi-music-note-beamed fs-4"></i></div>
        <div class="brand">{{ app_name }}</div>
      </div>
      <nav class="nav flex-column">
        {% for key, label, icon, href in nav %}
        <a class="nav-link {% if active==key %}active{% endif %}" href="{{ href }}"><i class="bi {{ icon }} ms-2"></i>{{ label }}</a>
        {% endfor %}
        <a class="nav-link" href="{{ url_for('admin_logout') }}"><i class="bi bi-box-arrow-right ms-2"></i>خروج</a>
      </nav>
    </aside>
    <main class="col-lg-10 p-0">
      <div class="topbar px-4 py-3 d-flex justify-content-between align-items-center">
        <div>
          <h1 class="h4 m-0 fw-bold">{{ title }}</h1>
          <small class="text-muted">{{ now }}</small>
        </div>
        <div class="hide-mobile"><span class="badge rounded-pill text-bg-light p-2">ادمین: {{ session.get('admin_user') }}</span></div>
      </div>
      <div class="content p-4">
        {% with messages = get_flashed_messages(with_categories=true) %}
          {% if messages %}
            {% for cat,msg in messages %}<div class="alert alert-{{ 'danger' if cat=='error' else cat }}">{{ msg }}</div>{% endfor %}
          {% endif %}
        {% endwith %}
        {{ content|safe }}
      </div>
    </main>
  </div>
</div>
{% endif %}
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

NAV_ITEMS = [
    ("dashboard", "داشبورد", "bi-speedometer2", "/admin"),
    ("users", "کاربران", "bi-people", "/admin/users"),
    ("plans", "پلن‌ها و پرداخت", "bi-credit-card", "/admin/plans"),
    ("payments", "رسیدها", "bi-receipt", "/admin/payments"),
    ("conversions", "تبدیل‌ها", "bi-music-note-list", "/admin/conversions"),
    ("broadcast", "پیام همگانی", "bi-megaphone", "/admin/broadcast"),
    ("settings", "تنظیمات", "bi-gear", "/admin/settings"),
]


def render_admin(title: str, active: str, body_template: str, **context: Any) -> str:
    body = render_template_string(
        body_template,
        db=db,
        STYLE_PRESETS=STYLE_PRESETS,
        fmt_num=fmt_num,
        fmt_dt=fmt_dt,
        now_iso=now_iso,
        **context,
    )
    return render_template_string(
        BASE_HTML,
        title=title,
        app_name=APP_NAME,
        active=active,
        nav=NAV_ITEMS,
        content=body,
        now=now_dt().strftime("%Y/%m/%d %H:%M"),
        login_page=False,
    )


def render_login_page(content: str) -> str:
    rendered_content = render_template_string(content)
    return render_template_string(
        BASE_HTML,
        title="ورود",
        app_name=APP_NAME,
        active="",
        nav=[],
        content=rendered_content,
        now="",
        login_page=True,
    )


def admin_required(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin_ok"):
            return redirect(url_for("admin_login", next=request.path))
        return fn(*args, **kwargs)

    return wrapper


@flask_app.route("/")
def root():
    return redirect(url_for("admin_dashboard"))


@flask_app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        ok_user = username == db.setting("admin_username", ADMIN_USERNAME)
        ok_pass = check_password_hash(db.setting("admin_password_hash"), password)
        if ok_user and ok_pass:
            session["admin_ok"] = True
            session["admin_user"] = username
            db.log_admin(username, "login")
            return redirect(request.args.get("next") or url_for("admin_dashboard"))
        flash("نام کاربری یا رمز عبور اشتباه است.", "error")
    content = r"""
    <div class="login-wrap d-flex align-items-center justify-content-center p-3">
      <div class="login-card p-4 p-md-5 w-100">
        <div class="text-center mb-4">
          <div class="d-inline-flex align-items-center justify-content-center rounded-4 mb-3" style="width:64px;height:64px;background:linear-gradient(135deg,#8b5cf6,#06b6d4);color:white"><i class="bi bi-music-note-beamed fs-2"></i></div>
          <h1 class="h4 fw-bold">ورود به پنل ادمین</h1>
          <p class="text-muted m-0">مدیریت کاربران، پلن‌ها، رسیدها و آمار</p>
        </div>
        {% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for cat,msg in messages %}<div class="alert alert-danger">{{ msg }}</div>{% endfor %}{% endif %}{% endwith %}
        <form method="post">
          <div class="mb-3"><label class="form-label">نام کاربری</label><input name="username" class="form-control form-control-lg" required autofocus></div>
          <div class="mb-4"><label class="form-label">رمز عبور</label><input name="password" type="password" class="form-control form-control-lg" required></div>
          <button class="btn btn-primary btn-lg w-100">ورود</button>
        </form>
        <div class="mt-3 small text-muted">رمز پیش‌فرض از فایل .env خوانده می‌شود. حتماً در سرور تغییرش بده.</div>
      </div>
    </div>
    """
    return render_login_page(content)


@flask_app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@flask_app.route("/admin")
@admin_required
def admin_dashboard():
    today = today_key()
    stats = {
        "users": db.one("SELECT COUNT(*) c FROM users")["c"],
        "paid_users": db.one("SELECT COUNT(*) c FROM users WHERE plan_id!='free'")["c"],
        "conversions_today": db.one("SELECT COUNT(*) c FROM conversions WHERE substr(created_at,1,10)=?", [today])["c"],
        "done_today": db.one("SELECT COUNT(*) c FROM conversions WHERE status='done' AND substr(created_at,1,10)=?", [today])["c"],
        "pending_payments": db.one("SELECT COUNT(*) c FROM payment_receipts WHERE status='pending'")["c"],
        "approved_revenue": db.one("SELECT COALESCE(SUM(amount_toman),0) c FROM payment_receipts WHERE status='approved'")["c"],
        "total_conversions": db.one("SELECT COUNT(*) c FROM conversions")["c"],
        "successful_conversions": db.one("SELECT COUNT(*) c FROM conversions WHERE status='done'")["c"],
        "failed_conversions": db.one("SELECT COUNT(*) c FROM conversions WHERE status='failed'")["c"],
        "avg_processing": db.one("SELECT ROUND(AVG(processing_time),1) c FROM conversions WHERE processing_time IS NOT NULL")["c"] or 0,
    }
    style_stats = db.all("SELECT style, COUNT(*) c FROM conversions WHERE status='done' GROUP BY style ORDER BY c DESC LIMIT 6")
    latest_users = db.all("SELECT * FROM users ORDER BY created_at DESC LIMIT 8")
    latest_conversions = db.all("SELECT c.*,u.username,u.first_name FROM conversions c LEFT JOIN users u ON u.telegram_id=c.telegram_id ORDER BY c.id DESC LIMIT 8")
    body = r"""
    <div class="row g-3 mb-4">
      {% set cards=[('کاربران',stats.users,'bi-people'),('کل تبدیل‌ها',stats.total_conversions,'bi-music-note-list'),('تبدیل موفق',stats.successful_conversions,'bi-check2-circle'),('تبدیل ناموفق',stats.failed_conversions,'bi-x-circle'),('میانگین پردازش',stats.avg_processing|string+' ثانیه','bi-stopwatch'),('درآمد تاییدشده',fmt_num(stats.approved_revenue)+' تومان','bi-cash-stack')] %}
      {% for label,value,icon in cards %}
      <div class="col-6 col-xl-2"><div class="card stat-card p-3 h-100"><div class="d-flex align-items-center gap-3"><div class="icon"><i class="bi {{ icon }}"></i></div><div><div class="text-muted small">{{ label }}</div><div class="h5 fw-bold mb-0">{{ value }}</div></div></div></div></div>
      {% endfor %}
    </div>
    <div class="row g-4">
      <div class="col-lg-4"><div class="card p-3 h-100"><h2 class="h5 fw-bold mb-3">سبک‌های محبوب</h2><div class="table-responsive"><table class="table"><thead><tr><th>سبک</th><th>تعداد</th></tr></thead><tbody>{% for st in style_stats %}<tr><td>{{ STYLE_PRESETS.get(st.style, STYLE_PRESETS['random'])['fa'] if STYLE_PRESETS else st.style }}</td><td>{{ st.c }}</td></tr>{% endfor %}</tbody></table></div></div></div>
      <div class="col-lg-4"><div class="card p-3"><h2 class="h5 fw-bold mb-3">آخرین کاربران</h2><div class="table-responsive"><table class="table"><thead><tr><th>کاربر</th><th>پلن</th><th>عضویت</th></tr></thead><tbody>{% for u in latest_users %}<tr><td><a href="/admin/users/{{ u.telegram_id }}">{{ u.first_name or '' }} @{{ u.username or '-' }}</a><br><small class="text-muted">{{ u.telegram_id }}</small></td><td><span class="badge badge-soft">{{ u.plan_id }}</span></td><td>{{ fmt_dt(u.created_at) }}</td></tr>{% endfor %}</tbody></table></div></div></div>
      <div class="col-lg-4"><div class="card p-3"><h2 class="h5 fw-bold mb-3">آخرین تبدیل‌ها</h2><div class="table-responsive"><table class="table"><thead><tr><th>کد</th><th>کاربر</th><th>وضعیت</th><th>زمان</th></tr></thead><tbody>{% for c in latest_conversions %}<tr><td>#{{ c.id }}</td><td>{{ c.first_name or '' }} @{{ c.username or '-' }}</td><td><span class="badge text-bg-{{ 'success' if c.status=='done' else 'danger' if c.status=='failed' else 'warning' }}">{{ c.status }}</span></td><td>{{ fmt_dt(c.created_at) }}</td></tr>{% endfor %}</tbody></table></div></div></div>
    </div>
    """
    return render_admin("داشبورد", "dashboard", body, stats=stats, style_stats=style_stats, latest_users=latest_users, latest_conversions=latest_conversions)


@flask_app.route("/admin/users")
@admin_required
def admin_users():
    q = request.args.get("q", "").strip()
    if q:
        like = f"%{q}%"
        users = db.all(
            """SELECT u.*,p.title plan_title FROM users u JOIN plans p ON p.id=u.plan_id
            WHERE CAST(u.telegram_id AS TEXT) LIKE ? OR username LIKE ? OR first_name LIKE ? OR last_name LIKE ?
            ORDER BY last_seen_at DESC LIMIT 200""",
            [like, like, like, like],
        )
    else:
        users = db.all("SELECT u.*,p.title plan_title FROM users u JOIN plans p ON p.id=u.plan_id ORDER BY last_seen_at DESC LIMIT 200")
    body = r"""
    <div class="card p-3">
      <form class="row g-2 mb-3"><div class="col-md-9"><input class="form-control" name="q" value="{{ request.args.get('q','') }}" placeholder="جستجو با آیدی، یوزرنیم یا نام"></div><div class="col-md-3"><button class="btn btn-primary w-100">جستجو</button></div></form>
      <div class="table-responsive"><table class="table table-hover"><thead><tr><th>کاربر</th><th>پلن</th><th>مصرف امروز</th><th>کل تبدیل</th><th>آخرین حضور</th><th></th></tr></thead><tbody>
      {% for u in users %}<tr><td><strong>{{ u.first_name or '' }} {{ u.last_name or '' }}</strong><br><small class="text-muted">{{ u.telegram_id }} | @{{ u.username or '-' }}</small></td><td><span class="badge badge-soft">{{ u.plan_title }}</span>{% if u.is_blocked %}<span class="badge text-bg-danger">مسدود</span>{% endif %}</td><td>{{ u.used_today }}</td><td>{{ u.total_conversions }}</td><td>{{ fmt_dt(u.last_seen_at) }}</td><td><a class="btn btn-sm btn-outline-primary" href="/admin/users/{{ u.telegram_id }}">مدیریت</a></td></tr>{% endfor %}
      </tbody></table></div>
    </div>
    """
    return render_admin("کاربران", "users", body, users=users, request=request)


@flask_app.route("/admin/users/<int:telegram_id>", methods=["GET", "POST"])
@admin_required
def admin_user_detail(telegram_id: int):
    if request.method == "POST":
        plan_id = request.form.get("plan_id", "free")
        expires = request.form.get("plan_expires_at", "").strip() or None
        is_blocked = 1 if request.form.get("is_blocked") == "1" else 0
        note = request.form.get("admin_note", "")
        db.execute(
            "UPDATE users SET plan_id=?, plan_expires_at=?, is_blocked=?, admin_note=? WHERE telegram_id=?",
            [plan_id, expires, is_blocked, note, telegram_id],
        )
        db.log_admin(session.get("admin_user", "admin"), "update_user", str(telegram_id))
        flash("کاربر بروزرسانی شد.", "success")
        send_telegram_message(telegram_id, "حساب شما توسط ادمین بروزرسانی شد. /start")
        return redirect(url_for("admin_user_detail", telegram_id=telegram_id))
    user = db.one("SELECT u.*,p.title plan_title FROM users u JOIN plans p ON p.id=u.plan_id WHERE telegram_id=?", [telegram_id])
    if not user:
        abort(404)
    plans = db.get_plans()
    conversions = db.all("SELECT * FROM conversions WHERE telegram_id=? ORDER BY id DESC LIMIT 20", [telegram_id])
    receipts = db.all("SELECT r.*,p.title plan_title FROM payment_receipts r JOIN plans p ON p.id=r.plan_id WHERE telegram_id=? ORDER BY id DESC LIMIT 20", [telegram_id])
    body = r"""
    <div class="row g-4">
      <div class="col-lg-5"><div class="card p-3"><h2 class="h5 fw-bold mb-3">مدیریت کاربر</h2>
        <div class="mb-3"><strong>{{ user.first_name or '' }} {{ user.last_name or '' }}</strong><br><small class="text-muted">{{ user.telegram_id }} | @{{ user.username or '-' }}</small></div>
        <form method="post">
          <div class="mb-3"><label class="form-label">پلن</label><select name="plan_id" class="form-select">{% for p in plans %}<option value="{{ p.id }}" {% if p.id==user.plan_id %}selected{% endif %}>{{ p.title }} - {{ fmt_num(p.price_toman) }} تومان</option>{% endfor %}</select></div>
          <div class="mb-3"><label class="form-label">انقضا پلن ISO</label><input name="plan_expires_at" class="form-control" value="{{ user.plan_expires_at or '' }}" placeholder="2026-07-20T12:00:00"></div>
          <div class="form-check form-switch mb-3"><input class="form-check-input" type="checkbox" name="is_blocked" value="1" {% if user.is_blocked %}checked{% endif %}><label class="form-check-label">مسدود باشد</label></div>
          <div class="mb-3"><label class="form-label">یادداشت ادمین</label><textarea name="admin_note" class="form-control" rows="3">{{ user.admin_note }}</textarea></div>
          <button class="btn btn-primary">ذخیره</button>
        </form>
      </div></div>
      <div class="col-lg-7"><div class="card p-3 mb-4"><h2 class="h5 fw-bold mb-3">تبدیل‌های اخیر</h2><div class="table-responsive"><table class="table"><thead><tr><th>کد</th><th>وضعیت</th><th>مدت</th><th>زمان</th></tr></thead><tbody>{% for c in conversions %}<tr><td>#{{ c.id }}</td><td>{{ c.status }}</td><td>{{ c.duration_seconds or 0 }}s</td><td>{{ fmt_dt(c.created_at) }}</td></tr>{% endfor %}</tbody></table></div></div>
      <div class="card p-3"><h2 class="h5 fw-bold mb-3">رسیدها</h2><div class="table-responsive"><table class="table"><thead><tr><th>پلن</th><th>مبلغ</th><th>وضعیت</th><th>زمان</th></tr></thead><tbody>{% for r in receipts %}<tr><td>{{ r.plan_title }}</td><td>{{ fmt_num(r.amount_toman) }}</td><td>{{ r.status }}</td><td>{{ fmt_dt(r.created_at) }}</td></tr>{% endfor %}</tbody></table></div></div></div>
    </div>
    """
    return render_admin("جزئیات کاربر", "users", body, user=user, plans=plans, conversions=conversions, receipts=receipts)


@flask_app.route("/admin/plans", methods=["GET", "POST"])
@admin_required
def admin_plans():
    if request.method == "POST":
        ids = request.form.getlist("id")
        for pid in ids:
            db.execute(
                """UPDATE plans SET title=?,price_toman=?,daily_limit=?,max_seconds=?,valid_days=?,payment_link=?,description=?,is_active=?,sort_order=? WHERE id=?""",
                [
                    request.form.get(f"title_{pid}", ""),
                    int(request.form.get(f"price_{pid}", "0") or 0),
                    int(request.form.get(f"daily_{pid}", "0") or 0),
                    int(request.form.get(f"max_{pid}", "0") or 0),
                    int(request.form.get(f"days_{pid}", "30") or 30),
                    request.form.get(f"link_{pid}", ""),
                    request.form.get(f"desc_{pid}", ""),
                    1 if request.form.get(f"active_{pid}") == "1" else 0,
                    int(request.form.get(f"sort_{pid}", "0") or 0),
                    pid,
                ],
            )
        db.log_admin(session.get("admin_user", "admin"), "update_plans")
        flash("پلن‌ها ذخیره شدند.", "success")
        return redirect(url_for("admin_plans"))
    plans = db.get_plans()
    body = r"""
    <form method="post">
      <div class="card p-3 mb-3"><div class="d-flex justify-content-between align-items-center"><div><h2 class="h5 fw-bold">پلن‌ها و لینک پرداخت</h2><p class="text-muted mb-0">قیمت‌ها و لینک‌های پرداخت از همین‌جا قابل ویرایش هستند.</p></div><button class="btn btn-primary">ذخیره همه</button></div></div>
      {% for p in plans %}
      <input type="hidden" name="id" value="{{ p.id }}">
      <div class="card p-3 mb-3">
        <div class="row g-3 align-items-end">
          <div class="col-md-2"><label class="form-label">شناسه</label><input class="form-control" value="{{ p.id }}" disabled></div>
          <div class="col-md-2"><label class="form-label">عنوان</label><input name="title_{{ p.id }}" class="form-control" value="{{ p.title }}"></div>
          <div class="col-md-2"><label class="form-label">قیمت تومان</label><input name="price_{{ p.id }}" type="number" class="form-control" value="{{ p.price_toman }}"></div>
          <div class="col-md-2"><label class="form-label">سهمیه روزانه</label><input name="daily_{{ p.id }}" type="number" class="form-control" value="{{ p.daily_limit }}"></div>
          <div class="col-md-2"><label class="form-label">حداکثر ثانیه</label><input name="max_{{ p.id }}" type="number" class="form-control" value="{{ p.max_seconds }}"></div>
          <div class="col-md-2"><label class="form-label">اعتبار روز</label><input name="days_{{ p.id }}" type="number" class="form-control" value="{{ p.valid_days }}"></div>
          <div class="col-md-8"><label class="form-label">لینک پرداخت</label><input name="link_{{ p.id }}" class="form-control" value="{{ p.payment_link }}"></div>
          <div class="col-md-2"><label class="form-label">ترتیب</label><input name="sort_{{ p.id }}" type="number" class="form-control" value="{{ p.sort_order }}"></div>
          <div class="col-md-2"><div class="form-check form-switch"><input class="form-check-input" type="checkbox" name="active_{{ p.id }}" value="1" {% if p.is_active %}checked{% endif %}><label class="form-check-label">فعال</label></div></div>
          <div class="col-12"><label class="form-label">توضیح</label><textarea name="desc_{{ p.id }}" class="form-control" rows="2">{{ p.description }}</textarea></div>
        </div>
      </div>
      {% endfor %}
    </form>
    """
    return render_admin("پلن‌ها و پرداخت", "plans", body, plans=plans)


@flask_app.route("/admin/payments")
@admin_required
def admin_payments():
    status = request.args.get("status", "")
    if status:
        receipts = db.all(
            """SELECT r.*,u.username,u.first_name,p.title plan_title FROM payment_receipts r
            JOIN users u ON u.telegram_id=r.telegram_id JOIN plans p ON p.id=r.plan_id
            WHERE r.status=? ORDER BY r.id DESC LIMIT 200""",
            [status],
        )
    else:
        receipts = db.all(
            """SELECT r.*,u.username,u.first_name,p.title plan_title FROM payment_receipts r
            JOIN users u ON u.telegram_id=r.telegram_id JOIN plans p ON p.id=r.plan_id
            ORDER BY r.id DESC LIMIT 200"""
        )
    body = r"""
    <div class="card p-3">
      <div class="d-flex flex-wrap gap-2 mb-3"><a class="btn btn-outline-secondary" href="/admin/payments">همه</a><a class="btn btn-outline-warning" href="/admin/payments?status=pending">معلق</a><a class="btn btn-outline-success" href="/admin/payments?status=approved">تاییدشده</a><a class="btn btn-outline-danger" href="/admin/payments?status=rejected">ردشده</a></div>
      <div class="table-responsive"><table class="table table-hover"><thead><tr><th>کد</th><th>کاربر</th><th>پلن</th><th>مبلغ</th><th>وضعیت</th><th>پیام</th><th>زمان</th><th></th></tr></thead><tbody>
      {% for r in receipts %}<tr><td>#{{ r.id }}</td><td><a href="/admin/users/{{ r.telegram_id }}">{{ r.first_name or '' }} @{{ r.username or '-' }}</a><br><small>{{ r.telegram_id }}</small></td><td>{{ r.plan_title }}</td><td>{{ fmt_num(r.amount_toman) }}</td><td><span class="badge text-bg-{{ 'warning' if r.status=='pending' else 'success' if r.status=='approved' else 'danger' }}">{{ r.status }}</span></td><td style="max-width:220px">{{ r.message }}</td><td>{{ fmt_dt(r.created_at) }}</td><td>{% if r.status=='pending' %}<a class="btn btn-sm btn-success" href="/admin/payments/{{ r.id }}/approve">تایید</a> <a class="btn btn-sm btn-outline-danger" href="/admin/payments/{{ r.id }}/reject">رد</a>{% endif %}</td></tr>{% endfor %}
      </tbody></table></div>
    </div>
    """
    return render_admin("رسیدهای پرداخت", "payments", body, receipts=receipts)


@flask_app.route("/admin/payments/<int:receipt_id>/approve")
@admin_required
def approve_payment(receipt_id: int):
    r = db.one("SELECT * FROM payment_receipts WHERE id=?", [receipt_id])
    if not r:
        abort(404)
    db.set_user_plan(r["telegram_id"], r["plan_id"])
    db.execute("UPDATE payment_receipts SET status='approved', updated_at=? WHERE id=?", [now_iso(), receipt_id])
    db.log_admin(session.get("admin_user", "admin"), "approve_payment", str(receipt_id))
    send_telegram_message(r["telegram_id"], "✅ پرداخت شما تایید شد و پلن جدید روی حساب‌تان فعال شد. /start")
    flash("پرداخت تایید و پلن فعال شد.", "success")
    return redirect(url_for("admin_payments", status="pending"))


@flask_app.route("/admin/payments/<int:receipt_id>/reject")
@admin_required
def reject_payment(receipt_id: int):
    r = db.one("SELECT * FROM payment_receipts WHERE id=?", [receipt_id])
    if not r:
        abort(404)
    db.execute("UPDATE payment_receipts SET status='rejected', updated_at=? WHERE id=?", [now_iso(), receipt_id])
    db.log_admin(session.get("admin_user", "admin"), "reject_payment", str(receipt_id))
    send_telegram_message(r["telegram_id"], "❌ رسید پرداخت شما تایید نشد. لطفاً با پشتیبانی تماس بگیرید.")
    flash("رسید رد شد.", "success")
    return redirect(url_for("admin_payments", status="pending"))


@flask_app.route("/admin/conversions")
@admin_required
def admin_conversions():
    status = request.args.get("status", "")
    params: List[Any] = []
    where = ""
    if status:
        where = "WHERE c.status=?"
        params.append(status)
    conversions = db.all(
        f"""SELECT c.*,u.username,u.first_name FROM conversions c LEFT JOIN users u ON u.telegram_id=c.telegram_id
        {where} ORDER BY c.id DESC LIMIT 300""",
        params,
    )
    body = r"""
    <div class="card p-3">
      <div class="d-flex flex-wrap gap-2 mb-3"><a class="btn btn-outline-secondary" href="/admin/conversions">همه</a><a class="btn btn-outline-success" href="/admin/conversions?status=done">موفق</a><a class="btn btn-outline-warning" href="/admin/conversions?status=processing">در حال پردازش</a><a class="btn btn-outline-danger" href="/admin/conversions?status=failed">ناموفق</a></div>
      <div class="table-responsive"><table class="table table-hover"><thead><tr><th>کد</th><th>کاربر</th><th>نوع</th><th>سبک</th><th>وضعیت</th><th>مدت</th><th>جزئیات/خطا</th><th>زمان</th></tr></thead><tbody>
      {% for c in conversions %}<tr><td>#{{ c.id }}</td><td>{{ c.first_name or '' }} @{{ c.username or '-' }}<br><small>{{ c.telegram_id }}</small></td><td>{{ c.input_kind }}</td><td>{{ STYLE_PRESETS.get(c.style, STYLE_PRESETS['random'])['fa'] }}</td><td><span class="badge text-bg-{{ 'success' if c.status=='done' else 'danger' if c.status=='failed' else 'warning' }}">{{ c.status }}</span></td><td>{{ c.duration_seconds or 0 }}s</td><td style="max-width:320px"><small>{% if c.error %}{{ c.error }}{% else %}{{ c.detected_key or '—' }} | {{ c.bpm or '' }} BPM | نت: {{ c.melody_note_count or 0 }}{% endif %}</small></td><td>{{ fmt_dt(c.created_at) }}</td></tr>{% endfor %}
      </tbody></table></div>
    </div>
    """
    return render_admin("تبدیل‌ها", "conversions", body, conversions=conversions)


@flask_app.route("/admin/broadcast", methods=["GET", "POST"])
@admin_required
def admin_broadcast():
    result = None
    if request.method == "POST":
        text = request.form.get("text", "").strip()
        target = request.form.get("target", "all")
        if not text:
            flash("متن پیام خالی است.", "error")
        else:
            if target == "paid":
                users = db.all("SELECT telegram_id FROM users WHERE is_blocked=0 AND plan_id!='free'")
            else:
                users = db.all("SELECT telegram_id FROM users WHERE is_blocked=0")
            sent = 0
            failed = 0
            for u in users:
                resp = send_telegram_message(u["telegram_id"], text)
                if resp and resp.get("ok"):
                    sent += 1
                else:
                    failed += 1
                time.sleep(0.045)
            db.log_admin(session.get("admin_user", "admin"), "broadcast", f"target={target}, sent={sent}, failed={failed}")
            result = {"sent": sent, "failed": failed}
    body = r"""
    <div class="card p-3">
      <h2 class="h5 fw-bold mb-3">ارسال پیام همگانی</h2>
      {% if result %}<div class="alert alert-success">ارسال شد: {{ result.sent }} | ناموفق: {{ result.failed }}</div>{% endif %}
      <form method="post">
        <div class="mb-3"><label class="form-label">مخاطب</label><select name="target" class="form-select"><option value="all">همه کاربران غیرمسدود</option><option value="paid">فقط کاربران پولی</option></select></div>
        <div class="mb-3"><label class="form-label">متن پیام</label><textarea name="text" class="form-control" rows="7" placeholder="متن پیام فارسی…"></textarea></div>
        <button class="btn btn-primary">ارسال</button>
      </form>
    </div>
    """
    return render_admin("پیام همگانی", "broadcast", body, result=result)


@flask_app.route("/admin/settings", methods=["GET", "POST"])
@admin_required
def admin_settings():
    keys = ["bot_title", "welcome_text", "support_text", "admin_telegram_id", "processing_note"]
    if request.method == "POST":
        for k in keys:
            db.set_setting(k, request.form.get(k, ""))
        new_user = request.form.get("admin_username", "").strip()
        new_pass = request.form.get("admin_password", "").strip()
        if new_user:
            db.set_setting("admin_username", new_user)
        if new_pass:
            db.set_setting("admin_password_hash", generate_password_hash(new_pass))
        db.log_admin(session.get("admin_user", "admin"), "update_settings")
        flash("تنظیمات ذخیره شد.", "success")
        return redirect(url_for("admin_settings"))
    settings = {k: db.setting(k) for k in keys}
    admin_user = db.setting("admin_username")
    body = r"""
    <div class="card p-3">
      <h2 class="h5 fw-bold mb-3">تنظیمات اصلی</h2>
      <form method="post">
        <div class="row g-3">
          <div class="col-md-6"><label class="form-label">عنوان ربات</label><input name="bot_title" class="form-control" value="{{ settings.bot_title }}"></div>
          <div class="col-md-6"><label class="form-label">آیدی عددی تلگرام ادمین</label><input name="admin_telegram_id" class="form-control" value="{{ settings.admin_telegram_id }}" placeholder="123456789"></div>
          <div class="col-12"><label class="form-label">متن خوشامد</label><textarea name="welcome_text" class="form-control" rows="4">{{ settings.welcome_text }}</textarea></div>
          <div class="col-12"><label class="form-label">متن پشتیبانی</label><textarea name="support_text" class="form-control" rows="3">{{ settings.support_text }}</textarea></div>
          <div class="col-12"><label class="form-label">یادداشت کیفیت زیر خروجی</label><textarea name="processing_note" class="form-control" rows="2">{{ settings.processing_note }}</textarea></div>
          <hr>
          <div class="col-md-6"><label class="form-label">نام کاربری ادمین</label><input name="admin_username" class="form-control" value="{{ admin_user }}"></div>
          <div class="col-md-6"><label class="form-label">رمز جدید ادمین</label><input name="admin_password" type="password" class="form-control" placeholder="برای تغییر، رمز جدید را بنویس"></div>
        </div>
        <button class="btn btn-primary mt-3">ذخیره تنظیمات</button>
      </form>
    </div>
    """
    return render_admin("تنظیمات", "settings", body, settings=settings, admin_user=admin_user)


@flask_app.route("/outputs/<path:filename>")
@admin_required
def serve_output(filename: str):
    return send_from_directory(OUTPUTS_DIR, filename)


def run_flask() -> None:
    flask_app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    logger.info("Starting %s | data=%s | db=%s", APP_NAME, DATA_DIR, DB_PATH)
    web_thread = threading.Thread(target=run_flask, daemon=True)
    web_thread.start()
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set. Admin panel is running, bot is not started.")
        while True:
            time.sleep(3600)
    bot_app = build_bot_app()
    logger.info("Telegram bot polling started")
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
