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
        self.seed_defaults()

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

@dataclass
class ProcessResult:
    raw_midi_path: Path
    arranged_midi_path: Path
    mp3_path: Path
    duration_seconds: int
    notes_count: int


def convert_to_wav(input_path: Path, wav_path: Path) -> int:
    """Convert any Telegram audio/voice to mono wav and return duration seconds."""
    wav_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_path),
        "-ac",
        "1",
        "-ar",
        "22050",
        "-vn",
        str(wav_path),
    ]
    proc = run_cmd(cmd, timeout=240)
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg convert failed: " + proc.stderr[-1000:])
    return probe_duration_seconds(wav_path)


def probe_duration_seconds(path: Path) -> int:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    proc = run_cmd(cmd, timeout=30)
    if proc.returncode != 0:
        return 0
    try:
        return int(math.ceil(float(proc.stdout.strip())))
    except Exception:
        return 0


def basic_pitch_to_midi(wav_path: Path, midi_path: Path) -> int:
    """Use Spotify Basic Pitch to transcribe audio to MIDI."""
    global BASIC_PITCH_MODEL
    midi_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        from basic_pitch import ICASSP_2022_MODEL_PATH
        from basic_pitch.inference import Model, predict
    except Exception as exc:
        raise RuntimeError(
            "Basic Pitch نصب یا قابل اجرا نیست. requirements.txt را نصب کنید. خطا: " + str(exc)
        ) from exc

    with BASIC_PITCH_LOCK:
        if BASIC_PITCH_MODEL is None:
            logger.info("Loading Basic Pitch model: %s", ICASSP_2022_MODEL_PATH)
            BASIC_PITCH_MODEL = Model(ICASSP_2022_MODEL_PATH)
        _model_output, midi_data, note_events = predict(str(wav_path), BASIC_PITCH_MODEL)

    midi_data.write(str(midi_path))
    return len(note_events or [])


def estimate_root(notes: List[Any]) -> int:
    if not notes:
        return 0
    counts = [0.0] * 12
    for n in notes:
        counts[n.pitch % 12] += max(1, n.end - n.start) * max(1, n.velocity)
    return int(np.argmax(np.array(counts)))


def clamp_pitch(pitch: int, lo: int = 24, hi: int = 96) -> int:
    while pitch < lo:
        pitch += 12
    while pitch > hi:
        pitch -= 12
    return int(pitch)


def arrange_midi(raw_midi_path: Path, arranged_midi_path: Path) -> Tuple[int, float]:
    """Make a simple song arrangement around transcribed melody: lead + pad + bass + drums."""
    import pretty_midi

    pm = pretty_midi.PrettyMIDI(str(raw_midi_path))
    melody_notes: List[Any] = []
    for inst in pm.instruments:
        if not inst.is_drum:
            melody_notes.extend(inst.notes)
    melody_notes = sorted(melody_notes, key=lambda n: (n.start, n.pitch))

    arranged = pretty_midi.PrettyMIDI(initial_tempo=100)
    lead = pretty_midi.Instrument(program=pretty_midi.instrument_name_to_program("Lead 2 (sawtooth)"), name="ملودی تبدیل‌شده")
    pad = pretty_midi.Instrument(program=pretty_midi.instrument_name_to_program("Pad 2 (warm)"), name="پد هارمونی")
    bass = pretty_midi.Instrument(program=pretty_midi.instrument_name_to_program("Synth Bass 1"), name="بیس")
    drums = pretty_midi.Instrument(program=0, is_drum=True, name="درام")

    duration = max([n.end for n in melody_notes], default=8.0) + 1.0
    root = estimate_root(melody_notes)
    # Major-ish progression: I - vi - IV - V. Works okay for demo arrangement.
    progression = [0, 9, 5, 7]
    bar = 2.0

    for n in melody_notes:
        start = max(0.0, float(n.start))
        end = max(start + 0.05, float(n.end))
        pitch = clamp_pitch(int(n.pitch), 48, 88)
        velocity = int(max(45, min(112, n.velocity + 8)))
        lead.notes.append(pretty_midi.Note(velocity=velocity, pitch=pitch, start=start, end=end))

    t = 0.0
    chord_index = 0
    while t < duration:
        degree = progression[chord_index % len(progression)]
        chord_root_pc = (root + degree) % 12
        chord_root = clamp_pitch(48 + chord_root_pc, 40, 64)
        # major/minor triad by progression position
        third = 3 if degree == 9 else 4
        chord = [chord_root, chord_root + third, chord_root + 7]
        for p in chord:
            pad.notes.append(pretty_midi.Note(velocity=42, pitch=clamp_pitch(p, 48, 72), start=t, end=min(t + bar, duration)))
        bass.notes.append(pretty_midi.Note(velocity=72, pitch=clamp_pitch(chord_root - 12, 28, 52), start=t, end=min(t + bar * 0.85, duration)))
        chord_index += 1
        t += bar

    # Simple 4/4 beat: kick on 1&3, snare on 2&4, hats on eighths.
    beat = 0.5
    steps = int(math.ceil(duration / beat))
    for i in range(steps):
        st = i * beat
        if i % 4 in (0, 2):
            drums.notes.append(pretty_midi.Note(velocity=96, pitch=36, start=st, end=st + 0.08))
        if i % 4 == 2:
            drums.notes.append(pretty_midi.Note(velocity=88, pitch=38, start=st, end=st + 0.1))
        drums.notes.append(pretty_midi.Note(velocity=46 if i % 2 else 58, pitch=42, start=st, end=st + 0.04))

    arranged.instruments.extend([lead, pad, bass, drums])
    arranged.write(str(arranged_midi_path))
    return len(melody_notes), duration


def adsr_envelope(length: int, sr: int, attack: float = 0.01, release: float = 0.08) -> np.ndarray:
    env = np.ones(length, dtype=np.float32)
    a = min(length, int(sr * attack))
    r = min(length, int(sr * release))
    if a > 1:
        env[:a] = np.linspace(0, 1, a)
    if r > 1:
        env[-r:] *= np.linspace(1, 0, r)
    return env


def add_tone(audio: np.ndarray, sr: int, start: float, end: float, pitch: int, velocity: int, kind: str = "lead") -> None:
    s = max(0, int(start * sr))
    e = min(len(audio), int(end * sr))
    if e <= s:
        return
    length = e - s
    t = np.arange(length, dtype=np.float32) / sr
    freq = 440.0 * (2.0 ** ((pitch - 69) / 12.0))
    amp = (velocity / 127.0) * 0.14
    if kind == "bass":
        wave = np.sin(2 * np.pi * freq * t) + 0.35 * np.sin(2 * np.pi * freq * 2 * t)
        amp *= 0.75
    elif kind == "pad":
        wave = np.sin(2 * np.pi * freq * t) + 0.22 * np.sin(2 * np.pi * (freq * 1.005) * t)
        amp *= 0.28
    else:
        wave = np.sin(2 * np.pi * freq * t) + 0.25 * np.sin(2 * np.pi * 2 * freq * t) + 0.08 * np.sin(2 * np.pi * 3 * freq * t)
    env = adsr_envelope(length, sr, attack=0.015 if kind != "pad" else 0.08, release=0.06 if kind != "pad" else 0.18)
    audio[s:e] += (wave * env * amp).astype(np.float32)


def add_kick(audio: np.ndarray, sr: int, start: float, velocity: int) -> None:
    s = int(start * sr)
    length = int(0.18 * sr)
    e = min(len(audio), s + length)
    if e <= s:
        return
    t = np.arange(e - s, dtype=np.float32) / sr
    freq = 90 * np.exp(-18 * t) + 38
    phase = 2 * np.pi * np.cumsum(freq) / sr
    env = np.exp(-12 * t)
    audio[s:e] += np.sin(phase) * env * (velocity / 127.0) * 0.7


def add_snare(audio: np.ndarray, sr: int, start: float, velocity: int) -> None:
    s = int(start * sr)
    length = int(0.16 * sr)
    e = min(len(audio), s + length)
    if e <= s:
        return
    rng = np.random.default_rng(1234 + s)
    t = np.arange(e - s, dtype=np.float32) / sr
    noise = rng.normal(0, 1, e - s).astype(np.float32)
    tone = np.sin(2 * np.pi * 190 * t) * 0.25
    env = np.exp(-18 * t)
    audio[s:e] += (noise * 0.28 + tone) * env * (velocity / 127.0)


def add_hat(audio: np.ndarray, sr: int, start: float, velocity: int) -> None:
    s = int(start * sr)
    length = int(0.055 * sr)
    e = min(len(audio), s + length)
    if e <= s:
        return
    rng = np.random.default_rng(4321 + s)
    t = np.arange(e - s, dtype=np.float32) / sr
    noise = rng.normal(0, 1, e - s).astype(np.float32)
    env = np.exp(-60 * t)
    audio[s:e] += noise * env * (velocity / 127.0) * 0.16


def synthesize_midi_to_mp3(midi_path: Path, wav_path: Path, mp3_path: Path) -> None:
    """Built-in lightweight synth; no paid soundfont required."""
    import pretty_midi

    pm = pretty_midi.PrettyMIDI(str(midi_path))
    sr = 44100
    duration = max(pm.get_end_time() + 1.0, 4.0)
    audio = np.zeros(int(duration * sr), dtype=np.float32)

    for inst in pm.instruments:
        if inst.is_drum:
            for n in inst.notes:
                if n.pitch == 36:
                    add_kick(audio, sr, n.start, n.velocity)
                elif n.pitch in (38, 40):
                    add_snare(audio, sr, n.start, n.velocity)
                else:
                    add_hat(audio, sr, n.start, n.velocity)
        else:
            name = (inst.name or "").lower()
            kind = "lead"
            if "bass" in name or "بیس" in name:
                kind = "bass"
            elif "pad" in name or "پد" in name:
                kind = "pad"
            for n in inst.notes:
                add_tone(audio, sr, n.start, n.end, n.pitch, n.velocity, kind=kind)

    # Soft limiter / normalization
    peak = float(np.max(np.abs(audio))) if len(audio) else 0.0
    if peak > 0:
        audio = audio / max(peak, 1.0) * 0.92 if peak > 1.0 else audio * 0.92
    audio_i16 = np.int16(np.clip(audio, -1, 1) * 32767)
    wavfile.write(str(wav_path), sr, audio_i16)

    proc = run_cmd(["ffmpeg", "-y", "-i", str(wav_path), "-codec:a", "libmp3lame", "-b:a", "192k", str(mp3_path)], timeout=240)
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg mp3 failed: " + proc.stderr[-1000:])


def process_audio_to_song(input_path: Path, conversion_id: int) -> ProcessResult:
    """Full pipeline: Telegram audio -> wav -> Basic Pitch MIDI -> arranged MIDI -> MP3."""
    with PROCESS_SEMAPHORE:
        outdir = OUTPUTS_DIR / str(conversion_id)
        outdir.mkdir(parents=True, exist_ok=True)
        wav_path = outdir / "input.wav"
        raw_midi_path = outdir / "melody_raw.mid"
        arranged_midi_path = outdir / "song_arranged.mid"
        synth_wav_path = outdir / "song.wav"
        mp3_path = outdir / "song.mp3"

        duration = convert_to_wav(input_path, wav_path)
        notes_count = basic_pitch_to_midi(wav_path, raw_midi_path)
        melody_count, _arr_duration = arrange_midi(raw_midi_path, arranged_midi_path)
        synthesize_midi_to_mp3(arranged_midi_path, synth_wav_path, mp3_path)
        return ProcessResult(raw_midi_path, arranged_midi_path, mp3_path, duration, max(notes_count, melody_count))

# -----------------------------------------------------------------------------
# Telegram Bot
# -----------------------------------------------------------------------------
BTN_CONVERT = "🎙 تبدیل وویس به آهنگ"
BTN_PLANS = "💳 خرید اشتراک"
BTN_STATUS = "📊 وضعیت من"
BTN_HELP = "❓ راهنما"
BTN_SUPPORT = "☎️ پشتیبانی"


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[BTN_CONVERT, BTN_PLANS], [BTN_STATUS, BTN_HELP], [BTN_SUPPORT]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="وویس یا فایل صوتی بفرست…",
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
    menu_texts = {BTN_CONVERT, BTN_PLANS, BTN_STATUS, BTN_HELP, BTN_SUPPORT}
    if text not in menu_texts and await handle_receipt(update, context):
        return
    if text == BTN_CONVERT:
        await update.message.reply_text(
            "🎙 عالی! حالا یک وویس، آواز یا فایل صوتی بفرست. بهتره صدا واضح و تک‌ملودی باشه.",
            reply_markup=main_keyboard(),
        )
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

    conv_id = db.execute(
        """INSERT INTO conversions(telegram_id,input_kind,status,duration_seconds,created_at)
        VALUES(?,?,?,?,?)""",
        [tg_user.id, kind, "queued", duration or 0, now_iso()],
    ).lastrowid
    input_path = UPLOADS_DIR / f"{conv_id}{ext}"
    db.execute("UPDATE conversions SET input_path=? WHERE id=?", [str(input_path), conv_id])

    await update.message.reply_text(
        "⏳ فایل دریافت شد. دارم ملودی رو تشخیص می‌دم و آهنگ می‌سازم… ممکنه چند دقیقه طول بکشه.",
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

        result: ProcessResult = await asyncio.to_thread(process_audio_to_song, input_path, conv_id)
        db.execute(
            """UPDATE conversions SET status='done', completed_at=?, wav_path=?, raw_midi_path=?, arranged_midi_path=?, mp3_path=?, duration_seconds=? WHERE id=?""",
            [
                now_iso(),
                str(OUTPUTS_DIR / str(conv_id) / "input.wav"),
                str(result.raw_midi_path),
                str(result.arranged_midi_path),
                str(result.mp3_path),
                result.duration_seconds,
                conv_id,
            ],
        )
        db.increment_usage(tg_user.id)

        caption = (
            "✅ آهنگت آماده شد!\n"
            f"نت‌های تشخیص‌داده‌شده: حدود {result.notes_count}\n"
            f"زمان فایل: {result.duration_seconds} ثانیه\n\n"
            f"{db.setting('processing_note')}"
        )
        await update.message.reply_audio(audio=open(result.mp3_path, "rb"), filename="voice2song.mp3", caption=caption)
        await update.message.reply_document(document=open(result.arranged_midi_path, "rb"), filename="voice2song_arranged.mid")
        await update.message.reply_document(document=open(result.raw_midi_path, "rb"), filename="voice2song_raw_melody.mid")
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
    }
    latest_users = db.all("SELECT * FROM users ORDER BY created_at DESC LIMIT 8")
    latest_conversions = db.all("SELECT c.*,u.username,u.first_name FROM conversions c LEFT JOIN users u ON u.telegram_id=c.telegram_id ORDER BY c.id DESC LIMIT 8")
    body = r"""
    <div class="row g-3 mb-4">
      {% set cards=[('کاربران',stats.users,'bi-people'),('کاربران پولی',stats.paid_users,'bi-gem'),('تبدیل‌های امروز',stats.conversions_today,'bi-music-note'),('موفق امروز',stats.done_today,'bi-check2-circle'),('رسیدهای معلق',stats.pending_payments,'bi-hourglass-split'),('درآمد تاییدشده',fmt_num(stats.approved_revenue)+' تومان','bi-cash-stack')] %}
      {% for label,value,icon in cards %}
      <div class="col-6 col-xl-2"><div class="card stat-card p-3 h-100"><div class="d-flex align-items-center gap-3"><div class="icon"><i class="bi {{ icon }}"></i></div><div><div class="text-muted small">{{ label }}</div><div class="h5 fw-bold mb-0">{{ value }}</div></div></div></div></div>
      {% endfor %}
    </div>
    <div class="row g-4">
      <div class="col-lg-6"><div class="card p-3"><h2 class="h5 fw-bold mb-3">آخرین کاربران</h2><div class="table-responsive"><table class="table"><thead><tr><th>کاربر</th><th>پلن</th><th>عضویت</th></tr></thead><tbody>{% for u in latest_users %}<tr><td><a href="/admin/users/{{ u.telegram_id }}">{{ u.first_name or '' }} @{{ u.username or '-' }}</a><br><small class="text-muted">{{ u.telegram_id }}</small></td><td><span class="badge badge-soft">{{ u.plan_id }}</span></td><td>{{ fmt_dt(u.created_at) }}</td></tr>{% endfor %}</tbody></table></div></div></div>
      <div class="col-lg-6"><div class="card p-3"><h2 class="h5 fw-bold mb-3">آخرین تبدیل‌ها</h2><div class="table-responsive"><table class="table"><thead><tr><th>کد</th><th>کاربر</th><th>وضعیت</th><th>زمان</th></tr></thead><tbody>{% for c in latest_conversions %}<tr><td>#{{ c.id }}</td><td>{{ c.first_name or '' }} @{{ c.username or '-' }}</td><td><span class="badge text-bg-{{ 'success' if c.status=='done' else 'danger' if c.status=='failed' else 'warning' }}">{{ c.status }}</span></td><td>{{ fmt_dt(c.created_at) }}</td></tr>{% endfor %}</tbody></table></div></div></div>
    </div>
    """
    return render_admin("داشبورد", "dashboard", body, stats=stats, latest_users=latest_users, latest_conversions=latest_conversions)


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
      <div class="table-responsive"><table class="table table-hover"><thead><tr><th>کد</th><th>کاربر</th><th>نوع</th><th>وضعیت</th><th>مدت</th><th>خطا</th><th>زمان</th></tr></thead><tbody>
      {% for c in conversions %}<tr><td>#{{ c.id }}</td><td>{{ c.first_name or '' }} @{{ c.username or '-' }}<br><small>{{ c.telegram_id }}</small></td><td>{{ c.input_kind }}</td><td><span class="badge text-bg-{{ 'success' if c.status=='done' else 'danger' if c.status=='failed' else 'warning' }}">{{ c.status }}</span></td><td>{{ c.duration_seconds or 0 }}s</td><td style="max-width:320px"><small>{{ c.error or '' }}</small></td><td>{{ fmt_dt(c.created_at) }}</td></tr>{% endfor %}
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
