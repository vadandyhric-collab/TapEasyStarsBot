import time
import random
import asyncio
import logging
import html
import threading
import aiohttp
import os
import requests
from typing import Dict, List, Optional, Any

import telegram
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)
import aiosqlite
from cachetools import TTLCache
from flask import Flask

# ================= НАСТРОЙКИ ЛОГИРОВАНИЯ =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ================= НАСТРОЙКИ =================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

BOT_USERNAME = "TapEasyStars_bot"

# ================= НАСТРОЙКИ SUBGRAM =================
SUBGRAM_API_KEY = os.getenv("SUBGRAM_API_KEY", "a7a606fa753150efea79cef5310cabbdcfd141d9092dc6f5b3e2871cadc50a6e")
SUBGRAM_URL = "https://api.subgram.org/get-sponsors"
SUBGRAM_TIMEOUT = 5
SUBGRAM_RETRIES = 2

# Награда за выполнение одного задания
TASK_REWARD = 0.5

# Минимальная сумма вывода
MIN_WITHDRAW = 15

# ================= РЕФЕРАЛЬНЫЕ БОНУСЫ =================
REF_BONUS_INVITER = 3
REF_BONUS_NEW = 0
REF_PERCENT = 0.03

# ================= КАНАЛЫ =================
CHANNEL_1_LINK = "https://t.me/TapEasyStars"
CHANNEL_2_LINK = "https://t.me/TapEasyVivod"
CHANNEL_1_ID = -1003728951415
CHANNEL_2_ID = -1002857821966

MODERATION_CHAT_ID = -1002857821966
MODERATOR_IDS = [1539010179]
WITHDRAW_CHANNEL_LINK = "https://t.me/TapEasyVivod"

# ================= НАСТРОЙКИ БАЗЫ ДАННЫХ =================
# Сохраняем БД в папке приложения (или в /tmp на Render)
DB_PATH = os.path.join(os.path.dirname(__file__), "users.db") if os.getenv("RENDER") else "users.db"

MAX_LIMIT = 500
RESTORE_PER_MINUTE = 4.2
CLICK_MIN = 0.5
CLICK_MAX = 2.0

# ================= КЭШ =================
user_cache = TTLCache(maxsize=20000, ttl=600)
subgram_cache = TTLCache(maxsize=2000, ttl=300)

# ================= БАЗА ДАННЫХ =================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
        logger.info("База данных инициализирована")
    
    def _init_db(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance REAL DEFAULT 0,
                total_earned REAL DEFAULT 0,
                referral_earned REAL DEFAULT 0,
                multiplier REAL DEFAULT 0,
                limit_used REAL DEFAULT 0,
                limit_last_update REAL DEFAULT 0,
                referrals_count INTEGER DEFAULT 0,
                start_verified INTEGER DEFAULT 0,
                last_spam_time REAL DEFAULT 0,
                created_at REAL DEFAULT 0,
                last_activity REAL DEFAULT 0,
                username TEXT DEFAULT ''
            )
        ''')
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_last_activity ON users(last_activity)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_username ON users(username)")
        
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN referral_earned REAL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN multiplier REAL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN last_activity REAL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN username TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                user_id INTEGER PRIMARY KEY,
                referrer_id INTEGER
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS completed_tasks (
                user_id INTEGER,
                task_id TEXT,
                completed_at REAL,
                PRIMARY KEY (user_id, task_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS frozen_balance (
                request_id TEXT PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                created_at REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS withdraw_requests (
                request_id TEXT PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                status TEXT,
                created_at REAL,
                subscription_ok INTEGER,
                video_link TEXT,
                moderator_id INTEGER,
                submitted_at REAL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_max_request_id_sync(self) -> int:
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        ids = []
        cursor.execute("SELECT request_id FROM withdraw_requests")
        for row in cursor.fetchall():
            val = row[0]
            if val and val.isdigit():
                ids.append(int(val))
        cursor.execute("SELECT request_id FROM frozen_balance")
        for row in cursor.fetchall():
            val = row[0]
            if val and val.isdigit():
                ids.append(int(val))
        conn.close()
        return max(ids) if ids else 0
    
    async def get_user(self, user_id: int) -> Dict[str, Any]:
        if user_id in user_cache:
            return user_cache[user_id]
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            
            if not row:
                current_time = time.time()
                await db.execute(
                    "INSERT INTO users (user_id, balance, total_earned, referral_earned, multiplier, limit_used, limit_last_update, "
                    "referrals_count, start_verified, last_spam_time, created_at, last_activity, username) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (user_id, 0.0, 0.0, 0.0, 0.0, 0.0, current_time, 0, 0, current_time, current_time, current_time, '')
                )
                await db.commit()
                
                user_data = {
                    "user_id": user_id,
                    "balance": 0.0,
                    "total_earned": 0.0,
                    "referral_earned": 0.0,
                    "multiplier": 0.0,
                    "limit_used": 0.0,
                    "limit_last_update": current_time,
                    "referrals_count": 0,
                    "start_verified": False,
                    "last_spam_time": current_time,
                    "last_activity": current_time,
                    "username": ''
                }
            else:
                user_data = dict(row)
                user_data["start_verified"] = bool(user_data["start_verified"])
            
            user_cache[user_id] = user_data
            return user_data
    
    async def update_user_activity(self, user_id: int, username: str = None):
        now = time.time()
        async with aiosqlite.connect(self.db_path) as db:
            if username is not None:
                await db.execute(
                    "UPDATE users SET last_activity = ?, username = ? WHERE user_id = ?",
                    (now, username, user_id)
                )
            else:
                await db.execute(
                    "UPDATE users SET last_activity = ? WHERE user_id = ?",
                    (now, user_id)
                )
            await db.commit()
        if user_id in user_cache:
            user_cache[user_id]["last_activity"] = now
            if username is not None:
                user_cache[user_id]["username"] = username
    
    async def update_balance(self, user_id: int, amount: float, is_withdraw: bool = False, is_referral_bonus: bool = False):
        async with aiosqlite.connect(self.db_path) as db:
            if is_withdraw:
                await db.execute(
                    "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                    (amount, user_id)
                )
            else:
                if is_referral_bonus:
                    await db.execute(
                        "UPDATE users SET balance = balance + ?, referral_earned = referral_earned + ? WHERE user_id = ?",
                        (amount, amount, user_id)
                    )
                else:
                    await db.execute(
                        "UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?",
                        (amount, amount, user_id)
                    )
            await db.commit()
        
        if user_id in user_cache:
            if is_withdraw:
                user_cache[user_id]["balance"] -= amount
            else:
                user_cache[user_id]["balance"] += amount
                if is_referral_bonus:
                    user_cache[user_id]["referral_earned"] += amount
                else:
                    user_cache[user_id]["total_earned"] += amount
    
    async def update_limit(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            row = await db.execute(
                "SELECT limit_used, limit_last_update FROM users WHERE user_id = ?",
                (user_id,)
            )
            result = await row.fetchone()
            
            if result:
                limit_used, last_update = result
                now = time.time()
                minutes = (now - last_update) / 60
                restored = minutes * RESTORE_PER_MINUTE
                new_limit_used = max(0.0, limit_used - restored)
                
                await db.execute(
                    "UPDATE users SET limit_used = ?, limit_last_update = ? WHERE user_id = ?",
                    (new_limit_used, now, user_id)
                )
                await db.commit()
                
                if user_id in user_cache:
                    user_cache[user_id]["limit_used"] = new_limit_used
                    user_cache[user_id]["limit_last_update"] = now
                
                return new_limit_used
            
            return 0.0
    
    async def add_referral(self, user_id: int, referrer_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT referrer_id FROM referrals WHERE user_id = ?", (user_id,))
            if await cursor.fetchone():
                return False
            
            await db.execute("INSERT INTO referrals (user_id, referrer_id) VALUES (?, ?)", (user_id, referrer_id))
            await db.execute("UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = ?", (referrer_id,))
            await db.commit()
            
            if referrer_id in user_cache:
                user_cache[referrer_id]["referrals_count"] += 1
            
            return True
    
    async def get_referrer(self, user_id: int) -> Optional[int]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT referrer_id FROM referrals WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            return row[0] if row else None
    
    async def add_completed_task(self, user_id: int, task_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO completed_tasks (user_id, task_id, completed_at) VALUES (?, ?, ?)",
                (user_id, task_id, time.time())
            )
            await db.commit()
    
    async def is_task_completed(self, user_id: int, task_id: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM completed_tasks WHERE user_id = ? AND task_id = ?",
                (user_id, task_id)
            )
            return await cursor.fetchone() is not None
    
    async def get_completed_tasks(self, user_id: int) -> Dict[str, bool]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT task_id FROM completed_tasks WHERE user_id = ?", (user_id,))
            rows = await cursor.fetchall()
            return {row[0]: True for row in rows}
    
    async def add_withdraw_request(self, request_id: str, user_id: int, amount: float, status: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO withdraw_requests (request_id, user_id, amount, status, created_at, subscription_ok) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (request_id, user_id, amount, status, time.time(), 0)
            )
            await db.commit()
    
    async def update_withdraw_request(self, request_id: str, **kwargs):
        async with aiosqlite.connect(self.db_path) as db:
            set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values()) + [request_id]
            await db.execute(f"UPDATE withdraw_requests SET {set_clause} WHERE request_id = ?", values)
            await db.commit()
    
    async def get_withdraw_request(self, request_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM withdraw_requests WHERE request_id = ?", (request_id,))
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def get_user_withdraw_requests(self, user_id: int) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def add_frozen_balance(self, request_id: str, user_id: int, amount: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO frozen_balance (request_id, user_id, amount, created_at) VALUES (?, ?, ?, ?)",
                (request_id, user_id, amount, time.time())
            )
            await db.commit()
    
    async def remove_frozen_balance(self, request_id: str) -> Optional[float]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT amount FROM frozen_balance WHERE request_id = ?", (request_id,))
            row = await cursor.fetchone()
            if row:
                amount = row[0]
                await db.execute("DELETE FROM frozen_balance WHERE request_id = ?", (request_id,))
                await db.commit()
                return amount
            return None
    
    async def update_user_verified(self, user_id: int, verified: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET start_verified = ? WHERE user_id = ?", (1 if verified else 0, user_id))
            await db.commit()
            if user_id in user_cache:
                user_cache[user_id]["start_verified"] = verified
    
    async def get_all_user_ids(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT user_id FROM users")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
    
    async def get_all_users_sorted(self, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            offset = (page - 1) * per_page
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cursor.fetchone())[0]
            cursor = await db.execute(
                "SELECT user_id, username, referrals_count FROM users "
                "ORDER BY CASE WHEN username IS NULL OR username = '' THEN 1 ELSE 0 END, username COLLATE NOCASE "
                "LIMIT ? OFFSET ?",
                (per_page, offset)
            )
            rows = await cursor.fetchall()
            users = [dict(row) for row in rows]
            return {
                "users": users,
                "total": total,
                "page": page,
                "pages": (total + per_page - 1) // per_page
            }
    
    async def get_statistics(self) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]
            
            one_hour_ago = time.time() - 3600
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE last_activity > ?", (one_hour_ago,))
            active_hour = (await cursor.fetchone())[0]
            
            one_day_ago = time.time() - 86400
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE last_activity > ?", (one_day_ago,))
            active_day = (await cursor.fetchone())[0]
            
            one_month_ago = time.time() - 2592000
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE last_activity > ?", (one_month_ago,))
            active_month = (await cursor.fetchone())[0]
            
            return {
                "total_users": total_users,
                "active_hour": active_hour,
                "active_day": active_day,
                "active_month": active_month
            }
    
    async def update_limit_usage(self, user_id: int, amount: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET limit_used = limit_used + ? WHERE user_id = ?",
                (amount, user_id)
            )
            await db.commit()
            if user_id in user_cache:
                user_cache[user_id]["limit_used"] += amount

# ================= ИНИЦИАЛИЗАЦИЯ =================
db = Database(DB_PATH)
last_request_id = db.get_max_request_id_sync()
_request_id_lock = threading.Lock()

# ================= SUBGRAM API =================
async def subgram_request_with_retries(payload: dict, retries: int = SUBGRAM_RETRIES) -> dict | None:
    headers = {"Auth": SUBGRAM_API_KEY}
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(SUBGRAM_URL, headers=headers, json=payload, timeout=SUBGRAM_TIMEOUT) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 404:
                        return {"status": "ok"}
                    else:
                        logger.warning(f"SubGram API returned status {resp.status}")
                        return None
        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
            logger.warning(f"SubGram API attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(1)
            else:
                return None
        except Exception as e:
            logger.error(f"SubGram API unexpected error: {e}")
            return None
    return None

async def get_subgram_sponsors(user_id: int, chat_id: int, user_data: dict, action: str = "subscribe") -> dict | None:
    cache_key = f"{user_id}_{action}"
    if cache_key in subgram_cache:
        return subgram_cache[cache_key]
    
    payload = {
        "user_id": user_id,
        "chat_id": chat_id,
        "first_name": user_data.get("first_name", ""),
        "username": user_data.get("username", ""),
        "language_code": user_data.get("language_code", ""),
        "is_premium": user_data.get("is_premium", False),
        "action": action,
        "get_links": 1,
        "use_smart_link": 1,
        "show_quiz": 0,
        "max_sponsors": 5,
        "time_purge": 1800
    }
    
    response = await subgram_request_with_retries(payload)
    if response:
        subgram_cache[cache_key] = response
    return response

async def invalidate_subgram_cache(user_id: int, action: str = "subscribe"):
    cache_key = f"{user_id}_{action}"
    if cache_key in subgram_cache:
        del subgram_cache[cache_key]
        logger.debug(f"SubGram cache invalidated for {cache_key}")

# ================= УТИЛИТЫ =================
def is_admin(user_id: int) -> bool:
    return user_id in MODERATOR_IDS

def get_display_name(user) -> str:
    if user.username:
        return f"@{html.escape(user.username)}"
    else:
        name = user.first_name or ""
        if user.last_name:
            name += " " + user.last_name
        name = name.strip()
        if not name:
            name = f"id{user.id}"
        return html.escape(name)

def mask_username(username: str) -> str:
    if not username:
        return "@пользователь"
    raw = username
    length = len(raw)
    if length == 4:
        masked = raw[0] + '##' + raw[3]
    elif length == 5:
        masked = raw[:2] + '##' + raw[4]
    elif length >= 6:
        masked = raw[:2] + '#' * (length - 4) + raw[-2:]
    else:
        masked = raw
    return '@' + html.escape(masked)

def generate_fake_username() -> str:
    first_names = ["alex", "max", "mike", "john", "jane", "kate", "anna", "lisa", "olga", "sveta", "dima", "artem", "vlad", "nik", "lena", "ira", "katya", "sasha", "pasha", "misha", "oleg", "igor", "yana", "masha", "nina"]
    last_names = ["smith", "jones", "brown", "wilson", "lee", "kim", "kuzmin", "petrov", "ivanov", "sokolov", "morozov", "volkov", "novikov", "kozlov", "popov", "zaytsev", "smirnov", "fedotov"]
    adjectives = ["cool", "super", "mega", "ultra", "pro", "top", "best", "great", "smart", "fast", "happy", "lucky", "brave", "calm", "wild", "epic"]
    nouns = ["star", "sun", "moon", "sky", "cat", "dog", "fox", "wolf", "bear", "hawk", "eagle", "tiger", "lion", "panda", "dragon", "phoenix", "raven", "falcon"]

    styles = ["first_last", "first_adjective", "adjective_noun", "first_number", "random_word"]
    
    style = random.choice(styles)
    
    if style == "first_last":
        first = random.choice(first_names)
        last = random.choice(last_names)
        username = f"{first}_{last}"
    elif style == "first_adjective":
        first = random.choice(first_names)
        adj = random.choice(adjectives)
        username = f"{first}_{adj}"
    elif style == "adjective_noun":
        adj = random.choice(adjectives)
        noun = random.choice(nouns)
        username = f"{adj}_{noun}"
    elif style == "first_number":
        first = random.choice(first_names)
        num = random.randint(10, 99)
        username = f"{first}{num}"
    else:
        length = random.randint(5, 8)
        vowels = "aeiouy"
        consonants = "bcdfghjklmnpqrstvwxz"
        username = ""
        for i in range(length):
            if i % 2 == 0:
                username += random.choice(consonants)
            else:
                username += random.choice(vowels)
        if random.random() < 0.3:
            username += "_" + random.choice(nouns)[:3]
    
    if style != "first_number":
        username = username.rstrip("0123456789")
    
    if username and username[0].isdigit():
        username = "user_" + username
    
    username = username.lower()
    username = ''.join(ch for ch in username if ch.isalnum() or ch == '_')
    username = username.rstrip('_')
    
    if len(username) < 5:
        letters = "abcdefghijklmnopqrstuvwxyz"
        while len(username) < 5:
            username += random.choice(letters)
    elif len(username) > 8:
        username = username[:8]
    
    if not username or username.isdigit():
        username = "user" + str(random.randint(100, 999))
    
    if not username[0].isalpha():
        username = "u" + username
    
    return username

def generate_request_id() -> str:
    global last_request_id
    with _request_id_lock:
        last_request_id += 1
        return str(last_request_id)

async def is_subscribed(context, user_id, chat_id):
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

# ================= БЕЗОПАСНАЯ ОТПРАВКА СООБЩЕНИЙ =================
async def safe_send_message(context, chat_id: int, text: str, **kwargs) -> Optional[telegram.Message]:
    try:
        return await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except telegram.error.Forbidden:
        logger.warning(f"Cannot send message to user {chat_id} (blocked or left)")
    except telegram.error.RetryAfter as e:
        logger.warning(f"Rate limit hit for {chat_id}, waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return await safe_send_message(context, chat_id, text, **kwargs)
    except Exception as e:
        logger.error(f"Error sending message to {chat_id}: {e}")
    return None

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    username = user.username or ""

    await db.get_user(uid)
    await db.update_user_activity(uid, username)

    if update.message and update.message.text:
        args = update.message.text.split()
        if len(args) > 1:
            try:
                ref_id = int(args[1])
                if ref_id != uid:
                    existing_referrer = await db.get_referrer(uid)
                    if not existing_referrer:
                        await db.add_referral(uid, ref_id)
                        await db.update_balance(ref_id, REF_BONUS_INVITER, is_withdraw=False, is_referral_bonus=True)
            except Exception:
                pass

    await start_earning_process(update, context, show_ref=False)
    await show_main_menu(update, context)

# ================= ОСНОВНОЕ МЕНЮ =================
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not update.message:
        return

    keyboard = [
        [KeyboardButton("🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟")],
        [
            KeyboardButton("👥 РЕФЕРАЛЫ", api_kwargs={"style": "primary"}),
            KeyboardButton("📋 ЗАДАНИЯ", api_kwargs={"style": "danger"})
        ],
        [KeyboardButton("ВЫВОД⭐", api_kwargs={"style": "success"})],
    ]
    
    if is_admin(uid):
        keyboard.append([KeyboardButton("⚙️ АДМИН-ПАНЕЛЬ", api_kwargs={"style": "danger"})])
    
    welcome_text = (
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Здесь ты можешь зарабатывать звезды⭐ просто нажимая кнопку\n\n"
        "💰 <b>Что нужно делать:</b>\n"
        "• Нажимай кнопку «Заработать звезды⭐» и выполняй простые задания\n"
        "• Приглашай друзей по реферальной ссылке и получай 3⭐ за каждого друга 🤑\n\n"
        "🚀 <b>Начни прямо сейчас и собирай звезды⭐!</b>\n\n"
        "👇 Используй кнопки ниже"
    )

    try:
        await update.message.reply_text(
            welcome_text,
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
    except telegram.error.Forbidden:
        pass

# ================= ПРОЦЕСС "ЗАРАБОТАТЬ ЗВЕЗДЫ" =================
def get_task_description(link: str) -> str:
    if "t.me/" in link:
        parts = link.split("t.me/")[-1].split("?")[0]
        if "bot" in parts.lower():
            return "🟢 Зайди в бота и нажми «Старт» (или начни диалог) 1 раз"
        else:
            return "🟢 Отправь заявку на вступление в канал и нажми «Подтвердить»"
    else:
        return "🟢 Перейди по ссылке и выполни действие (подпишись/лайкни/оставь комментарий)"

async def show_current_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = context.user_data.get("earning_tasks", [])
    current = context.user_data.get("earning_current", 0)

    if current >= len(tasks):
        await finish_earning(update, context)
        return

    task = tasks[current]
    link = task["link"]
    action_text = get_task_description(link)
    
    if "t.me/" in link and "bot" not in link.split("t.me/")[-1].split("?")[0].lower():
        text = (
            "💡 Получай Звёзды за простые задания! 👇\n\n"
            f"{action_text}\n\n"
            f"Вознаграждение: +{task['reward']}⭐"
        )
    else:
        text = (
            "💡 Получай Звёзды за простые задания! 👇\n\n"
            f"{action_text}\n"
            f"Ссылка: <a href='{link}'>клик</a>\n\n"
            f"Вознаграждение: +{task['reward']}⭐"
        )
    
    keyboard = [
        [
            InlineKeyboardButton("ПЕРЕЙТИ", url=link),
            InlineKeyboardButton("✅ ПРОВЕРИТЬ", callback_data=f"check_earning_{current}")
        ],
        [InlineKeyboardButton("⏩ ПРОПУСТИТЬ", callback_data=f"skip_earning_{current}")]
    ]

    last_msg_id = context.user_data.get("earning_last_msg_id")
    if last_msg_id:
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_msg_id)
        except Exception:
            pass

    msg = await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )
    context.user_data["earning_last_msg_id"] = msg.message_id

async def resend_current_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = context.user_data.get("earning_tasks", [])
    current = context.user_data.get("earning_current", 0)

    if not tasks or current >= len(tasks):
        await update.message.reply_text("🎉 У вас нет активных заданий. Нажмите кнопку снова для загрузки новых.")
        return

    task = tasks[current]
    link = task["link"]
    action_text = get_task_description(link)

    if "t.me/" in link and "bot" not in link.split("t.me/")[-1].split("?")[0].lower():
        text = (
            "💡 Получай Звёзды за простые задания! 👇\n\n"
            f"{action_text}\n\n"
            f"Вознаграждение: +{task['reward']}⭐"
        )
    else:
        text = (
            "💡 Получай Звёзды за простые задания! 👇\n\n"
            f"{action_text}\n"
            f"Ссылка: <a href='{link}'>клик</a>\n\n"
            f"Вознаграждение: +{task['reward']}⭐"
        )

    keyboard = [
        [
            InlineKeyboardButton("ПЕРЕЙТИ", url=link),
            InlineKeyboardButton("✅ ПРОВЕРИТЬ", callback_data=f"check_earning_{current}")
        ],
        [InlineKeyboardButton("⏩ ПРОПУСТИТЬ", callback_data=f"skip_earning_{current}")]
    ]

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )

async def start_earning_process(update: Update, context: ContextTypes.DEFAULT_TYPE, show_ref: bool = True):
    if context.user_data.get("earning_active"):
        await resend_current_task(update, context)
        return

    user = update.effective_user
    uid = user.id
    chat_id = update.effective_chat.id

    if show_ref:
        user_data = await db.get_user(uid)
        count = user_data["referrals_count"]
        link = f"t.me/{BOT_USERNAME}?start={uid}"
        ref_text = (
            "Получай +3 ⭐️ за каждого приглашенного друга! 🎉\n"
            "Приглашай по этой ссылке своих друзей, отправляй её во все чаты и зарабатывай Звёзды\n\n"
            f"<i>📎 Твоя реферальная ссылка:</i>\n"
            f"<code>{link}</code>\n\n"
            f"<b>Приглашено тобой: {count}</b>"
        )
        invite_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "📨 ОТПРАВИТЬ ССЫЛКУ ДРУЗЬЯМ",
                switch_inline_query=f"⭐ Зарабатывай звезды за простые задания! Присоединяйся: {link}"
            )
        ]])
        await update.message.reply_text(ref_text, parse_mode="HTML", reply_markup=invite_keyboard)
        await asyncio.sleep(0.1)

    user_lang = user.language_code or "ru"
    user_data_for_api = {
        "first_name": user.first_name,
        "username": user.username,
        "language_code": user_lang,
        "is_premium": user.is_premium,
    }
    response = await get_subgram_sponsors(uid, chat_id, user_data_for_api, action="subscribe")

    if not response:
        await update.message.reply_text("❌ Не удалось загрузить задания. Проверьте подключение к интернету.")
        return

    if response.get("status") != "warning":
        await update.message.reply_text("🎉 На данный момент нет доступных заданий. Загляните позже!")
        return

    sponsors = response.get("additional", {}).get("sponsors", [])
    if not sponsors:
        await update.message.reply_text("🎉 На данный момент нет доступных заданий. Загляните позже!")
        return

    tasks = []
    for sponsor in sponsors:
        if sponsor.get("available_now") and sponsor.get("status") == "unsubscribed":
            tasks.append({
                "link": sponsor.get("link"),
                "reward": TASK_REWARD,
                "type": sponsor.get("type", "channel")
            })

    if not tasks:
        await update.message.reply_text("🎉 На данный момент нет доступных заданий. Загляните позже!")
        return

    context.user_data["earning_tasks"] = tasks
    context.user_data["earning_current"] = 0
    context.user_data["earning_active"] = True
    context.user_data["earning_last_msg_id"] = None

    await show_current_task(update, context)

async def finish_earning(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("earning_active", None)
    context.user_data.pop("earning_tasks", None)
    context.user_data.pop("earning_current", None)
    last_msg_id = context.user_data.pop("earning_last_msg_id", None)
    if last_msg_id:
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_msg_id)
        except Exception:
            pass
    await update.message.reply_text("🎉 Все доступные задания выполнены! Возвращайся позже за новыми.")

# ================= АДМИН ПАНЕЛЬ =================
async def show_admin_panel(sender, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📢 Отправить сообщение всем", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⭐ Выдать звёзды пользователю", callback_data="admin_give_stars")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("◀️ Выйти из админки", callback_data="admin_exit")]
    ]
    text = "🔧 <b>Панель администратора</b>\n\nВыберите действие:"
    if hasattr(sender, 'edit_message_text'):
        await sender.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await sender.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    await show_admin_panel(update.message, context)

async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE, query: telegram.CallbackQuery):
    keyboard = [[InlineKeyboardButton("◀️ Отмена", callback_data="cancel_broadcast")]]
    await query.edit_message_text(
        "📝 Введите текст сообщения для рассылки (можно использовать HTML):\n\n"
        "Для отмены нажмите кнопку ниже.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data["admin_action"] = "broadcast"

async def cancel_broadcast(query: telegram.CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("admin_action", None)
    await query.edit_message_text("❌ Рассылка отменена.")
    await show_admin_panel(query.message, context)

async def admin_give_stars_list(update: Update, context: ContextTypes.DEFAULT_TYPE, query: telegram.CallbackQuery, page: int = 1):
    data = await db.get_all_users_sorted(page=page, per_page=10)
    users = data["users"]
    total = data["total"]
    current_page = data["page"]
    total_pages = data["pages"]

    if not users:
        text = "👥 Пользователи не найдены."
        await query.edit_message_text(text, parse_mode="HTML")
        return

    lines = ["⭐ <b>Выберите пользователя для начисления звёзд:</b>\n"]
    for u in users:
        user_id = u["user_id"]
        username = u["username"] or "нет"
        lines.append(f"• {username} | ID: <code>{user_id}</code>")
    text = "\n".join(lines)
    if total_pages > 1:
        text += f"\n\n📄 Страница {current_page} из {total_pages}"

    keyboard = []
    for u in users:
        user_id = u["user_id"]
        username = u["username"] or str(user_id)
        display = username[:30] if len(username) <= 30 else username[:27] + "..."
        keyboard.append([InlineKeyboardButton(f"{display} ({user_id})", callback_data=f"give_user_select_{user_id}")])

    nav_buttons = []
    if current_page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"give_user_list_{current_page-1}"))
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"give_user_list_{current_page+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("◀️ Отмена", callback_data="admin_panel_back")])

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data["admin_action"] = "give_user_select"

async def admin_give_stars_start(update: Update, context: ContextTypes.DEFAULT_TYPE, query: telegram.CallbackQuery):
    await admin_give_stars_list(update, context, query, page=1)

async def admin_stats_show(update: Update, context: ContextTypes.DEFAULT_TYPE, query: telegram.CallbackQuery):
    stats = await db.get_statistics()
    stats_text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👤 Всего пользователей: <b>{stats['total_users']}</b>\n\n"
        f"📈 Активность:\n"
        f"• За последний час: <b>{stats['active_hour']}</b>\n"
        f"• За последние 24 часа: <b>{stats['active_day']}</b>\n"
        f"• За последние 30 дней: <b>{stats['active_month']}</b>"
    )
    keyboard = [[InlineKeyboardButton("◀️ НАЗАД", callback_data="admin_panel_back")]]
    await query.edit_message_text(stats_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE, query: telegram.CallbackQuery, page: int = 1):
    data = await db.get_all_users_sorted(page=page, per_page=20)
    users = data["users"]
    total = data["total"]
    current_page = data["page"]
    total_pages = data["pages"]

    if not users:
        text = "👥 Пользователи не найдены."
    else:
        lines = ["👥 <b>Список пользователей</b>\n"]
        for u in users:
            user_id = u["user_id"]
            username = u["username"] or "нет"
            referrals = u["referrals_count"]
            lines.append(f"• {username} | ID: <code>{user_id}</code> | 👥 {referrals}")
        text = "\n".join(lines)
        if total_pages > 1:
            text += f"\n\n📄 Страница {current_page} из {total_pages}"

    keyboard = []
    nav_buttons = []
    if current_page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"admin_users_page_{current_page-1}"))
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"admin_users_page_{current_page+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("◀️ В админ-панель", callback_data="admin_panel_back")])

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_exit_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, query: telegram.CallbackQuery):
    context.user_data.pop("admin_action", None)
    await query.edit_message_text("👋 Вы вышли из админ-панели.")
    uid = update.effective_user.id
    keyboard = [
        [KeyboardButton("🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟")],
        [
            KeyboardButton("👥 РЕФЕРАЛЫ", api_kwargs={"style": "primary"}),
            KeyboardButton("📋 ЗАДАНИЯ", api_kwargs={"style": "danger"})
        ],
        [KeyboardButton("ВЫВОД⭐", api_kwargs={"style": "success"})],
    ]
    if is_admin(uid):
        keyboard.append([KeyboardButton("⚙️ АДМИН-ПАНЕЛЬ", api_kwargs={"style": "danger"})])
    await query.message.reply_text(
        "Главное меню",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

# ================= КОМАНДЫ АДМИНА =================
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    context.user_data["admin_action"] = "broadcast"
    keyboard = [[InlineKeyboardButton("◀️ Отмена", callback_data="cancel_broadcast")]]
    await update.message.reply_text(
        "📝 Введите текст сообщения для рассылки (можно использовать HTML):\n\n"
        "Для отмены нажмите кнопку ниже.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def give_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    await show_admin_panel(update.message, context)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    stats = await db.get_statistics()
    stats_text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👤 Всего пользователей: <b>{stats['total_users']}</b>\n\n"
        f"📈 Активность:\n"
        f"• За последний час: <b>{stats['active_hour']}</b>\n"
        f"• За последние 24 часа: <b>{stats['active_day']}</b>\n"
        f"• За последние 30 дней: <b>{stats['active_month']}</b>"
    )
    await update.message.reply_text(stats_text, parse_mode="HTML")

# ================= MESSAGE HANDLER =================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user = update.effective_user
    uid = user.id
    text = update.message.text.strip()
    username = user.username or ""

    await db.update_user_activity(uid, username)
    user_data = await db.get_user(uid)
    await db.update_limit(uid)

    if context.user_data.get("admin_action") == "give_amount":
        if text in ["🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟", "👥 РЕФЕРАЛЫ", "📋 ЗАДАНИЯ", "ВЫВОД⭐", "⚙️ АДМИН-ПАНЕЛЬ"] or text.startswith('/'):
            context.user_data.pop("admin_action", None)
            await update.message.reply_text("❌ Выдача звёзд отменена.")
            await show_main_menu(update, context)
            return
        await handle_give_amount_input(update, context)
        return

    if context.user_data.get("admin_action") == "broadcast":
        if text in ["🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟", "👥 РЕФЕРАЛЫ", "📋 ЗАДАНИЯ", "ВЫВОД⭐", "⚙️ АДМИН-ПАНЕЛЬ"] or text.startswith('/'):
            context.user_data.pop("admin_action", None)
            await update.message.reply_text("❌ Рассылка отменена.")
            await show_main_menu(update, context)
            return
        await handle_broadcast_input(update, context)
        return

    if context.user_data.get("admin_action") == "give_user_id":
        if text in ["🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟", "👥 РЕФЕРАЛЫ", "📋 ЗАДАНИЯ", "ВЫВОД⭐", "⚙️ АДМИН-ПАНЕЛЬ"] or text.startswith('/'):
            context.user_data.pop("admin_action", None)
            await update.message.reply_text("❌ Выдача звёзд отменена.")
            await show_main_menu(update, context)
            return
        await handle_give_user_id_input(update, context)
        return

    menu_buttons = ["🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟", "👥 РЕФЕРАЛЫ", "📋 ЗАДАНИЯ", "ВЫВОД⭐", "⚙️ АДМИН-ПАНЕЛЬ"]

    if context.user_data.get("waiting_link_for"):
        req_id = context.user_data["waiting_link_for"]
        if text in menu_buttons:
            context.user_data.pop("waiting_link_for", None)
        elif text.startswith(("http://", "https://")) and any(domain in text.lower() for domain in ["tiktok", "youtube", "instagram", "youtu.be"]):
            await handle_video_link(update, context, req_id, text)
            return
        else:
            return

    try:
        if text == "🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟":
            await start_earning_process(update, context, show_ref=True)
            return

        if text == "👥 РЕФЕРАЛЫ":
            link = f"t.me/{BOT_USERNAME}?start={uid}"
            count = user_data["referrals_count"]
            ref_earned = round(user_data.get("referral_earned", 0.0), 2)

            ref_text = (
                "👥 <b>РЕФЕРАЛЬНАЯ ПРОГРАММА</b> 👥\n\n"
                "🎁 <b>Бонусы за приглашения:</b>\n"
                f"   +{REF_BONUS_INVITER}⭐ — тебе за каждого друга!\n\n"
                "📊 <b>Статистика:</b>\n"
                f"   👤 Приглашено: <b>{count}</b>\n"
                f"   💰 Заработано с рефералов: <b>{ref_earned}⭐</b>\n\n"
                "🔗 <b>Твоя уникальная ссылка:</b>\n"
                f"<code>{link}</code>"
            )
            invite_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "📨 ПРИГЛАСИТЬ ДРУГА",
                    switch_inline_query=f"⭐ Лучший бот для заработка звёзд! 🚀✨\nПрисоединяйся: {link}"
                )
            ]])
            await update.message.reply_text(
                ref_text,
                parse_mode="HTML",
                reply_markup=invite_keyboard
            )
            return

        if text == "📋 ЗАДАНИЯ":
            await start_earning_process(update, context, show_ref=False)
            return

        if text == "ВЫВОД⭐":
            balance = round(user_data["balance"], 2)
            kb = [[InlineKeyboardButton("💸 ВЫВОД", callback_data="quick_withdraw", api_kwargs={"style": "success"})]]
            await update.message.reply_text(
                f"💰 <b>Баланс</b> <code>{balance}⭐</code>\n"
                f"📌 мин. <b>{MIN_WITHDRAW}⭐</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb)
            )
            return

        if text == "⚙️ АДМИН-ПАНЕЛЬ":
            if not is_admin(uid):
                await update.message.reply_text("⛔ Доступ запрещён.")
                return
            await show_admin_panel(update.message, context)
            return

        if is_admin(uid):
            if text == "📢 Отправить сообщение всем":
                context.user_data["admin_action"] = "broadcast"
                keyboard = [[InlineKeyboardButton("◀️ Отмена", callback_data="cancel_broadcast")]]
                await update.message.reply_text(
                    "📝 Введите текст сообщения для рассылки (можно использовать HTML):\n\n"
                    "Для отмены нажмите кнопку ниже.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            elif text == "⭐ Выдать звёзды пользователю":
                await admin_give_stars_list(update, context, update.message, page=1)
                return
            elif text == "📊 Статистика":
                await handle_statistics(update, context)
                return
            elif text == "◀️ Выйти из админки":
                await exit_admin(update, context)
                return

    except telegram.error.Forbidden:
        pass
    except Exception as e:
        logger.error(f"Error in message_handler: {e}")
        try:
            await update.message.reply_text("❌ <b>Произошла внутренняя ошибка. Попробуйте позже.</b>", parse_mode="HTML")
        except telegram.error.Forbidden:
            pass

# ================= АДМИНСКИЕ ФУНКЦИИ =================
async def handle_broadcast_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        context.user_data.clear()
        return

    message_text = update.message.text
    await update.message.reply_text("⏳ Начинаю рассылку...")

    user_ids = await db.get_all_user_ids()
    sent = 0
    failed = 0
    batch_size = 20

    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i+batch_size]
        results = await asyncio.gather(*[
            safe_send_message(context, uid, message_text, parse_mode="HTML") for uid in batch
        ], return_exceptions=True)
        for res in results:
            if isinstance(res, Exception) or res is None:
                failed += 1
            else:
                sent += 1

    await update.message.reply_text(
        f"✅ Рассылка завершена!\n📨 Отправлено: <b>{sent}</b>\n❌ Не удалось отправить: <b>{failed}</b>",
        parse_mode="HTML"
    )
    context.user_data.clear()
    await show_admin_panel(update.message, context)

async def handle_give_user_id_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        context.user_data.clear()
        return

    user_input = update.message.text.strip()
    target_uid = None
    if user_input.isdigit():
        target_uid = int(user_input)
    else:
        username = user_input.lstrip('@')
        async with aiosqlite.connect(DB_PATH) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT user_id FROM users WHERE username = ?", (username,))
            row = await cursor.fetchone()
            if row:
                target_uid = row["user_id"]
        if not target_uid:
            await update.message.reply_text(f"❌ Пользователь с username @{username} не найден в базе.")
            context.user_data.pop("admin_action", None)
            await show_admin_panel(update.message, context)
            return

    context.user_data["give_target"] = target_uid
    context.user_data["admin_action"] = "give_amount"
    await update.message.reply_text(f"💰 Введите количество звёзд для выдачи пользователю <code>{target_uid}</code>:", parse_mode="HTML")

async def handle_give_amount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        context.user_data.clear()
        return

    try:
        amount = float(update.message.text.strip())
        target_uid = context.user_data["give_target"]

        await db.update_balance(target_uid, amount, is_withdraw=False, is_referral_bonus=False)
        user_data = await db.get_user(target_uid)

        await update.message.reply_text(
            f"✅ Пользователю <b>{target_uid}</b> успешно начислено <b>{amount}⭐</b>.\n💰 Текущий баланс: <b>{round(user_data['balance'], 2)}⭐</b>",
            parse_mode="HTML"
        )

        await safe_send_message(
            context, target_uid,
            f"🎁 Вам начислено <b>{amount}⭐</b> администратором!",
            parse_mode="HTML"
        )

    except ValueError:
        await update.message.reply_text("❌ Некорректная сумма. Введите число.")
        return
    except Exception as e:
        await update.message.reply_text(f"❌ Произошла ошибка: {e}")
        return

    context.user_data.clear()
    await show_admin_panel(update.message, context)

async def handle_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    stats = await db.get_statistics()
    stats_text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👤 Всего пользователей: <b>{stats['total_users']}</b>\n\n"
        f"📈 Активность:\n"
        f"• За последний час: <b>{stats['active_hour']}</b>\n"
        f"• За последние 24 часа: <b>{stats['active_day']}</b>\n"
        f"• За последние 30 дней: <b>{stats['active_month']}</b>"
    )
    await update.message.reply_text(stats_text, parse_mode="HTML")

async def exit_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    context.user_data.clear()
    keyboard = [
        [KeyboardButton("🌟 ЗАРАБОТАТЬ ЗВЕЗДЫ 🌟")],
        [
            KeyboardButton("👥 РЕФЕРАЛЫ", api_kwargs={"style": "primary"}),
            KeyboardButton("📋 ЗАДАНИЯ", api_kwargs={"style": "danger"})
        ],
        [KeyboardButton("ВЫВОД⭐", api_kwargs={"style": "success"})],
    ]
    if is_admin(uid):
        keyboard.append([KeyboardButton("⚙️ АДМИН-ПАНЕЛЬ", api_kwargs={"style": "danger"})])
    await update.message.reply_text(
        "👋 Вы вышли из админ-панели.",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

# ================= CALLBACK HANDLER =================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data

    try:
        if context.user_data.get("waiting_link_for") and not data.startswith("sendlink_"):
            context.user_data.pop("waiting_link_for", None)

        user_data = await db.get_user(uid)

        if data.startswith("check_earning_"):
            task_index = int(data.split("_")[2])
            tasks = context.user_data.get("earning_tasks")
            current = context.user_data.get("earning_current")

            if tasks is None or current != task_index:
                await query.answer("❌ Задание устарело. Начните процесс заново.", show_alert=True)
                return

            processing_key = f"processing_{task_index}"
            if context.user_data.get(processing_key):
                await query.answer("⏳ Подождите, проверка уже выполняется...", show_alert=False)
                return
            context.user_data[processing_key] = True

            try:
                task = tasks[task_index]

                user = update.effective_user
                user_lang = user.language_code or "ru"
                user_data_for_api = {
                    "first_name": user.first_name,
                    "username": user.username,
                    "language_code": user_lang,
                    "is_premium": user.is_premium,
                }
                
                await invalidate_subgram_cache(uid, action="subscribe")
                await asyncio.sleep(5)
                
                response = await get_subgram_sponsors(uid, query.message.chat.id, user_data_for_api, action="subscribe")

                logger.info(f"SubGram response for user {uid}: {response}")

                if not response:
                    await query.answer("❌ Ошибка связи. Попробуйте позже.", show_alert=True)
                    return

                if response.get("status") == "warning":
                    sponsors = response.get("additional", {}).get("sponsors", [])
                    if len(sponsors) > task_index:
                        sponsor = sponsors[task_index]
                        if sponsor.get("status") == "unsubscribed":
                            await query.answer("❌ Вы не выполнили условия.", show_alert=True)
                            return
                        elif sponsor.get("status") == "subscribed":
                            reward = task["reward"]
                            await db.update_balance(uid, reward, is_withdraw=False, is_referral_bonus=False)

                            success_text = (
                                f"✅ <b>Задание выполнено!</b>\n\n"
                                f"Получено: +{reward}⭐️\n\n"
                                "❗️Не отписывайся от канала в течение как минимум 3 дней. "
                                "В противном случае, ты получишь штраф или блокировку аккаунта."
                            )
                            try:
                                await query.edit_message_text(
                                    success_text,
                                    parse_mode="HTML",
                                    reply_markup=None
                                )
                            except telegram.error.BadRequest as e:
                                if "Message is not modified" not in str(e):
                                    raise
                            except Exception as e:
                                logger.error(f"Failed to edit message: {e}")

                            await invalidate_subgram_cache(uid, action="subscribe")

                            context.user_data.pop("earning_last_msg_id", None)
                            context.user_data["earning_current"] = current + 1
                            await show_next_task_from_callback(query, context)
                            return
                    else:
                        await query.answer("❌ Ошибка проверки задания", show_alert=True)
                elif response.get("status") == "ok":
                    reward = task["reward"]
                    await db.update_balance(uid, reward, is_withdraw=False, is_referral_bonus=False)
                    success_text = (
                        f"✅ <b>Задание выполнено!</b>\n\n"
                        f"Получено: +{reward}⭐️\n\n"
                        "❗️Не отписывайся от канала в течение как минимум 3 дней. "
                        "В противном случае, ты получишь штраф или блокировку аккаунта."
                    )
                    try:
                        await query.edit_message_text(
                            success_text,
                            parse_mode="HTML",
                            reply_markup=None
                        )
                    except telegram.error.BadRequest as e:
                        if "Message is not modified" not in str(e):
                            raise
                    except Exception as e:
                        logger.error(f"Failed to edit message: {e}")

                    await invalidate_subgram_cache(uid, action="subscribe")

                    context.user_data.pop("earning_last_msg_id", None)
                    context.user_data["earning_current"] = current + 1
                    await show_next_task_from_callback(query, context)
                else:
                    await query.answer("❌ Не удалось проверить подписку. Попробуйте позже.", show_alert=True)
            finally:
                context.user_data.pop(processing_key, None)
            return

        if data.startswith("skip_earning_"):
            task_index = int(data.split("_")[2])
            tasks = context.user_data.get("earning_tasks")
            current = context.user_data.get("earning_current")

            if tasks is None or current != task_index:
                await query.answer("❌ Задание устарело. Начните процесс заново.", show_alert=True)
                return

            await query.answer("⏩ Задание пропущено", show_alert=False)
            last_msg_id = context.user_data.get("earning_last_msg_id")
            if last_msg_id:
                try:
                    await context.bot.delete_message(chat_id=query.message.chat.id, message_id=last_msg_id)
                except Exception:
                    pass
            context.user_data.pop("earning_last_msg_id", None)
            context.user_data["earning_current"] = current + 1
            await show_next_task_from_callback(query, context)
            return

        # Админ-панель (callback)
        if data == "admin_broadcast":
            await admin_broadcast_start(update, context, query)
            return
        if data == "cancel_broadcast":
            await cancel_broadcast(query, context)
            return
        if data == "admin_give_stars":
            await admin_give_stars_start(update, context, query)
            return
        if data == "admin_stats":
            await admin_stats_show(update, context, query)
            return
        if data == "admin_users":
            await admin_users_list(update, context, query, page=1)
            return
        if data.startswith("admin_users_page_"):
            page = int(data.split("_")[-1])
            await admin_users_list(update, context, query, page=page)
            return
        if data == "admin_exit":
            await admin_exit_panel(update, context, query)
            return
        if data == "admin_panel_back":
            await show_admin_panel(query.message, context)
            return

        # Выбор пользователя для выдачи звёзд
        if data.startswith("give_user_list_"):
            page = int(data.split("_")[-1])
            await admin_give_stars_list(update, context, query, page=page)
            return
        if data.startswith("give_user_select_"):
            target_uid = int(data.split("_")[-1])
            context.user_data["give_target"] = target_uid
            context.user_data["admin_action"] = "give_amount"
            await query.edit_message_text(
                f"💰 Введите количество звёзд для выдачи пользователю <code>{target_uid}</code>:",
                parse_mode="HTML"
            )
            return

        # Вывод средств
        if data == "quick_withdraw":
            balance = user_data["balance"]
            if balance < MIN_WITHDRAW:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"❌ <b>Недостаточно средств для вывода!</b>\nМинимальная сумма: <b>{MIN_WITHDRAW}⭐</b>",
                    parse_mode="HTML"
                )
                return

            kb = [
                [InlineKeyboardButton("15⭐", callback_data="withdraw_15", api_kwargs={"style": "primary"}),
                 InlineKeyboardButton("25⭐", callback_data="withdraw_25", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("50⭐", callback_data="withdraw_50", api_kwargs={"style": "primary"}),
                 InlineKeyboardButton("100⭐", callback_data="withdraw_100", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("250⭐", callback_data="withdraw_250", api_kwargs={"style": "primary"}),
                 InlineKeyboardButton("500⭐", callback_data="withdraw_500", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("1000⭐", callback_data="withdraw_1000", api_kwargs={"style": "primary"}),
                 InlineKeyboardButton("2500⭐", callback_data="withdraw_2500", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("5000⭐", callback_data="withdraw_5000", api_kwargs={"style": "primary"}),
                 InlineKeyboardButton("10000⭐", callback_data="withdraw_10000", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("◀️ НАЗАД", callback_data="back_to_balance", api_kwargs={"style": "danger"})]
            ]
            await query.edit_message_text(
                "💸 <b>ВЫБЕРИ СУММУ ДЛЯ ВЫВОДА:</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb)
            )
            return

        if data == "back_to_balance":
            balance = round(user_data["balance"], 2)
            kb = [[InlineKeyboardButton("💸 ВЫВОД", callback_data="quick_withdraw", api_kwargs={"style": "success"})]]
            await query.edit_message_text(
                f"💰 <b>Баланс</b> <code>{balance}⭐</code>\n"
                f"📌 мин. <b>{MIN_WITHDRAW}⭐</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb)
            )
            return

        if data.startswith("withdraw_"):
            try:
                amount = int(data.split("_")[1])
            except:
                return

            if amount < MIN_WITHDRAW:
                await query.edit_message_text(f"❌ Минимальная сумма вывода — <b>{MIN_WITHDRAW}⭐</b>", parse_mode="HTML")
                return

            if user_data["balance"] < amount:
                await query.edit_message_text("❌ <b>Недостаточно средств!</b>", parse_mode="HTML")
                return

            request_id = generate_request_id()

            await db.update_balance(uid, amount, is_withdraw=True)
            await db.add_frozen_balance(request_id, uid, amount)
            await db.add_withdraw_request(request_id, uid, amount, "pending_channels")

            user_obj = await context.bot.get_chat(uid)
            raw_name = user_obj.username or user_obj.first_name or f"id{uid}"
            masked_display = mask_username(raw_name)
            user_display = f"@{raw_name}" if user_obj.username else masked_display

            await query.edit_message_text(
                f"✔️ <b>Заявка отправлена!</b>\n\n"
                f"💰 <b>Сумма:</b> <code>{amount}⭐</code>\n"
                f"👤 <b>Получатель:</b> {get_display_name(user_obj)}\n\n"
                f"⚠️ Модератор скоро просмотрит вашу заявку, ожидайте.\n"
                f"‼️ <i>Если вы отпишетесь от каналов, заявка будет автоматически отклонена</i> ‼️",
                parse_mode="HTML",
                reply_markup=None
            )

            await safe_send_message(
                context, uid,
                text=(
                    f"📢 Ваша заявка выложена.\n"
                    f"🔍 <a href='{WITHDRAW_CHANNEL_LINK}'>посмотреть свою заявку</a>"
                ),
                parse_mode="HTML",
                disable_web_page_preview=True
            )

            mod_msg = (
                f"⌛️ Создана заявка на вывод!\n\n"
                f"👤 Пользователь: {user_display}\n"
                f"⭐️ Количество: {amount} Звёзд\n\n"
                f"👉 @{BOT_USERNAME}"
            )
            try:
                await context.bot.send_message(
                    chat_id=MODERATION_CHAT_ID,
                    text=mod_msg,
                    parse_mode="HTML"
                )
            except Exception:
                pass

            await send_video_task(update, context, uid, request_id, is_callback=True)
            return

        if data.startswith("sendlink_"):
            req_id = data.split("_", 1)[1]
            request = await db.get_withdraw_request(req_id)

            if not request or request["user_id"] != uid or request["status"] != "pending_video":
                await query.edit_message_text("❌ <b>Заявка не найдена или уже обработана.</b>", parse_mode="HTML")
                return

            context.user_data["waiting_link_for"] = req_id
            await query.edit_message_text(
                "📎 <b>Отправьте ссылку на ваше видео</b>",
                parse_mode="HTML"
            )
            return

    except telegram.error.Forbidden:
        pass
    except Exception as e:
        logger.error(f"Error in button_handler: {e}")
        try:
            await query.edit_message_text("❌ <b>Произошла внутренняя ошибка. Попробуйте позже.</b>", parse_mode="HTML")
        except Exception:
            pass

async def show_next_task_from_callback(query: telegram.CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    tasks = context.user_data.get("earning_tasks", [])
    current = context.user_data.get("earning_current", 0)

    if current >= len(tasks):
        await finish_earning_from_callback(query, context)
        return

    task = tasks[current]
    link = task["link"]
    action_text = get_task_description(link)

    if "t.me/" in link and "bot" not in link.split("t.me/")[-1].split("?")[0].lower():
        text = (
            "💡 Получай Звёзды за простые задания! 👇\n\n"
            f"{action_text}\n\n"
            f"Вознаграждение: +{task['reward']}⭐"
        )
    else:
        text = (
            "💡 Получай Звёзды за простые задания! 👇\n\n"
            f"{action_text}\n"
            f"Ссылка: <a href='{link}'>клик</a>\n\n"
            f"Вознаграждение: +{task['reward']}⭐"
        )

    keyboard = [
        [
            InlineKeyboardButton("ПЕРЕЙТИ", url=link),
            InlineKeyboardButton("✅ ПРОВЕРИТЬ", callback_data=f"check_earning_{current}")
        ],
        [InlineKeyboardButton("⏩ ПРОПУСТИТЬ", callback_data=f"skip_earning_{current}")]
    ]

    last_msg_id = context.user_data.get("earning_last_msg_id")
    if last_msg_id:
        try:
            await context.bot.delete_message(chat_id=query.message.chat.id, message_id=last_msg_id)
        except Exception:
            pass

    msg = await query.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )
    context.user_data["earning_last_msg_id"] = msg.message_id

async def finish_earning_from_callback(query: telegram.CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("earning_active", None)
    context.user_data.pop("earning_tasks", None)
    context.user_data.pop("earning_current", None)
    last_msg_id = context.user_data.pop("earning_last_msg_id", None)
    if last_msg_id:
        try:
            await context.bot.delete_message(chat_id=query.message.chat.id, message_id=last_msg_id)
        except Exception:
            pass
    await query.message.reply_text("🎉 Все доступные задания выполнены! Возвращайся позже за новыми.")

# ================= ФУНКЦИЯ ОТПРАВКИ ЗАДАНИЯ НА ВИДЕО =================
async def send_video_task(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int, req_id: str, is_callback: bool = False):
    request = await db.get_withdraw_request(req_id)
    if request and request["status"] == "pending_channels":
        await db.update_withdraw_request(req_id, status="pending_video")
        await db.update_withdraw_request(req_id, subscription_ok=1)

    video_task_msg = (
        "📹 <b>ПОСЛЕДНЕЕ ЗАДАНИЕ ДЛЯ ВЫВОДА:</b>\n\n"
        "1️⃣ Снимите короткое видео (TikTok, YouTube Shorts, Instagram Reels) с упоминанием нашего бота.\n"
        "2️⃣ Видео должно набрать <b>минимум 500 просмотров</b>.\n"
        "3️⃣ Если видео наберёт <b>1000+ просмотров</b>, вы получите бонус: +10% от суммы вывода за каждые 500 просмотров свыше 1000 (макс 50%).\n\n"
        "💡 Примеры видео вы можете посмотреть в нашем <a href='https://t.me/TapEasyStars'>канале</a>.\n\n"
        "📤 Когда видео готово, нажмите кнопку ниже, чтобы отправить ссылку."
    )
    kb = [[InlineKeyboardButton("📤 ОТПРАВИТЬ ССЫЛКУ НА ВИДЕО", callback_data=f"sendlink_{req_id}", api_kwargs={"style": "primary"})]]

    await safe_send_message(
        context, uid,
        text=video_task_msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb)
    )

# ================= ОБРАБОТКА ССЫЛКИ =================
async def handle_video_link(update: Update, context: ContextTypes.DEFAULT_TYPE, req_id: str, link: str):
    uid = update.effective_user.id

    request = await db.get_withdraw_request(req_id)
    if not request or request["user_id"] != uid or request["status"] != "pending_video":
        await update.message.reply_text("❌ <b>Заявка не найдена или уже обработана.</b>", parse_mode="HTML")
        context.user_data.pop("waiting_link_for", None)
        return

    await db.update_withdraw_request(req_id, video_link=link, status="pending_review", submitted_at=time.time())
    context.user_data.pop("waiting_link_for", None)

    await update.message.reply_text(
        "✅ <b>Ссылка получена!</b>\n\n"
        "📬 Она отправлена модераторам на проверку.\n"
        "⏳ Ожидайте ответа (обычно в течение 24 часов).\n\n"
        "🔍 Вы можете отслеживать статус в канале:",
        parse_mode="HTML"
    )

    check_kb = [[InlineKeyboardButton("🔍 ПРОВЕРИТЬ ЗАЯВКУ", url=f"{WITHDRAW_CHANNEL_LINK}", api_kwargs={"style": "primary"})]]
    await update.message.reply_text(
        "Нажмите кнопку ниже, чтобы перейти в канал 👇",
        reply_markup=InlineKeyboardMarkup(check_kb)
    )

# ================= ФЕЙКОВЫЕ ЗАЯВКИ =================
async def fake_withdraw_notification(context: ContextTypes.DEFAULT_TYPE):
    r = random.random()
    if r < 0.01:
        amount = 100
    elif r < 0.10:
        amount = 50
    elif r < 0.30:
        amount = 25
    else:
        amount = 15
    
    username = generate_fake_username()
    masked_username = mask_username(username)

    msg = (
        f"✅ Отправлена выплата!\n\n"
        f"👤 Пользователь: {masked_username}\n"
        f"⭐️ Количество: {amount} Звёзд\n\n"
        f"👉 @{BOT_USERNAME}"
    )
    try:
        await context.bot.send_message(
            chat_id=MODERATION_CHAT_ID,
            text=msg,
            parse_mode="HTML"
        )
    except Exception:
        pass

    next_interval = random.randint(60, 300)
    context.job_queue.run_once(fake_withdraw_notification, next_interval, name="fake_withdraw")

# ================= ПЕРИОДИЧЕСКАЯ ОЧИСТКА КЭША =================
async def clear_caches_periodically(context: ContextTypes.DEFAULT_TYPE):
    user_cache.clear()
    subgram_cache.clear()
    logger.info("User and SubGram caches cleared")

# ================= KEEP-ALIVE ДЛЯ RENDER =================
def keep_alive():
    """Пингует бота каждые 10 минут, чтобы он не уснул на Render"""
    hostname = os.getenv("RENDER_EXTERNAL_HOSTNAME", "localhost")
    url = f"https://{hostname}" if hostname != "localhost" else "http://localhost"
    while True:
        try:
            if hostname != "localhost":
                requests.get(url, timeout=5)
                logger.info("Keep-alive ping sent")
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")
        time.sleep(600)  # 10 минут

def run_health_server():
    """Простой веб-сервер для health check на Render"""
    health_app = Flask(__name__)
    
    @health_app.route('/')
    def health():
        return "Bot is running!", 200
    
    @health_app.route('/health')
    def health_check():
        return "OK", 200
    
    port = int(os.environ.get('PORT', 8080))
    health_app.run(host='0.0.0.0', port=port)

# ================= ЗАПУСК =================
def main():
    # Запускаем health сервер и keep-alive на Render
    if os.getenv("RENDER_EXTERNAL_HOSTNAME"):
        threading.Thread(target=run_health_server, daemon=True).start()
        threading.Thread(target=keep_alive, daemon=True).start()
        print("✅ Render mode: health server + keep-alive activated")
        logger.info("Render mode activated with health server")
    
    print(f"🔢 Максимальный ID заявок из базы: {last_request_id} (следующий будет {last_request_id + 1})")
    print("\n" + "="*60)
    print("🚀 БОТ ЗАПУЩЕН (ОПТИМИЗИРОВАННАЯ ВЕРСИЯ С SUBGRAM)")
    print("="*60)
    print("✅ Добавлена кнопка 'АДМИН-ПАНЕЛЬ' в главном меню (видна только админам)")
    print("✅ Админ-панель работает через инлайн-кнопки")
    print("✅ Выдача звёзд по ID или @username")
    print("✅ Статистика активности: за час, день, месяц")
    print("✅ Команды /broadcast, /give, /stats, /admin сохранены")
    print("✅ Убраны номера заявок в канале и сообщениях")
    print("✅ Новый формат сообщений для заявок")
    print("✅ Видео-задание появляется отдельным сообщением")
    print("✅ Добавлена возможность отмены рассылки и выдачи звёзд")
    print("✅ Добавлена кнопка 'Пользователи' с пагинацией и сортировкой по username")
    print("✅ Добавлена навигация 'Назад' в админ-панели")
    print("✅ При выдаче звёзд теперь показывается список пользователей с кнопками")
    print("✅ Фейковые выплаты: 15⭐ – 70%, 25⭐ – 20%, 50⭐ – 9%, 100⭐ – 1%")
    print("✅ Имена для фейковых выводов генерируются реалистично, всегда не короче 5 символов, без цифр в конце (кроме стиля first_number)")
    print("✅ Ускорена загрузка заданий (убрана задержка после реферальной ссылки)")
    print("✅ Улучшено описание заданий для разных типов ссылок")
    print("✅ При повторном нажатии на кнопку 'Заработать звезды' или 'Задания' бот отправляет новое сообщение с текущим заданием")
    print("✅ При проверке задания, если не выполнено, показывается модальное окно 'Вы не выполнили условия.'")
    print("✅ Добавлена принудительная инвалидация кэша SubGram перед проверкой задания")
    print("✅ Установлен time_purge = 30 минут (1800 секунд) – оптимально для частого выполнения заданий")
    print("✅ Задержка перед проверкой увеличена до 5 секунд")
    print("✅ При проверке задания используется action='subscribe' для более надёжной фиксации статуса")
    print("✅ Добавлено логирование ответа SubGram для отладки")
    print("✅ Длина фейковых ников теперь от 5 до 8 символов")
    print("✅ Кнопка 'Рефералы' и 'Задания' на одной строке, кнопка 'Вывод⭐' отдельно снизу")
    print("✅ Команда /start запускает задания без реферальной ссылки и показывает клавиатуру")
    print("✅ Кнопки имеют цвета: Вывод⭐ – зелёный, Рефералы – синий, Задания – красный")
    print("✅ Добавлена защита от двойного нажатия на кнопку 'Проверить'")
    print("✅ ОПТИМИЗАЦИИ: увеличенные кэши (user 20000, subgram 2000), индексы в БД, пакетная рассылка")
    print("✅ В видео-задание добавлена ссылка на канал с примерами")
    print("="*60)
    print("⚠️ Для остановки нажмите Ctrl+C")
    print("="*60 + "\n")

    app = ApplicationBuilder().token(TOKEN).concurrent_updates(50).build()

    if app.job_queue:
        app.job_queue.run_repeating(clear_caches_periodically, interval=3600, first=3600)
        app.job_queue.run_once(fake_withdraw_notification, 30, name="fake_withdraw_first")

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("give", give_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if isinstance(context.error, telegram.error.Forbidden):
            return
        logger.error(f"Unhandled error: {context.error}")

    app.add_error_handler(error_handler)

    try:
        app.run_polling()
    except KeyboardInterrupt:
        print("\n" + "="*60)
        print("🛑 Бот остановлен")
        print("="*60)
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    main()
