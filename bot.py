#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import threading
import time
import sqlite3
from datetime import datetime, timedelta, timezone
import uuid
import re
import vobject

import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil import tz
from websocket import create_connection, WebSocketConnectionClosedException

try:
    import caldav
except ImportError:
    caldav = None
    
try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:
    Fernet = None
    InvalidToken = Exception
    
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "DISABLED")
FERNET = None
if ENCRYPTION_KEY and ENCRYPTION_KEY != "DISABLED":
    if Fernet is None:
        raise RuntimeError("cryptography не установлена, а ENCRYPTION_KEY задан. Установи пакет `cryptography` или убери ENCRYPTION_KEY.")
    # ENCRYPTION_KEY должен быть urlsafe-base64 ключом, сгенерированным Fernet.generate_key()
    FERNET = Fernet(ENCRYPTION_KEY.encode("utf-8"))

ENCRYPTION_MISCONFIGURED = False

MENTION_RE = re.compile(r"@([a-zA-Z0-9._-]+)")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

DB_PATH = os.getenv("DB_PATH", "./calendar_bot.db")

from contextlib import contextmanager

@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    try:
        yield conn
    finally:
        conn.close()

MATTERMOST_BASE_URL = os.getenv("MATTERMOST_BASE_URL", "https://your-mattermost-url.com")
MATTERMOST_BOT_TOKEN = os.getenv("MATTERMOST_BOT_TOKEN", "REPLACE_ME")
BOT_USER_ID = None
BOT_USERNAME = os.getenv("MATTERMOST_BOT_USERNAME", "calendar_bot")
CALDAV_BASE_URL = os.getenv("CALDAV_BASE_URL", "https://calendar.mail.ru")
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
# ALLOWED_EMAILS — список разрешённых e-mail.
# Если пусто → бот доступен для всех пользователей.
raw_allowed = os.getenv("ALLOWED_EMAILS", "").strip()

if not raw_allowed:
    # Пусто → разрешены все e-mail
    ALLOWED_EMAILS = None
else:
    # Разделители: запятая, пробел, точка с запятой, перевод строки
    ALLOWED_EMAILS = {
        e.strip().lower()
        for e in re.split(r"[,\s;]+", raw_allowed)
        if e.strip()
    }

def is_email_allowed(email: str) -> bool:
    if not email:
        return False
    if ALLOWED_EMAILS is None:
        return True  # всем можно
    return email.strip().lower() in ALLOWED_EMAILS

def allowed_emails_human() -> str:
    if ALLOWED_EMAILS is None:
        return "любой пользователь"
    emails = sorted(ALLOWED_EMAILS)
    return ", ".join(emails)

base_actions_url = os.getenv("MM_ACTIONS_URL")
if base_actions_url:
    base_actions_url = base_actions_url.rstrip("/")
    MM_ACTIONS_URL = base_actions_url + "/mattermost/actions"
else:
    MM_ACTIONS_URL = "https://your-bot-url.example.com/mattermost/actions"

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=TZ_NAME)

CALDAV_PRINCIPAL_PATH = os.getenv("CALDAV_PRINCIPAL_PATH", "/principals/")

def build_principal_path_from_email(email: str) -> str:
    base = CALDAV_PRINCIPAL_PATH or "/principals/"
    base = base.rstrip("/")

    if "@" not in email:
        return base + "/"

    localpart, domain = email.split("@", 1)
    localpart = localpart.strip()
    domain = domain.strip()

    return f"{base}/{domain}/{localpart}/"

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mattermost_user_id TEXT UNIQUE,
                email TEXT,
                caldav_password TEXT,
                state TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS meeting_drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mattermost_user_id TEXT,
                step TEXT,
                title TEXT,
                date TEXT,
                time TEXT,
                duration_min INTEGER,
                participants TEXT,
                description TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS event_detail_posts (
                mattermost_user_id TEXT PRIMARY KEY,
                post_id TEXT,
                updated_at TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_alarms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mattermost_user_id TEXT,
                event_uid TEXT,
                alarm_time TEXT,
                created_at TEXT,
                UNIQUE(mattermost_user_id, event_uid, alarm_time)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mattermost_user_id TEXT,
                uid TEXT,
                start TEXT,
                end TEXT,
                status TEXT,
                summary TEXT,
                updated_at TEXT,
                UNIQUE(mattermost_user_id, uid)
            )
            """
        )
        conn.commit()

def create_draft(mattermost_user_id, step="ASK_TITLE"):
    now = datetime.now(timezone.utc).isoformat()
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO meeting_drafts
            (mattermost_user_id, step, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (mattermost_user_id, step, now, now),
        )
        draft_id = c.lastrowid
        conn.commit()
    return draft_id

def get_active_draft(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT id, step, title, date, time, duration_min, participants, description
            FROM meeting_drafts
            WHERE mattermost_user_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (mattermost_user_id,),
        )
        row = c.fetchone()

    if not row:
        return None

    return {
        "id": row[0],
        "step": row[1],
        "title": row[2],
        "date": row[3],
        "time": row[4],
        "duration_min": row[5],
        "participants": row[6],
        "description": row[7],
    }

def update_draft(draft_id, **fields):
    if not fields:
        return

    now = datetime.now(timezone.utc).isoformat()
    set_parts = []
    values = []

    for k, v in fields.items():
        set_parts.append(f"{k} = ?")
        values.append(v)

    set_parts.append("updated_at = ?")
    values.append(now)
    values.append(draft_id)

    sql = f"UPDATE meeting_drafts SET {', '.join(set_parts)} WHERE id = ?"

    with db_conn() as conn:
        c = conn.cursor()
        c.execute(sql, values)
        conn.commit()

def delete_draft(draft_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM meeting_drafts WHERE id = ?", (draft_id,))
        conn.commit()

def set_last_detail_post(mattermost_user_id, post_id):
    now = datetime.now(timezone.utc).isoformat()
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO event_detail_posts (mattermost_user_id, post_id, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(mattermost_user_id)
            DO UPDATE SET post_id = excluded.post_id, updated_at = excluded.updated_at
            """,
            (mattermost_user_id, post_id, now),
        )
        conn.commit()

def get_last_detail_post(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT post_id FROM event_detail_posts WHERE mattermost_user_id = ?",
            (mattermost_user_id,),
        )
        row = c.fetchone()

    return row[0] if row else None

def clear_last_detail_post(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "DELETE FROM event_detail_posts WHERE mattermost_user_id = ?",
            (mattermost_user_id,),
        )
        conn.commit()

def alarm_already_sent(mattermost_user_id, event_uid, alarm_dt):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT 1 FROM sent_alarms
            WHERE mattermost_user_id = ? AND event_uid = ? AND alarm_time = ?
            """,
            (mattermost_user_id, event_uid, alarm_dt.isoformat()),
        )
        row = c.fetchone()

    return row is not None

def mark_alarm_sent(mattermost_user_id, event_uid, alarm_dt):
    now = datetime.now(timezone.utc).isoformat()
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR IGNORE INTO sent_alarms
            (mattermost_user_id, event_uid, alarm_time, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (mattermost_user_id, event_uid, alarm_dt.isoformat(), now),
        )
        conn.commit()

def get_primary_calendar(email, password):
    client, principal = get_caldav_client(email, password)
    calendars = principal.calendars()

    if not calendars:
        raise RuntimeError("No calendars found for user")

    preferred_names = ["Main", "Основной"]
    selected = None
    for c in calendars:
        name = getattr(c, "name", None)
        if name in preferred_names:
            selected = c
            break

    if selected is None:
        selected = calendars[0]

    return selected

def get_user(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT mattermost_user_id, email, caldav_password, state "
            "FROM users WHERE mattermost_user_id = ?",
            (mattermost_user_id,),
        )
        row = c.fetchone()

    if not row:
        return None

    raw_pwd = row[2]

    # Авто-миграция: если шифрование включено, пароль есть,
    # и он ещё не в формате enc:... → зашифровать и обновить в БД.
    if FERNET is not None and raw_pwd and not raw_pwd.startswith("enc:"):
        encrypted = FERNET.encrypt(raw_pwd.encode("utf-8")).decode("utf-8")
        encrypted = f"enc:{encrypted}"
        with db_conn() as conn:
            c = conn.cursor()
            c.execute(
                "UPDATE users SET caldav_password = ?, updated_at = ? "
                "WHERE mattermost_user_id = ?",
                (encrypted, datetime.now(timezone.utc).isoformat(), mattermost_user_id),
            )
            conn.commit()
        # во внешнем коде всё равно нужен открытый пароль
        decrypted_pwd = raw_pwd
    else:
        # обычная ветка: либо уже enc:..., либо шифрование выключено,
        # либо пароля нет
        decrypted_pwd = decrypt_secret(raw_pwd) if raw_pwd else None

    return {
        "mattermost_user_id": row[0],
        "email": row[1],
        "caldav_password": decrypted_pwd,
        "state": row[3],
    }

def logout_user(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM meeting_drafts WHERE mattermost_user_id = ?", (mattermost_user_id,))
        c.execute("DELETE FROM event_detail_posts WHERE mattermost_user_id = ?", (mattermost_user_id,))
        c.execute("DELETE FROM sent_alarms WHERE mattermost_user_id = ?", (mattermost_user_id,))
        c.execute("DELETE FROM users WHERE mattermost_user_id = ?", (mattermost_user_id,))
        conn.commit()

def check_encryption_misconfiguration():
    """
    Если шифрование отключено (ENCRYPTION_KEY='DISABLED'), но в базе уже есть
    пароли с префиксом enc:, считаем конфигурацию некорректной и
    отключаем бота.
    """
    global ENCRYPTION_MISCONFIGURED

    # Если шифрование включено — всё ок
    if FERNET is not None:
        ENCRYPTION_MISCONFIGURED = False
        return

    # Шифрование выключено, проверяем базу
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute(
                "SELECT caldav_password FROM users "
                "WHERE caldav_password LIKE 'enc:%' "
                "LIMIT 1"
            )
            row = c.fetchone()
    except Exception:
        ENCRYPTION_MISCONFIGURED = False
        return

    ENCRYPTION_MISCONFIGURED = row is not None
        
def encrypt_secret(value: str) -> str:
    """
    Шифрует строку. Если FERNET не настроен или значение пустое – возвращаем как есть.
    Храним в БД в виде: "enc:<token>".
    """
    if not value:
        return ""
    if FERNET is None:
        return value
    token = FERNET.encrypt(value.encode("utf-8"))
    return "enc:" + token.decode("utf-8")


def decrypt_secret(value: str) -> str:
    """
    Расшифровывает строку из БД.
    Если нет префикса "enc:" или FERNET не настроен – возвращаем как есть.
    При ошибке расшифровки – возвращаем пустую строку.
    """
    if not value:
        return ""
    if not value.startswith("enc:"):
        return value
    if FERNET is None:
        return value

    token = value[4:].encode("utf-8")
    try:
        return FERNET.decrypt(token).decode("utf-8")
    except InvalidToken:
        return ""

def upsert_user(mattermost_user_id, email, caldav_password=None, state="NEW"):
    now = datetime.now(timezone.utc).isoformat()
    existing = get_user(mattermost_user_id)

    encrypted_pwd = None
    if caldav_password is not None:
        encrypted_pwd = encrypt_secret(caldav_password)

    with db_conn() as conn:
        c = conn.cursor()
        if existing:
            c.execute(
                """
                UPDATE users
                SET email = ?, caldav_password = ?, state = ?, updated_at = ?
                WHERE mattermost_user_id = ?
                """,
                (email, encrypted_pwd, state, now, mattermost_user_id),
            )
        else:
            c.execute(
                """
                INSERT INTO users (mattermost_user_id, email, caldav_password, state, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (mattermost_user_id, email, encrypted_pwd, state, now, now),
            )
        conn.commit()

def get_all_ready_users():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT mattermost_user_id, email, caldav_password FROM users WHERE state = 'READY'"
        )
        rows = c.fetchall()

    users = []
    for row in rows:
        raw_pwd = row[2]
        decrypted_pwd = decrypt_secret(raw_pwd) if raw_pwd else None
        users.append(
            {
                "mattermost_user_id": row[0],
                "email": row[1],
                "caldav_password": decrypted_pwd,
            }
        )
    return users

def mm_headers():
    return {
        "Authorization": f"Bearer {MATTERMOST_BOT_TOKEN}",
        "Content-Type": "application/json",
    }

def mm_get(path):
    url = MATTERMOST_BASE_URL.rstrip("/") + path
    resp = requests.get(url, headers=mm_headers())
    resp.raise_for_status()
    return resp.json()

def mm_post(path, data):
    url = MATTERMOST_BASE_URL.rstrip("/") + path
    resp = requests.post(url, headers=mm_headers(), json=data)
    resp.raise_for_status()
    return resp.json()

def mm_put(path, data):
    url = MATTERMOST_BASE_URL.rstrip("/") + path
    resp = requests.put(url, headers=mm_headers(), json=data)
    resp.raise_for_status()
    return resp.json()

def mm_get_users_by_usernames(usernames):
    usernames = [u.strip() for u in usernames if u and u.strip()]
    if not usernames:
        return {}

    try:
        users = mm_post("/api/v4/users/usernames", usernames)
    except Exception:
        return {}

    mapping = {}
    for u in users:
        uname = (u.get("username") or "").strip().lower()
        email = (u.get("email") or "").strip()
        if uname and email:
            mapping[uname] = email
    return mapping

def mm_update_post_raw(post_id, message=None, props=None):
    data = {"id": post_id}
    if message is not None:
        data["message"] = message
    if props is not None:
        data["props"] = props
    return mm_put(f"/api/v4/posts/{post_id}", data)

def mm_get_post(post_id):
    return mm_get(f"/api/v4/posts/{post_id}")

def clear_post_buttons(post_id):
    if not post_id:
        return
    try:
        post = mm_get_post(post_id)
        msg = post.get("message", "")
        mm_update_post_raw(post_id, message=msg, props={})
    except Exception:
        pass
    
def clear_last_bot_buttons_in_channel(channel_id):
    try:
        data = mm_get(f"/api/v4/channels/{channel_id}/posts?page=0&per_page=30")
    except Exception:
        return

    order = data.get("order", [])
    posts = data.get("posts", {})

    for pid in order:
        post = posts.get(pid) or {}
        if post.get("user_id") != BOT_USER_ID:
            continue

        props = post.get("props") or {}
        attachments = props.get("attachments") or []
        if not attachments:
            continue

        # Не трогаем главное меню
        first = attachments[0] or {}
        if first.get("text") == "Главное меню":
            continue

        clear_post_buttons(pid)
        break

def mm_get_me():
    return mm_get("/api/v4/users/me")

def init_bot_identity():
    global BOT_USER_ID
    me = mm_get_me()
    BOT_USER_ID = me["id"]

def mm_update_post(post_id, message):
    data = {
        "id": post_id,
        "message": message,
    }
    return mm_put(f"/api/v4/posts/{post_id}", data)

def mm_get_user(user_id):
    return mm_get(f"/api/v4/users/{user_id}")

def mm_get_channel(channel_id):
    return mm_get(f"/api/v4/channels/{channel_id}")

def mm_send_dm(user_id, message, props=None):
    if not BOT_USER_ID:
        raise RuntimeError("BOT_USER_ID is not initialized")

    data = [BOT_USER_ID, user_id]
    channel = mm_post("/api/v4/channels/direct", data)
    channel_id = channel["id"]

    post_data = {"channel_id": channel_id, "message": message}
    if props:
        post_data["props"] = props

    return mm_post("/api/v4/posts", post_data)

def mm_send_long_dm(user_id, text, chunk_size=3500):
    while text:
        part = text[:chunk_size]
        text = text[chunk_size:]
        mm_send_dm(user_id, part)

def build_cancel_action():
    return {
        "name": "Отмена",
        "style": "danger",
        "integration": {
            "url": MM_ACTIONS_URL,
            "context": {"action": "cancel_meeting"},
        },
    }

def build_cancel_only_props():
    return {
        "attachments": [
            {
                "text": "",
                "actions": [build_cancel_action()],
            }
        ]
    }

def build_event_rsvp_props(uid):
    return {
        "attachments": [
            {
                "text": "Ответить на приглашение:",
                "actions": [
                    {
                        "name": "Принять",
                        "style": "primary",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {
                                "action": "event_rsvp",
                                "choice": "ACCEPTED",
                                "uid": uid,
                            },
                        },
                    },
                    {
                        "name": "Возможно",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {
                                "action": "event_rsvp",
                                "choice": "TENTATIVE",
                                "uid": uid,
                            },
                        },
                    },
                    {
                        "name": "Отклонить",
                        "style": "danger",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {
                                "action": "event_rsvp",
                                "choice": "DECLINED",
                                "uid": uid,
                            },
                        },
                    },
                ],
            }
        ]
    }

def build_participants_step_props():
    return {
        "attachments": [
            {
                "text": "",
                "actions": [
                    {
                        "name": "Не выбирать",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {"action": "skip_participants"},
                        },
                    },
                    build_cancel_action(),
                ],
            }
        ]
    }

def build_description_step_props():
    return {
        "attachments": [
            {
                "text": "",
                "actions": [
                    {
                        "name": "Не добавлять",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {"action": "skip_description"},
                        },
                    },
                    build_cancel_action(),
                ],
            }
        ]
    }

def build_location_step_props():
    return {
        "attachments": [
            {
                "text": "",
                "actions": [
                    {
                        "name": "Не добавлять",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {"action": "skip_location"},
                        },
                    },
                    build_cancel_action(),
                ],
            }
        ]
    }

def build_main_menu_props():
    integration_url = MM_ACTIONS_URL

    attachments = [
        {
            "text": "Главное меню",
            "actions": [
                {
                    "name": "Сводка на сегодня",
                    "integration": {
                        "url": integration_url,
                        "context": {"action": "summary_today"},
                    },
                },
                {
                    "name": "Текущие / будущие встречи сегодня",
                    "integration": {
                        "url": integration_url,
                        "context": {"action": "summary_today_future"},
                    },
                },
                {
                    "name": "Создать встречу",
                    "integration": {
                        "url": integration_url,
                        "context": {"action": "create_meeting"},
                    },
                },
                {
                    "name": "Разлогиниться",
                    "style": "danger",
                    "integration": {
                        "url": integration_url,
                        "context": {"action": "logout_confirm"},
                    },
                },
            ],
        }
    ]

    return {"attachments": attachments}

def build_logout_confirm_props():
    return {
        "attachments": [
            {
                "text": "",
                "actions": [
                    {
                        "name": "Да",
                        "style": "danger",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {"action": "logout_yes"},
                        },
                    },
                    {
                        "name": "Нет",
                        "style": "primary",
                        "integration": {
                            "url": MM_ACTIONS_URL,
                            "context": {"action": "logout_no"},
                        },
                    },
                ],
            }
        ]
    }

def send_main_menu(user_id):
    clear_last_detail_post(user_id)
    props = build_main_menu_props()
    mm_send_dm(user_id, "Выбери действие:", props=props)

def get_caldav_client(email, password):
    if caldav is None:
        raise RuntimeError("Модуль caldav не установлен")

    base_url = CALDAV_BASE_URL.rstrip("/")
    principal_path = build_principal_path_from_email(email)
    principal_url = base_url + principal_path

    client = caldav.DAVClient(
        url=base_url,
        username=email,
        password=password,
    )

    principal = caldav.Principal(client=client, url=principal_url)
    return client, principal

def get_events_for_tracking(email, password):
    """
    Сырые события для трекинга изменений.
    Берём диапазон: вчера..+30 дней.
    Не фильтруем STATUS=CANCELLED.
    """
    if caldav is None:
        return []

    tz_local = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_local)

    start_range = (now_local - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_range = (now_local + timedelta(days=30)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )

    cal = get_primary_calendar(email, password)
    events = cal.date_search(start_range, end_range)
    result = []

    for event in events:
        try:
            vevent = event.vobject_instance.vevent
        except Exception:
            continue

        summary = getattr(vevent, "summary", None)
        description = getattr(vevent, "description", None)
        dtstart = vevent.dtstart.value
        dtend_prop = getattr(vevent, "dtend", None)
        dtend = dtend_prop.value if dtend_prop else None

        uid_prop = getattr(vevent, "uid", None)
        uid = uid_prop.value if uid_prop else None
        if not uid:
            continue

        status_prop = getattr(vevent, "status", None)
        status = status_prop.value.upper() if status_prop else "CONFIRMED"

        desc_val = description.value if description else ""

        url_prop = getattr(vevent, "url", None)
        url = url_prop.value if url_prop else None

        attendees = []
        for comp in vevent.contents.get("attendee", []):
            val = comp.value
            if isinstance(val, str) and val.lower().startswith("mailto:"):
                val = val[7:]

            params = getattr(comp, "params", {}) or {}
            partstats = params.get("PARTSTAT") or params.get("partstat") or ["NEEDS-ACTION"]
            a_status = str(partstats[0]).upper()

            attendees.append(
                {
                    "email": val,
                    "status": a_status,
                }
            )

        if not isinstance(dtstart, datetime):
            continue
        if dtstart.tzinfo is None:
            dtstart = dtstart.replace(tzinfo=tz_local)
        if dtend and dtend.tzinfo is None:
            dtend = dtend.replace(tzinfo=tz_local)

        result.append(
            {
                "uid": uid,
                "summary": summary.value if summary else "(без названия)",
                "description": desc_val,
                "start": dtstart,
                "end": dtend,
                "url": url,
                "attendees": attendees,
                "status": status,
            }
        )

    # сортируем по старту
    result.sort(key=lambda e: e["start"])
    return result

def get_today_events(email, password, only_future=False):
    if caldav is None:
        return []

    tz_local = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_local)
    start_day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = start_day + timedelta(days=1)

    cal = get_primary_calendar(email, password)
    events = cal.date_search(start_day, end_day)
    result = []

    for idx, event in enumerate(events):
        try:
            vevent = event.vobject_instance.vevent
        except Exception:
            continue

        summary = getattr(vevent, "summary", None)
        description = getattr(vevent, "description", None)
        dtstart = vevent.dtstart.value
        dtend_prop = getattr(vevent, "dtend", None)
        dtend = dtend_prop.value if dtend_prop else None

        uid_prop = getattr(vevent, "uid", None)
        uid = uid_prop.value if uid_prop else None
        
        # --- НОВОЕ: фильтруем отменённые события ---
        status_prop = getattr(vevent, "status", None)
        status = status_prop.value.upper() if status_prop else "CONFIRMED"
        if status == "CANCELLED":
            # пропускаем отменённые инстансы/встречи
            continue
        # --- КОНЕЦ НОВОГО БЛОКА ---

        desc_val = description.value if description else ""

        url_prop = getattr(vevent, "url", None)
        url = url_prop.value if url_prop else None

        attendees = []
        for comp in vevent.contents.get("attendee", []):
            val = comp.value
            if isinstance(val, str) and val.lower().startswith("mailto:"):
                val = val[7:]

            params = getattr(comp, "params", {}) or {}
            partstats = params.get("PARTSTAT") or params.get("partstat") or ["NEEDS-ACTION"]
            status = str(partstats[0]).upper()

            attendees.append(
                {
                    "email": val,
                    "status": status,
                }
            )

        if not isinstance(dtstart, datetime):
            continue
        if dtstart.tzinfo is None:
            dtstart = dtstart.replace(tzinfo=tz_local)
        if dtend and dtend.tzinfo is None:
            dtend = dtend.replace(tzinfo=tz_local)

        alarms = []
        for alarm in vevent.contents.get("valarm", []):
            trig = getattr(alarm, "trigger", None)
            if not trig:
                continue
            trig_val = trig.value
            alarm_dt = None
            if isinstance(trig_val, datetime):
                if trig_val.tzinfo is None:
                    alarm_dt = trig_val.replace(tzinfo=tz_local)
                else:
                    alarm_dt = trig_val.astimezone(tz_local)
            elif isinstance(trig_val, timedelta):
                alarm_dt = dtstart + trig_val
            elif isinstance(trig_val, str):
                m = re.match(r"-PT(\d+)M", trig_val)
                if m:
                    minutes = int(m.group(1))
                    alarm_dt = dtstart - timedelta(minutes=minutes)
                else:
                    m = re.match(r"-PT(\d+)H", trig_val)
                    if m:
                        hours = int(m.group(1))
                        alarm_dt = dtstart - timedelta(hours=hours)

            if alarm_dt:
                if alarm_dt.tzinfo is None:
                    alarm_dt = alarm_dt.replace(tzinfo=tz_local)
                else:
                    alarm_dt = alarm_dt.astimezone(tz_local)
                alarms.append(alarm_dt)

        if only_future:
            if dtend and dtend < now_local:
                continue
            if not dtend and dtstart < now_local:
                continue

        result.append(
            {
                "uid": uid,
                "summary": summary.value if summary else "(без названия)",
                "description": desc_val,
                "start": dtstart,
                "end": dtend,
                "url": url,
                "attendees": attendees,
                "alarms": alarms,
            }
        )

    result.sort(key=lambda e: e["start"])
    return result

def debug_dump_caldav_events(user_id):
    user = get_user(user_id)
    if not user or user["state"] != "READY":
        mm_send_dm(user_id, "Сначала нужно авторизоваться.")
        return

    email = user["email"]
    pwd = user["caldav_password"]

    try:
        cal = get_primary_calendar(email, pwd)
    except Exception as e:
        mm_send_dm(user_id, f"Ошибка при получении календаря: {e}")
        return

    try:
        tz_local = tz.gettz(TZ_NAME)
        now_local = datetime.now(tz_local)
        start_day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = start_day + timedelta(days=1)

        events = cal.date_search(start_day, end_day)
    except Exception as e:
        mm_send_dm(user_id, f"Ошибка при загрузке событий за сегодня: {e}")
        return

    if not events:
        mm_send_dm(user_id, "На сегодня событий в календаре не найдено.")
        return

    chunks = []
    for i, ev in enumerate(events, 1):
        try:
            raw = ev.data  # сырой iCalendar текст от CalDAV
        except Exception as e:
            raw = f"<ошибка чтения ev.data: {e}>"
        chunks.append(f"===== EVENT #{i} =====\n{raw}")

    full_text = "\n\n".join(chunks)
    mm_send_long_dm(user_id, full_text)

def resolve_participants_from_text(text: str):
    if not text:
        return []

    emails = {m.strip().lower() for m in EMAIL_RE.findall(text)}

    usernames = {m.strip().lower() for m in MENTION_RE.findall(text)}

    if usernames:
        username_map = mm_get_users_by_usernames(list(usernames))
        for uname, email in username_map.items():
            if email:
                emails.add(email.strip().lower())

    return sorted(emails)

def create_calendar_event_from_draft(email, password, draft):
    tz_local = tz.gettz(TZ_NAME)

    try:
        date_obj = datetime.strptime(draft["date"], "%Y-%m-%d").date()
        time_obj = datetime.strptime(draft["time"], "%H:%M").time()
    except Exception as e:
        raise ValueError(f"Неверный формат даты/времени в черновике: {e}")

    start_dt = datetime.combine(date_obj, time_obj)
    start_dt = start_dt.replace(tzinfo=tz_local)

    duration_min = draft["duration_min"] or 30
    end_dt = start_dt + timedelta(minutes=duration_min)

    title = draft["title"] or "(без названия)"
    description = (draft.get("description") or "").strip()
    location = (draft.get("location") or "").strip()

    participants_raw = draft["participants"] or ""
    participants = []
    for part in re.split(r"[,\s]+", participants_raw):
        p = part.strip()
        if p:
            participants.append(p)

    cal = get_primary_calendar(email, password)

    vcal = vobject.iCalendar()
    vevent = vcal.add("vevent")
    vevent.add("uid").value = str(uuid.uuid4())
    vevent.add("summary").value = title
    vevent.add("dtstart").value = start_dt
    vevent.add("dtend").value = end_dt

    if description:
        vevent.add("description").value = description

    if location:
        vevent.add("location").value = location

    organizer = vevent.add("organizer")
    organizer.value = f"mailto:{email}"
    organizer.params["CN"] = [email]

    for addr in participants:
        att = vevent.add("attendee")
        att.value = f"mailto:{addr}"
        att.params["CN"] = [addr]
        att.params["ROLE"] = ["REQ-PARTICIPANT"]

    ical_str = vcal.serialize()
    cal.add_event(ical_str)

    return {
        "title": title,
        "start": start_dt,
        "end": end_dt,
        "participants": participants,
        "description": description,
    }

def start_create_meeting_flow(user_id):
    user = get_user(user_id)
    if not user or user["state"] != "READY":
        mm_send_dm(user_id, "Сначала нужно авторизоваться в календаре.")
        return

    create_draft(user_id, step="ASK_TITLE")
    mm_send_dm(
        user_id,
        "Давай создадим встречу.\n\nКак назвать встречу? Напиши название одним сообщением.",
        props=build_cancel_only_props(),
    )

def send_date_choice_menu(user_id):
    integration_url = MM_ACTIONS_URL

    attachments = [
        {
            "text": "Выбери дату встречи:",
            "actions": [
                {
                    "name": "Сегодня",
                    "integration": {
                        "url": integration_url,
                        "context": {
                            "action": "create_meeting_pick_date",
                            "choice": "today",
                        },
                    },
                },
                {
                    "name": "Завтра",
                    "integration": {
                        "url": integration_url,
                        "context": {
                            "action": "create_meeting_pick_date",
                            "choice": "tomorrow",
                        },
                    },
                },
                {
                    "name": "Послезавтра",
                    "integration": {
                        "url": integration_url,
                        "context": {
                            "action": "create_meeting_pick_date",
                            "choice": "after_tomorrow",
                        },
                    },
                },
                {
                    "name": "Другая дата",
                    "integration": {
                        "url": integration_url,
                        "context": {
                            "action": "create_meeting_pick_date",
                            "choice": "custom",
                        },
                    },
                },
                build_cancel_action(),
            ],
        }
    ]

    props = {"attachments": attachments}
    mm_send_dm(user_id, "Выбери дату встречи:", props=props)

def format_events_summary_with_select(events, title="Встречи на сегодня"):
    if not events:
        return f"### {title}\n\nНа сегодня встреч нет 👌", None

    def escape_md(text: str) -> str:
        return text.replace("|", "\\|")

    def one_line(text: str) -> str:
        t = re.sub(r"[\r\n\t]+", " ", text)
        t = re.sub(r"\s{2,}", " ", t)
        return t.strip()

    lines = []
    lines.append(f"### {title}\n")
    lines.append("| Название | Когда |")
    lines.append("|----------|-------|")

    options = []
    events_ctx = []

    for idx, ev in enumerate(events):
        start = ev["start"]
        end = ev["end"]

        when = format_when(start, end)

        summary = ev.get("summary") or "(без названия)"
        summary_clean = one_line(summary)
        summary_md = escape_md(summary_clean)

        lines.append(f"| {summary_md} | {when} |")

        description = (ev.get("description") or "").strip()
        description = one_line(description) if description else ""
        if len(description) > 400:
            description = description[:397] + "…"

        attendees = ev.get("attendees") or []
        url = ev.get("url") or ""

        events_ctx.append(
            {
                "title": summary_clean,
                "when_human": when,
                "attendees": attendees,
                "description": description,
                "url": url,
            }
        )

        option_text = summary_clean
        if len(option_text) > 80:
            option_text = option_text[:77] + "…"

        options.append(
            {
                "text": option_text,
                "value": str(idx),
            }
        )

    text = "\n".join(lines)

    integration_url = MM_ACTIONS_URL

    props = {
        "attachments": [
            {
                "text": "Выбери встречу, чтобы посмотреть подробности:",
                "actions": [
                    {
                        "name": "Встреча",
                        "type": "select",
                        "options": options,
                        "integration": {
                            "url": integration_url,
                            "context": {
                                "action": "show_event_details_select",
                                "events": events_ctx,
                            },
                        },
                    }
                ],
            }
        ]
    }

    return text, props

def format_when(start, end):
    if end and isinstance(end, datetime) and start.date() == end.date():
        return f"{start.strftime('%d.%m.%Y %H:%M')}–{end.strftime('%H:%M')}"
    elif end and isinstance(end, datetime):
        return f"{start.strftime('%d.%m.%Y %H:%M')}–{end.strftime('%d.%m.%Y %H:%M')}"
    else:
        return start.strftime("%d.%m.%Y %H:%M")

def load_tracked_events_for_user(mattermost_user_id):
    """
    Загружаем сохранённое состояние событий пользователя.
    Возвращаем dict[uid] -> запись.
    """
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT uid, start, end, status, summary
            FROM tracked_events
            WHERE mattermost_user_id = ?
            """,
            (mattermost_user_id,),
        )
        rows = c.fetchall()

    res = {}
    for uid, start, end, status, summary in rows:
        res[uid] = {
            "uid": uid,
            "start": start,
            "end": end,
            "status": status,
            "summary": summary,
        }
    return res


def upsert_tracked_event(mattermost_user_id, ev):
    """
    ev: dict с ключами uid, start, end, status, summary
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    start_str = ev["start"].isoformat() if isinstance(ev["start"], datetime) else str(ev["start"])
    end_val = ev.get("end")
    end_str = end_val.isoformat() if isinstance(end_val, datetime) else (end_val or "")

    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO tracked_events (mattermost_user_id, uid, start, end, status, summary, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mattermost_user_id, uid)
            DO UPDATE SET
                start = excluded.start,
                end = excluded.end,
                status = excluded.status,
                summary = excluded.summary,
                updated_at = excluded.updated_at
            """,
            (
                mattermost_user_id,
                ev["uid"],
                start_str,
                end_str,
                ev.get("status") or "",
                ev.get("summary") or "",
                now_iso,
            ),
        )
        conn.commit()

STATUS_EMOJI = {
    "ACCEPTED": "✅",
    "DECLINED": "❌",
    "TENTATIVE": "❓",
    "NEEDS-ACTION": "⏳",
}

def format_event_details(title, when_human, attendees, description, url, header_prefix=None):
    lines = []

    if header_prefix:
        lines.append(header_prefix)

    title = title or "(без названия)"
    lines.append(f"**{title}**")

    if when_human:
        lines.append(f"Когда: {when_human}")

    if attendees:
        lines.append("\nУчастники:")
        for a in attendees:
            if isinstance(a, dict):
                email = a.get("email") or ""
                status = (a.get("status") or "NEEDS-ACTION").upper()
            else:
                email = str(a)
                status = "NEEDS-ACTION"

            emoji = STATUS_EMOJI.get(status, "⏳")
            lines.append(f"- {email} {emoji}")
    else:
        lines.append("\nУчастники: —")

    description = (description or "").strip()
    if description:
        lines.append("\nОписание:")
        lines.append(description)
    else:
        lines.append("\nОписание: —")

    # Поле "Где" — может содержать URL, адрес или любой текст
    location = (url or "").strip()  # url используется как поле location

    if location:
        lines.append("\nГде:")
        # Разбиваем по переносам, чтобы поддержать многострочные локации
        for part in location.splitlines():
            lines.append(part)
    else:
        lines.append("\nГде: —")

        return "\n".join(lines)

def handle_show_event_details_select(user_id, payload):
    context = payload.get("context", {}) or {}

    value = context.get("selected_option")
    if value is None:
        value = payload.get("selected_option")

    if value is None:
        return

    try:
        idx = int(value)
    except ValueError:
        mm_send_dm(user_id, "Некорректный индекс встречи.")
        return

    events_ctx = context.get("events") or []
    if not isinstance(events_ctx, list) or idx < 0 or idx >= len(events_ctx):
        mm_send_dm(user_id, "Выбранная встреча не найдена.")
        return

    ev = events_ctx[idx]

    title = ev.get("title")
    when_human = ev.get("when_human") or ""
    attendees = ev.get("attendees") or []
    description = ev.get("description") or ""
    url = ev.get("url") or ""

    text = format_event_details(title, when_human, attendees, description, url)

    last_post_id = get_last_detail_post(user_id)

    if last_post_id:
        try:
            mm_update_post(last_post_id, text)
            return
        except Exception:
            clear_last_detail_post(user_id)

    post = mm_send_dm(user_id, text)
    detail_post_id = post.get("id")
    if detail_post_id:
        set_last_detail_post(user_id, detail_post_id)

def handle_show_event_details(user_id, context):
    title = context.get("title")
    when_human = context.get("when_human") or ""
    attendees = context.get("attendees") or []
    description = context.get("description") or ""
    url = context.get("url") or ""

    text = format_event_details(title, when_human, attendees, description, url)
    mm_send_dm(user_id, text)

def handle_event_rsvp(user_id, uid, choice):
    """
    choice: 'ACCEPTED', 'DECLINED', 'TENTATIVE'
    """
    user = get_user(user_id)
    if not user or user["state"] != "READY":
        mm_send_dm(user_id, "Сначала нужно авторизоваться.")
        return

    if choice not in ("ACCEPTED", "DECLINED", "TENTATIVE"):
        mm_send_dm(user_id, "Неизвестный ответ на приглашение.")
        return

    email = user["email"]
    pwd = user["caldav_password"]

    try:
        updated = update_event_partstat(email, pwd, uid, choice)
    except Exception:
        updated = False

    if updated:
        human = {
            "ACCEPTED": "приняли",
            "DECLINED": "отклонили",
            "TENTATIVE": "ответили «возможно»",
        }[choice]
        mm_send_dm(user_id, f"Ок, вы {human} приглашение на встречу.")
    else:
        mm_send_dm(
            user_id,
            "Не получилось обновить статус встречи в календаре. "
            "Возможно, CalDAV не даёт изменить это событие."
        )


def update_event_partstat(email, password, uid, new_status):
    """
    Пытаемся найти событие по UID и обновить PARTSTAT для текущего пользователя.
    Возвращает True при успехе, False при ошибке/отсутствии.
    """
    if caldav is None:
        return False

    client, principal = get_caldav_client(email, password)
    cal = get_primary_calendar(email, password)

    try:
        events = cal.events()
    except Exception:
        return False

    updated = False

    for ev in events:
        try:
            vcal = ev.vobject_instance
        except Exception:
            continue

        changed_any = False

        # На случай, если в одном ресурсе несколько VEVENT
        for comp in vcal.components():
            if comp.name != "VEVENT":
                continue

            uid_prop = getattr(comp, "uid", None)
            if not uid_prop or uid_prop.value != uid:
                continue

            attendees = comp.contents.get("attendee", [])
            for att in attendees:
                val = att.value
                if isinstance(val, str) and val.lower().startswith("mailto:"):
                    addr = val[7:]
                else:
                    addr = val

                if addr.lower() != email.lower():
                    continue

                att.params["PARTSTAT"] = [new_status]
                changed_any = True

        if changed_any:
            try:
                ev.save()
                updated = True
            except Exception:
                continue

    return updated

def handle_meeting_draft_step(user_id, channel_id, user, draft, text):
    step = draft["step"]
    txt = text.strip()

    if txt.lower() in ("отмена", "/cancel", "cancel", "стоп", "/stop"):
        delete_draft(draft["id"])
        mm_send_dm(user_id, "Создание встречи отменено.")
        return True

    if step == "ASK_TITLE":
        if not txt:
            clear_last_bot_buttons_in_channel(channel_id)
            mm_send_dm(
                user_id,
                "Название встречи не может быть пустым. Напиши любое название.",
                props=build_cancel_only_props(),
            )
            return True

        update_draft(draft["id"], title=txt, step="ASK_DATE")
        clear_last_bot_buttons_in_channel(channel_id)
        mm_send_dm(user_id, f"Ок, встреча будет называться:\n**{txt}**")
        send_date_choice_menu(user_id)
        return True

    if step == "ASK_CUSTOM_DATE":
        try:
            date_obj = datetime.strptime(txt, "%d.%m.%Y").date()
        except ValueError:
            clear_last_bot_buttons_in_channel(channel_id)
            mm_send_dm(
                user_id,
                "Не понял дату. Введи, пожалуйста, в формате **DD.MM.YYYY**, например `21.11.2025`.",
                props=build_cancel_only_props(),
            )
            return True

        update_draft(draft["id"], date=date_obj.isoformat(), step="ASK_TIME")
        clear_last_bot_buttons_in_channel(channel_id)
        mm_send_dm(
            user_id,
            f"Дата встречи: {date_obj.strftime('%d.%m.%Y')}.\n\nВо сколько начать? Формат HH:MM (24 часа).",
            props=build_cancel_only_props(),
        )
        return True

    if step == "ASK_TIME":
        try:
            _ = datetime.strptime(txt, "%H:%M").time()
        except ValueError:
            clear_last_bot_buttons_in_channel(channel_id)
            mm_send_dm(
                user_id,
                "Не понял время. Введи, пожалуйста, в формате **HH:MM**, например `14:30`.",
                props=build_cancel_only_props(),
            )
            return True

        update_draft(draft["id"], time=txt, step="ASK_DURATION")
        clear_last_bot_buttons_in_channel(channel_id)
        mm_send_dm(
            user_id,
            "Сколько длится встреча? В минутах. Например: `30` или `60`.",
            props=build_cancel_only_props(),
        )
        return True

    if step == "ASK_DURATION":
        try:
            duration_min = int(txt)
            if duration_min <= 0 or duration_min > 1440:
                raise ValueError()
        except ValueError:
            clear_last_bot_buttons_in_channel(channel_id)
            mm_send_dm(
                user_id,
                "Не понял длительность. Введи число минут, например `30` или `60`.",
                props=build_cancel_only_props(),
            )
            return True

        update_draft(draft["id"], duration_min=duration_min, step="ASK_PARTICIPANTS")
        clear_last_bot_buttons_in_channel(channel_id)
        mm_send_dm(
            user_id,
            "Кого пригласить на встречу?\n"
            "Можно указывать участников в любом формате:\n"
            "• @username — бот сам найдёт e-mail\n"
            "• email@example.com — можно несколько через запятую или с новой строки\n\n"
            "Пример:\n"
            "@ivanov, @petrova\n"
            "external@mail.com\n\n"
            "Если никого не нужно приглашать, нажми кнопку «Не выбирать».",
            props=build_participants_step_props(),
        )
        return True

    if step == "ASK_PARTICIPANTS":
        if txt.lower() in ("нет", "нет.", "no", "none"):
            participants = ""
        else:
            emails = resolve_participants_from_text(txt)
            participants = ", ".join(emails) if emails else ""

        update_draft(draft["id"], participants=participants, step="ASK_DESCRIPTION")
        clear_last_bot_buttons_in_channel(channel_id)
        mm_send_dm(
            user_id,
            "Добавь описание встречи (повестка и т.п.).\n"
            "Если не нужно — нажми кнопку «Не добавлять».",
            props=build_description_step_props(),
        )
        return True

    if step == "ASK_DESCRIPTION":
        description = "" if txt.lower() in ("нет", "нет.", "no", "none") else txt
        update_draft(draft["id"], description=description, step="ASK_LOCATION")
        clear_last_bot_buttons_in_channel(channel_id)
        mm_send_dm(
            user_id,
            "Добавь ссылку на встречу.\n"
            "Если не нужно — нажми кнопку «Не добавлять».",
            props=build_location_step_props(),
        )
        return True

    if step == "ASK_LOCATION":
        location = "" if txt.lower() in ("нет", "нет.", "no", "none") else txt
        update_draft(draft["id"], step="CREATING")
        clear_last_bot_buttons_in_channel(channel_id)
        try:
            event_info = create_calendar_event_from_draft(
                user["email"],
                user["caldav_password"],
                {**draft, "location": location},
            )
        except Exception:
            import traceback
            traceback.print_exc()
            mm_send_dm(
                user_id,
                "⚠️ Не удалось создать встречу в календаре. "
                "Проверь, пожалуйста, корректность даты/времени и попробуй ещё раз.",
            )
            update_draft(draft["id"], step="ASK_LOCATION")
            return True

        delete_draft(draft["id"])

        start = event_info["start"].strftime("%d.%m.%Y %H:%M")
        end = event_info["end"].strftime("%H:%M")
        participants_text = (
            ", ".join(event_info["participants"]) if event_info["participants"] else "—"
        )

        mm_send_dm(
            user_id,
            "✅ Встреча создана в календаре.\n\n"
            f"**{event_info['title']}**\n"
            f"Когда: {start}–{end}\n"
            f"Участники: {participants_text}\n"
            f"Описание: {(event_info['description'] or '—')}",
        )
        return True

    return False

def send_new_event_notification(mattermost_user_id, ev):
    when_str = format_when(ev["start"], ev.get("end"))
    text = format_event_details(
        title=ev.get("summary"),
        when_human=when_str,
        attendees=ev.get("attendees") or [],
        description=ev.get("description") or "",
        url=ev.get("url") or "",
        header_prefix="### 🆕 Новая встреча",
    )
    props = build_event_rsvp_props(ev["uid"])
    mm_send_dm(mattermost_user_id, text, props=props)


def send_event_rescheduled_notification(mattermost_user_id, old_ev, new_ev):
    old_when = format_when(
        datetime.fromisoformat(old_ev["start"]),
        datetime.fromisoformat(old_ev["end"]) if old_ev["end"] else None,
    )
    new_when = format_when(new_ev["start"], new_ev.get("end"))

    lines = [
        "### 🔁 Встреча перенесена",
        f"**{new_ev.get('summary') or old_ev.get('summary') or '(без названия)'}**",
        f"Было: {old_when}",
        f"Стало: {new_when}",
        "",
    ]

    details = format_event_details(
        title=new_ev.get("summary"),
        when_human=new_when,
        attendees=new_ev.get("attendees") or [],
        description=new_ev.get("description") or "",
        url=new_ev.get("url") or "",
    )
    text = "\n".join(lines) + "\n" + details
    props = build_event_rsvp_props(new_ev["uid"])
    mm_send_dm(mattermost_user_id, text, props=props)


def send_event_cancelled_notification(mattermost_user_id, ev):
    when_str = format_when(ev["start"], ev.get("end"))
    lines = [
        "### ❌ Встреча отменена",
        f"**{ev.get('summary') or '(без названия)'}**",
        f"Когда было запланировано: {when_str}",
    ]
    mm_send_dm(mattermost_user_id, "\n".join(lines))

WELCOME_TEXT_TEMPLATE = """Привет! Для начала надо авторизоваться в твоём календаре.

Логин я твой уже знаю: {email}

А вот с паролем немного сложнее. Перейди по ссылке:
https://account.mail.ru/user/2-step-auth/passwords/
и создай пароль приложения. Скопируй его и пришли мне в ответ одним сообщением.
"""

def handle_new_dm_message(user_id, channel_id, text):
    if ENCRYPTION_MISCONFIGURED:
        mm_send_dm(
            user_id,
            "Внимание! База паролей зашифрована, а ключ шифрования не задан.\n"
            "Обратитесь к администратору — бот временно недоступен."
        )
        return

    user = get_user(user_id)

    if not user:
        user_info = mm_get_user(user_id)
        user_email = user_info.get("email")

        if not is_email_allowed(user_email):
            mm_send_dm(
                user_id,
                "Мне пока не разрешили работать с тобой... Обратись к администратору",
            )
            return

        upsert_user(
            mattermost_user_id=user_id,
            email=user_email,
            caldav_password=None,
            state="WAITING_FOR_APP_PASSWORD",
        )
        welcome = WELCOME_TEXT_TEMPLATE.format(email=user_email)
        mm_send_dm(user_id, welcome)
        return

    user_email = user["email"]

    if not is_email_allowed(user_email):
            mm_send_dm(
                user_id,
                "Мне пока не разрешили работать с тобой... Обратись к администратору",
            )
            return

    if user["state"] == "WAITING_FOR_APP_PASSWORD":
        app_password = text.strip()
        upsert_user(
            mattermost_user_id=user_id,
            email=user["email"],
            caldav_password=app_password,
            state="READY",
        )
        mm_send_dm(
            user_id,
            "Спасибо! Я сохранил пароль приложения и подключился к календарю.\n\nВот твоё главное меню:",
        )
        send_main_menu(user_id)
        return

    if user["state"] == "READY":
        txt_lower = text.strip().lower()

        if txt_lower.startswith("debug caldav"):
            debug_dump_caldav_events(user_id)
            return

        draft = get_active_draft(user_id)
        if draft:
            if handle_meeting_draft_step(user_id, channel_id, user, draft, text):
                return

        if BOT_USERNAME in text or "@" + BOT_USERNAME in text:
            send_main_menu(user_id)
        else:
            mm_send_dm(
                user_id,
                "Я уже подключен к твоему календарю.\n"
                f"Напиши `@{BOT_USERNAME}` или нажми кнопку в последнем сообщении, чтобы открыть меню.",
            )
        return

    mm_send_dm(user_id, "Не совсем понимаю твоё состояние, попробуй ещё раз.")

def websocket_loop():
    ws_url = MATTERMOST_BASE_URL.replace("https://", "wss://").replace(
        "http://", "ws://"
    )
    ws_url = ws_url.rstrip("/") + "/api/v4/websocket"

    while True:
        try:
            ws = create_connection(
                ws_url,
                header=[f"Authorization: Bearer {MATTERMOST_BOT_TOKEN}"],
            )

            while True:
                msg = ws.recv()
                if not msg:
                    continue
                data = json.loads(msg)

                if data.get("event") != "posted":
                    continue

                data_payload = data.get("data", {}) or {}
                post_raw = data_payload.get("post")

                if not post_raw:
                    continue

                channel_type = data_payload.get("channel_type")

                post = json.loads(post_raw)
                channel_id = post.get("channel_id")
                user_id = post.get("user_id")
                message = post.get("message", "")

                if post.get("user_id") == BOT_USER_ID:
                    continue

                if channel_type is not None:
                    if channel_type != "D":
                        continue
                else:
                    try:
                        channel = mm_get_channel(channel_id)
                    except Exception:
                        continue

                    if channel.get("type") != "D":
                        continue

                handle_new_dm_message(user_id, channel_id, message)

        except WebSocketConnectionClosedException:
            time.sleep(3)
            continue
        except Exception:
            time.sleep(5)
            continue

def job_daily_summary():
    if ENCRYPTION_MISCONFIGURED:
        return
    
    users = get_all_ready_users()
    for user in users:
        try:
            events = get_today_events(
                user["email"], user["caldav_password"], only_future=False
            )
            text, props = format_events_summary_with_select(
                events, title="Встречи на сегодня"
            )
            mm_send_dm(user["mattermost_user_id"], text, props=props)
        except Exception:
            continue

def job_event_alarms():
    if ENCRYPTION_MISCONFIGURED:
        return
    
    tz_local = tz.gettz(TZ_NAME)
    now = datetime.now(tz_local)
    window_start = now - timedelta(minutes=1)
    window_end = now + timedelta(minutes=1)

    users = get_all_ready_users()

    for user in users:
        mm_user_id = user["mattermost_user_id"]
        email = user["email"]
        pwd = user["caldav_password"]

        try:
            events = get_today_events(email, pwd, only_future=False)
        except Exception:
            continue

        for ev in events:
            uid = ev.get("uid")
            if not uid:
                continue

            title = ev.get("summary") or "(без названия)"
            description = ev.get("description") or ""
            attendees = ev.get("attendees") or []
            url = ev.get("url") or ""
            start = ev.get("start")
            end = ev.get("end")

            if not isinstance(start, datetime):
                continue

            if start.tzinfo is None:
                start = start.replace(tzinfo=tz_local)
            else:
                start = start.astimezone(tz_local)

            if isinstance(end, datetime):
                if end.tzinfo is None:
                    end_local = end.replace(tzinfo=tz_local)
                else:
                    end_local = end.astimezone(tz_local)
            else:
                end_local = None

            alarms = ev.get("alarms") or []

            alarms_sorted = sorted(alarms)
            total_alarms = len(alarms_sorted)

            for idx, alarm_dt in enumerate(alarms_sorted):
                if alarm_dt is None:
                    continue

                if alarm_dt.tzinfo is None:
                    alarm_dt = alarm_dt.replace(tzinfo=tz_local)
                else:
                    alarm_dt = alarm_dt.astimezone(tz_local)

                uid_valarm = f"{uid}::VALARM"

                if not (window_start <= alarm_dt <= window_end):
                    continue

                if alarm_already_sent(mm_user_id, uid_valarm, alarm_dt):
                    continue

                when_str = format_when(start, end_local)

                ordinal = f"{idx+1}/{total_alarms}"

                lines = [
                    f"⏰ Напоминание о встрече ({ordinal})",
                    f"**{title}**",
                    f"Когда: {when_str}",
                ]
                if url:
                    lines.append(f"URL: {url}")

                try:
                    mm_send_dm(mm_user_id, "\n".join(lines))
                    mark_alarm_sent(mm_user_id, uid_valarm, alarm_dt)
                except Exception:
                    continue

            pre_dt = start - timedelta(minutes=1)

            if not (window_start <= pre_dt <= window_end):
                continue

            uid_pre = f"{uid}::PRESTART"

            if alarm_already_sent(mm_user_id, uid_pre, pre_dt):
                continue

            when_str = format_when(start, end_local)

            text = format_event_details(
                title=title,
                when_human=when_str,
                attendees=attendees,
                description=description,
                url=url,
                header_prefix="### ⏰ Начинается встреча!",
            )

            try:
                mm_send_dm(mm_user_id, text)
                mark_alarm_sent(mm_user_id, uid_pre, pre_dt)
            except Exception:
                continue

def job_event_changes():
    """
    Отслеживает:
    - новые встречи (в будущем/прошлом в пределах окна) → уведомление + кнопки Принять/Отклонить/Возможно
    - изменения даты/времени → уведомление о переносе
    - отмену встречи (STATUS:CANCELLED) → уведомление об отмене
    """
    if ENCRYPTION_MISCONFIGURED:
        return

    users = get_all_ready_users()
    for user in users:
        mm_user_id = user["mattermost_user_id"]
        email = user["email"]
        pwd = user["caldav_password"]

        try:
            new_events = get_events_for_tracking(email, pwd)
        except Exception:
            continue

        # старое состояние
        old_map = load_tracked_events_for_user(mm_user_id)
        first_sync = len(old_map) == 0

        # мапа uid -> ev
        new_map = {ev["uid"]: ev for ev in new_events}

        # если это первый прогон для пользователя — просто заполняем таблицу,
        # чтобы не спамить всеми старыми событиями
        if first_sync:
            for ev in new_events:
                upsert_tracked_event(mm_user_id, ev)
            continue

        for uid, ev in new_map.items():
            old_ev = old_map.get(uid)

            if not old_ev:
                # новое событие
                upsert_tracked_event(mm_user_id, ev)
                if ev.get("status") == "CANCELLED":
                    # сразу пришло как отменённое — ничего не говорим
                    continue
                send_new_event_notification(mm_user_id, ev)
                continue

            # уже было — смотрим изменения
            old_start = old_ev["start"]
            old_end = old_ev["end"]
            old_status = (old_ev["status"] or "").upper()

            new_start = ev["start"].isoformat() if isinstance(ev["start"], datetime) else str(ev["start"])
            new_end_val = ev.get("end")
            new_end = new_end_val.isoformat() if isinstance(new_end_val, datetime) else (new_end_val or "")
            new_status = (ev.get("status") or "").upper()

            moved = (old_start != new_start) or (old_end != new_end)
            status_changed = (old_status != new_status)

            # сразу сохраняем новое состояние
            upsert_tracked_event(mm_user_id, ev)

            if status_changed and new_status == "CANCELLED":
                # встреча отменена
                send_event_cancelled_notification(mm_user_id, ev)
            elif moved:
                # дата/время изменились
                send_event_rescheduled_notification(mm_user_id, old_ev, ev)

        # Можно было бы ещё обрабатывать случаи, когда событие пропало полностью
        # из new_map (удалено без STATUS:CANCELLED), но Mail.ru обычно шлёт CANCELLED.

def handle_action_summary(user_id, only_future):
    user = get_user(user_id)
    if not user or user["state"] != "READY":
        mm_send_dm(user_id, "Сначала нужно авторизоваться.")
        return

    try:
        events = get_today_events(
            user["email"], user["caldav_password"], only_future=only_future
        )
    except Exception:
        mm_send_dm(
            user_id,
            "⚠️ Не удалось получить события из календаря (ошибка CalDAV).\n"
            "Мы ещё не настроили точный URL/формат для Mail.ru. "
            "Пока это прототип, поэтому просто сообщаю об ошибке.",
        )
        return

    title = (
        "Текущие / будущие встречи на сегодня"
        if only_future
        else "Встречи на сегодня"
    )
    text, props = format_events_summary_with_select(events, title=title)
    mm_send_dm(user_id, text, props=props)

def handle_create_meeting_pick_date(user_id, choice):
    draft = get_active_draft(user_id)
    if not draft:
        mm_send_dm(user_id, "Черновик встречи не найден. Нажми «Создать встречу» ещё раз.")
        return

    tz_local = tz.gettz(TZ_NAME)
    today = datetime.now(tz_local).date()

    if choice in ("today", "tomorrow", "after_tomorrow"):
        if choice == "today":
            date_obj = today
        elif choice == "tomorrow":
            date_obj = today + timedelta(days=1)
        else:
            date_obj = today + timedelta(days=2)

        update_draft(draft["id"], date=date_obj.isoformat(), step="ASK_TIME")
        mm_send_dm(
            user_id,
            f"Дата встречи: {date_obj.strftime('%d.%m.%Y')}.\n\nВо сколько начать? Формат HH:MM (24 часа), по твоему часовому поясу.",
            props=build_cancel_only_props(),
        )
    elif choice == "custom":
        update_draft(draft["id"], step="ASK_CUSTOM_DATE")
        mm_send_dm(
            user_id,
            "Введи дату встречи в формате **DD.MM.YYYY**, например `21.11.2025`.",
            props=build_cancel_only_props(),
        )
    else:
        mm_send_dm(user_id, f"Неизвестный выбор даты: {choice}")

def handle_skip_participants(user_id):
    draft = get_active_draft(user_id)
    if not draft:
        mm_send_dm(user_id, "Черновик встречи не найден.")
        return
    update_draft(draft["id"], participants="", step="ASK_DESCRIPTION")
    mm_send_dm(
        user_id,
        "Добавь описание встречи (повестка и т.п.).\n"
        "Если не нужно — нажми кнопку «Не добавлять».",
        props=build_description_step_props(),
    )

def handle_skip_description(user_id):
    draft = get_active_draft(user_id)
    if not draft:
        mm_send_dm(user_id, "Черновик встречи не найден.")
        return
    update_draft(draft["id"], description="", step="ASK_LOCATION")
    mm_send_dm(
        user_id,
        "Добавь ссылку на встречу.\n"
        "Если не нужно — нажми кнопку «Не добавлять».",
        props=build_location_step_props(),
    )

def handle_skip_location(user_id):
    draft = get_active_draft(user_id)
    user = get_user(user_id)
    if not draft or not user or user["state"] != "READY":
        mm_send_dm(user_id, "Черновик встречи не найден или нет авторизации.")
        return

    update_draft(draft["id"], step="CREATING")
    try:
        event_info = create_calendar_event_from_draft(
            user["email"],
            user["caldav_password"],
            {**draft, "location": ""},
        )
    except Exception:
        mm_send_dm(
            user_id,
            "⚠️ Не удалось создать встречу в календаре. "
            "Проверь, пожалуйста, корректность даты/времени и попробуй ещё раз.",
        )
        update_draft(draft["id"], step="ASK_LOCATION")
        return

    delete_draft(draft["id"])

    start = event_info["start"].strftime("%d.%m.%Y %H:%M")
    end = event_info["end"].strftime("%H:%M")
    participants_text = (
        ", ".join(event_info["participants"]) if event_info["participants"] else "—"
    )

    mm_send_dm(
        user_id,
        "✅ Встреча создана в календаре.\n\n"
        f"**{event_info['title']}**\n"
        f"Когда: {start}–{end}\n"
        f"Участники: {participants_text}\n"
        f"Описание: {(event_info['description'] or '—')}",
    )

def handle_cancel_meeting(user_id):
    draft = get_active_draft(user_id)
    if draft:
        delete_draft(draft["id"])
    mm_send_dm(user_id, "Создание встречи отменено.")

@app.route("/mattermost/actions", methods=["POST"])
def mattermost_actions():
    payload = request.json

    user_id = payload.get("user_id")
    context = payload.get("context", {}) or {}
    action = context.get("action")
    post_id = payload.get("post_id")

    if not user_id or not action:
        return jsonify({"error": "bad request"}), 400

    if ENCRYPTION_MISCONFIGURED:
        # Не трогаем базу, только предупредим пользователя
        mm_send_dm(
            user_id,
            "Внимание! База паролей зашифрована, а ключ шифрования не задан.\n"
            "Обратитесь к администратору — бот временно недоступен."
        )
        return jsonify({})

    try:
        if action == "summary_today":
            clear_last_detail_post(user_id)
            handle_action_summary(user_id, only_future=False)

        elif action == "summary_today_future":
            clear_last_detail_post(user_id)
            handle_action_summary(user_id, only_future=True)

        elif action == "create_meeting":
            clear_last_detail_post(user_id)
            start_create_meeting_flow(user_id)

        elif action == "logout_confirm":
            clear_last_detail_post(user_id)
            mm_send_dm(
                user_id,
                "Вы уверены? Пароль придётся задавать заново, напоминания о встречах перестанут работать.",
                props=build_logout_confirm_props(),
            )

        elif action == "logout_yes":
            clear_post_buttons(post_id)
            logout_user(user_id)
            mm_send_dm(
                user_id,
                "Вы разлогинились. Чтобы снова подключить календарь, напишите мне любое сообщение.",
            )

        elif action == "logout_no":
            clear_post_buttons(post_id)
            mm_send_dm(user_id, "Ок, остаёмся подключенными к календарю.")

        elif action == "create_meeting_pick_date":
            clear_post_buttons(post_id)
            choice = context.get("choice")
            handle_create_meeting_pick_date(user_id, choice)

        elif action == "show_event_details_select":
            handle_show_event_details_select(user_id, payload)

        elif action == "skip_participants":
            clear_post_buttons(post_id)
            handle_skip_participants(user_id)

        elif action == "skip_description":
            clear_post_buttons(post_id)
            handle_skip_description(user_id)

        elif action == "skip_location":
            clear_post_buttons(post_id)
            handle_skip_location(user_id)

        elif action == "cancel_meeting":
            clear_post_buttons(post_id)
            handle_cancel_meeting(user_id)

        elif action == "event_rsvp":
            uid = context.get("uid")
            choice = (context.get("choice") or "").upper()
            handle_event_rsvp(user_id, uid, choice)

        else:
            mm_send_dm(user_id, f"Неизвестное действие: {action}")
    except Exception:
        mm_send_dm(
            user_id,
            "⚠️ Пока не удалось получить данные календаря. "
            "Скорее всего, ещё не настроен доступ к CalDAV Mail.ru или сервер отвечает ошибкой.",
        )

    return jsonify({})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

def main():
    init_db()
    check_encryption_misconfiguration()
    init_bot_identity()

    scheduler.add_job(job_daily_summary, "cron", hour=14, minute=0)
    scheduler.add_job(job_event_alarms, "interval", minutes=1)
    scheduler.add_job(job_event_changes, "interval", minutes=5)
    scheduler.start()

    t = threading.Thread(target=websocket_loop, daemon=True)
    t.start()

    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()