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
import logging

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

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

from contextlib import contextmanager

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "DISABLED")
FERNET = None
if ENCRYPTION_KEY and ENCRYPTION_KEY != "DISABLED":
    if Fernet is None:
        raise RuntimeError("cryptography не установлена, а ENCRYPTION_KEY задан")
    FERNET = Fernet(ENCRYPTION_KEY.encode("utf-8"))

ENCRYPTION_MISCONFIGURED = False

MENTION_RE = re.compile(r"@([a-zA-Z0-9._-]+)")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

DB_PATH = os.getenv("DB_PATH", "./calendar_bot.db")

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

raw_allowed = os.getenv("ALLOWED_EMAILS", "").strip()
if not raw_allowed:
    ALLOWED_EMAILS = None
else:
    ALLOWED_EMAILS = {
        e.strip().lower()
        for e in re.split(r"[,\s;]+", raw_allowed)
        if e.strip()
    }

base_actions_url = os.getenv("MM_ACTIONS_URL")
if base_actions_url:
    base_actions_url = base_actions_url.rstrip("/")
    MM_ACTIONS_URL = base_actions_url + "/mattermost/actions"
else:
    MM_ACTIONS_URL = "https://your-bot-url.example.com/mattermost/actions"

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=TZ_NAME)

CALDAV_PRINCIPAL_PATH = os.getenv("CALDAV_PRINCIPAL_PATH", "/principals/")

WELCOME_TEXT_TEMPLATE = """Привет! Для начала надо авторизоваться в твоём календаре.

Логин я твой уже знаю: {email}

А вот с паролем немного сложнее. Перейди по ссылке:
https://account.mail.ru/user/2-step-auth/passwords/
и создай пароль приложения. Скопируй его и пришли мне в ответ одним сообщением.
"""

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
                location TEXT,
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
            CREATE TABLE IF NOT EXISTS event_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mattermost_user_id TEXT,
                uid TEXT,
                start TEXT,
                end TEXT,
                status TEXT,
                summary TEXT,
                organizer_email TEXT,
                updated_at TEXT,
                UNIQUE(mattermost_user_id, uid)
            )
            """
        )
        conn.commit()

def is_email_allowed(email: str) -> bool:
    if not email:
        return False
    if ALLOWED_EMAILS is None:
        return True
    return email.strip().lower() in ALLOWED_EMAILS

def encrypt_secret(value: str) -> str:
    if not value:
        return ""
    if FERNET is None:
        return value
    token = FERNET.encrypt(value.encode("utf-8"))
    return "enc:" + token.decode("utf-8")

def decrypt_secret(value: str) -> str:
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

def check_encryption_misconfiguration():
    global ENCRYPTION_MISCONFIGURED
    if FERNET is not None:
        ENCRYPTION_MISCONFIGURED = False
        return
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
        decrypted_pwd = raw_pwd
    else:
        decrypted_pwd = decrypt_secret(raw_pwd) if raw_pwd else None
    return {
        "mattermost_user_id": row[0],
        "email": row[1],
        "caldav_password": decrypted_pwd,
        "state": row[3],
    }

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

def logout_user(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM meeting_drafts WHERE mattermost_user_id = ?", (mattermost_user_id,))
        c.execute("DELETE FROM event_detail_posts WHERE mattermost_user_id = ?", (mattermost_user_id,))
        c.execute("DELETE FROM event_snapshots WHERE mattermost_user_id = ?", (mattermost_user_id,))
        c.execute("DELETE FROM users WHERE mattermost_user_id = ?", (mattermost_user_id,))
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
            SELECT id, step, title, date, time, duration_min, participants, description, location
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
        "location": row[8],
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

def mm_get_post(post_id):
    return mm_get(f"/api/v4/posts/{post_id}")

def mm_update_post_raw(post_id, message=None, props=None):
    data = {"id": post_id}
    if message is not None:
        data["message"] = message
    if props is not None:
        data["props"] = props
    return mm_put(f"/api/v4/posts/{post_id}", data)

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
        first = attachments[0] or {}
        if first.get("text") == "Главное меню":
            continue
        clear_post_buttons(pid)
        break

def mm_get_me():
    return mm_get("/api/v4/users/me")

def init_bot_identity():
    global BOT_USER_ID, BOT_USERNAME
    me = mm_get_me()
    BOT_USER_ID = me["id"]
    BOT_USERNAME = (me.get("username") or BOT_USERNAME or "").strip()

def mm_update_post(post_id, message):
    data = {"id": post_id, "message": message}
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
    client = caldav.DAVClient(url=base_url, username=email, password=password)
    principal = caldav.Principal(client=client, url=principal_url)
    return client, principal

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

def extract_organizer_email(vevent):
    organizer = getattr(vevent, "organizer", None)
    if not organizer:
        return None
    val = organizer.value
    if isinstance(val, str) and val.lower().startswith("mailto:"):
        val = val[7:]
    return (val or "").strip().lower() or None

def get_events_for_tracking(email, password):
    if caldav is None:
        return []
    tz_local = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_local).replace(tzinfo=tz_local)
    start_day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = start_day + timedelta(days=2)
    cal = get_primary_calendar(email, password)
    events = cal.date_search(start_day, end_day)
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
        else:
            dtstart = dtstart.astimezone(tz_local)
        if dtend and isinstance(dtend, datetime):
            if dtend.tzinfo is None:
                dtend = dtend.replace(tzinfo=tz_local)
            else:
                dtend = dtend.astimezone(tz_local)
        organizer_email = extract_organizer_email(vevent)
        if status == "CANCELLED":
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
                "status": status,
                "organizer_email": organizer_email,
            }
        )
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
        status_prop = getattr(vevent, "status", None)
        status = status_prop.value.upper() if status_prop else "CONFIRMED"
        if status == "CANCELLED":
            continue
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
        else:
            dtstart = dtstart.astimezone(tz_local)
        if dtend and isinstance(dtend, datetime):
            if dtend.tzinfo is None:
                dtend = dtend.replace(tzinfo=tz_local)
            else:
                dtend = dtend.astimezone(tz_local)
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
            raw = ev.data
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

def create_calendar_event_from_draft(mattermost_user_id, email, password, draft):
    tz_local = tz.gettz(TZ_NAME)
    try:
        date_obj = datetime.strptime(draft["date"], "%Y-%m-%d").date()
        time_obj = datetime.strptime(draft["time"], "%H:%M").time()
    except Exception as e:
        raise ValueError(f"Неверный формат даты/времени в черновике: {e}")
    start_dt = datetime.combine(date_obj, time_obj).replace(tzinfo=tz_local)
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
    uid = str(uuid.uuid4())
    vevent.add("uid").value = uid
    vevent.add("summary").value = title
    vevent.add("dtstart").value = start_dt
    vevent.add("dtend").value = end_dt
    vevent.add("status").value = "CONFIRMED"
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
        "uid": uid,
        "title": title,
        "start": start_dt,
        "end": end_dt,
        "participants": participants,
        "description": description,
        "location": location,
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
                "start": start.isoformat() if isinstance(start, datetime) else "",
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
    location = (url or "").strip()
    if location:
        lines.append("\nГде:")
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
            "⚠️ Не удалось получить события из календаря (ошибка CalDAV).",
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
            user_id,
            user["email"],
            user["caldav_password"],
            draft,
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
        f"Описание: {(event_info['description'] or '—')}\n"
        f"Где: {(event_info['location'] or '—')}",
    )

def handle_cancel_meeting(user_id):
    draft = get_active_draft(user_id)
    if draft:
        delete_draft(draft["id"])
    mm_send_dm(user_id, "Создание встречи отменено.")

def load_snapshots_for_user(mattermost_user_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT uid, start, end, status, summary, organizer_email
            FROM event_snapshots
            WHERE mattermost_user_id = ?
            """,
            (mattermost_user_id,),
        )
        rows = c.fetchall()
    res = {}
    for uid, start, end, status, summary, org in rows:
        res[uid] = {
            "uid": uid,
            "start": start,
            "end": end,
            "status": status,
            "summary": summary,
            "organizer_email": org,
        }
    return res

def upsert_snapshot(mattermost_user_id, ev):
    now_iso = datetime.now(timezone.utc).isoformat()
    start_str = ev["start"].isoformat() if isinstance(ev["start"], datetime) else str(ev["start"])
    end_val = ev.get("end")
    end_str = end_val.isoformat() if isinstance(end_val, datetime) else (end_val or "")
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO event_snapshots (mattermost_user_id, uid, start, end, status, summary, organizer_email, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mattermost_user_id, uid)
            DO UPDATE SET
                start = excluded.start,
                end = excluded.end,
                status = excluded.status,
                summary = excluded.summary,
                organizer_email = excluded.organizer_email,
                updated_at = excluded.updated_at
            """,
            (
                mattermost_user_id,
                ev["uid"],
                start_str,
                end_str,
                ev.get("status") or "",
                ev.get("summary") or "",
                (ev.get("organizer_email") or "") if isinstance(ev.get("organizer_email"), str) else "",
                now_iso,
            ),
        )
        conn.commit()

def delete_snapshot(mattermost_user_id, uid):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "DELETE FROM event_snapshots WHERE mattermost_user_id = ? AND uid = ?",
            (mattermost_user_id, uid),
        )
        conn.commit()

def cleanup_old_snapshots():
    tz_local = tz.gettz(TZ_NAME)
    today = datetime.now(tz_local).date()
    midnight_today = datetime.combine(today, datetime.min.time()).replace(tzinfo=tz_local)
    cutoff_iso = (midnight_today - timedelta(days=1)).isoformat()
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "DELETE FROM event_snapshots WHERE start < ?",
            (cutoff_iso,),
        )
        conn.commit()

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
    mm_send_dm(mattermost_user_id, text)

def send_event_rescheduled_notification(mattermost_user_id, old_ev, new_ev):
    old_start_str = old_ev["start"]
    old_end_str = old_ev["end"]
    try:
        old_start = datetime.fromisoformat(old_start_str)
    except Exception:
        old_start = None
    try:
        old_end = datetime.fromisoformat(old_end_str) if old_end_str else None
    except Exception:
        old_end = None
    new_start = new_ev["start"]
    new_end = new_ev.get("end")
    old_when = format_when(old_start, old_end) if old_start else "(ранее было другое время)"
    new_when = format_when(new_start, new_end)
    title = new_ev.get("summary") or old_ev.get("summary") or "(без названия)"
    lines = [
        "### 🔁 Встреча перенесена",
        f"**{title}**",
        f"Было: {old_when}",
        f"Стало: {new_when}",
    ]
    mm_send_dm(mattermost_user_id, "\n".join(lines))

def send_event_cancelled_notification(mattermost_user_id, ev):
    when_str = format_when(ev["start"], ev.get("end"))
    lines = [
        "### ❌ Встреча отменена",
        f"**{ev.get('summary') or '(без названия)'}**",
        f"Когда было запланировано: {when_str}",
    ]
    mm_send_dm(mattermost_user_id, "\n".join(lines))

def job_events_sync():
    if ENCRYPTION_MISCONFIGURED:
        return
    tz_local = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_local)
    users = get_all_ready_users()
    for user in users:
        mm_user_id = user["mattermost_user_id"]
        email = user["email"]
        pwd = user["caldav_password"]
        try:
            new_events = get_events_for_tracking(email, pwd)
        except Exception:
            continue
        old_map = load_snapshots_for_user(mm_user_id)
        first_sync = len(old_map) == 0
        new_map = {ev["uid"]: ev for ev in new_events}
        if first_sync:
            for ev in new_events:
                upsert_snapshot(mm_user_id, ev)
            continue
        for uid, ev in new_map.items():
            old_ev = old_map.get(uid)
            org = (ev.get("organizer_email") or "").lower()
            is_organizer = org and org == email.lower()
            if not old_ev:
                upsert_snapshot(mm_user_id, ev)
                if not is_organizer:
                    send_new_event_notification(mm_user_id, ev)
                continue
            old_start = old_ev["start"]
            old_end = old_ev["end"]
            new_start = ev["start"].isoformat() if isinstance(ev["start"], datetime) else str(ev["start"])
            new_end_val = ev.get("end")
            new_end = new_end_val.isoformat() if isinstance(new_end_val, datetime) else (new_end_val or "")
            moved = (old_start != new_start) or (old_end != new_end)
            upsert_snapshot(mm_user_id, ev)
            if moved and not is_organizer:
                send_event_rescheduled_notification(mm_user_id, old_ev, ev)
        for uid, old_ev in old_map.items():
            if uid in new_map:
                continue
            try:
                start_old = datetime.fromisoformat(old_ev["start"])
            except Exception:
                continue
            end_str = old_ev.get("end") or ""
            end_old = None
            if end_str:
                try:
                    end_old = datetime.fromisoformat(end_str)
                except Exception:
                    end_old = None
            if end_old and end_old < now_local:
                delete_snapshot(mm_user_id, uid)
                continue
            if not end_old and start_old < now_local:
                delete_snapshot(mm_user_id, uid)
                continue
            org = (old_ev.get("organizer_email") or "").lower()
            is_organizer = org and org == email.lower()
            pseudo_ev = {
                "uid": uid,
                "summary": old_ev.get("summary"),
                "start": start_old,
                "end": end_old,
            }
            if not is_organizer:
                send_event_cancelled_notification(mm_user_id, pseudo_ev)
            delete_snapshot(mm_user_id, uid)

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
        update_draft(draft["id"], location=location, step="CREATING")
        clear_last_bot_buttons_in_channel(channel_id)
        try:
            event_info = create_calendar_event_from_draft(
                user_id,
                user["email"],
                user["caldav_password"],
                {**draft, "location": location},
            )
        except Exception:
            mm_send_dm(
                user_id,
                "⚠️ Не удалось создать встречу в календаре. "
                "Проверь, пожалуйста, корректность даты/времени и попробуй ещё раз.",
            )
            update_draft(draft["id"], step="ASK_LOCATION")
            return True
        delete_draft(draft["id"])
        start = event_info["start"].strftime("%d.%м.%Y %H:%M".replace("%м", "%m"))
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
            f"Описание: {(event_info['description'] or '—')}\n"
            f"Где: {(event_info['location'] or '—')}",
        )
        return True
    return False

def handle_new_dm_message(user_id, channel_id, text):
    logger.info("DM message: user_id=%s channel_id=%s text=%r", user_id, channel_id, text)
    if ENCRYPTION_MISCONFIGURED:
        mm_send_dm(
            user_id,
            "Внимание! База паролей зашифрована, а ключ шифрования не задан.\n"
            "Обратитесь к администратору — бот временно недоступен.",
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
        txt_stripped = text.strip()
        txt_lower = txt_stripped.lower()
        bot_name = (BOT_USERNAME or "").lower()
        logger.debug("Mention detection: BOT_USERNAME=%r bot_name=%r txt_lower=%r", BOT_USERNAME, bot_name, txt_lower)

        if txt_lower.startswith("debug caldav"):
            debug_dump_caldav_events(user_id)
            return

        draft = get_active_draft(user_id)
        if draft:
            if handle_meeting_draft_step(user_id, channel_id, user, draft, text):
                return

        if bot_name and (
            txt_lower == bot_name
            or txt_lower == f"@{bot_name}"
            or bot_name in txt_lower
            or f"@{bot_name}" in txt_lower
        ):
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
    logger.info("Starting websocket loop to %s", ws_url)

    while True:
        try:
            logger.info("Connecting to Mattermost websocket...")
            ws = create_connection(
                ws_url,
                header=[f"Authorization: Bearer {MATTERMOST_BOT_TOKEN}"],
            )
            logger.info("Websocket connected")

            while True:
                try:
                    msg = ws.recv()
                except WebSocketConnectionClosedException:
                    logger.warning("Websocket recv() got WebSocketConnectionClosedException")
                    break
                except Exception as e:
                    logger.warning("Error receiving from websocket: %s", e)
                    break

                if not msg:
                    continue

                print(msg)
                
                try:
                    data = json.loads(msg)
                except Exception as e:
                    logger.warning("Failed to parse websocket message as JSON: %s", e)
                    continue

                event_type = data.get("event")
                if event_type != "posted":
                    continue

                data_payload = data.get("data", {}) or {}
                post_raw = data_payload.get("post")
                if not post_raw:
                    continue

                channel_type = data_payload.get("channel_type")

                try:
                    post = json.loads(post_raw)
                except Exception as e:
                    logger.warning("Failed to parse 'post' field JSON: %s", e)
                    continue

                channel_id = post.get("channel_id")
                user_id = post.get("user_id")
                message = post.get("message", "")

                logger.debug(
                    "WS posted event: user_id=%s channel_type=%s channel_id=%s message=%r",
                    user_id,
                    channel_type,
                    channel_id,
                    message,
                )

                if user_id == BOT_USER_ID:
                    logger.debug("Skipping message from bot itself (user_id=%s)", user_id)
                    continue

                is_dm = False
                if channel_type is not None:
                    is_dm = channel_type == "D"
                    if not is_dm:
                        logger.debug(
                            "Skip non-DM message: user_id=%s channel_type=%s channel_id=%s",
                            user_id,
                            channel_type,
                            channel_id,
                        )
                        continue
                else:
                    try:
                        channel = mm_get_channel(channel_id)
                    except Exception as e:
                        logger.warning("Failed to get channel %s: %s", channel_id, e)
                        continue

                    is_dm = channel.get("type") == "D"
                    if not is_dm:
                        logger.debug(
                            "Skip non-DM message after channel lookup: user_id=%s channel_id=%s type=%r",
                            user_id,
                            channel_id,
                            channel.get("type"),
                        )
                        continue

                logger.info(
                    "Incoming DM to bot from user_id=%s in channel_id=%s message=%r",
                    user_id,
                    channel_id,
                    message,
                )
                try:
                    handle_new_dm_message(user_id, channel_id, message)
                except Exception as e:
                    logger.exception("Error in handle_new_dm_message: %s", e)

        except WebSocketConnectionClosedException:
            logger.warning("Websocket connection closed, reconnecting in 3 seconds...")
            time.sleep(3)
            continue
        except Exception as e:
            logger.exception("Unexpected error in websocket_loop: %s", e)
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

@app.route("/mattermost/actions", methods=["POST"])
def mattermost_actions():
    payload = request.json
    logger.info("Webhook /mattermost/actions called with payload=%s", payload)
    user_id = payload.get("user_id")
    context = payload.get("context", {}) or {}
    action = context.get("action")
    post_id = payload.get("post_id")
    if not user_id or not action:
        return jsonify({"error": "bad request"}), 400
    if ENCRYPTION_MISCONFIGURED:
        mm_send_dm(
            user_id,
            "Внимание! База паролей зашифрована, а ключ шифрования не задан.\n"
            "Обратитесь к администратору — бот временно недоступен.",
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
    logger.info("Bot started with BOT_USER_ID=%s BOT_USERNAME=%s", BOT_USER_ID, BOT_USERNAME)
    scheduler.add_job(job_daily_summary, "cron", hour=14, minute=0)
    scheduler.add_job(job_events_sync, "interval", minutes=1)  # TO_BE_UPDATED
    scheduler.add_job(cleanup_old_snapshots, "cron", hour=0, minute=0)
    scheduler.start()
    t = threading.Thread(target=websocket_loop, daemon=True)
    t.start()
    port = int(os.getenv("PORT", "8000"))
    logger.info("Starting Flask app on port %s", port)
    app.run(host="0.0.0.0", port=port)

