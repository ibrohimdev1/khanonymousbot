import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from config import settings


DB_PATH = Path(settings.db_path)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with closing(get_connection()) as conn, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                language TEXT NOT NULL DEFAULT 'uz',
                created_at TEXT NOT NULL,
                is_banned INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        # Eski jadvallarga is_banned ustunini qo'shamiz
        try:
            conn.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Ustun allaqachon mavjud

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id INTEGER NOT NULL,
                receiver_id INTEGER NOT NULL,
                receiver_message_id INTEGER NOT NULL,
                sender_message_id INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blocked (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                blocker_id INTEGER NOT NULL,
                blocked_user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receiver_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL,
                message_text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS link_clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receiver_id INTEGER NOT NULL,
                visitor_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


def _now() -> str:
    return datetime.utcnow().isoformat()


def ensure_user(user_id: int, language: str = "uz") -> None:
    with closing(get_connection()) as conn, conn:
        cur = conn.execute(
            "SELECT user_id FROM users WHERE user_id = ?", (user_id,)
        )
        if cur.fetchone() is None:
            conn.execute(
                "INSERT INTO users (user_id, language, created_at, is_banned) VALUES (?, ?, ?, 0)",
                (user_id, language, _now()),
            )


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            "SELECT user_id, language, created_at, is_banned FROM users WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    if not user:
        return False
    return bool(user.get("is_banned", 0))


def ban_user(user_id: int) -> None:
    ensure_user(user_id)
    with closing(get_connection()) as conn, conn:
        conn.execute(
            "UPDATE users SET is_banned = 1 WHERE user_id = ?",
            (user_id,),
        )


def unban_user(user_id: int) -> None:
    ensure_user(user_id)
    with closing(get_connection()) as conn, conn:
        conn.execute(
            "UPDATE users SET is_banned = 0 WHERE user_id = ?",
            (user_id,),
        )


def set_language(user_id: int, language: str) -> None:
    ensure_user(user_id)
    with closing(get_connection()) as conn, conn:
        conn.execute(
            "UPDATE users SET language = ? WHERE user_id = ?",
            (language, user_id),
        )


def get_language(user_id: int) -> str:
    user = get_user(user_id)
    if not user:
        return "uz"
    return user.get("language", "uz") or "uz"


def save_message(
    sender_id: int,
    receiver_id: int,
    receiver_message_id: int,
    sender_message_id: Optional[int] = None,
) -> int:
    with closing(get_connection()) as conn, conn:
        cur = conn.execute(
            """
            INSERT INTO messages (
                sender_id, receiver_id,
                receiver_message_id, sender_message_id,
                created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (sender_id, receiver_id, receiver_message_id, sender_message_id, _now()),
        )
        return cur.lastrowid


def get_message_by_receiver_message_id(
    receiver_message_id: int,
) -> Optional[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            SELECT id, sender_id, receiver_id, receiver_message_id,
                   sender_message_id, created_at
            FROM messages
            WHERE receiver_message_id = ?
            """,
            (receiver_message_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def block_user(blocker_id: int, blocked_user_id: int) -> None:
    with closing(get_connection()) as conn, conn:
        cur = conn.execute(
            """
            SELECT id FROM blocked
            WHERE blocker_id = ? AND blocked_user_id = ?
            """,
            (blocker_id, blocked_user_id),
        )
        if cur.fetchone() is None:
            conn.execute(
                """
                INSERT INTO blocked (blocker_id, blocked_user_id, created_at)
                VALUES (?, ?, ?)
                """,
                (blocker_id, blocked_user_id, _now()),
            )


def is_blocked(blocker_id: int, blocked_user_id: int) -> bool:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            SELECT id FROM blocked
            WHERE blocker_id = ? AND blocked_user_id = ?
            """,
            (blocker_id, blocked_user_id),
        )
        return cur.fetchone() is not None


def save_report(receiver_id: int, sender_id: int, message_text: str) -> int:
    with closing(get_connection()) as conn, conn:
        cur = conn.execute(
            """
            INSERT INTO reports (receiver_id, sender_id, message_text, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (receiver_id, sender_id, message_text, _now()),
        )
        return cur.lastrowid


def save_link_click(receiver_id: int, visitor_id: int) -> int:
    ensure_user(receiver_id)
    ensure_user(visitor_id)
    with closing(get_connection()) as conn, conn:
        cur = conn.execute(
            """
            INSERT INTO link_clicks (receiver_id, visitor_id, created_at)
            VALUES (?, ?, ?)
            """,
            (receiver_id, visitor_id, _now()),
        )
        return cur.lastrowid


def get_users_count() -> int:
    with closing(get_connection()) as conn:
        cur = conn.execute("SELECT COUNT(*) as cnt FROM users")
        row = cur.fetchone()
        return row["cnt"] if row else 0


def get_messages_count() -> int:
    with closing(get_connection()) as conn:
        cur = conn.execute("SELECT COUNT(*) as cnt FROM messages")
        row = cur.fetchone()
        return row["cnt"] if row else 0


def get_today_stats() -> Dict[str, int]:
    today = datetime.utcnow().date().isoformat()
    with closing(get_connection()) as conn:
        users_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE DATE(created_at) = ?",
            (today,),
        )
        users_row = users_cur.fetchone()
        users_count = users_row["cnt"] if users_row else 0

        messages_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE DATE(created_at) = ?",
            (today,),
        )
        messages_row = messages_cur.fetchone()
        messages_count = messages_row["cnt"] if messages_row else 0

        reports_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM reports WHERE DATE(created_at) = ?",
            (today,),
        )
        reports_row = reports_cur.fetchone()
        reports_count = reports_row["cnt"] if reports_row else 0

        return {
            "users": users_count,
            "messages": messages_count,
            "reports": reports_count,
        }


def get_last_7_days_stats() -> list:
    from datetime import timedelta

    stats = []
    for i in range(6, -1, -1):
        date = (datetime.utcnow() - timedelta(days=i)).date().isoformat()
        with closing(get_connection()) as conn:
            users_cur = conn.execute(
                "SELECT COUNT(*) as cnt FROM users WHERE DATE(created_at) = ?",
                (date,),
            )
            users_row = users_cur.fetchone()
            users_count = users_row["cnt"] if users_row else 0

            messages_cur = conn.execute(
                "SELECT COUNT(*) as cnt FROM messages WHERE DATE(created_at) = ?",
                (date,),
            )
            messages_row = messages_cur.fetchone()
            messages_count = messages_row["cnt"] if messages_row else 0

            reports_cur = conn.execute(
                "SELECT COUNT(*) as cnt FROM reports WHERE DATE(created_at) = ?",
                (date,),
            )
            reports_row = reports_cur.fetchone()
            reports_count = reports_row["cnt"] if reports_row else 0

            stats.append(
                {
                    "date": date,
                    "users": users_count,
                    "messages": messages_count,
                    "reports": reports_count,
                }
            )
    return stats


def get_top_senders(limit: int = 10) -> list:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            SELECT sender_id, COUNT(*) as cnt
            FROM messages
            GROUP BY sender_id
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_top_receivers(limit: int = 10) -> list:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            SELECT receiver_id, COUNT(*) as cnt
            FROM messages
            GROUP BY receiver_id
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_banned_users(limit: int = 20) -> list:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            SELECT user_id, created_at, is_banned
            FROM users
            WHERE is_banned = 1
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_reports_list(limit: int = 20) -> list:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            SELECT id, receiver_id, sender_id, message_text, created_at
            FROM reports
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_user_stats(user_id: int) -> Optional[Dict[str, Any]]:
    user = get_user(user_id)
    if not user:
        return None

    with closing(get_connection()) as conn:
        sent_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE sender_id = ?",
            (user_id,),
        )
        sent_row = sent_cur.fetchone()
        sent_count = sent_row["cnt"] if sent_row else 0

        received_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE receiver_id = ?",
            (user_id,),
        )
        received_row = received_cur.fetchone()
        received_count = received_row["cnt"] if received_row else 0

        return {
            "user_id": user_id,
            "language": user.get("language", "uz"),
            "created_at": user.get("created_at"),
            "is_banned": bool(user.get("is_banned", 0)),
            "sent_count": sent_count,
            "received_count": received_count,
        }


def get_profile_stats(user_id: int) -> Optional[Dict[str, Any]]:
    user = get_user(user_id)
    if not user:
        return None

    today = datetime.utcnow().date().isoformat()

    with closing(get_connection()) as conn:
        today_messages_cur = conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM messages
            WHERE receiver_id = ? AND DATE(created_at) = ?
            """,
            (user_id, today),
        )
        today_messages_row = today_messages_cur.fetchone()
        today_messages = today_messages_row["cnt"] if today_messages_row else 0

        total_messages_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE receiver_id = ?",
            (user_id,),
        )
        total_messages_row = total_messages_cur.fetchone()
        total_messages = total_messages_row["cnt"] if total_messages_row else 0

        today_clicks_cur = conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM link_clicks
            WHERE receiver_id = ? AND DATE(created_at) = ?
            """,
            (user_id, today),
        )
        today_clicks_row = today_clicks_cur.fetchone()
        today_clicks = today_clicks_row["cnt"] if today_clicks_row else 0

        total_clicks_cur = conn.execute(
            "SELECT COUNT(*) as cnt FROM link_clicks WHERE receiver_id = ?",
            (user_id,),
        )
        total_clicks_row = total_clicks_cur.fetchone()
        total_clicks = total_clicks_row["cnt"] if total_clicks_row else 0

        rank_cur = conn.execute(
            """
            SELECT COUNT(*) + 1 as rank
            FROM users u
            WHERE (
                SELECT COUNT(*) FROM messages m WHERE m.receiver_id = u.user_id
            ) > ?
            """,
            (total_messages,),
        )
        rank_row = rank_cur.fetchone()
        rank = rank_row["rank"] if rank_row else 1

        return {
            "today_messages": today_messages,
            "today_clicks": today_clicks,
            "total_messages": total_messages,
            "total_clicks": total_clicks,
            "rank": rank,
        }


def get_all_users() -> list:
    with closing(get_connection()) as conn:
        cur = conn.execute("SELECT user_id FROM users WHERE is_banned = 0")
        return [row["user_id"] for row in cur.fetchall()]


init_db()

