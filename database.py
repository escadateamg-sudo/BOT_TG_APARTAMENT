import asyncio
import aiosqlite
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        # Для Render.com використовуємо /tmp для тимчасових файлів
        self.db_path = os.getenv('DB_PATH', '/tmp/bot_database.db')
        self._init_done = False
        
        # Створюємо директорію якщо не існує
        db_dir = os.path.dirname(self.db_path)
        os.makedirs(db_dir, exist_ok=True)

    async def init_pool(self):
        """Ініціалізація бази даних SQLite"""
        try:
            # Перевіряємо чи встановлено aiosqlite
            try:
                import aiosqlite
                logger.info("✅ aiosqlite доступний")
            except ImportError:
                logger.error("❌ aiosqlite не встановлено")
                raise

            # Тестуємо з'єднання
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute('SELECT 1')
                logger.info(f"✅ SQLite підключено: {self.db_path}")
            
            await self.create_tables()
            self._init_done = True
            logger.info("✅ База даних ініціалізована")
            
        except Exception as e:
            logger.error(f"❌ Помилка ініціалізації БД: {e}")
            raise

    def get_connection(self):
        """Отримання з'єднання з БД"""
        return aiosqlite.connect(self.db_path)

    async def create_tables(self):
        """Створення таблиць"""
        async with self.get_connection() as conn:
            # Таблиця користувачів
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    is_blocked BOOLEAN DEFAULT FALSE,
                    block_reason TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблиця міст
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    code TEXT PRIMARY KEY,
                    name_uk TEXT NOT NULL,
                    channel_url TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблиця псевдонімів міст
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS city_aliases (
                    city_code TEXT,
                    alias TEXT,
                    PRIMARY KEY (city_code, alias),
                    FOREIGN KEY (city_code) REFERENCES cities (code)
                )
            """)

            # Таблиця дій користувачів (для статистики)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    city_code TEXT,
                    city_name TEXT,
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблиця дій адміністратора
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_tg_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Створюємо індекси для оптимізації
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_active 
                ON users (user_id) WHERE is_blocked = FALSE
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_actions_date 
                ON user_actions (created_at DESC)
            """)

            await conn.commit()
            logger.info("✅ Всі таблиці створено")

    async def save_user(self, user_id: int, username: str = None, first_name: str = None):
        """Збереження користувача"""
        now = datetime.utcnow().isoformat()
        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT OR REPLACE INTO users (user_id, username, first_name, updated_at)
                VALUES (?, ?, ?, ?)
            """, (user_id, username, first_name, now))
            await conn.commit()

    async def get_users_count(self) -> int:
        """Кількість активних користувачів"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE NOT is_blocked")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_all_users(self) -> List[Dict]:
        """Всі активні користувачі для розсилки"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT user_id, username, first_name 
                FROM users 
                WHERE NOT is_blocked 
                ORDER BY created_at
            """)
            rows = await cursor.fetchall()
            return [{"user_id": row[0], "username": row[1], "first_name": row[2]} for row in rows]

    async def set_user_blocked(self, user_id: int, is_blocked: bool, reason: str = None):
        """Блокування/розблокування користувача"""
        now = datetime.utcnow().isoformat()
        async with self.get_connection() as conn:
            await conn.execute("""
                UPDATE users SET is_blocked = ?, block_reason = ?, updated_at = ?
                WHERE user_id = ?
            """, (is_blocked, reason, now, user_id))
            await conn.commit()

    async def log_city_selection(self, user_id: int, city_code: str, city_name: str):
        """Логування вибору міста користувачем"""
        now = datetime.utcnow().isoformat()
        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO user_actions (user_id, action, city_code, city_name, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, 'city_selected', city_code, city_name, now))
            await conn.commit()

    async def find_city_by_alias(self, alias: str) -> Optional[Dict]:
        """Пошук міста по псевдоніму"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT c.code, c.name_uk, c.channel_url 
                FROM cities c
                LEFT JOIN city_aliases ca ON c.code = ca.city_code
                WHERE LOWER(c.code) = LOWER(?) OR LOWER(ca.alias) = LOWER(?)
                LIMIT 1
            """, (alias, alias))
            row = await cursor.fetchone()
            if row:
                return {"code": row[0], "name_uk": row[1], "channel_url": row[2]}
            return None

    async def find_cities_by_prefix(self, prefix: str, limit: int = 5) -> List[Dict]:
        """Пошук міст по префіксу"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT DISTINCT c.code, c.name_uk, c.channel_url 
                FROM cities c
                LEFT JOIN city_aliases ca ON c.code = ca.city_code
                WHERE LOWER(c.name_uk) LIKE LOWER(?) OR LOWER(ca.alias) LIKE LOWER(?)
                LIMIT ?
            """, (f"{prefix}%", f"{prefix}%", limit))
            rows = await cursor.fetchall()
            return [{"code": row[0], "name_uk": row[1], "channel_url": row[2]} for row in rows]

    async def get_available_cities(self) -> List[Dict]:
        """Міста з доступними каналами"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT code, name_uk, channel_url 
                FROM cities 
                WHERE channel_url IS NOT NULL AND channel_url != ''
                ORDER BY name_uk
            """)
            rows = await cursor.fetchall()
            return [{"code": row[0], "name_uk": row[1], "channel_url": row[2]} for row in rows]

    async def get_admin_stats(self) -> Dict:
        """Статистика для адміна"""
        stats = {}
        async with self.get_connection() as conn:
            # Загальна кількість користувачів
            cursor = await conn.execute("SELECT COUNT(*) FROM users")
            row = await cursor.fetchone()
            stats['total_users'] = row[0] if row else 0

            # Активні користувачі
            cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE NOT is_blocked")
            row = await cursor.fetchone()
            stats['active_users'] = row[0] if row else 0

            # Заблоковані користувачі
            cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE is_blocked")
            row = await cursor.fetchone()
            stats['blocked_users'] = row[0] if row else 0

            # Нові користувачі за 7 днів
            week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
            cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE created_at >= ?", (week_ago,))
            row = await cursor.fetchone()
            stats['new_users_7d'] = row[0] if row else 0

            # Топ міст за останні 30 днів
            month_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
            cursor = await conn.execute("""
                SELECT city_code, city_name, COUNT(*) as count
                FROM user_actions 
                WHERE action = 'city_selected' AND created_at >= ?
                GROUP BY city_code, city_name
                ORDER BY count DESC
                LIMIT 5
            """, (month_ago,))
            rows = await cursor.fetchall()
            stats['top_cities'] = [
                {"city_code": row[0], "city_name_uk": row[1], "count": row[2]} 
                for row in rows
            ]

            # Загальна кількість відписок
            stats['total_unsubscriptions'] = stats['blocked_users']
            stats['unsubscribed_7d'] = 0  # Можна додати логіку для відстеження

        return stats
