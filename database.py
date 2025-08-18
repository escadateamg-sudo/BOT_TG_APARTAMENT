import asyncio
import aiosqlite
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.db_path = "bot_database.db"
        self.use_postgres = False
        self._pool = None

    async def init_pool(self):
        """Ініціалізація бази даних"""
        try:
            # Перевіряємо чи встановлено asyncpg
            try:
                import asyncpg
                # Тут можна додати логіку для PostgreSQL якщо потрібно
                logger.info("✅ asyncpg доступний")
            except ImportError:
                logger.warning("⚠️ asyncpg не встановлено. Використовується SQLite")

            # Перевіряємо чи встановлено aiosqlite
            try:
                import aiosqlite
                logger.info("✅ aiosqlite доступний")
            except ImportError:
                logger.warning("⚠️ aiosqlite не встановлено")

            await self.create_tables()
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

            await conn.commit()

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
        """Кількість користувачів"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE NOT is_blocked")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_all_users(self) -> List[Dict]:
        """Всі активні користувачі"""
        async with self.get_connection() as conn:
            cursor = await conn.execute("SELECT user_id, username, first_name FROM users WHERE NOT is_blocked")
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
        await self.log_admin_action(user_id, 'city_selected', {
            'city_code': city_code,
            'city_name': city_name
        })

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

            # Загальна кількість відписок (заблокованих)
            stats['total_unsubscriptions'] = stats['blocked_users']
            stats['unsubscribed_7d'] = 0

        return stats

    async def seed_cities_data(self):
        """Заповнення початкових даних міст"""
        cities_data = [
            # Пріоритетні міста (перші в списку)
            {
                'code': 'kyiv',
                'name_uk': 'Київ',
                'channel_url': 'https://t.me/+1Dn41QYXr00yMTNi',
                'aliases': ['київ', 'киев', 'kyiv', 'kiev']
            },
            {
                'code': 'kharkiv',
                'name_uk': 'Харків',
                'channel_url': 'https://t.me/+thVaxxh_vR85MjVi',
                'aliases': ['харків', 'харьков', 'kharkiv', 'kharkov']
            },
            {
                'code': 'dnipro',
                'name_uk': 'Дніпро',
                'channel_url': 'https://t.me/+N1GBEYNwmohjODAy',
                'aliases': ['дніпро', 'днепр', 'dnipro', 'dnepr']
            },
            {
                'code': 'lviv',
                'name_uk': 'Львів',
                'channel_url': 'https://t.me/+6n24GOCizpQ0NzMy',
                'aliases': ['львів', 'львов', 'lviv', 'lwow']
            },
            {
                'code': 'odesa',
                'name_uk': 'Одеса',
                'channel_url': 'https://t.me/+I9c4gGScQe40Nzdi',
                'aliases': ['одеса', 'одесса', 'odesa', 'odessa']
            },
            # Інші міста
            {
                'code': 'poltava',
                'name_uk': 'Полтава',
                'channel_url': 'https://t.me/+z5qd0XB1QWQ0MWNi',
                'aliases': ['полтава', 'poltava']
            },
            {
                'code': 'zhytomyr',
                'name_uk': 'Житомир',
                'channel_url': 'https://t.me/+njAe0h54d7IyOWRi',
                'aliases': ['житомир', 'zhytomyr']
            },
            {
                'code': 'zaporizhzhia',
                'name_uk': 'Запоріжжя',
                'channel_url': 'https://t.me/+fJmKDoQ-a6BjY2Vi',
                'aliases': ['запоріжжя', 'запорожье', 'zaporizhzhia']
            },
            {
                'code': 'ivano-frankivsk',
                'name_uk': 'Івано-Франківськ',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['івано-франківськ', 'ivano-frankivsk']
            },
            {
                'code': 'chernivtsi',
                'name_uk': 'Чернівці',
                'channel_url': 'https://t.me/+MqdooK82_eA5Mjhi',
                'aliases': ['чернівці', 'черновцы', 'chernivtsi']
            },
            {
                'code': 'khmelnytskyi',
                'name_uk': 'Хмельницький',
                'channel_url': 'https://t.me/+InPiC-xZZ_o0ZTQ6',
                'aliases': ['хмельницький', 'хмельницкий', 'khmelnytskyi']
            },
            {
                'code': 'ternopil',
                'name_uk': 'Тернопіль',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['тернопіль', 'тернополь', 'ternopil']
            },
            {
                'code': 'cherkasy',
                'name_uk': 'Черкаси',
                'channel_url': 'https://t.me/+k3gzY_lCrwo4ZTYy',
                'aliases': ['черкаси', 'cherkasy']
            },
            {
                'code': 'vinnytsia',
                'name_uk': 'Вінниця',
                'channel_url': 'https://t.me/+DkXJOA1Z2RRlMGQy',
                'aliases': ['вінниця', 'винница', 'vinnytsia']
            },
            {
                'code': 'uzhhorod',
                'name_uk': 'Ужгород',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['ужгород', 'uzhhorod']
            },
            {
                'code': 'lutsk',
                'name_uk': 'Луцьк',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['луцьк', 'луцк', 'lutsk']
            },
            {
                'code': 'rivne',
                'name_uk': 'Рівне',
                'channel_url': 'https://t.me/+LY-YQ_JD3oNiMGI6',
                'aliases': ['рівне', 'ровно', 'rivne']
            },
            {
                'code': 'kropyvnytskyi',
                'name_uk': 'Кропивницький',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['кропивницький', 'кропивницкий', 'kropyvnytskyi']
            },
            {
                'code': 'mykolaiv',
                'name_uk': 'Миколаїв',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['миколаїв', 'николаев', 'mykolaiv']
            },
            {
                'code': 'sumy',
                'name_uk': 'Суми',
                'channel_url': 'https://t.me/+dCNkA-INwd40OWYy',
                'aliases': ['суми', 'sumy']
            },
            {
                'code': 'chernihiv',
                'name_uk': 'Чернігів',
                'channel_url': 'https://t.me/+VOsRB3Zm3mQzMWM6',
                'aliases': ['чернігів', 'чернигов', 'chernihiv']
            },
            {
                'code': 'kherson',
                'name_uk': 'Херсон',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['херсон', 'kherson']
            },
            {
                'code': 'kremenchuk',
                'name_uk': 'Кременчук',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['кременчук', 'kremenchuk']
            },
            {
                'code': 'bila-tserkva',
                'name_uk': 'Біла Церква',
                'channel_url': 'https://t.me/+LGpIp61JC_w3ZDM6',
                'aliases': ['біла церква', 'белая церковь', 'bila-tserkva']
            },
            {
                'code': 'brovary',
                'name_uk': 'Бровари',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['бровари', 'brovary']
            },
            {
                'code': 'boryspil',
                'name_uk': 'Бориспіль',
                'channel_url': 'https://t.me/+BhDo7PnTuB41Y2Yy',
                'aliases': ['бориспіль', 'борисполь', 'boryspil']
            },
            {
                'code': 'irpin',
                'name_uk': 'Ірпінь',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['ірпінь', 'ирпень', 'irpin']
            },
            {
                'code': 'bucha',
                'name_uk': 'Буча',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['буча', 'bucha']
            },
            {
                'code': 'uman',
                'name_uk': 'Умань',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['умань', 'uman']
            },
            {
                'code': 'oleksandriia',
                'name_uk': 'Олександрія',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['олександрія', 'александрия', 'oleksandriia']
            },
            {
                'code': 'mukachevo',
                'name_uk': 'Мукачево',
                'channel_url': 'https://t.me/+v_9k4nwiQ_RlY2M6',
                'aliases': ['мукачево', 'mukachevo']
            },
            {
                'code': 'drohobych',
                'name_uk': 'Дрогобич',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['дрогобич', 'drohobych']
            },
            {
                'code': 'stryi',
                'name_uk': 'Стрий',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['стрий', 'stryi']
            },
            {
                'code': 'chervonohrad',
                'name_uk': 'Червоноград',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['червоноград', 'chervonohrad']
            },
            {
                'code': 'kolomyia',
                'name_uk': 'Коломия',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['коломия', 'kolomyia']
            },
            {
                'code': 'izmail',
                'name_uk': 'Ізмаїл',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['ізмаїл', 'измаил', 'izmail']
            },
            {
                'code': 'chornomorsk',
                'name_uk': 'Чорноморськ',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['чорноморськ', 'черноморск', 'chornomorsk']
            },
            {
                'code': 'yuzhne',
                'name_uk': 'Южне',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['южне', 'yuzhne']
            },
            {
                'code': 'podilsk',
                'name_uk': 'Подільськ',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['подільськ', 'подольск', 'podilsk']
            },
            {
                'code': 'kryvyi-rih',
                'name_uk': 'Кривий Ріг',
                'channel_url': 'https://t.me/+9-w3x2jR8ik1OTNi',
                'aliases': ['кривий ріг', 'кривой рог', 'kryvyi-rih']
            },
            {
                'code': 'kamianske',
                'name_uk': 'Кам\'янське',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['кам\'янське', 'каменское', 'kamianske']
            },
            {
                'code': 'pavlohrad',
                'name_uk': 'Павлоград',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['павлоград', 'pavlohrad']
            },
            {
                'code': 'lozova',
                'name_uk': 'Лозова',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['лозова', 'lozova']
            },
            {
                'code': 'chuhuiv',
                'name_uk': 'Чугуїв',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['чугуїв', 'чугуев', 'chuhuiv']
            },
            {
                'code': 'slavutych',
                'name_uk': 'Славутич',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['славутич', 'slavutych']
            },
            {
                'code': 'truskavets',
                'name_uk': 'Трускавець',
                'channel_url': '',  # Додасте пізніше
                'aliases': ['трускавець', 'трускавец', 'truskavets']
            }
        ]

        async with self.get_connection() as conn:
            for city in cities_data:
                # Вставляємо місто
                now = datetime.utcnow().isoformat()
                await conn.execute("""
                    INSERT OR REPLACE INTO cities (code, name_uk, channel_url, updated_at)
                    VALUES (?, ?, ?, ?)
                """, (city['code'], city['name_uk'], city['channel_url'], now))

                # Видаляємо старі aliases
                await conn.execute("DELETE FROM city_aliases WHERE city_code = ?", (city['code'],))

                # Додаємо нові aliases
                for alias in city['aliases']:
                    await conn.execute("""
                        INSERT OR IGNORE INTO city_aliases (city_code, alias)
                        VALUES (?, ?)
                    """, (city['code'], alias))

            await conn.commit()

        logger.info("✅ Дані міст ініціалізовано")

    async def log_admin_action(self, admin_tg_id: int, action: str, payload: Dict = None):
        """Логування дій адміністратора"""
        now = datetime.utcnow().isoformat()
        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO admin_actions (admin_tg_id, action, payload_json, created_at)
                VALUES (?, ?, ?, ?)
            """, (admin_tg_id, action, json.dumps(payload or {}), now))
            await conn.commit()

    async def close(self):
        """Закриття з'єднання"""
        if self._pool:
            await self._pool.close()