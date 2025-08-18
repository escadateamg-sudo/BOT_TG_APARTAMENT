import asyncio
import asyncpg
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        # Получаем DATABASE_URL из переменных окружения (External URL для Render)
        self.database_url = os.getenv(
            'DATABASE_URL', 
            'postgresql://botuser:AdtVc8H6oXNAWoR0ZbZIC1Cz6Sb56NF4@dpg-d2hp9ibipnbc73cvnj60-a.frankfurt-postgres.render.com/botdb_6cfm'
        )
        self._pool = None
        self.use_postgres = True

    async def init_pool(self):
        """Инициализация пула соединений PostgreSQL"""
        try:
            self._pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=20,
                command_timeout=60,
                server_settings={
                    'jit': 'off'
                }
            )
            logger.info("✅ PostgreSQL пул соединений создан")
            
            # Создаем таблицы при первом запуске
            await self.create_tables()
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания пула PostgreSQL: {e}")
            raise

    async def close(self):
        """Закрытие пула соединений"""
        if self._pool:
            await self._pool.close()
            logger.info("✅ PostgreSQL пул соединений закрыт")

    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для получения соединения из пула"""
        if not self._pool:
            raise RuntimeError("Пул соединений не инициализирован")
        
        conn = await self._pool.acquire()
        try:
            yield conn
        finally:
            await self._pool.release(conn)

    async def create_tables(self):
        """Создание всех необходимых таблиц"""
        async with self.get_connection() as conn:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_blocked BOOLEAN DEFAULT FALSE,
                    blocked_reason TEXT,
                    subscription_end TIMESTAMP
                )
            ''')

            # Таблица городов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cities (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    name_uk TEXT NOT NULL,
                    name_ru TEXT,
                    name_en TEXT,
                    channel_url TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица алиасов городов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS city_aliases (
                    id SERIAL PRIMARY KEY,
                    city_code TEXT NOT NULL REFERENCES cities(code) ON DELETE CASCADE,
                    alias TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(alias)
                )
            ''')

            # Таблица действий пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_actions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    action_type TEXT NOT NULL,
                    city_code TEXT,
                    city_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица действий администратора
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admin_actions (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL,
                    action_type TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Создаем индексы для оптимизации
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_last_activity ON users(last_activity)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_cities_code ON cities(code)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_cities_active ON cities(is_active)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_city_aliases_alias ON city_aliases(alias)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_actions_user_id ON user_actions(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_actions_created_at ON user_actions(created_at)')

        logger.info("✅ Таблицы PostgreSQL созданы/обновлены")

    async def save_user(self, user_id: int, username: str = None, first_name: str = None):
        """Сохранение пользователя с использованием ON CONFLICT"""
        async with self.get_connection() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_activity)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_activity = CURRENT_TIMESTAMP
            ''', user_id, username, first_name)

    async def get_users_count(self) -> int:
        """Получение количества пользователей"""
        async with self.get_connection() as conn:
            result = await conn.fetchval('SELECT COUNT(*) FROM users WHERE NOT is_blocked')
            return result or 0

    async def get_all_users(self) -> List[Dict]:
        """Получение всех активных пользователей"""
        async with self.get_connection() as conn:
            rows = await conn.fetch('SELECT user_id, username, first_name FROM users WHERE NOT is_blocked')
            return [dict(row) for row in rows]

    async def set_user_blocked(self, user_id: int, is_blocked: bool, reason: str = None):
        """Блокировка/разблокировка пользователя"""
        async with self.get_connection() as conn:
            await conn.execute('''
                UPDATE users 
                SET is_blocked = $1, blocked_reason = $2
                WHERE user_id = $3
            ''', is_blocked, reason, user_id)

    async def log_city_selection(self, user_id: int, city_code: str, city_name: str):
        """Логирование выбора города"""
        async with self.get_connection() as conn:
            await conn.execute('''
                INSERT INTO user_actions (user_id, action_type, city_code, city_name)
                VALUES ($1, 'city_selected', $2, $3)
            ''', user_id, city_code, city_name)

    async def find_city_by_alias(self, alias: str) -> Optional[Dict]:
        """Поиск города по алиасу"""
        async with self.get_connection() as conn:
            # Сначала ищем точное совпадение по коду
            row = await conn.fetchrow('''
                SELECT code, name_uk, name_ru, name_en, channel_url
                FROM cities 
                WHERE LOWER(code) = LOWER($1) AND is_active = TRUE
            ''', alias)
            
            if row:
                return dict(row)

            # Затем ищем по алиасам
            row = await conn.fetchrow('''
                SELECT c.code, c.name_uk, c.name_ru, c.name_en, c.channel_url
                FROM cities c
                JOIN city_aliases ca ON c.code = ca.city_code
                WHERE LOWER(ca.alias) = LOWER($1) AND c.is_active = TRUE
            ''', alias)
            
            return dict(row) if row else None

    async def find_cities_by_prefix(self, prefix: str, limit: int = 10) -> List[Dict]:
        """Поиск городов по префиксу"""
        async with self.get_connection() as conn:
            rows = await conn.fetch('''
                SELECT DISTINCT c.code, c.name_uk, c.name_ru, c.name_en, c.channel_url
                FROM cities c
                LEFT JOIN city_aliases ca ON c.code = ca.city_code
                WHERE c.is_active = TRUE AND (
                    LOWER(c.name_uk) LIKE LOWER($1) OR
                    LOWER(c.name_ru) LIKE LOWER($1) OR
                    LOWER(c.code) LIKE LOWER($1) OR
                    LOWER(ca.alias) LIKE LOWER($1)
                )
                ORDER BY c.name_uk
                LIMIT $2
            ''', f'{prefix}%', limit)
            
            return [dict(row) for row in rows]

    async def get_available_cities(self) -> List[Dict]:
        """Получение всех доступных городов с каналами"""
        async with self.get_connection() as conn:
            rows = await conn.fetch('''
                SELECT code, name_uk, name_ru, name_en, channel_url
                FROM cities 
                WHERE is_active = TRUE AND channel_url IS NOT NULL
                ORDER BY name_uk
            ''')
            
            return [dict(row) for row in rows]

    async def get_admin_stats(self) -> Dict:
        """Получение статистики для администратора"""
        async with self.get_connection() as conn:
            # Общая статистика пользователей
            total_users = await conn.fetchval('SELECT COUNT(*) FROM users') or 0
            active_users = await conn.fetchval('SELECT COUNT(*) FROM users WHERE NOT is_blocked') or 0
            blocked_users = await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_blocked') or 0

            # Статистика за последние 7 дней
            week_ago = datetime.now() - timedelta(days=7)
            new_users_7d = await conn.fetchval(
                'SELECT COUNT(*) FROM users WHERE registration_date >= $1', week_ago
            ) or 0

            # Топ городов за последние 30 дней
            month_ago = datetime.now() - timedelta(days=30)
            top_cities = await conn.fetch('''
                SELECT city_code, city_name, COUNT(*) as count
                FROM user_actions 
                WHERE action_type = 'city_selected' AND created_at >= $1
                GROUP BY city_code, city_name
                ORDER BY count DESC
                LIMIT 10
            ''', month_ago)

            return {
                'total_users': total_users,
                'active_users': active_users,
                'blocked_users': blocked_users,
                'total_unsubscriptions': blocked_users,  # Для совместимости
                'new_users_7d': new_users_7d,
                'unsubscribed_7d': 0,  # Заглушка
                'top_cities': [{'city_name_uk': row['city_name'], 'count': row['count']} for row in top_cities]
            }

    async def log_admin_action(self, admin_id: int, action_type: str, details: str = None):
        """Логирование действий администратора"""
        async with self.get_connection() as conn:
            await conn.execute('''
                INSERT INTO admin_actions (admin_id, action_type, details)
                VALUES ($1, $2, $3)
            ''', admin_id, action_type, details)

    async def seed_cities_data(self):
        """Заполнение начальных данных городов"""
        cities_data = [
            {
                'code': 'kyiv',
                'name_uk': 'Київ',
                'name_ru': 'Киев',
                'name_en': 'Kyiv',
                'channel_url': 'https://t.me/+qhZZnTVBluMyOWNi',
                'aliases': ['kyiv', 'kiev', 'киев', 'київ', 'столица', 'столиця']
            },
            {
                'code': 'kharkiv',
                'name_uk': 'Харків',
                'name_ru': 'Харьков',
                'name_en': 'Kharkiv',
                'channel_url': 'https://t.me/+qhZZnTVBluMyOWNi',
                'aliases': ['kharkiv', 'kharkov', 'харків', 'харьков']
            },
            {
                'code': 'odesa',
                'name_uk': 'Одеса',
                'name_ru': 'Одесса',
                'name_en': 'Odesa',
                'channel_url': 'https://t.me/+qhZZnTVBluMyOWNi',
                'aliases': ['odesa', 'odessa', 'одеса', 'одесса']
            },
            {
                'code': 'dnipro',
                'name_uk': 'Дніпро',
                'name_ru': 'Днепр',
                'name_en': 'Dnipro',
                'channel_url': 'https://t.me/+qhZZnTVBluMyOWNi',
                'aliases': ['dnipro', 'dnepr', 'дніпро', 'днепр', 'днепропетровск']
            },
            {
                'code': 'lviv',
                'name_uk': 'Львів',
                'name_ru': 'Львов',
                'name_en': 'Lviv',
                'channel_url': 'https://t.me/+qhZZnTVBluMyOWNi',
                'aliases': ['lviv', 'lvov', 'львів', 'львов']
            }
        ]

        async with self.get_connection() as conn:
            for city_data in cities_data:
                # Вставляем город (ON CONFLICT для предотвращения дублирования)
                await conn.execute('''
                    INSERT INTO cities (code, name_uk, name_ru, name_en, channel_url)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (code) 
                    DO UPDATE SET 
                        name_uk = EXCLUDED.name_uk,
                        name_ru = EXCLUDED.name_ru,
                        name_en = EXCLUDED.name_en,
                        channel_url = EXCLUDED.channel_url,
                        is_active = TRUE
                ''', city_data['code'], city_data['name_uk'], city_data['name_ru'], 
                     city_data['name_en'], city_data['channel_url'])

                # Добавляем алиасы
                for alias in city_data['aliases']:
                    await conn.execute('''
                        INSERT INTO city_aliases (city_code, alias)
                        VALUES ($1, $2)
                        ON CONFLICT (alias) DO NOTHING
                    ''', city_data['code'], alias)

        logger.info("✅ Данные городов загружены в PostgreSQL")
