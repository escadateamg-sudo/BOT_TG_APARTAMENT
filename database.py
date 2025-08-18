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
