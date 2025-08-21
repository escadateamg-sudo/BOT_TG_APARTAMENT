import asyncio
import logging
import os
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from contextlib import asynccontextmanager
from collections import defaultdict
from functools import lru_cache

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, StateFilter, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (Message, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, BotCommand,
                           ReplyKeyboardMarkup, KeyboardButton,
                           ReplyKeyboardRemove)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# Імпорт бази даних
from database import Database

# Завантаження змінних середовища
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️ python-dotenv не встановлено, використовуємо системні змінні")

# Перевірка DATABASE_URL для Replit PostgreSQL
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL:
    print("✅ DATABASE_URL знайдено - використовуємо PostgreSQL")
else:
    print("⚠️ DATABASE_URL не встановлено - перевірте налаштування бази даних")

# ===== КОНФІГУРАЦІЯ =====
BOT_TOKEN = os.getenv('BOT_TOKEN')

def validate_bot_token(token: str) -> bool:
    """Перевіряє формат токена Telegram бота"""
    if not token:
        return False

    parts = token.split(':')
    if len(parts) != 2:
        return False

    try:
        int(parts[0])
    except ValueError:
        return False

    if len(parts[1]) < 28:
        return False

    return True

if not BOT_TOKEN:
    print("❌ ПОМИЛКА: BOT_TOKEN не встановлено!")
    print("💡 Встановіть змінну BOT_TOKEN в налаштуваннях Replit")
    exit(1)

if not validate_bot_token(BOT_TOKEN):
    print("❌ ПОМИЛКА: BOT_TOKEN має неправильний формат!")
    print("💡 Токен має виглядати як: 123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11")
    print("🤖 Отримайте новий токен від @BotFather")
    exit(1)

try:
    ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
except ValueError:
    ADMIN_ID = 0

if ADMIN_ID == 0:
    print("❌ КРИТИЧНА ПОМИЛКА: ADMIN_ID не встановлено!")
    print("💡 Встановіть змінну ADMIN_ID в налаштуваннях Replit")
    exit(1)

ESCADA_CHANNEL = '@Escada_Ukraine'
ESCADA_CHANNEL_LINK = 'https://t.me/+qhZZnTVBluMyOWNi'
ADMIN_CONTACT = 'Escada_m'

# Константи для антиспаму
RATE_LIMIT_THRESHOLD = 5
RATE_LIMIT_WINDOW = 10
MESSAGE_COOLDOWN = 2

# ===== СТАНИ FSM =====
class BotStates(StatesGroup):
    waiting_for_city = State()
    waiting_for_broadcast_message = State()
    waiting_for_rental_form = State()
    admin_menu = State()

# ===== НАЛАШТУВАННЯ ЛОГІНГУ =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Тільки консоль для продакшену
    ])
logger = logging.getLogger(__name__)

# ===== ІНІЦІАЛІЗАЦІЯ =====
storage = MemoryStorage()

try:
    bot = Bot(token=BOT_TOKEN,
              default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    logger.info("✅ Бот успішно ініціалізовано")
except Exception as e:
    logger.error(f"❌ Помилка ініціалізації бота: {e}")
    print(f"❌ Не вдалося ініціалізувати бота: {e}")
    print("💡 Перевірте правильність BOT_TOKEN")
    exit(1)

dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# База даних
db = Database()

# Антиспам система
_antispam_lock = threading.Lock()
user_message_counts: Dict[int, List[float]] = defaultdict(list)
last_message_times: Dict[int, float] = {}
blocked_users: Set[int] = set()

# Кеш для перевірки підписки
subscription_cache: Dict[int, tuple] = {}
SUBSCRIPTION_CACHE_TTL = 30

# Кеш для запобігання дублювання повідомлень
message_cache: Dict[str, str] = {}

# Обмеження розміру кешів
MAX_CACHE_SIZE = 1000  # Зменшено для економії пам'яті

# Періодичне очищення кешів
async def cleanup_caches_periodically():
    """Періодичне очищення кешів"""
    while True:
        try:
            await asyncio.sleep(1800)  # Кожні 30 хвилин
            current_time = time.time()

            with _antispam_lock:
                # Очищуємо старі записи антиспаму
                expired_users = []
                for user_id, messages in user_message_counts.items():
                    user_message_counts[user_id] = [
                        msg_time for msg_time in messages
                        if current_time - msg_time < RATE_LIMIT_WINDOW
                    ]
                    if not user_message_counts[user_id]:
                        expired_users.append(user_id)

                for user_id in expired_users:
                    del user_message_counts[user_id]
                    last_message_times.pop(user_id, None)

                # Очищуємо кеш підписок
                expired_subscriptions = [
                    user_id for user_id, (_, timestamp) in subscription_cache.items()
                    if current_time - timestamp > 60
                ]
                for user_id in expired_subscriptions:
                    del subscription_cache[user_id]

                # Обмежуємо розмір кешів
                if len(subscription_cache) > MAX_CACHE_SIZE:
                    subscription_cache.clear()

                if len(message_cache) > MAX_CACHE_SIZE:
                    message_cache.clear()

            logger.info("✅ Кеші очищено")
        except Exception as e:
            logger.error(f"❌ Помилка очищення кешів: {e}")

# ===== АНТИСПАМ MIDDLEWARE =====
async def check_rate_limit(user_id: int) -> bool:
    """Потокобезпечна перевірка rate limit для користувача"""
    with _antispam_lock:
        if user_id in blocked_users:
            return False

        current_time = time.time()

        user_message_counts[user_id] = [
            msg_time for msg_time in user_message_counts[user_id]
            if current_time - msg_time < RATE_LIMIT_WINDOW
        ]

        if user_id in last_message_times:
            if current_time - last_message_times[user_id] < MESSAGE_COOLDOWN:
                return False

        if len(user_message_counts[user_id]) >= RATE_LIMIT_THRESHOLD:
            blocked_users.add(user_id)
            logger.warning(f"Користувач {user_id} заблокований за спам")
            asyncio.create_task(db.set_user_blocked(user_id, True, 'spam'))
            return False

        user_message_counts[user_id].append(current_time)
        last_message_times[user_id] = current_time

        return True

# ===== ДОПОМІЖНІ ФУНКЦІЇ =====
async def find_city(city_input: str) -> Optional[Dict]:
    """Пошук міста по введенню користувача"""
    city = await db.find_city_by_alias(city_input)
    if city:
        return city

    cities = await db.find_cities_by_prefix(city_input, 1)
    return cities[0] if cities else None

async def check_subscription_fresh(user_id: int, force_refresh: bool = False) -> bool:
    """Свіжа перевірка підписки з мінімальним кешуванням"""
    current_time = time.time()

    if not force_refresh and user_id in subscription_cache:
        is_subscribed, timestamp = subscription_cache[user_id]
        if current_time - timestamp < 30:
            return is_subscribed

    try:
        member = await bot.get_chat_member(ESCADA_CHANNEL, user_id)
        is_subscribed = member.status in ['member', 'administrator', 'creator']

        subscription_cache[user_id] = (is_subscribed, current_time)

        logger.info(f"Підписка користувача {user_id}: {is_subscribed}")
        return is_subscribed

    except TelegramForbiddenError:
        subscription_cache[user_id] = (False, current_time)
        logger.info(f"Користувач {user_id} не підписаний (Forbidden)")
        return False

    except Exception as e:
        logger.error(f"Помилка API при перевірці підписки {user_id}: {e}")
        subscription_cache[user_id] = (False, current_time)
        return False

async def check_subscription_cached(user_id: int) -> bool:
    """Основна функція перевірки підписки"""
    return await check_subscription_fresh(user_id, force_refresh=False)

# Кеш для міст
_cities_cache = {}
_cities_cache_time = 0
CITIES_CACHE_TTL = 300

async def get_available_cities() -> List[Dict]:
    """Повертає міста з доступними каналами з кешуванням"""
    global _cities_cache, _cities_cache_time

    current_time = time.time()

    if _cities_cache and (current_time - _cities_cache_time < CITIES_CACHE_TTL):
        return _cities_cache

    try:
        cities = await db.get_available_cities()
        _cities_cache = cities
        _cities_cache_time = current_time
        return cities
    except Exception as e:
        logger.error(f"Помилка завантаження міст з БД: {e}")
        if _cities_cache:
            logger.warning("Використовуємо старий кеш міст")
            return _cities_cache
        return []

def create_main_keyboard() -> ReplyKeyboardMarkup:
    """Головне меню"""
    keyboard = [[KeyboardButton(text="🏙 Обрати місто")],
                [KeyboardButton(text="📝 Здати квартиру"),
                 KeyboardButton(text="📢 Підписатися на канал")],
                [KeyboardButton(text="✅ Перевірити підписку"),
                 KeyboardButton(text="ℹ️ Допомога")]]
    return ReplyKeyboardMarkup(keyboard=keyboard,
                               resize_keyboard=True,
                               one_time_keyboard=False)

async def create_cities_keyboard() -> InlineKeyboardMarkup:
    """Створює клавіатуру з містами"""
    builder = InlineKeyboardBuilder()
    cities = await get_available_cities()

    for i in range(0, len(cities), 2):
        row = [
            InlineKeyboardButton(text=f"🏙 {cities[i]['name_uk']}",
                                 callback_data=f"city_{cities[i]['code']}")
        ]
        if i + 1 < len(cities):
            row.append(
                InlineKeyboardButton(
                    text=f"🏙 {cities[i + 1]['name_uk']}",
                    callback_data=f"city_{cities[i + 1]['code']}"))
        builder.row(*row)

    builder.row(
        InlineKeyboardButton(text="🔙 Назад до меню",
                             callback_data="back_to_menu"))
    return builder.as_markup()

def create_subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура для підписки"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Підписатися", url=ESCADA_CHANNEL_LINK)
    builder.button(text="✅ Перевірити підписку",
                   callback_data="check_subscription")
    builder.button(text="🔙 Назад до меню", callback_data="back_to_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_keyboard() -> InlineKeyboardMarkup:
    """Адмін панель"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="📩 Розсилка", callback_data="admin_broadcast")
    builder.button(text="👥 Користувачі", callback_data="admin_users")
    builder.button(text="🏙 Міста", callback_data="admin_cities")
    builder.button(text="🔄 Очистити кеш", callback_data="admin_clear_cache")
    builder.adjust(2)
    return builder.as_markup()

async def safe_edit_message(callback: CallbackQuery,
                            text: str,
                            reply_markup: InlineKeyboardMarkup = None):
    """Безпечне редагування повідомлення"""
    try:
        message_key = f"{callback.message.chat.id}_{callback.message.message_id}"

        if message_cache.get(message_key) == text:
            await callback.answer()
            return

        await callback.message.edit_text(text, reply_markup=reply_markup)
        message_cache[message_key] = text

    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"Помилка редагування повідомлення: {e}")
        await callback.answer()
    except Exception as e:
        logger.error(f"Критична помилка редагування: {e}")
        try:
            await callback.message.answer(text, reply_markup=reply_markup)
        except Exception as e2:
            logger.error(f"Помилка відправки нового повідомлення: {e2}")

# ===== ОБРОБНИКИ КОМАНД =====
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Команда /start"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    await state.clear()
    user = message.from_user

    await db.save_user(user.id, user.username, user.first_name)

    user_name = user.first_name or "друже"
    welcome_text = (
        f"👋 <b>Вітаємо, {user_name}!</b>\n\n"
        f"🏠 Бот для пошуку каналів з орендою житла без Ріелтора\n\n"
        f"📍 Оберіть потрібну дію з меню:")

    await message.answer(welcome_text, reply_markup=create_main_keyboard())

@router.message(F.text == "🏙 Обрати місто")
async def handle_select_city(message: Message, state: FSMContext):
    """Вибір міста"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    text = "🏙 <b>Оберіть ваше місто:</b>\n\nНатисніть на кнопку з назвою міста нижче:"
    keyboard = await create_cities_keyboard()
    await message.answer(text, reply_markup=keyboard)
    await state.set_state(BotStates.waiting_for_city)

@router.message(F.text == "📝 Здати квартиру")
async def handle_rent_apartment(message: Message):
    """Здача квартири"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    text = (f"🏠 <b>Здача квартири</b>\n\n"
            f"📝 Для розміщення оголошення про здачу квартири "
            f"зверніться до нашого адміністратора:\n\n"
            f"👤 {ADMIN_CONTACT}\n\n"
            f"Адміністратор допоможе вам:\n"
            f"• Оформити оголошення\n"
            f"• Розмістити в потрібному каналі\n"
            f"• Відповісти на всі питання")

    builder = InlineKeyboardBuilder()
    builder.button(text="👤 Написати адміну",
                   url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")
    builder.button(text="🔙 Назад до меню", callback_data="back_to_menu")
    builder.adjust(1)

    await message.answer(text, reply_markup=builder.as_markup())

@router.message(F.text == "📢 Підписатися на канал")
async def handle_subscribe(message: Message):
    """Підписка на канал"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    text = (f"📢 <b>Головний канал Escada</b>\n\n"
            f"Підпишіться на наш головний канал, щоб отримувати:\n"
            f"• Нові оголошення про оренду\n"
            f"• Корисні поради\n"
            f"• Новини ринку нерухомості\n\n"
            f"📱 {ESCADA_CHANNEL}")

    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Підписатися", url=ESCADA_CHANNEL_LINK)
    builder.button(text="🔙 Назад до меню", callback_data="back_to_menu")
    builder.adjust(1)

    await message.answer(text, reply_markup=builder.as_markup())

@router.message(F.text == "✅ Перевірити підписку")
async def handle_check_subscription(message: Message):
    """Перевірка підписки"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    is_subscribed = await check_subscription_cached(user_id)

    if is_subscribed:
        text = (
            f"✅ <b>Відмінно!</b>\n\n"
            f"Ви підписані на {ESCADA_CHANNEL}\n\n"
            f"Тепер можете обирати місто та отримувати доступ до каналів з орендою житла!"
        )
    else:
        text = (
            f"❌ <b>Підписку не знайдено</b>\n\n"
            f"Для використання бота необхідно підписатися на наш головний канал:\n\n"
            f"📢 {ESCADA_CHANNEL}")

    await message.answer(text, reply_markup=create_main_keyboard())

@router.message(F.text == "ℹ️ Допомога")
async def handle_help(message: Message):
    """Допомога"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    help_key = f"help_{user_id}"
    current_time = time.time()

    if help_key in message_cache:
        last_help_time = float(message_cache[help_key])
        if current_time - last_help_time < 5:
            return

    message_cache[help_key] = str(current_time)

    help_text = (f"ℹ️ <b>Довідка по боту</b>\n\n"
                 f"🏠 <b>Що робить бот:</b>\n"
                 f"• Допомагає знайти канали з орендою житла\n"
                 f"• Підбирає канал для вашого міста\n"
                 f"• Допомагає розмістити оголошення\n\n"
                 f"📋 <b>Як користуватися:</b>\n"
                 f"1️⃣ Підпишіться на {ESCADA_CHANNEL}\n"
                 f"2️⃣ Оберіть ваше місто\n"
                 f"3️⃣ Отримайте посилання на канал\n\n"
                 f"🆘 <b>Підтримка:</b> @{ADMIN_CONTACT}\n"
                 f"📢 <b>Головний канал:</b> {ESCADA_CHANNEL}")

    await message.answer(help_text, reply_markup=create_main_keyboard())

# ===== АДМІНСЬКІ КОМАНДИ =====
@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    """Адмін панель"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Вибачте, команда не розпізнана")
        return

    await state.set_state(BotStates.admin_menu)

    users_count = await db.get_users_count()
    available_cities = len(await get_available_cities())
    text = (f"👑 <b>Адмін панель</b>\n\n"
            f"👥 Користувачів: <b>{users_count}</b>\n"
            f"🏙 Доступних міст: <b>{available_cities}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}")

    await message.answer(text, reply_markup=create_admin_keyboard())

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Статистика"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Вибачте, команда не розпізнана")
        return

    users_count = await db.get_users_count()
    available_cities = len(await get_available_cities())

    stats_text = (f"📊 <b>Детальна статистика:</b>\n\n"
                  f"👥 Всього користувачів: <b>{users_count}</b>\n"
                  f"🏙 Доступних міст: <b>{available_cities}</b>\n"
                  f"🚫 Заблокованих за спам: <b>{len(blocked_users)}</b>\n"
                  f"💾 Кеш підписок: <b>{len(subscription_cache)}</b>\n"
                  f"💬 Кеш повідомлень: <b>{len(message_cache)}</b>")

    await message.answer(stats_text)

# ===== CALLBACK ОБРОБНИКИ =====
@router.callback_query(F.data.startswith("city_"))
async def process_city_selection(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору міста"""
    city_code = callback.data.replace("city_", "")
    user_id = callback.from_user.id

    city = await db.find_city_by_alias(city_code)
    if not city or not city['channel_url']:
        keyboard = await create_cities_keyboard()
        await safe_edit_message(
            callback,
            f"⏳ <b>Місто не знайдено або канал недоступний</b>\n\nОберіть інше місто:",
            keyboard)
        return

    await state.update_data(selected_city=city_code, city_name=city['name_uk'])

    is_subscribed = await check_subscription_cached(user_id)

    if is_subscribed:
        await send_city_channel(callback, city, user_id)
        await state.clear()
    else:
        subscription_text = (
            f"🏠 <b>Ви обрали: {city['name_uk']}</b>\n\n"
            f"✨ Для доступу до каналу спочатку підпішіться на наш головний канал:\n\n"
            f"📢 <b>Escada News📰</b>")
        await safe_edit_message(callback, subscription_text,
                                create_subscription_keyboard())

    await callback.answer()

@router.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery, state: FSMContext):
    """Перевірка підписки через callback"""
    user_id = callback.from_user.id
    data = await state.get_data()
    city_code = data.get('selected_city')

    if not city_code:
        await callback.answer("❌ Помилка: місто не обрано", show_alert=True)
        return

    await callback.answer("🔄 Перевіряю підписку...")
    is_subscribed = await check_subscription_fresh(user_id, force_refresh=True)

    if is_subscribed:
        city = await db.find_city_by_alias(city_code)
        if city:
            await send_city_channel(callback, city, user_id)
            await state.clear()
            try:
                await callback.message.answer("✅ Підписка підтверджена! Канал відкрито.")
            except:
                pass
        else:
            await callback.answer("❌ Помилка: місто не знайдено", show_alert=True)
    else:
        await callback.answer(
            "❌ Підписку не знайдено. Переконайтесь, що ви підписані на канал!",
            show_alert=True)

@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    """Повернення до головного меню"""
    await state.clear()

    text = (f"🏠 <b>Головне меню</b>\n\n"
            f"Оберіть потрібну дію з меню нижче:")

    await callback.message.edit_text(text)
    await callback.answer()

# ===== АДМІНСЬКІ CALLBACK'и =====
@router.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback: CallbackQuery):
    """Детальна статистика для адміна"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Немає доступу", show_alert=True)
        return

    await db.log_admin_action(callback.from_user.id, 'view_stats')

    stats = await db.get_admin_stats()
    available_cities = len(await get_available_cities())

    stats_text = (f"📊 <b>Розширена статистика</b>\n\n"
                  f"👥 Всього користувачів: <b>{stats['total_users']}</b>\n"
                  f"✅ Активних: <b>{stats['active_users']}</b>\n"
                  f"🚫 Заблокованих: <b>{stats['blocked_users']}</b>\n\n"
                  f"📈 <b>За 7 днів:</b>\n"
                  f"🆕 Нових: <b>{stats['new_users_7d']}</b>\n\n"
                  f"🏙 Доступних міст: <b>{available_cities}</b>\n"
                  f"💾 Кеш підписок: <b>{len(subscription_cache)}</b>\n"
                  f"⏰ Оновлено: {datetime.now().strftime('%H:%M:%S')}")

    if stats.get('top_cities'):
        stats_text += f"\n\n🔥 <b>Топ міст (30 днів):</b>\n"
        for i, city_stat in enumerate(stats['top_cities'][:5], 1):
            stats_text += f"{i}. {city_stat['city_name_uk']}: <b>{city_stat['count']}</b>\n"

    await safe_edit_message(callback, stats_text, create_admin_keyboard())

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Розсилка повідомлень"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Немає доступу", show_alert=True)
        return

    users_count = await db.get_users_count()

    text = (f"📢 <b>Розсилка повідомлень</b>\n\n"
            f"👥 Активних користувачів: <b>{users_count}</b>\n\n"
            f"📝 Надішліть повідомлення для розсилки:\n\n"
            f"✅ <b>Підтримується:</b>\n"
            f"• Текстові повідомлення\n"
            f"• Фото з підписом\n"
            f"• Форматування HTML\n\n"
            f"❌ Для скасування: /cancel")

    await safe_edit_message(callback, text)
    await state.set_state(BotStates.waiting_for_broadcast_message)
    await callback.answer()

@router.callback_query(F.data == "admin_users")
async def admin_users_callback(callback: CallbackQuery):
    """Інформація про користувачів"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Немає доступу", show_alert=True)
        return

    stats = await db.get_admin_stats()

    text = (f"👥 <b>Інформація про користувачів</b>\n\n"
            f"Всього користувачів: <b>{stats['total_users']}</b>\n"
            f"Активних: <b>{stats['active_users']}</b>\n"
            f"Заблокованих: <b>{stats['blocked_users']}</b>\n\n"
            f"📈 <b>За останній тиждень:</b>\n"
            f"Нових користувачів: <b>{stats['new_users_7d']}</b>")

    await safe_edit_message(callback, text, create_admin_keyboard())

@router.callback_query(F.data == "admin_cities")
async def admin_cities_callback(callback: CallbackQuery):
    """Управління містами"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Немає доступу", show_alert=True)
        return

    cities = await get_available_cities()

    text = f"🏙 <b>Управління містами</b>\n\n"
    text += f"📊 Всього міст: <b>{len(cities)}</b>\n\n"

    if cities:
        text += "<b>Активні міста:</b>\n"
        for i, city in enumerate(cities[:10], 1):
            text += f"{i}. {city['name_uk']} - {city['code']}\n"

        if len(cities) > 10:
            text += f"... та ще {len(cities) - 10} міст\n"
    else:
        text += "❌ Немає активних міст"

    await safe_edit_message(callback, text, create_admin_keyboard())

@router.callback_query(F.data == "admin_clear_cache")
async def admin_clear_cache(callback: CallbackQuery):
    """Очищення кешу"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Немає доступу", show_alert=True)
        return

    with _antispam_lock:
        subscription_cache.clear()
        message_cache.clear()
        blocked_users.clear()
        user_message_counts.clear()
        last_message_times.clear()

    await callback.answer("✅ Всі кеші очищено!", show_alert=True)

@router.message(Command("checksub"))
async def cmd_admin_check_subscription(message: Message):
    """Адмінська команда для перевірки підписки користувача"""
    if message.from_user.id != ADMIN_ID:
        return

    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer("❌ Використання: /checksub USER_ID")
            return

        user_id = int(parts[1])
        is_subscribed = await check_subscription_fresh(user_id, force_refresh=True)

        status = "✅ Підписаний" if is_subscribed else "❌ Не підписаний"
        await message.answer(f"Користувач {user_id}: {status}")

    except ValueError:
        await message.answer("❌ Некоректний USER_ID")
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")

# ===== ОБРОБНИК РОЗСИЛКИ =====
@router.message(StateFilter(BotStates.waiting_for_broadcast_message))
async def process_broadcast(message: Message, state: FSMContext):
    """Обробка розсилки"""
    if message.from_user.id != ADMIN_ID:
        return

    users = await db.get_all_users()

    if not users:
        await message.answer("❌ Немає користувачів для розсилки")
        await state.clear()
        return

    is_photo = message.photo is not None
    text_content = message.caption if is_photo else message.text
    photo_file_id = message.photo[-1].file_id if is_photo else None

    if not text_content and not is_photo:
        await message.answer("❌ Повідомлення не містить тексту або фото")
        return

    status_msg = await message.answer(
        f"📤 <b>Розпочинаю розсилку...</b>\n\n"
        f"👥 Користувачів: {len(users)}\n"
        f"📄 Тип: {'фото з текстом' if is_photo else 'текстове повідомлення'}")

    sent = 0
    failed = 0
    blocked = 0

    for user in users:
        try:
            if is_photo:
                await bot.send_photo(chat_id=user['user_id'],
                                     photo=photo_file_id,
                                     caption=text_content)
            else:
                await bot.send_message(user['user_id'], text_content)

            sent += 1

        except TelegramForbiddenError:
            blocked += 1
            await db.set_user_blocked(user['user_id'], True, 'blocked')
        except Exception as e:
            failed += 1
            logger.warning(f"Помилка відправки користувачу {user['user_id']}: {e}")

        await asyncio.sleep(0.05)

        if (sent + failed + blocked) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📤 <b>Розсилка в процесі...</b>\n\n"
                    f"✅ Відправлено: {sent}\n"
                    f"❌ Помилок: {failed}\n"
                    f"🚫 Заблокували: {blocked}\n"
                    f"📊 Прогрес: {sent + failed + blocked}/{len(users)}")
            except:
                pass

    message_type = "фото" if is_photo else "текстове повідомлення"
    final_text = (
        f"✅ <b>Розсилка завершена!</b>\n\n"
        f"📤 Відправлено: <b>{sent}</b>\n"
        f"❌ Помилок: <b>{failed}</b>\n"
        f"🚫 Заблокували бота: <b>{blocked}</b>\n"
        f"📄 Тип: <b>{message_type}</b>\n\n"
        f"📊 Успішність: <b>{sent/(sent+failed+blocked)*100:.1f}%</b>")

    await status_msg.edit_text(final_text)
    await state.clear()

# ===== ДОПОМІЖНА ФУНКЦІЯ =====
async def send_city_channel(callback: CallbackQuery, city: Dict, user_id: int):
    """Відправка посилання на канал"""
    await db.log_city_selection(callback.from_user.id, city['code'], city['name_uk'])

    success_text = (f"✅ <b>Дякуємо за підписку!</b>\n\n"
                    f"🏠 <b>Ваше місто: {city['name_uk']}</b>\n\n"
                    f"📢 Ось посилання на канал з орендою житла:")

    builder = InlineKeyboardBuilder()
    builder.button(text=f"🔗 Канал {city['name_uk']}", url=city['channel_url'])
    builder.button(text="🏙 Обрати інше місто", callback_data="back_to_menu")
    builder.button(text="📝 Здати квартиру", url=f"https://t.me/{ADMIN_CONTACT}")
    builder.adjust(1)

    await safe_edit_message(callback, success_text, builder.as_markup())

# ===== ОБРОБНИКИ ТЕКСТОВИХ ПОВІДОМЛЕНЬ =====
@router.message(StateFilter(BotStates.waiting_for_city))
async def handle_city_text_input(message: Message, state: FSMContext):
    """Обробка введення міста текстом"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    city_input = message.text.strip()
    city = await find_city(city_input)

    if city and city['channel_url']:
        is_subscribed = await check_subscription_cached(user_id)

        if is_subscribed:
            await db.log_city_selection(user_id, city['code'], city['name_uk'])

            text = (f"✅ <b>Знайдено: {city['name_uk']}</b>\n\n"
                    f"📢 Ось посилання на канал:")

            builder = InlineKeyboardBuilder()
            builder.button(text=f"🔗 Канал {city['name_uk']}", url=city['channel_url'])
            builder.button(text="🔙 Назад до меню", callback_data="back_to_menu")
            builder.adjust(1)

            await message.answer(text, reply_markup=builder.as_markup())
            await state.clear()
        else:
            await state.update_data(selected_city=city['code'], city_name=city['name_uk'])

            subscription_text = (
                f"🏠 <b>Знайдено: {city['name_uk']}</b>\n\n"
                f"✨ Для доступу до каналу спочатку підпішіться:\n\n"
                f"📢 <b>Escada News📰</b>")

            await message.answer(subscription_text, reply_markup=create_subscription_keyboard())
    elif city:
        keyboard = await create_cities_keyboard()
        await message.answer(
            f"⏳ <b>Місто: {city['name_uk']}</b>\n\n"
            f"❗️ Канал для цього міста поки недоступний.\n\n"
            f"Оберіть інше місто:",
            reply_markup=keyboard)
    else:
        keyboard = await create_cities_keyboard()
        await message.answer(
            f"❌ <b>Місто '{city_input}' не знайдено</b>\n\n"
            f"💡 <b>Поради:</b>\n"
            f"• Перевірте правильність написання\n"
            f"• Спробуйте повну назву міста\n"
            f"• Оберіть зі списку нижче:",
            reply_markup=keyboard)

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Скасування дії"""
    current_state = await state.get_state()
    await state.clear()

    if current_state:
        await message.answer(
            "❌ <b>Дію скасовано</b>\n\n🔄 Повертаємося до головного меню:",
            reply_markup=create_main_keyboard())
    else:
        await message.answer("✅ Немає активних дій для скасування")

# ===== ОБРОБНИК НЕВІДОМИХ КОМАНД =====
@router.message(F.text.startswith('/'))
async def handle_unknown_commands(message: Message):
    """Обробник невідомих команд"""
    await message.answer("❌ Вибачте, команда не розпізнана")

# ===== ОБРОБНИК ІНШИХ ПОВІДОМЛЕНЬ =====
@router.message()
async def handle_other_messages(message: Message):
    """Обробник всіх інших повідомлень"""
    user_id = message.from_user.id

    if not await check_rate_limit(user_id):
        return

    user_name = message.from_user.first_name or "друже"

    help_text = (f"👋 <b>Привіт, {user_name}!</b>\n\n"
                 f"🏠 Цей бот допомагає знаходити канали з орендою житла.\n\n"
                 f"🚀 Використовуйте меню нижче для навігації:")

    await message.answer(help_text, reply_markup=create_main_keyboard())

# ===== ФУНКЦІЇ ЗАПУСКУ =====
async def set_bot_commands():
    """Встановлення команд бота"""
    commands = [
        BotCommand(command="start", description="🚀 Почати роботу"),
        BotCommand(command="help", description="ℹ️ Допомога"),
        BotCommand(command="cancel", description="❌ Скасувати дію"),
    ]
    await bot.set_my_commands(commands)

async def main():
    """Головна функція запуску"""
    logger.info("🚀 Запуск бота...")

    try:
        # Ініціалізуємо БД
        await db.init_pool()
        logger.info("✅ База даних ініціалізована")

        # Заповнюємо початкові дані
        await db.seed_cities_data()

        # Перевіряємо бота
        try:
            bot_info = await bot.get_me()
            logger.info(f"✅ Бот підключено: @{bot_info.username}")
        except Exception as e:
            logger.error(f"❌ Помилка підключення до API Telegram: {e}")
            print("❌ Перевірте правильність BOT_TOKEN та з'єднання з інтернетом")
            return

        # Встановлюємо команди
        await set_bot_commands()

        # Запускаємо фонове очищення кешів
        asyncio.create_task(cleanup_caches_periodically())

        # Повідомляємо адміна про запуск
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID, f"🚀 <b>Бот запущено!</b>\n\n"
                    f"🤖 @{bot_info.username}\n"
                    f"⏰ {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}\n"
                    f"👤 Контакт: {ADMIN_CONTACT}\n"
                    f"🛡 Антиспам: активний")
            except Exception as e:
                logger.warning(f"Не вдалося повідомити адміна: {e}")

        # Пропускаємо старі оновлення
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Старі оновлення пропущено")

        # Запускаємо polling
        logger.info("🔄 Запуск polling...")
        await dp.start_polling(bot)

    except Exception as e:
        logger.error(f"❌ Критична помилка: {e}")
        raise
    finally:
        await db.close()
        logger.info("🛑 Бот зупинено")

if __name__ == "__main__":
    print("🤖 Telegram бот для пошуку житла")
    print(f"👤 Контакт адміна: {ADMIN_CONTACT}")
    print("=" * 50)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Бот зупинено користувачем")
    except Exception as e:
        print(f"\n💥 Критична помилка: {e}")
        exit(1)

