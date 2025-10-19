# -*- coding: utf-8 -*-
"""
Designer Clicker Bot — single-file edition (patched)
===================================================
Полностью рабочий Telegram-кликер «Дизайнер» в одном файле.
Технологии: Python 3.11+ (совместимо с 3.12), aiogram 3.x, SQLAlchemy 2.x (async), SQLite (aiosqlite).

Как запустить:
1) Установите зависимости:
   pip install aiogram SQLAlchemy[asyncio] aiosqlite pydantic python-dotenv

2) Создайте .env рядом с этим файлом и укажите BOT_TOKEN:
   BOT_TOKEN=1234567890:AAFxY-YourRealTelegramBotTokenHere
   # необязательно:
   DATABASE_URL=sqlite+aiosqlite:///./designer.db
   DAILY_BONUS_RUB=100

3) Запуск:
   python designer_clicker_bot.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import wraps
from math import floor
from typing import AsyncIterator, Deque, Dict, List, Literal, Optional, Set, Tuple, Any

# --- .env ---
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# --- aiogram ---
from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

# --- SQLAlchemy ---
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    Index,
    case,
    delete,
    select,
    func,
    update,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.exc import IntegrityError

# ----------------------------------------------------------------------------
# Конфиг и логирование
# ----------------------------------------------------------------------------


@dataclass
class Settings:
    """Простые настройки из окружения. Pydantic не обязателен, чтобы сэкономить импорт."""
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./designer.db")
    DAILY_BONUS_RUB: int = int(os.getenv("DAILY_BONUS_RUB", "100"))
    BASE_ADMIN_ID: int = int(os.getenv("BASE_ADMIN_ID", "0"))


SETTINGS = Settings()


MAX_OFFLINE_SECONDS = 12 * 60 * 60
BASE_CLICK_LIMIT = 10
MAX_CLICK_LIMIT = 30
RANDOM_EVENT_CLICK_INTERVAL = 20
RANDOM_EVENT_CLICK_PROB = 0.25
RANDOM_EVENT_ORDER_PROB = 0.35
SKILL_LEVEL_INTERVAL = 5

COMBO_RESET_SECONDS = 3.0
FREE_ORDER_PROGRESS_PCT = 0.1
FAST_ORDER_SECONDS = 300
HIGH_ORDER_MIN_LEVEL = 5
REQ_CLICKS_REDUCTION_CAP = 0.30
SHOP_DISCOUNT_CAP = 0.20
NEGATIVE_EVENT_REDUCTION_CAP = 0.90
TEAM_DISCOUNT_CAP = 0.80


@dataclass
class ComboTracker:
    bonus: float = 0.0
    last_ts: float = 0.0


_combo_states: Dict[int, ComboTracker] = {}


class JsonLogFormatter(logging.Formatter):
    """Formatter that emits structured JSON lines for easier ingestion."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - short implementation
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in payload or key in {
                "args",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }:
                continue
            payload.setdefault("extras", {})[key] = value
        return json.dumps(payload, ensure_ascii=False)


_handler = logging.StreamHandler()
_handler.setFormatter(JsonLogFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger("designer_clicker_single")

# ----------------------------------------------------------------------------
# I18N — русские строки и подписи кнопок
# ----------------------------------------------------------------------------


class RU:
    # Главное меню
    BTN_CLICK = "🖱️ Клик"
    BTN_ORDERS = "📋 Заказы"
    BTN_UPGRADES = "🛠️ Улучшения"
    BTN_SHOP = "🛒 Магазин"
    BTN_TEAM = "👥 Команда"
    BTN_WARDROBE = "🎽 Гардероб"
    BTN_PROFILE = "👤 Профиль"
    BTN_STATS = "🏆 Топ"
    BTN_ACHIEVEMENTS = "🏆 Достижения"
    BTN_CAMPAIGN = "📜 Кампания"
    BTN_SKILLS = "🎯 Навыки"
    BTN_QUEST = "😈 Квест"
    BTN_STUDIO = "🏢 Студия"

    # Общие
    BTN_MENU = "🏠 Меню"
    BTN_TO_MENU = "🏠 Перейти в меню"
    BTN_PREV = "⏮️ Страница назад"
    BTN_NEXT = "Страница вперёд ▶️"
    BTN_TAKE = "🚀 Взять заказ"
    BTN_CANCEL = "❌ Отмена"
    BTN_CONFIRM = "✅ Подтвердить"
    BTN_EQUIP = "🧩 Экипировать"
    BTN_BUY = "💳 Купить"
    BTN_UPGRADE = "⚙️ Повысить"
    BTN_BOOSTS = "⚡ Бусты"
    BTN_EQUIPMENT = "🧰 Экипировка"
    BTN_DAILY = "🎁 Ежедневный бонус"
    BTN_CANCEL_ORDER = "🛑 Отменить заказ"
    BTN_BACK = "◀️ Назад"
    BTN_RETURN_ORDER = "🔙 Вернуться к заказу"
    BTN_HOME = "🏠 Меню"
    BTN_TUTORIAL_NEXT = "➡️ Далее"
    BTN_TUTORIAL_SKIP = "⏭️ Пропустить"
    BTN_SHOW_ACHIEVEMENTS = "🏆 Посмотреть достижения"
    BTN_CAMPAIGN_CLAIM = "🎁 Забрать награду"
    BTN_STUDIO_CONFIRM = "✨ Открыть студию"

    # Сообщения
    BOT_STARTED = "Бот запущен."
    WELCOME = "🎨 Добро пожаловать в «Дизайнер»! У вас уже 200 ₽ стартового капитала — попробуйте любой раздел ниже."
    MENU_HINT = "📍 Главное меню: выберите раздел."
    MENU_WITH_ORDER_HINT = "📍 Главное меню: продолжайте заказ или откройте другой раздел."
    TOO_FAST = "⏳ Темп слишком высокий. Дождитесь восстановления лимита."
    NO_ACTIVE_ORDER = "🧾 У вас нет активного заказа. Загляните в раздел «Заказы»."
    CLICK_PROGRESS = "🖱️ Прогресс: {cur}/{req} кликов ({pct}%)."
    ORDER_TAKEN = "🚀 Вы взяли заказ «{title}». Удачи!"
    ORDER_ALREADY = "⚠️ У вас уже есть активный заказ."
    ORDER_DONE = "✅ Заказ завершён! Награда: {rub} ₽, XP: {xp}."
    ORDER_CANCELED = "↩️ Заказ отменён. Прогресс сброшен."
    ORDER_RESUME = "🧾 Продолжаем заказ «{title}». Кликай, чтобы продвинуться."
    INSUFFICIENT_FUNDS = "💸 Недостаточно средств."
    PURCHASE_OK = "🛒 Покупка успешна!"
    UPGRADE_OK = "🔼 Повышение выполнено."
    EQUIP_OK = "🧩 Экипировка активирована."
    EQUIP_NOITEM = "🕹️ Сначала купите предмет."
    DAILY_OK = "🎁 Начислен ежедневный бонус: {rub} ₽."
    DAILY_WAIT = "⏰ Бонус уже получен. Загляните позже."
    PROFILE = (
        "🧑‍💼 {name} · 🏅 Ур. {lvl}\n"
        "✨ XP: {xp}/{xp_need} {xp_bar} {xp_pct}%\n"
        "💰 Баланс: {rub} ₽ · 📈 Ср. доход: {avg} ₽\n"
        "🖱️ Сила клика: {cp} · 💤 Пассив: {passive}/мин\n"
        "📌 Заказ: {order}\n"
        "🛡️ Баффы: {buffs}\n"
        "📜 Кампания: {campaign}\n"
        "🏢 Репутация: {rep}"
    )
    TEAM_HEADER = "👥 Команда (доход/мин, уровень, цена повышения):"
    TEAM_LOCKED = "👥 Команда откроется со 2 уровня."
    SHOP_HEADER = "🛒 Магазин: выберите раздел для прокачки."
    WARDROBE_HEADER = "🎽 Гардероб: слоты и доступные предметы."
    ORDERS_HEADER = "📋 Доступные заказы"
    UPGRADES_HEADER = "🛠️ Улучшения: выберите раздел."
    STATS_HEADER = "🏆 Топ-5 по среднему доходу"
    STATS_ROW = "{idx}. Игрок: {name} — {value} ₽"
    STATS_EMPTY_ROW = "{idx}. Отсутствует"
    STATS_POSITION = "📈 Ваше место: {rank} из {total}"
    STATS_POSITION_MISSING = "📈 Вы не в рейтинге"
    ACHIEVEMENT_UNLOCK = "🏆 Новое достижение: {title}!"
    ACHIEVEMENTS_TITLE = "🏆 Достижения"
    ACHIEVEMENTS_EMPTY = "Пока нет достижений — продолжайте играть!"
    ACHIEVEMENTS_ENTRY = "{icon} {name} — {desc}"
    TUTORIAL_DONE = "🎓 Обучение завершено! Нажмите кнопку в меню, чтобы продолжить."
    TUTORIAL_HINT = "⚡ Готовы начать играть? Нажмите «{button}» внизу."
    EVENT_POSITIVE = "{title}"
    EVENT_NEGATIVE = "{title}"
    EVENT_BUFF = "{title}"
    EVENT_BUFF_ACTIVE = "🔔 Активен бафф: {title} (до {expires})"
    QUEST_LOCKED = "😈 Квест доступен с 2 уровня. Продолжайте прокачку!"
    QUEST_ALREADY_DONE = "😈 Вы уже усмирили клиента из ада!"
    QUEST_INTRO = "🔥 Клиент из ада появился в чате. Готовы к испытанию?"
    QUEST_STEP = "{text}"
    QUEST_FINISH = "😈 Квест завершён! Награда: {rub} ₽ и {xp} XP."
    QUEST_ITEM_GAIN = "📜 Вы получили талисман клиента — терпение +{pct}%!"
    CAMPAIGN_HEADER = "📜 Кампания «От фрилансера до студии»"
    CAMPAIGN_STATUS = "Глава {chapter}/{total}: {title}\nЦель: {goal}\nПрогресс: {progress}%"
    CAMPAIGN_DONE = "Глава выполнена! Заберите награду."
    CAMPAIGN_EMPTY = "Кампания недоступна — прокачайте уровень."
    CAMPAIGN_REWARD = "🎁 Награда кампании: +{rub} ₽ и +{xp} XP."
    SKILL_PROMPT = "🎯 Выберите навык:"
    SKILL_PICKED = "🎯 Получен навык «{name}»."
    SKILL_LIST_HEADER = "🎯 Навыки:"
    SKILL_LIST_EMPTY = "Навыки ещё не выбраны."
    STUDIO_LOCKED = "🏢 Студия доступна с 20 уровня."
    STUDIO_INFO = "🏢 Репутация: {rep}\nПовторные открытия: {resets}\nБонус дохода: +{bonus}%"
    STUDIO_CONFIRM = "Сбросить прогресс и открыть студию? Получите +{gain} репутации."
    STUDIO_DONE = "✨ Вы открыли студию! Репутация выросла на {gain}."

    # Форматирование
    CURRENCY = "₽"


# ----------------------------------------------------------------------------
# Клавиатуры (только ReplyKeyboard)
# ----------------------------------------------------------------------------


def _reply_keyboard(rows: List[List[str]]) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=cell) for cell in row] for row in rows],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
    )


def kb_main_menu(has_active_order: bool = False) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = []
    if has_active_order:
        rows.append([RU.BTN_RETURN_ORDER])
    rows.append([RU.BTN_ORDERS])
    rows.append([RU.BTN_UPGRADES])
    rows.append([RU.BTN_PROFILE])
    return _reply_keyboard(rows)


def kb_active_order_controls() -> ReplyKeyboardMarkup:
    return _reply_keyboard([[RU.BTN_CLICK, RU.BTN_TO_MENU]])


def kb_numeric_page(show_prev: bool, show_next: bool, add_back: bool = True) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[str(i) for i in range(1, 6)]]
    nav_row: List[str] = []
    if show_prev:
        nav_row.append(RU.BTN_PREV)
    if show_next:
        nav_row.append(RU.BTN_NEXT)
    if nav_row:
        rows.append(nav_row)
    if add_back:
        rows.append([RU.BTN_BACK])
    return _reply_keyboard(rows)


def kb_confirm(confirm_text: str = RU.BTN_CONFIRM, add_menu: bool = False) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[confirm_text, RU.BTN_CANCEL]]
    if add_menu:
        rows.append([RU.BTN_BACK])
    return _reply_keyboard(rows)


def kb_upgrades_menu(include_team: bool) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[RU.BTN_SHOP], [RU.BTN_WARDROBE]]
    if include_team:
        rows.append([RU.BTN_TEAM])
    rows.append([RU.BTN_BACK])
    return _reply_keyboard(rows)


def kb_shop_menu() -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[RU.BTN_BOOSTS, RU.BTN_EQUIPMENT], [RU.BTN_BACK]]
    return _reply_keyboard(rows)


def kb_profile_menu(has_active_order: bool) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[RU.BTN_DAILY, RU.BTN_SKILLS]]
    _ = has_active_order  # Signature kept for compatibility with legacy callers.
    rows.append([RU.BTN_STATS, RU.BTN_ACHIEVEMENTS])
    rows.append([RU.BTN_CAMPAIGN, RU.BTN_STUDIO])
    rows.append([RU.BTN_BACK])
    return _reply_keyboard(rows)


def kb_tutorial() -> ReplyKeyboardMarkup:
    rows = [[RU.BTN_TUTORIAL_NEXT, RU.BTN_TUTORIAL_SKIP], [RU.BTN_BACK]]
    return _reply_keyboard(rows)


def kb_achievement_prompt() -> ReplyKeyboardMarkup:
    rows = [[RU.BTN_SHOW_ACHIEVEMENTS], [RU.BTN_BACK]]
    return _reply_keyboard(rows)


def kb_skill_choices(count: int) -> ReplyKeyboardMarkup:
    rows = [[str(i + 1) for i in range(count)], [RU.BTN_BACK]]
    return _reply_keyboard(rows)


def kb_quest_options(options: List[str]) -> ReplyKeyboardMarkup:
    rows = [[opt] for opt in options]
    rows.append([RU.BTN_BACK])
    return _reply_keyboard(rows)


async def build_main_menu_markup(
    session: Optional[AsyncSession] = None,
    user: Optional[User] = None,
    tg_id: Optional[int] = None,
) -> ReplyKeyboardMarkup:
    """Return main menu keyboard, showing resume button if order is active."""

    if session is None:
        async with session_scope() as new_session:
            return await build_main_menu_markup(new_session, user=user, tg_id=tg_id)
    if user is None and tg_id is not None:
        user = await get_user_by_tg(session, tg_id)
    if user is not None:
        active = await get_active_order(session, user)
        return kb_main_menu(has_active_order=bool(active))
    return kb_main_menu()


async def main_menu_for_message(
    message: Message, session: Optional[AsyncSession] = None, user: Optional[User] = None
) -> ReplyKeyboardMarkup:
    """Shortcut to build main menu keyboard for a Telegram message."""

    return await build_main_menu_markup(session=session, user=user, tg_id=message.from_user.id)


TUTORIAL_STEPS = [
    {
        "text": "👋 Привет! Начните с раздела «Заказы» — там берём первые задачи и зарабатываем ₽.",
        "button": RU.BTN_ORDERS,
    },
    {
        "text": "🖱️ После взятия заказа жмите «Клик» и доводите прогресс до 100%.",
        "button": RU.BTN_CLICK,
    },
    {
        "text": "🛠️ Раздел «Улучшения» собрал магазин, гардероб и команду для роста дохода.",
        "button": RU.BTN_UPGRADES,
    },
    {
        "text": "👤 В «Профиле» смотрите статистику, достижения и забирайте бонусы.",
        "button": RU.BTN_PROFILE,
    },
]


async def send_tutorial_step_message(message: Message, step: int) -> None:
    """Send a tutorial step with contextual hint buttons."""

    if step >= len(TUTORIAL_STEPS):
        await message.answer(
            RU.TUTORIAL_DONE,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    payload = TUTORIAL_STEPS[step]
    hint = RU.TUTORIAL_HINT.format(button=payload["button"])
    await message.answer(f"{payload['text']}\n\n{hint}", reply_markup=kb_tutorial())


# ----------------------------------------------------------------------------
# Обёртка обработчиков
# ----------------------------------------------------------------------------

ERROR_MESSAGE = "Произошла ошибка. Попробуйте позже."


def safe_handler(func):
    """Обёртка для обработчиков сообщений, чтобы логировать ошибки и отвечать пользователю."""

    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        try:
            return await func(message, *args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - важно логировать любые сбои
            logger.exception("Unhandled error in %s", func.__name__, exc_info=exc)
            if isinstance(message, Message):
                try:
                    await message.answer(
                        ERROR_MESSAGE,
                        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("Failed to send error notification to user")

    return wrapper


# ----------------------------------------------------------------------------
# Утилиты
# ----------------------------------------------------------------------------

def utcnow() -> datetime:
    """Return the current UTC time as naive datetime in UTC zone.

    SQLite does not preserve timezone info in ``DateTime`` columns reliably,
    therefore values loaded back are usually naive. Returning a naive datetime
    keeps arithmetic consistent when we subtract stored values from the current
    timestamp.
    """

    return datetime.utcnow()


def ensure_naive(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalize datetime to naive UTC representation."""

    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def slice_page(items: List, page: int, page_size: int = 5) -> Tuple[List, bool, bool]:
    """Return sublist for pagination along with availability of prev/next pages."""

    start = page * page_size
    end = start + page_size
    sub = items[start:end]
    has_prev = page > 0
    has_next = end < len(items)
    return sub, has_prev, has_next


# ----------------------------------------------------------------------------
# ORM модели
# ----------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    first_name: Mapped[str] = mapped_column(String(128), default="")
    balance: Mapped[int] = mapped_column(Integer, default=200)
    cp_base: Mapped[int] = mapped_column(Integer, default=1)  # базовая сила клика
    reward_mul: Mapped[float] = mapped_column(Float, default=0.0)  # добавочный % к награде (0.10=+10%)
    passive_mul: Mapped[float] = mapped_column(Float, default=0.0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    xp: Mapped[int] = mapped_column(Integer, default=0)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    daily_bonus_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    tutorial_stage: Mapped[int] = mapped_column(Integer, default=0)
    tutorial_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    clicks_total: Mapped[int] = mapped_column(Integer, default=0)
    orders_completed: Mapped[int] = mapped_column(Integer, default=0)
    passive_income_collected: Mapped[int] = mapped_column(Integer, default=0)
    daily_bonus_claims: Mapped[int] = mapped_column(Integer, default=0)

    orders: Mapped[List["UserOrder"]] = relationship(back_populates="user")


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    base_clicks: Mapped[int] = mapped_column(Integer)
    min_level: Mapped[int] = mapped_column(Integer, default=1)
    __table_args__ = (Index("ix_orders_min_level", "min_level"),)


class UserOrder(Base):
    __tablename__ = "user_orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    progress_clicks: Mapped[int] = mapped_column(Integer, default=0)
    required_clicks: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished: Mapped[bool] = mapped_column(Boolean, default=False)
    canceled: Mapped[bool] = mapped_column(Boolean, default=False)
    reward_snapshot_mul: Mapped[float] = mapped_column(Float, default=1.0)

    user: Mapped["User"] = relationship(back_populates="orders")
    order: Mapped["Order"] = relationship()
    __table_args__ = (
        Index("ix_user_orders_active", "user_id", "finished", "canceled"),
    )


class Boost(Base):
    __tablename__ = "boosts"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    type: Mapped[str] = mapped_column(String(30))
    base_cost: Mapped[int] = mapped_column(Integer)
    growth: Mapped[float] = mapped_column(Float)
    step_value: Mapped[float] = mapped_column(Float)


class UserBoost(Base):
    __tablename__ = "user_boosts"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    boost_id: Mapped[int] = mapped_column(ForeignKey("boosts.id", ondelete="CASCADE"))
    level: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (UniqueConstraint("user_id", "boost_id", name="uq_user_boost"),)


class TeamMember(Base):
    __tablename__ = "team_members"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    base_income_per_min: Mapped[float] = mapped_column(Float)
    base_cost: Mapped[int] = mapped_column(Integer)


class UserTeam(Base):
    __tablename__ = "user_team"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("team_members.id", ondelete="CASCADE"))
    level: Mapped[int] = mapped_column(Integer, default=0)  # 0 — не нанят

    __table_args__ = (UniqueConstraint("user_id", "member_id", name="uq_user_team"),)


class Item(Base):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    slot: Mapped[Literal["laptop", "phone", "tablet", "monitor", "chair", "charm"]] = mapped_column(String(20))
    tier: Mapped[int] = mapped_column(Integer)
    bonus_type: Mapped[Literal["cp_pct", "passive_pct", "req_clicks_pct", "reward_pct", "ratelimit_plus"]] = mapped_column(String(30))
    bonus_value: Mapped[float] = mapped_column(Float)
    price: Mapped[int] = mapped_column(Integer)
    min_level: Mapped[int] = mapped_column(Integer, default=1)
    __table_args__ = (Index("ix_items_slot_tier", "slot", "tier"),)


class UserItem(Base):
    __tablename__ = "user_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("items.id", ondelete="CASCADE"))
    __table_args__ = (UniqueConstraint("user_id", "item_id", name="uq_user_item"),)


class UserEquipment(Base):
    __tablename__ = "user_equipment"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    slot: Mapped[Literal["laptop", "phone", "tablet", "monitor", "chair", "charm"]] = mapped_column(String(20))
    item_id: Mapped[Optional[int]] = mapped_column(ForeignKey("items.id", ondelete="SET NULL"), nullable=True)
    __table_args__ = (UniqueConstraint("user_id", "slot", name="uq_user_slot"),)


class EconomyLog(Base):
    __tablename__ = "economy_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    type: Mapped[str] = mapped_column(String(30))
    amount: Mapped[float] = mapped_column(Float, default=0.0)
    meta: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (Index("ix_economy_user_created", "user_id", "created_at"),)


class Achievement(Base):
    __tablename__ = "achievements"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(150))
    description: Mapped[str] = mapped_column(String(300))
    trigger: Mapped[str] = mapped_column(String(30))
    threshold: Mapped[int] = mapped_column(Integer)
    icon: Mapped[str] = mapped_column(String(8), default="🏆")


class UserAchievement(Base):
    __tablename__ = "user_achievements"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    achievement_id: Mapped[int] = mapped_column(ForeignKey("achievements.id", ondelete="CASCADE"))
    progress: Mapped[int] = mapped_column(Integer, default=0)
    unlocked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    notified: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (UniqueConstraint("user_id", "achievement_id", name="uq_user_achievement"),)


class RandomEvent(Base):
    __tablename__ = "random_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(200))
    kind: Mapped[Literal["bonus", "penalty", "buff"]] = mapped_column(String(20))
    amount: Mapped[float] = mapped_column(Float)
    duration_sec: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    weight: Mapped[int] = mapped_column(Integer, default=1)
    min_level: Mapped[int] = mapped_column(Integer, default=1)


class UserBuff(Base):
    __tablename__ = "user_buffs"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    code: Mapped[str] = mapped_column(String(50))
    title: Mapped[str] = mapped_column(String(200))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict] = mapped_column(JSON)

    __table_args__ = (
        Index("ix_user_buffs_active", "user_id", "expires_at"),
    )


class UserQuest(Base):
    __tablename__ = "user_quests"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    quest_code: Mapped[str] = mapped_column(String(50))
    stage: Mapped[int] = mapped_column(Integer, default=0)
    is_done: Mapped[bool] = mapped_column(Boolean, default=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)

    __table_args__ = (UniqueConstraint("user_id", "quest_code", name="uq_user_quest"),)


class CampaignProgress(Base):
    __tablename__ = "campaign_progress"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    chapter: Mapped[int] = mapped_column(Integer, default=1)
    is_done: Mapped[bool] = mapped_column(Boolean, default=False)
    progress: Mapped[dict] = mapped_column(JSON, default=dict)

    __table_args__ = (UniqueConstraint("user_id", name="uq_campaign_user"),)


class Skill(Base):
    __tablename__ = "skills"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    branch: Mapped[Literal["web", "brand", "art"]] = mapped_column(String(20))
    effect: Mapped[dict] = mapped_column(JSON)
    min_level: Mapped[int] = mapped_column(Integer, default=1)


class UserSkill(Base):
    __tablename__ = "user_skills"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    skill_code: Mapped[str] = mapped_column(ForeignKey("skills.code", ondelete="CASCADE"))
    taken_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("user_id", "skill_code", name="uq_user_skill"),)


class UserPrestige(Base):
    __tablename__ = "user_prestige"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    reputation: Mapped[int] = mapped_column(Integer, default=0)
    resets: Mapped[int] = mapped_column(Integer, default=0)
    last_reset_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)

    __table_args__ = (UniqueConstraint("user_id", name="uq_user_prestige"),)


# ----------------------------------------------------------------------------
# Подключение к БД
# ----------------------------------------------------------------------------

engine = create_async_engine(SETTINGS.DATABASE_URL, echo=False, future=True)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_models() -> None:
    """Create database tables if they do not exist."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Provide a transactional scope for database work with automatic commit/rollback."""
    async with async_session_maker() as session:
        try:
            async with session.begin():
                yield session
        except Exception:
            logger.exception("Session rollback due to error.")
            raise


async def prepare_database() -> None:
    """Ensure that database schema and seed data are initialized exactly once."""
    async with session_scope() as session:
        await ensure_schema(session)
        await seed_if_needed(session)


async def ensure_schema(session: AsyncSession) -> None:
    """Add missing columns/tables for backward compatibility without full migrations."""

    async def _existing_columns(table: str) -> Set[str]:
        rows = await session.execute(text(f"PRAGMA table_info({table})"))
        return {row[1] for row in rows}

    user_columns = await _existing_columns("users")
    if "tutorial_stage" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN tutorial_stage INTEGER NOT NULL DEFAULT 0"))
    if "tutorial_completed_at" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN tutorial_completed_at DATETIME"))
    if "clicks_total" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN clicks_total INTEGER NOT NULL DEFAULT 0"))
    if "orders_completed" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN orders_completed INTEGER NOT NULL DEFAULT 0"))
    if "passive_income_collected" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN passive_income_collected INTEGER NOT NULL DEFAULT 0"))
    if "daily_bonus_claims" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN daily_bonus_claims INTEGER NOT NULL DEFAULT 0"))


# ----------------------------------------------------------------------------
# Сиды данных (встроенные)
# ----------------------------------------------------------------------------

SEED_ORDERS = [
    {"title": "Визитка для фрилансера", "base_clicks": 100, "min_level": 1},
    {"title": "Обложка для VK", "base_clicks": 180, "min_level": 1},
    {"title": "Логотип для кафе", "base_clicks": 300, "min_level": 2},
    {"title": "Лендинг (1 экран)", "base_clicks": 600, "min_level": 3},
    {"title": "Брендбук (мини)", "base_clicks": 1200, "min_level": 5},
    {"title": "Редизайн логотипа", "base_clicks": 800, "min_level": 4},
]

SEED_BOOSTS = [
    {"code": "reward_mastery", "name": "🎯 Награда", "type": "reward", "base_cost": 300, "growth": 1.28, "step_value": 0.10},
    {"code": "finger_training", "name": "🧪 Тренировка пальцев", "type": "cp", "base_cost": 380, "growth": 1.25, "step_value": 1},
    {"code": "passive_income_plus", "name": "💼 Пассивный доход", "type": "passive", "base_cost": 460, "growth": 1.27, "step_value": 0.10},
    {"code": "click_overdrive", "name": "⚡️ Клик", "type": "cp", "base_cost": 520, "growth": 1.25, "step_value": 1},
    {"code": "accelerated_learning", "name": "🧠 Ускоренное обучение", "type": "xp", "base_cost": 560, "growth": 1.22, "step_value": 0.08},
    {"code": "critical_strike", "name": "💥 Крит-удар", "type": "crit", "base_cost": 700, "growth": 1.28, "step_value": 0.02},
    {"code": "anti_brak", "name": "🧿 Антибрак", "type": "event_protection", "base_cost": 740, "growth": 1.26, "step_value": 0.10},
    {"code": "process_optimization", "name": "🎛️ Оптимизация процессов", "type": "passive", "base_cost": 760, "growth": 1.27, "step_value": 0.06},
    {"code": "combo_click", "name": "🔗 Комбо-клик", "type": "combo", "base_cost": 820, "growth": 1.25, "step_value": 0.2},
    {"code": "team_synergy", "name": "👥 Слаженная команда", "type": "team_income", "base_cost": 860, "growth": 1.28, "step_value": 0.07},
    {"code": "ergonomics", "name": "🪑 Эргономика", "type": "ratelimit", "base_cost": 900, "growth": 1.30, "step_value": 1},
    {"code": "requirement_relief", "name": "🧭 Снижение требований", "type": "req_clicks", "base_cost": 980, "growth": 1.35, "step_value": 0.03},
    {"code": "quick_briefs", "name": "📦 Быстрые брифы", "type": "free_order", "base_cost": 1040, "growth": 1.28, "step_value": 0.03},
    {"code": "contractor_discount", "name": "🧾 Скидки подрядчикам", "type": "team_discount", "base_cost": 1080, "growth": 1.30, "step_value": 0.05},
    {"code": "deep_offline", "name": "💤 Глубокий офлайн", "type": "offline_cap", "base_cost": 1140, "growth": 1.32, "step_value": 7200},
    {"code": "tight_deadlines", "name": "⏱️ Сжатые дедлайны", "type": "rush_reward", "base_cost": 1200, "growth": 1.30, "step_value": 0.05},
    {"code": "gear_tuning", "name": "🧰 Тюнинг экипировки", "type": "equipment_eff", "base_cost": 1280, "growth": 1.35, "step_value": 0.05},
    {"code": "night_flow", "name": "🌙 Ночной поток", "type": "night_passive", "base_cost": 1360, "growth": 1.33, "step_value": 0.12},
    {"code": "shop_wholesale", "name": "🛍️ Опт в магазине", "type": "shop_discount", "base_cost": 1420, "growth": 1.33, "step_value": 0.04},
    {"code": "premium_projects", "name": "🎯 Премиум-проекты", "type": "high_order_reward", "base_cost": 1500, "growth": 1.34, "step_value": 0.08},
]

BOOST_EXTRA_META: Dict[str, Dict[str, Any]] = {
    "critical_strike": {"crit_multiplier": 1.5},
    "combo_click": {"combo_cap": 2.0},
}

SEED_TEAM = [
    {"code": "junior", "name": "Junior Designer", "base_income_per_min": 4, "base_cost": 100},
    {"code": "middle", "name": "Middle Designer", "base_income_per_min": 10, "base_cost": 300},
    {"code": "senior", "name": "Senior Designer", "base_income_per_min": 22, "base_cost": 800},
    {"code": "pm", "name": "Project Manager", "base_income_per_min": 35, "base_cost": 1200},
]

SEED_ITEMS = [
    {"code": "laptop_t1", "name": "Ноутбук «NeoBook»", "slot": "laptop", "tier": 1, "bonus_type": "cp_pct", "bonus_value": 0.05, "price": 250, "min_level": 1},
    {"code": "laptop_t2", "name": "Ноутбук «PixelForge»", "slot": "laptop", "tier": 2, "bonus_type": "cp_pct", "bonus_value": 0.10, "price": 500, "min_level": 2},
    {"code": "laptop_t3", "name": "Ноутбук «Aurora Pro»", "slot": "laptop", "tier": 3, "bonus_type": "cp_pct", "bonus_value": 0.15, "price": 900, "min_level": 3},

    {"code": "phone_t1", "name": "Смартфон «City Lite»", "slot": "phone", "tier": 1, "bonus_type": "passive_pct", "bonus_value": 0.03, "price": 200, "min_level": 1},
    {"code": "phone_t2", "name": "Смартфон «Pulse Max»", "slot": "phone", "tier": 2, "bonus_type": "passive_pct", "bonus_value": 0.06, "price": 400, "min_level": 2},
    {"code": "phone_t3", "name": "Смартфон «Nova Edge»", "slot": "phone", "tier": 3, "bonus_type": "passive_pct", "bonus_value": 0.10, "price": 750, "min_level": 3},

    {"code": "tablet_t1", "name": "Планшет «TabFlow»", "slot": "tablet", "tier": 1, "bonus_type": "req_clicks_pct", "bonus_value": 0.02, "price": 300, "min_level": 1},
    {"code": "tablet_t2", "name": "Планшет «SketchWave»", "slot": "tablet", "tier": 2, "bonus_type": "req_clicks_pct", "bonus_value": 0.04, "price": 600, "min_level": 2},
    {"code": "tablet_t3", "name": "Планшет «FrameMaster»", "slot": "tablet", "tier": 3, "bonus_type": "req_clicks_pct", "bonus_value": 0.06, "price": 950, "min_level": 3},

    {"code": "monitor_t1", "name": "Монитор «PixelWide»", "slot": "monitor", "tier": 1, "bonus_type": "reward_pct", "bonus_value": 0.04, "price": 350, "min_level": 1},
    {"code": "monitor_t2", "name": "Монитор «VisionGrid»", "slot": "monitor", "tier": 2, "bonus_type": "reward_pct", "bonus_value": 0.08, "price": 700, "min_level": 2},
    {"code": "monitor_t3", "name": "Монитор «UltraCanvas»", "slot": "monitor", "tier": 3, "bonus_type": "reward_pct", "bonus_value": 0.12, "price": 1050, "min_level": 3},

    {"code": "chair_t1", "name": "Стул «Кафе»", "slot": "chair", "tier": 1, "bonus_type": "ratelimit_plus", "bonus_value": 0, "price": 150, "min_level": 1},
    {"code": "chair_t2", "name": "Стул «Balance»", "slot": "chair", "tier": 2, "bonus_type": "ratelimit_plus", "bonus_value": 1, "price": 400, "min_level": 2},
    {"code": "chair_t3", "name": "Стул «Flow»", "slot": "chair", "tier": 3, "bonus_type": "ratelimit_plus", "bonus_value": 1, "price": 600, "min_level": 3},
    {"code": "chair_t4", "name": "Стул «Gravity»", "slot": "chair", "tier": 4, "bonus_type": "ratelimit_plus", "bonus_value": 2, "price": 1000, "min_level": 4},
    {"code": "client_contract", "name": "Талисман клиента", "slot": "charm", "tier": 1, "bonus_type": "req_clicks_pct", "bonus_value": 0.03, "price": 0, "min_level": 2},
]

SEED_ACHIEVEMENTS = [
    {"code": "click_100", "name": "Разогрев пальцев", "description": "Совершите 100 кликов.", "trigger": "clicks", "threshold": 100, "icon": "🖱️"},
    {"code": "click_1000", "name": "Мастер клика", "description": "Совершите 1000 кликов.", "trigger": "clicks", "threshold": 1000, "icon": "⚡"},
    {"code": "order_first", "name": "Первый заказ", "description": "Закончите первый заказ.", "trigger": "orders", "threshold": 1, "icon": "📋"},
    {"code": "order_20", "name": "Портфолио растёт", "description": "Завершите 20 заказов.", "trigger": "orders", "threshold": 20, "icon": "🗂️"},
    {"code": "level_5", "name": "Ученик", "description": "Достигните 5 уровня.", "trigger": "level", "threshold": 5, "icon": "📈"},
    {"code": "level_10", "name": "Легенда студии", "description": "Достигните 10 уровня.", "trigger": "level", "threshold": 10, "icon": "🏅"},
    {"code": "balance_5000", "name": "Капиталист", "description": "Накопите 5000 ₽ на счету.", "trigger": "balance", "threshold": 5000, "icon": "💰"},
    {"code": "passive_2000", "name": "Доход во сне", "description": "Получите 2000 ₽ пассивного дохода.", "trigger": "passive_income", "threshold": 2000, "icon": "💤"},
    {"code": "team_3", "name": "Своя студия", "description": "Нанимайте или прокачайте 3 членов команды.", "trigger": "team", "threshold": 3, "icon": "👥"},
    {"code": "wardrobe_5", "name": "Коллекционер", "description": "Соберите 5 предметов экипировки.", "trigger": "items", "threshold": 5, "icon": "🎽"},
]

SEED_RANDOM_EVENTS = [
    {"code": "idea_spark", "title": "💡 Озарение! Клиент в восторге — +200₽.", "kind": "bonus", "amount": 200, "duration_sec": None, "weight": 5, "min_level": 1},
    {"code": "coffee_spill", "title": "☕ Кот пролил кофе на ноут — −150₽. Ну бывает…", "kind": "penalty", "amount": 150, "duration_sec": None, "weight": 4, "min_level": 1},
    {"code": "viral_post", "title": "📈 Вирусный пост! +10% к наградам на 10 мин.", "kind": "buff", "amount": 0.10, "duration_sec": 600, "weight": 3, "min_level": 3},
    {"code": "client_tip", "title": "🧾 Клиент оставил чаевые — +350₽.", "kind": "bonus", "amount": 350, "duration_sec": None, "weight": 2, "min_level": 2},
    {"code": "deadline_crunch", "title": "🔥 Горящий дедлайн! −10% к наградам на 5 мин.", "kind": "buff", "amount": -0.10, "duration_sec": 300, "weight": 2, "min_level": 4},
    {"code": "agency_feature", "title": "🎤 Про вас написали в блоге — +5% к пассивному доходу на 15 мин.", "kind": "buff", "amount": 0.05, "duration_sec": 900, "weight": 2, "min_level": 5},
    {"code": "software_crash", "title": "💥 Софт упал! −100 XP.", "kind": "penalty", "amount": 100, "duration_sec": None, "weight": 1, "min_level": 3},
    {"code": "mentor_call", "title": "📞 Ментор подсказал лайфхак — +150 XP.", "kind": "bonus", "amount": 150, "duration_sec": None, "weight": 2, "min_level": 2},
    {"code": "perfect_flow", "title": "🚀 Потоковое состояние! +15% к силе клика на 10 мин.", "kind": "buff", "amount": 0.15, "duration_sec": 600, "weight": 2, "min_level": 4},
]

RANDOM_EVENT_EFFECTS = {
    "idea_spark": {"balance": 200},
    "coffee_spill": {"balance": -150},
    "viral_post": {"buff": {"reward_pct": 0.10}},
    "client_tip": {"balance": 350},
    "deadline_crunch": {"buff": {"reward_pct": -0.10}},
    "agency_feature": {"buff": {"passive_pct": 0.05}},
    "software_crash": {"xp": -100},
    "mentor_call": {"xp": 150},
    "perfect_flow": {"buff": {"cp_pct": 0.15}},
}

SEED_SKILLS = [
    {"code": "web_master", "name": "Web-мастер", "branch": "web", "effect": {"reward_pct": 0.05}, "min_level": 5},
    {"code": "brand_evangelist", "name": "Бренд-евангелист", "branch": "brand", "effect": {"reward_pct": 0.03, "passive_pct": 0.02}, "min_level": 10},
    {"code": "art_director", "name": "Арт-директор", "branch": "art", "effect": {"passive_pct": 0.05}, "min_level": 5},
    {"code": "perfectionist", "name": "Перфекционист", "branch": "web", "effect": {"cp_add": 1}, "min_level": 5},
    {"code": "speed_runner", "name": "Спидранер", "branch": "web", "effect": {"req_clicks_pct": 0.03}, "min_level": 10},
    {"code": "team_leader", "name": "Лидер команды", "branch": "brand", "effect": {"passive_pct": 0.04}, "min_level": 15},
    {"code": "sales_guru", "name": "Sales-гуру", "branch": "brand", "effect": {"reward_pct": 0.06}, "min_level": 15},
    {"code": "ui_alchemist", "name": "UI-алхимик", "branch": "art", "effect": {"cp_pct": 0.05}, "min_level": 10},
    {"code": "automation_ninja", "name": "Автоматизатор", "branch": "web", "effect": {"passive_pct": 0.03, "cp_add": 1}, "min_level": 15},
    {"code": "brand_storyteller", "name": "Сторителлер", "branch": "brand", "effect": {"reward_pct": 0.04, "xp_pct": 0.05}, "min_level": 20},
]

CAMPAIGN_CHAPTERS = [
    {"chapter": 1, "title": "Первые заказы", "min_level": 1, "goal": {"orders_total": 3}, "reward": {"rub": 400, "xp": 150, "reward_pct": 0.01}},
    {"chapter": 2, "title": "Первые крупные клиенты", "min_level": 5, "goal": {"orders_min_level": {"count": 2, "min_level": 3}}, "reward": {"rub": 600, "xp": 250, "reward_pct": 0.01}},
    {"chapter": 3, "title": "Маленькая команда", "min_level": 10, "goal": {"team_level": {"members": 2, "level": 1}}, "reward": {"rub": 800, "xp": 350, "passive_pct": 0.02}},
    {"chapter": 4, "title": "Свой бренд", "min_level": 15, "goal": {"items_bought": 2}, "reward": {"rub": 1000, "xp": 500, "reward_pct": 0.015}},
]

QUEST_CODE_HELL_CLIENT = "hell_client"
HELL_CLIENT_FLOW = {
    "intro": {
        "text": "Клиент: «Давайте всё в фиолетовый и единорога!» Что делаем?",
        "options": [
            {"text": "Спокойно внести правки", "next": "step1", "delta": {"mood": 1}},
            {"text": "Попросить доплату", "next": "step1", "delta": {"budget": 1}},
            {"text": "Предложить альтернативу", "next": "step1", "delta": {"respect": 1}},
        ],
    },
    "step1": {
        "text": "Клиент забывает отправить материалы. Ваш ход?",
        "options": [
            {"text": "Напомнить вежливо", "next": "step2", "delta": {"mood": 1}},
            {"text": "Сделать мокап из стоков", "next": "step2", "delta": {"respect": -1, "speed": 1}},
            {"text": "Попросить предоплату", "next": "step2", "delta": {"budget": 1}},
        ],
    },
    "step2": {
        "text": "Сроки горят, а правок всё больше. Как реагируете?",
        "options": [
            {"text": "План на фидбек-раунды", "next": "finale", "delta": {"respect": 1}},
            {"text": "Доп. спринт за деньги", "next": "finale", "delta": {"budget": 1}},
            {"text": "Героически всё сделать", "next": "finale", "delta": {"speed": 1}},
        ],
    },
}

HELL_CLIENT_REWARDS = {
    "default": {"rub": 600, "xp": 300, "talism_chance": 0.35},
    "budget": {"rub": 800, "xp": 250, "talism_chance": 0.20},
    "mood": {"rub": 500, "xp": 320, "talism_chance": 0.30},
    "respect": {"rub": 550, "xp": 360, "talism_chance": 0.45},
    "speed": {"rub": 650, "xp": 280, "talism_chance": 0.25},
}


async def seed_if_needed(session: AsyncSession) -> None:
    """Идемпотентная загрузка сидов при первом старте."""
    # Заказы
    cnt = (await session.execute(select(func.count()).select_from(Order))).scalar_one()
    if cnt == 0:
        for d in SEED_ORDERS:
            session.add(Order(title=d["title"], base_clicks=d["base_clicks"], min_level=d["min_level"]))
    # Бусты
    cnt = (await session.execute(select(func.count()).select_from(Boost))).scalar_one()
    if cnt == 0:
        for d in SEED_BOOSTS:
            session.add(Boost(code=d["code"], name=d["name"], type=d["type"],
                              base_cost=d["base_cost"], growth=d["growth"], step_value=d["step_value"]))
    # Команда
    cnt = (await session.execute(select(func.count()).select_from(TeamMember))).scalar_one()
    if cnt == 0:
        for d in SEED_TEAM:
            session.add(TeamMember(code=d["code"], name=d["name"],
                                   base_income_per_min=d["base_income_per_min"], base_cost=d["base_cost"]))
    # Предметы
    cnt = (await session.execute(select(func.count()).select_from(Item))).scalar_one()
    if cnt == 0:
        for d in SEED_ITEMS:
            session.add(Item(code=d["code"], name=d["name"], slot=d["slot"], tier=d["tier"],
                             bonus_type=d["bonus_type"], bonus_value=d["bonus_value"],
                             price=d["price"], min_level=d["min_level"]))
    # Достижения
    cnt = (await session.execute(select(func.count()).select_from(Achievement))).scalar_one()
    if cnt == 0:
        for d in SEED_ACHIEVEMENTS:
            session.add(
                Achievement(
                    code=d["code"],
                    name=d["name"],
                    description=d["description"],
                    trigger=d["trigger"],
                    threshold=d["threshold"],
                    icon=d["icon"],
                )
            )
    # Случайные события
    cnt = (await session.execute(select(func.count()).select_from(RandomEvent))).scalar_one()
    if cnt == 0:
        for d in SEED_RANDOM_EVENTS:
            session.add(
                RandomEvent(
                    code=d["code"],
                    title=d["title"],
                    kind=d["kind"],
                    amount=d["amount"],
                    duration_sec=d["duration_sec"],
                    weight=d["weight"],
                    min_level=d["min_level"],
                )
            )
    # Навыки
    cnt = (await session.execute(select(func.count()).select_from(Skill))).scalar_one()
    if cnt == 0:
        for d in SEED_SKILLS:
            session.add(
                Skill(
                    code=d["code"],
                    name=d["name"],
                    branch=d["branch"],
                    effect=d["effect"],
                    min_level=d["min_level"],
                )
            )
    # Санируем старые записи user_orders без снимка множителя
    await session.execute(
        update(UserOrder)
        .where(UserOrder.reward_snapshot_mul <= 0)
        .values(reward_snapshot_mul=1.0)
    )


# ----------------------------------------------------------------------------
# Экономика: формулы и сервисы
# ----------------------------------------------------------------------------

def xp_to_level(n: int) -> int:
    return 100 * n * n


def upgrade_cost(base: int, growth: float, n: int) -> int:
    return round(base * (growth ** (n - 1)))


def required_clicks(base_clicks: int, level: int) -> int:
    return int(round(base_clicks * (1 + 0.15 * floor(level / 5))))


def base_reward_from_required(req: int, reward_mul: float = 1.0) -> int:
    return int(round(req * 0.6 * reward_mul))


async def get_user_stats(session: AsyncSession, user: User) -> dict:
    """Return aggregated user stats from boosts, экипировки, навыков и баффов."""

    rows = (
        await session.execute(
            select(Boost.code, Boost.type, UserBoost.level, Boost.step_value)
            .select_from(UserBoost)
            .join(Boost, Boost.id == UserBoost.boost_id)
            .where(UserBoost.user_id == user.id)
        )
    ).all()
    cp_add = 0
    reward_add = 0.0
    passive_add = 0.0
    xp_pct = 0.0
    req_clicks_pct_boost = 0.0
    ratelimit_plus = 0
    crit_chance = 0.0
    crit_multiplier = 1.0
    team_income_pct = 0.0
    free_order_chance = 0.0
    team_discount_pct = 0.0
    offline_cap_bonus = 0.0
    rush_reward_pct = 0.0
    equipment_eff_pct = 0.0
    night_passive_pct = 0.0
    shop_discount_pct = 0.0
    high_order_reward_pct = 0.0
    negative_event_reduction = 0.0
    combo_step = 0.0
    combo_cap = 0.0
    for code, btype, lvl, step in rows:
        if lvl <= 0 or step == 0:
            continue
        value = lvl * step
        if btype == "cp":
            cp_add += int(value)
        elif btype == "reward":
            reward_add += value
        elif btype == "passive":
            passive_add += value
        elif btype == "xp":
            xp_pct += value
        elif btype == "crit":
            crit_chance += value
            extra = BOOST_EXTRA_META.get(code, {})
            crit_multiplier = max(crit_multiplier, extra.get("crit_multiplier", crit_multiplier))
        elif btype == "event_protection":
            negative_event_reduction += value
        elif btype == "combo":
            combo_step += value
            extra = BOOST_EXTRA_META.get(code, {})
            combo_cap = max(combo_cap, extra.get("combo_cap", combo_cap))
        elif btype == "team_income":
            team_income_pct += value
        elif btype == "ratelimit":
            ratelimit_plus += int(round(value))
        elif btype == "req_clicks":
            req_clicks_pct_boost += value
        elif btype == "free_order":
            free_order_chance += value
        elif btype == "team_discount":
            team_discount_pct += value
        elif btype == "offline_cap":
            offline_cap_bonus += value
        elif btype == "rush_reward":
            rush_reward_pct += value
        elif btype == "equipment_eff":
            equipment_eff_pct += value
        elif btype == "night_passive":
            night_passive_pct += value
        elif btype == "shop_discount":
            shop_discount_pct += value
        elif btype == "high_order_reward":
            high_order_reward_pct += value

    equipment_multiplier = 1.0 + equipment_eff_pct
    items = (
        await session.execute(
            select(Item.bonus_type, Item.bonus_value)
            .join(UserEquipment, UserEquipment.item_id == Item.id)
            .where(UserEquipment.user_id == user.id, UserEquipment.item_id.is_not(None))
        )
    ).all()
    cp_pct = 0.0
    passive_pct = 0.0
    req_clicks_pct = 0.0
    reward_pct = 0.0
    for btype, val in items:
        boosted_val = val * equipment_multiplier
        if btype == "cp_pct":
            cp_pct += boosted_val
        elif btype == "passive_pct":
            passive_pct += boosted_val
        elif btype == "req_clicks_pct":
            req_clicks_pct += boosted_val
        elif btype == "reward_pct":
            reward_pct += boosted_val
        elif btype == "ratelimit_plus":
            ratelimit_plus += int(round(boosted_val))

    now = utcnow()
    active_buffs = (
        await session.execute(
            select(UserBuff).where(UserBuff.user_id == user.id)
        )
    ).scalars().all()
    expired_ids: List[int] = []
    for buff in active_buffs:
        expires = ensure_naive(buff.expires_at)
        if expires and expires <= now:
            expired_ids.append(buff.id)
            continue
        payload = buff.payload or {}
        cp_add += int(payload.get("cp_add", 0))
        cp_pct += payload.get("cp_pct", 0.0)
        reward_pct += payload.get("reward_pct", 0.0)
        passive_pct += payload.get("passive_pct", 0.0)
        req_clicks_pct += payload.get("req_clicks_pct", 0.0)
        xp_pct += payload.get("xp_pct", 0.0)
    if expired_ids:
        await session.execute(delete(UserBuff).where(UserBuff.id.in_(expired_ids)))

    skills = (
        await session.execute(
            select(Skill.effect)
            .join(UserSkill, UserSkill.skill_code == Skill.code)
            .where(UserSkill.user_id == user.id)
        )
    ).scalars().all()
    for effect in skills:
        if not effect:
            continue
        cp_add += int(effect.get("cp_add", 0))
        cp_pct += effect.get("cp_pct", 0.0)
        reward_pct += effect.get("reward_pct", 0.0)
        passive_pct += effect.get("passive_pct", 0.0)
        req_clicks_pct += effect.get("req_clicks_pct", 0.0)
        xp_pct += effect.get("xp_pct", 0.0)

    prestige = await session.scalar(select(UserPrestige).where(UserPrestige.user_id == user.id))
    prestige_pct = 0.0
    if prestige:
        prestige_pct = max(0.0, prestige.reputation * 0.01)
        reward_pct += prestige_pct
        passive_pct += prestige_pct
        cp_pct += prestige_pct

    crit_chance = max(0.0, min(0.95, crit_chance))
    negative_event_weight_mul = max(
        0.0,
        min(1.0, 1.0 - min(NEGATIVE_EVENT_REDUCTION_CAP, max(0.0, negative_event_reduction))),
    )
    req_clicks_pct_total = req_clicks_pct + min(REQ_CLICKS_REDUCTION_CAP, max(0.0, req_clicks_pct_boost))
    req_clicks_pct_total = max(0.0, min(0.95, req_clicks_pct_total))
    team_discount_pct = max(0.0, min(TEAM_DISCOUNT_CAP, team_discount_pct))
    shop_discount_pct = max(0.0, min(SHOP_DISCOUNT_CAP, shop_discount_pct))
    free_order_chance = max(0.0, min(0.95, free_order_chance))
    combo_cap = max(combo_cap, BOOST_EXTRA_META.get("combo_click", {}).get("combo_cap", 0.0)) if combo_step > 0 else combo_cap

    cp = int(round((user.cp_base + cp_add) * (1 + cp_pct)))
    reward_mul_total = 1.0 + user.reward_mul + reward_add + reward_pct
    passive_mul_total = 1.0 + user.passive_mul + passive_add + passive_pct
    return {
        "cp": max(1, cp),
        "reward_mul_total": max(0.0, reward_mul_total),
        "passive_mul_total": max(0.0, passive_mul_total),
        "req_clicks_pct": req_clicks_pct_total,
        "ratelimit_plus": ratelimit_plus,
        "xp_pct": max(0.0, xp_pct),
        "prestige_pct": prestige_pct,
        "crit_chance": crit_chance,
        "crit_multiplier": max(1.0, crit_multiplier),
        "team_income_pct": max(0.0, team_income_pct),
        "free_order_chance": free_order_chance,
        "team_upgrade_discount_pct": team_discount_pct,
        "offline_cap_bonus": max(0.0, offline_cap_bonus),
        "rush_reward_pct": max(0.0, rush_reward_pct),
        "equipment_eff_pct": max(0.0, equipment_eff_pct),
        "night_passive_pct": max(0.0, night_passive_pct),
        "shop_discount_pct": shop_discount_pct,
        "high_order_reward_pct": max(0.0, high_order_reward_pct),
        "negative_event_weight_mul": negative_event_weight_mul,
        "combo_step": max(0.0, combo_step),
        "combo_cap": max(0.0, combo_cap),
    }


def team_income_per_min(base_per_min: float, level: int) -> float:
    """Calculate per-minute income from a team member for the given level."""

    if level <= 0:
        return 0.0
    return base_per_min * (1 + 0.25 * (level - 1))


def is_night_now(now: Optional[datetime] = None) -> bool:
    """Return True if current local time is considered night (22:00-08:00)."""

    now = now or datetime.now()
    hour = now.hour
    return hour >= 22 or hour < 8


async def calc_passive_income_rate(session: AsyncSession, user: User, stats: Dict[str, Any]) -> float:
    """Return passive income in currency per second accounting for multipliers."""

    rows = (
        await session.execute(
            select(TeamMember.base_income_per_min, UserTeam.level)
            .join(UserTeam, TeamMember.id == UserTeam.member_id)
            .where(UserTeam.user_id == user.id)
        )
    ).all()
    per_min = sum(team_income_per_min(b, lvl) for b, lvl in rows)
    team_bonus = 1.0 + stats.get("team_income_pct", 0.0)
    passive_mul_total = stats.get("passive_mul_total", 1.0)
    rate = (per_min / 60.0) * passive_mul_total * team_bonus
    night_bonus = stats.get("night_passive_pct", 0.0)
    if night_bonus > 0 and is_night_now():
        rate *= 1.0 + night_bonus
    return rate


async def apply_offline_income(session: AsyncSession, user: User) -> int:
    """Apply passive income accumulated since the last interaction."""

    now = utcnow()
    last_seen = ensure_naive(user.last_seen) or now
    delta_raw = max(0.0, (now - last_seen).total_seconds())
    stats = await get_user_stats(session, user)
    offline_cap = MAX_OFFLINE_SECONDS + stats.get("offline_cap_bonus", 0.0)
    delta = min(delta_raw, offline_cap)
    user.last_seen = now
    user.updated_at = now
    rate = await calc_passive_income_rate(session, user, stats)
    amount = int(rate * delta)
    if delta_raw > MAX_OFFLINE_SECONDS:
        logger.info(
            "Offline income capped",
            extra={
                "tg_id": user.tg_id,
                "seconds_raw": int(delta_raw),
                "seconds_used": int(delta),
                "cap": int(offline_cap),
            },
        )
    if amount > 0:
        user.balance += amount
        user.passive_income_collected += amount
        session.add(
            EconomyLog(
                user_id=user.id,
                type="passive",
                amount=amount,
                meta={"sec": int(delta), "sec_raw": int(delta_raw)},
                created_at=now,
            )
        )
        logger.debug("Offline income for user %s: +%s", user.tg_id, amount)
    return amount


async def process_offline_income(
    session: AsyncSession,
    user: User,
    achievements: List[Tuple[Achievement, UserAchievement]],
) -> int:
    """Apply offline income and append relevant achievements if any."""

    gained = await apply_offline_income(session, user)
    if gained:
        achievements.extend(await evaluate_achievements(session, user, {"passive_income", "balance"}))
    return gained


def snapshot_required_clicks(order: Order, user_level: int, req_clicks_pct: float) -> int:
    """Calculate the effective clicks required for an order based on user stats."""

    base_req = required_clicks(order.base_clicks, user_level)
    reduced = int(round(base_req * (1 - req_clicks_pct)))
    return max(1, reduced)


def finish_order_reward(required_clicks_snapshot: int, reward_snapshot_mul: float) -> int:
    """Return reward amount for completed order based on snapshot multipliers."""

    mul = max(1.0, reward_snapshot_mul)
    return base_reward_from_required(required_clicks_snapshot, mul)


async def ensure_no_active_order(session: AsyncSession, user: User) -> bool:
    """Check that user does not have unfinished order."""

    stmt = select(UserOrder).where(
        UserOrder.user_id == user.id,
        UserOrder.finished.is_(False),
        UserOrder.canceled.is_(False),
    )
    return (await session.scalar(stmt)) is None


async def get_active_order(session: AsyncSession, user: User) -> Optional[UserOrder]:
    """Return current active order for user if any."""

    stmt = select(UserOrder).where(
        UserOrder.user_id == user.id,
        UserOrder.finished.is_(False),
        UserOrder.canceled.is_(False),
    )
    return await session.scalar(stmt)


async def add_xp_and_levelup(user: User, xp_gain: int) -> int:
    """Apply XP gain to user and increment level when threshold reached.

    Returns the number of levels gained in this operation.
    """

    start_level = user.level
    user.xp += xp_gain
    lvl = user.level
    while user.xp >= xp_to_level(lvl):
        user.xp -= xp_to_level(lvl)
        lvl += 1
    user.level = lvl
    return lvl - start_level


def is_negative_event(event: RandomEvent) -> bool:
    """Heuristic to classify events with penalties."""

    effect = RANDOM_EVENT_EFFECTS.get(event.code, {})
    if not effect:
        return False
    if effect.get("balance", 0) < 0:
        return True
    if effect.get("xp", 0) < 0:
        return True
    buff = effect.get("buff")
    if isinstance(buff, dict) and any(val < 0 for val in buff.values()):
        return True
    return False


async def pick_random_event(
    session: AsyncSession, user: User, stats: Optional[Dict[str, Any]] = None
) -> Optional[RandomEvent]:
    """Weighted random selection of event matching user level."""

    events = (
        await session.execute(
            select(RandomEvent).where(RandomEvent.min_level <= user.level)
        )
    ).scalars().all()
    if not events:
        return None
    negative_mul = 1.0
    if stats:
        negative_mul = stats.get("negative_event_weight_mul", 1.0)
    weights = []
    total_weight = 0.0
    for event in events:
        weight = float(max(1, event.weight))
        if is_negative_event(event):
            weight *= negative_mul
        weights.append(weight)
        total_weight += weight
    if total_weight <= 0:
        return None
    pick = random.uniform(0, total_weight)
    upto = 0.0
    for event, weight in zip(events, weights):
        upto += weight
        if pick <= upto:
            return event
    return events[-1]


async def apply_random_event(session: AsyncSession, user: User, event: RandomEvent, trigger: str) -> str:
    """Apply selected random event to the user and return announcement text."""

    effect = RANDOM_EVENT_EFFECTS.get(event.code, {})
    now = utcnow()
    message = event.title
    meta: Dict[str, Any] = {"event": event.code, "trigger": trigger}
    if "balance" in effect:
        delta = int(effect["balance"])
        user.balance = max(0, user.balance + delta)
        log_type = "event_bonus" if delta >= 0 else "event_penalty"
        session.add(
            EconomyLog(
                user_id=user.id,
                type=log_type,
                amount=delta,
                meta=meta,
                created_at=now,
            )
        )
    if "xp" in effect:
        xp_delta = int(effect["xp"])
        if xp_delta >= 0:
            await add_xp_and_levelup(user, xp_delta)
        else:
            user.xp = max(0, user.xp + xp_delta)
        meta["xp"] = xp_delta
        log_type = "event_bonus" if xp_delta >= 0 else "event_penalty"
        session.add(
            EconomyLog(
                user_id=user.id,
                type=log_type,
                amount=0.0,
                meta=meta,
                created_at=now,
            )
        )
    if "buff" in effect:
        payload = effect["buff"] or {}
        duration = event.duration_sec or 600
        expires = now + timedelta(seconds=duration)
        session.add(
            UserBuff(
                user_id=user.id,
                code=event.code,
                title=event.title,
                expires_at=expires,
                payload=payload,
            )
        )
        session.add(
            EconomyLog(
                user_id=user.id,
                type="event_buff",
                amount=0.0,
                meta={**meta, "buff": payload, "duration": duration},
                created_at=now,
            )
        )
        message = "\n".join(
            [
                RU.EVENT_BUFF.format(title=event.title),
                RU.EVENT_BUFF_ACTIVE.format(title=event.title, expires=expires.strftime("%H:%M")),
            ]
        )
    elif "balance" in effect and effect["balance"] >= 0:
        message = RU.EVENT_POSITIVE.format(title=event.title)
    elif "balance" in effect and effect["balance"] < 0:
        message = RU.EVENT_NEGATIVE.format(title=event.title)
    elif "xp" in effect:
        message = RU.EVENT_POSITIVE.format(title=event.title) if effect["xp"] >= 0 else RU.EVENT_NEGATIVE.format(title=event.title)
    return message


async def trigger_random_event(
    session: AsyncSession,
    user: User,
    trigger: str,
    probability: float,
    stats: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Roll random event with probability and return announcement if triggered."""

    if random.random() > probability:
        return None
    if stats is None:
        stats = await get_user_stats(session, user)
    event = await pick_random_event(session, user, stats)
    if not event:
        return None
    logger.info(
        "Random event triggered",
        extra={"tg_id": user.tg_id, "user_id": user.id, "event": event.code, "trigger": trigger},
    )
    return await apply_random_event(session, user, event, trigger)


def describe_effect(effect: Dict[str, Any]) -> str:
    parts = []
    for key, value in effect.items():
        if key in {"reward_pct", "passive_pct", "cp_pct", "req_clicks_pct", "xp_pct"}:
            parts.append(f"{key.replace('_', ' ')} {int(value * 100)}%")
        elif key == "cp_add":
            parts.append(f"+{int(value)} CP")
        else:
            parts.append(f"{key}: {value}")
    return ", ".join(parts)


async def get_available_skills(session: AsyncSession, user: User) -> List[Skill]:
    taken = (
        await session.execute(
            select(UserSkill.skill_code).where(UserSkill.user_id == user.id)
        )
    ).scalars().all()
    taken_codes = set(taken)
    skills = (
        await session.execute(
            select(Skill)
            .where(Skill.min_level <= user.level)
            .order_by(Skill.min_level, Skill.id)
        )
    ).scalars().all()
    return [s for s in skills if s.code not in taken_codes]


async def maybe_prompt_skill_choice(
    session: AsyncSession,
    message: Optional[Message],
    state: Optional[FSMContext],
    user: User,
    prev_level: int,
    levels_gained: int,
) -> None:
    """Offer skill selection when hitting milestone levels."""

    if levels_gained <= 0:
        return
    for lvl in range(prev_level + 1, user.level + 1):
        if lvl >= SKILL_LEVEL_INTERVAL and lvl % SKILL_LEVEL_INTERVAL == 0:
            available = await get_available_skills(session, user)
            if not available:
                return
            if not message or not state:
                return
            choices = random.sample(available, min(3, len(available)))
            lines = [RU.SKILL_PROMPT]
            for idx, skill in enumerate(choices, 1):
                lines.append(f"[{idx}] {skill.name} — {describe_effect(skill.effect)}")
            await state.set_state(SkillsState.picking)
            await state.update_data(skill_codes=[s.code for s in choices])
            await message.answer("\n".join(lines), reply_markup=kb_skill_choices(len(choices)))
            return


def get_campaign_definition(chapter: int) -> Optional[dict]:
    for entry in CAMPAIGN_CHAPTERS:
        if entry["chapter"] == chapter:
            return entry
    return None


async def get_campaign_progress_entry(session: AsyncSession, user: User) -> CampaignProgress:
    progress = await session.scalar(select(CampaignProgress).where(CampaignProgress.user_id == user.id))
    if not progress:
        progress = CampaignProgress(user_id=user.id, chapter=1, is_done=False, progress={})
        session.add(progress)
        await session.flush()
    return progress


def campaign_goal_progress(goal: dict, data: dict) -> float:
    if "orders_total" in goal:
        target = goal["orders_total"]
        return min(1.0, data.get("orders_total", 0) / max(1, target))
    if "orders_min_level" in goal:
        g = goal["orders_min_level"]
        done = data.get("orders_min_level", 0)
        return min(1.0, done / max(1, g.get("count", 1)))
    if "team_level" in goal:
        g = goal["team_level"]
        done = data.get("team_level", 0)
        return min(1.0, done / max(1, g.get("members", 1)))
    if "items_bought" in goal:
        target = goal["items_bought"]
        return min(1.0, data.get("items_bought", 0) / max(1, target))
    return 0.0


def campaign_goal_met(goal: dict, data: dict) -> bool:
    return campaign_goal_progress(goal, data) >= 1.0


async def update_campaign_progress(session: AsyncSession, user: User, event: str, payload: dict) -> None:
    progress = await get_campaign_progress_entry(session, user)
    definition = get_campaign_definition(progress.chapter)
    if not definition:
        return
    if user.level < definition.get("min_level", 1):
        return
    data = dict(progress.progress or {})
    if event == "order_finish":
        data["orders_total"] = data.get("orders_total", 0) + 1
        min_level = payload.get("order_min_level", 0)
        goal = definition.get("goal", {})
        if "orders_min_level" in goal and min_level >= goal["orders_min_level"].get("min_level", 0):
            data["orders_min_level"] = data.get("orders_min_level", 0) + 1
    elif event == "team_upgrade":
        goal = definition.get("goal", {})
        if "team_level" in goal:
            members_needed = goal["team_level"].get("members", 1)
            level_needed = goal["team_level"].get("level", 1)
            team_count = (
                await session.execute(
                    select(func.count())
                    .select_from(UserTeam)
                    .where(UserTeam.user_id == user.id, UserTeam.level >= level_needed)
                )
            ).scalar_one()
            data["team_level"] = int(team_count)
    elif event == "item_purchase":
        data["items_bought"] = data.get("items_bought", 0) + 1
    progress.progress = data
    if campaign_goal_met(definition.get("goal", {}), data):
        progress.is_done = True


def describe_campaign_goal(goal: dict) -> str:
    if "orders_total" in goal:
        return f"Завершить {goal['orders_total']} заказов"
    if "orders_min_level" in goal:
        g = goal["orders_min_level"]
        return f"Завершить {g.get('count', 1)} заказа(ов) ур. ≥ {g.get('min_level', 1)}"
    if "team_level" in goal:
        g = goal["team_level"]
        return f"Прокачать {g.get('members', 1)} членов команды до ур. ≥ {g.get('level', 1)}"
    if "items_bought" in goal:
        return f"Купить {goal['items_bought']} предмета экипировки"
    return "Прогресс неизвестен"


async def claim_campaign_reward(session: AsyncSession, user: User) -> Optional[Tuple[str, int, int]]:
    progress = await get_campaign_progress_entry(session, user)
    definition = get_campaign_definition(progress.chapter)
    if not definition or not progress.is_done:
        return None
    reward = definition.get("reward", {})
    stats = await get_user_stats(session, user)
    rub = reward.get("rub", 0)
    xp_base = reward.get("xp", 0)
    xp_gain = int(round(xp_base * (1 + stats.get("xp_pct", 0.0))))
    prev_level = user.level
    user.balance += rub
    levels_gained = await add_xp_and_levelup(user, xp_gain)
    if reward.get("reward_pct"):
        user.reward_mul += reward["reward_pct"]
    if reward.get("passive_pct"):
        user.passive_mul += reward["passive_pct"]
    if reward.get("cp_add"):
        user.cp_base += int(reward["cp_add"])
    now = utcnow()
    session.add(
        EconomyLog(
            user_id=user.id,
            type="campaign_reward",
            amount=rub,
            meta={"chapter": progress.chapter, "xp": xp_gain},
            created_at=now,
        )
    )
    progress.chapter += 1
    progress.is_done = False
    progress.progress = {}
    return RU.CAMPAIGN_REWARD.format(rub=rub, xp=xp_gain), prev_level, levels_gained


async def get_or_create_quest(session: AsyncSession, user: User, code: str) -> UserQuest:
    quest = await session.scalar(
        select(UserQuest).where(UserQuest.user_id == user.id, UserQuest.quest_code == code)
    )
    if not quest:
        quest = UserQuest(user_id=user.id, quest_code=code, stage=0, is_done=False, payload={})
        session.add(quest)
        await session.flush()
    return quest


def quest_get_stage_payload(quest: UserQuest) -> Dict[str, int]:
    payload = quest.payload or {}
    for key in ["mood", "budget", "respect", "speed"]:
        payload.setdefault(key, 0)
    quest.payload = payload
    return payload


def quest_choose_reward_key(payload: Dict[str, int]) -> str:
    best_key = "default"
    best_value = -999
    for key in ["mood", "budget", "respect", "speed"]:
        if payload.get(key, 0) > best_value:
            best_value = payload.get(key, 0)
            best_key = key
    return best_key if best_value > 0 else "default"


async def finalize_hell_client(
    session: AsyncSession,
    user: User,
    quest: UserQuest,
    message: Message,
    state: FSMContext,
) -> None:
    payload = quest_get_stage_payload(quest)
    reward_key = quest_choose_reward_key(payload)
    reward_data = HELL_CLIENT_REWARDS.get(reward_key, HELL_CLIENT_REWARDS["default"])
    stats = await get_user_stats(session, user)
    rub = reward_data.get("rub", 0)
    xp_base = reward_data.get("xp", 0)
    xp_gain = int(round(xp_base * (1 + stats.get("xp_pct", 0.0))))
    prev_level = user.level
    user.balance += rub
    levels_gained = await add_xp_and_levelup(user, xp_gain)
    now = utcnow()
    quest.is_done = True
    quest.stage = 999
    session.add(
        EconomyLog(
            user_id=user.id,
            type="quest_reward",
            amount=rub,
            meta={"quest": quest.quest_code, "reward_key": reward_key, "xp": xp_gain},
            created_at=now,
        )
    )
    await message.answer(
        RU.QUEST_FINISH.format(rub=rub, xp=xp_gain),
        reply_markup=await build_main_menu_markup(session, user=user),
    )
    await maybe_prompt_skill_choice(session, message, state, user, prev_level, levels_gained)
    chance = reward_data.get("talism_chance", 0.0)
    if random.random() <= chance:
        item = await session.scalar(select(Item).where(Item.code == "client_contract"))
        if item:
            has_item = await session.scalar(
                select(UserItem).where(UserItem.user_id == user.id, UserItem.item_id == item.id)
            )
            if not has_item:
                session.add(UserItem(user_id=user.id, item_id=item.id))
            equip = await session.scalar(
                select(UserEquipment).where(UserEquipment.user_id == user.id, UserEquipment.slot == item.slot)
            )
            if equip:
                equip.item_id = item.id
            else:
                session.add(UserEquipment(user_id=user.id, slot=item.slot, item_id=item.id))
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="quest_reward",
                    amount=0.0,
                    meta={"quest": quest.quest_code, "item": item.code},
                    created_at=now,
                )
            )
            await message.answer(RU.QUEST_ITEM_GAIN.format(pct=int(item.bonus_value * 100)))


async def send_hell_client_step(message: Message, stage_key: str) -> None:
    step = HELL_CLIENT_FLOW.get(stage_key)
    if not step:
        return
    options = [opt["text"] for opt in step.get("options", [])]
    await message.answer(
        RU.QUEST_STEP.format(text=step["text"]),
        reply_markup=kb_quest_options(options),
    )


async def process_hell_client_choice(
    message: Message,
    state: FSMContext,
    stage_key: str,
) -> None:
    choice = message.text or ""
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        quest = await get_or_create_quest(session, user, QUEST_CODE_HELL_CLIENT)
        if quest.is_done:
            await message.answer(
                RU.QUEST_ALREADY_DONE,
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            await state.clear()
            return
        step = HELL_CLIENT_FLOW.get(stage_key)
        if not step:
            await state.clear()
            return
        option = next((opt for opt in step.get("options", []) if opt["text"] == choice), None)
        if not option:
            await message.answer(RU.QUEST_STEP.format(text="Выберите вариант из списка."))
            return
        payload = quest_get_stage_payload(quest)
        for key, delta in option.get("delta", {}).items():
            payload[key] = payload.get(key, 0) + delta
        quest.payload = payload
        next_stage = option.get("next", "finale")
        if next_stage == "finale":
            await finalize_hell_client(session, user, quest, message, state)
            await state.clear()
        else:
            await state.set_state(getattr(HellClientState, next_stage))
            await send_hell_client_step(message, next_stage)


async def get_prestige_entry(session: AsyncSession, user: User) -> UserPrestige:
    prestige = await session.scalar(select(UserPrestige).where(UserPrestige.user_id == user.id))
    if not prestige:
        prestige = UserPrestige(user_id=user.id, reputation=0, resets=0)
        session.add(prestige)
        await session.flush()
    return prestige


async def perform_prestige_reset(session: AsyncSession, user: User, gain: int) -> None:
    prestige = await get_prestige_entry(session, user)
    now = utcnow()
    prestige.reputation += max(0, gain)
    prestige.resets += 1
    prestige.last_reset_at = now
    session.add(
        EconomyLog(
            user_id=user.id,
            type="prestige_reset",
            amount=0.0,
            meta={"gain": gain},
            created_at=now,
        )
    )
    user.balance = 200
    user.cp_base = 1
    user.reward_mul = 0.0
    user.passive_mul = 0.0
    user.level = 1
    user.xp = 0
    user.orders_completed = 0
    user.clicks_total = 0
    user.passive_income_collected = 0
    user.updated_at = now
    await session.execute(delete(UserBoost).where(UserBoost.user_id == user.id))
    await session.execute(delete(UserTeam).where(UserTeam.user_id == user.id))
    await session.execute(delete(UserItem).where(UserItem.user_id == user.id))
    await session.execute(delete(UserBuff).where(UserBuff.user_id == user.id))
    await session.execute(delete(UserSkill).where(UserSkill.user_id == user.id))
    await session.execute(delete(UserOrder).where(UserOrder.user_id == user.id))
    await session.execute(delete(UserAchievement).where(UserAchievement.user_id == user.id, UserAchievement.unlocked_at.is_(None)))
    await session.execute(delete(UserEquipment).where(UserEquipment.user_id == user.id))
    for slot in ["laptop", "phone", "tablet", "monitor", "chair", "charm"]:
        session.add(UserEquipment(user_id=user.id, slot=slot, item_id=None))
    await session.execute(delete(UserQuest).where(UserQuest.user_id == user.id, UserQuest.quest_code == QUEST_CODE_HELL_CLIENT))
    progress = await session.scalar(select(CampaignProgress).where(CampaignProgress.user_id == user.id))
    if progress:
        progress.chapter = 1
        progress.is_done = False
        progress.progress = {}



def project_next_item_params(item: Item) -> Tuple[float, int]:
    """Return projected bonus value and price for the next tier of an item."""

    if item.bonus_type == "ratelimit_plus":
        bonus = item.bonus_value + 1
    elif item.bonus_type in {"cp_pct", "passive_pct", "reward_pct"}:
        bonus = round(item.bonus_value * 1.25, 3)
    elif item.bonus_type == "req_clicks_pct":
        bonus = round(min(0.95, item.bonus_value + 0.02), 3)
    else:
        bonus = item.bonus_value
    price = int(round(item.price * 1.65))
    return bonus, price


async def get_next_items_for_user(session: AsyncSession, user: User) -> List[Item]:
    """Return only the next tier items per slot available for purchase."""

    items = (
        await session.execute(
            select(Item).where(Item.min_level <= user.level).order_by(Item.slot, Item.tier)
        )
    ).scalars().all()
    owned_ids = {
        row[0]
        for row in (
            await session.execute(select(UserItem.item_id).where(UserItem.user_id == user.id))
        ).all()
    }
    result: List[Item] = []
    grouped: Dict[str, List[Item]] = defaultdict(list)
    for item in items:
        grouped[item.slot].append(item)
    for slot_items in grouped.values():
        slot_items.sort(key=lambda x: x.tier)
        for item in slot_items:
            if item.id not in owned_ids:
                result.append(item)
                break
    result.sort(key=lambda it: (it.slot, it.tier))
    return result


async def get_achievement_progress_value(
    session: AsyncSession, user: User, trigger: str
) -> int:
    """Resolve current progress for the given achievement trigger."""

    if trigger == "clicks":
        return user.clicks_total
    if trigger == "orders":
        return user.orders_completed
    if trigger == "level":
        return user.level
    if trigger == "balance":
        return user.balance
    if trigger == "passive_income":
        return user.passive_income_collected
    if trigger == "team":
        value = await session.scalar(
            select(func.count()).select_from(UserTeam).where(UserTeam.user_id == user.id, UserTeam.level > 0)
        )
        return int(value or 0)
    if trigger == "items":
        value = await session.scalar(
            select(func.count()).select_from(UserItem).where(UserItem.user_id == user.id)
        )
        return int(value or 0)
    if trigger == "daily":
        return user.daily_bonus_claims
    return 0


async def evaluate_achievements(
    session: AsyncSession, user: User, triggers: Set[str]
) -> List[Tuple[Achievement, UserAchievement]]:
    """Check and unlock achievements for provided triggers, returning newly unlocked ones."""

    if not triggers:
        return []
    achievements = (
        await session.execute(
            select(Achievement)
            .where(Achievement.trigger.in_(list(triggers)))
            .order_by(Achievement.id)
        )
    ).scalars().all()
    if not achievements:
        return []
    existing = {
        ua.achievement_id: ua
        for ua in (
            await session.execute(
                select(UserAchievement).where(
                    UserAchievement.user_id == user.id,
                    UserAchievement.achievement_id.in_([ach.id for ach in achievements]),
                )
            )
        ).scalars()
    }
    unlocked: List[Tuple[Achievement, UserAchievement]] = []
    progress_cache: Dict[str, int] = {}

    async def _progress(trigger: str) -> int:
        if trigger not in progress_cache:
            progress_cache[trigger] = await get_achievement_progress_value(session, user, trigger)
        return progress_cache[trigger]

    for ach in achievements:
        ua = existing.get(ach.id)
        progress_value = await _progress(ach.trigger)
        if ua:
            ua.progress = progress_value
        if progress_value >= ach.threshold:
            if not ua:
                ua = UserAchievement(
                    user_id=user.id,
                    achievement_id=ach.id,
                    progress=progress_value,
                    unlocked_at=utcnow(),
                    notified=False,
                )
                session.add(ua)
            else:
                if ua.unlocked_at is None:
                    ua.unlocked_at = utcnow()
                ua.notified = ua.notified and ua.unlocked_at is not None
            if ua and not ua.notified:
                unlocked.append((ach, ua))
        else:
            if not ua:
                session.add(
                    UserAchievement(
                        user_id=user.id,
                        achievement_id=ach.id,
                        progress=progress_value,
                        unlocked_at=None,
                        notified=False,
                    )
                )
    return unlocked


async def notify_new_achievements(
    message: Message, unlocked: List[Tuple[Achievement, UserAchievement]]
) -> None:
    """Send notification about unlocked achievements and mark them as notified."""

    if not unlocked:
        return
    lines = [RU.ACHIEVEMENT_UNLOCK.format(title=f"{ach.icon} {ach.name}") for ach, _ in unlocked]
    await message.answer("\n".join(lines), reply_markup=kb_achievement_prompt())
    for _, ua in unlocked:
        ua.notified = True


def _income_components() -> Tuple[Any, Any]:
    """Utility to build CASE sums for passive and active income aggregation."""

    passive_sum = func.sum(
        case((EconomyLog.type == "passive", EconomyLog.amount), else_=0.0)
    )
    active_sum = func.sum(
        case((EconomyLog.type == "order_finish", EconomyLog.amount), else_=0.0)
    )
    return passive_sum, active_sum


def format_money(value: float) -> str:
    """Format ruble values with spaces as thousands separators."""

    return f"{int(round(value)):,}".replace(",", " ")


def format_price(value: float) -> str:
    """Format ruble amounts with the currency sign."""

    return f"{format_money(value)}{RU.CURRENCY}"


def apply_percentage_discount(value: float, pct: float, *, cap: Optional[float] = None) -> int:
    """Return integer price after applying percentage discount with optional cap."""

    if cap is not None:
        pct = min(pct, cap)
    pct = max(0.0, min(0.99, pct))
    discounted = int(round(value * (1 - pct)))
    if value <= 0:
        return int(round(value))
    return max(1, discounted)


def format_stat(value: float) -> str:
    """Format numeric stat values without trailing zeros."""

    if abs(value - round(value)) < 1e-6:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def render_progress_bar(
    current: float,
    total: float,
    *,
    length: int = 10,
    filled_char: str = "▰",
    empty_char: str = "▱",
) -> str:
    """Render a textual progress bar with the requested symbols."""

    if length <= 0:
        return ""
    if total <= 0:
        ratio = 1.0 if current > 0 else 0.0
    else:
        ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * length))
    if filled == 0 and ratio > 0.0:
        filled = 1
    filled = max(0, min(length, filled))
    return f"{filled_char * filled}{empty_char * (length - filled)}"


def percentage(current: float, total: float) -> int:
    """Return percentage progress for the given values."""

    if total <= 0:
        return 100 if current > 0 else 0
    return int(max(0.0, min(100.0, round((current / total) * 100))))


def circled_number(idx: int) -> str:
    """Return circled unicode digit for indices 1..20, fallback to regular digits."""

    if 1 <= idx <= 20:
        return chr(0x245F + idx)  # 0x2460 is 1, hence +idx then -1 via constant.
    return str(idx)


ORDER_ICON_KEYWORDS: Tuple[Tuple[str, str], ...] = (
    ("визит", "💳"),
    ("логотип", "🎨"),
    ("облож", "🖼️"),
    ("баннер", "🪧"),
    ("сайт", "💻"),
    ("пост", "📢"),
    ("фирмен", "🏢"),
    ("презента", "📊"),
)


def pick_order_icon(title: str) -> str:
    """Pick a representative emoji for an order title."""

    lower = title.lower()
    for keyword, icon in ORDER_ICON_KEYWORDS:
        if keyword in lower:
            return icon
    return "📝"


async def fetch_average_income_rows(session: AsyncSession) -> List[Tuple[int, str, float]]:
    """Return per-user average income composed of passive and active totals."""

    passive_sum, active_sum = _income_components()
    income_agg = (
        select(
            EconomyLog.user_id.label("user_id"),
            passive_sum.label("passive_total"),
            active_sum.label("active_total"),
        )
        .group_by(EconomyLog.user_id)
        .subquery()
    )

    rows = (
        await session.execute(
            select(
                User.id,
                User.first_name,
                func.coalesce(income_agg.c.passive_total, 0.0),
                func.coalesce(income_agg.c.active_total, 0.0),
            )
            .outerjoin(income_agg, income_agg.c.user_id == User.id)
            .order_by(User.id)
        )
    ).all()

    result: List[Tuple[int, str, float]] = []
    for uid, name, passive_total, active_total in rows:
        total = float(passive_total or 0.0) + float(active_total or 0.0)
        display_name = name or f"Игрок {uid}"
        result.append((uid, display_name, total))
    return result


async def fetch_user_average_income(session: AsyncSession, user_id: int) -> float:
    """Calculate a single user's combined passive and active income."""

    passive_sum, active_sum = _income_components()
    row = await session.execute(
        select(
            func.coalesce(passive_sum, 0.0),
            func.coalesce(active_sum, 0.0),
        ).where(EconomyLog.user_id == user_id)
    )
    passive_total, active_total = row.one()
    return float(passive_total or 0.0) + float(active_total or 0.0)


# ----------------------------------------------------------------------------
# Анти-флуд (middleware)
# ----------------------------------------------------------------------------

class RateLimiter:
    """Sliding-window rate limiter per Telegram user."""

    def __init__(self) -> None:
        self._events: Dict[int, Deque[float]] = defaultdict(lambda: deque(maxlen=100))

    def allow(self, user_id: int, limit_per_sec: int, now: Optional[float] = None) -> bool:
        """Return True if event allowed under given rate, False otherwise."""

        t = time.monotonic() if now is None else now
        dq = self._events[user_id]
        while dq and t - dq[0] > 1.0:
            dq.popleft()
        if len(dq) >= limit_per_sec:
            return False
        dq.append(t)
        return True


class RateLimitMiddleware(BaseMiddleware):
    """Middleware ограничения кликов/сек. Поднимает предупреждение и блокирует обработчик при превышении."""
    def __init__(self, limit_getter):
        super().__init__()
        self.limiter = RateLimiter()
        self.limit_getter = limit_getter

    async def __call__(self, handler, event: Message, data):
        try:
            if isinstance(event, Message) and (event.text or "") == RU.BTN_CLICK:
                tg_id = event.from_user.id
                limit = await self.limit_getter(tg_id)
                if not self.limiter.allow(tg_id, limit):
                    logger.debug("Rate limit hit", extra={"tg_id": tg_id, "limit": limit})
                    await event.answer(RU.TOO_FAST)
                    return
        except Exception as e:
            logger.exception("RateLimitMiddleware error: %s", e)
        return await handler(event, data)


async def get_user_click_limit(tg_id: int) -> int:
    """Базовый лимит 10/сек + бонус от экипировки стула (до 15)."""

    async with session_scope() as session:
        user = await session.scalar(select(User).where(User.tg_id == tg_id))
        if not user:
            return BASE_CLICK_LIMIT
        stats = await get_user_stats(session, user)
        limit = BASE_CLICK_LIMIT + int(stats.get("ratelimit_plus", 0))
    return max(1, min(MAX_CLICK_LIMIT, limit))


# ----------------------------------------------------------------------------
# FSM состояния
# ----------------------------------------------------------------------------


class TutorialState(StatesGroup):
    step = State()


class OrdersState(StatesGroup):
    browsing = State()
    confirm = State()


class ShopState(StatesGroup):
    root = State()
    boosts = State()
    equipment = State()
    confirm_boost = State()
    confirm_item = State()


class TeamState(StatesGroup):
    browsing = State()
    confirm = State()


class WardrobeState(StatesGroup):
    browsing = State()
    equip_confirm = State()


class ProfileState(StatesGroup):
    confirm_cancel = State()


class HellClientState(StatesGroup):
    intro = State()
    step1 = State()
    step2 = State()
    finale = State()


class SkillsState(StatesGroup):
    picking = State()


class StudioState(StatesGroup):
    confirm = State()


# ----------------------------------------------------------------------------
# Роутер и обработчики
# ----------------------------------------------------------------------------

router = Router()


async def get_or_create_user(tg_id: int, first_name: str) -> Tuple[User, bool]:
    """Fetch existing user or create a new record."""

    async with session_scope() as session:
        user = await session.scalar(select(User).where(User.tg_id == tg_id))
        created = False
        if not user:
            created = True
            now = utcnow()
            user = User(
                tg_id=tg_id,
                first_name=first_name or "",
                balance=200,
                cp_base=1,
                reward_mul=0.0,
                passive_mul=0.0,
                level=1,
                xp=0,
                last_seen=now,
                created_at=now,
                updated_at=now,
            )
            session.add(user)
            try:
                await session.flush()
            except IntegrityError:
                await session.rollback()
                logger.warning(
                    "Race while creating user", extra={"tg_id": tg_id}
                )
                return await get_or_create_user(tg_id, first_name)
            for slot in ["laptop", "phone", "tablet", "monitor", "chair", "charm"]:
                session.add(UserEquipment(user_id=user.id, slot=slot, item_id=None))
            session.add(UserPrestige(user_id=user.id))
            session.add(CampaignProgress(user_id=user.id, chapter=1, is_done=False, progress={}))
            logger.info("New user created", extra={"tg_id": tg_id, "user_id": user.id})
        else:
            await apply_offline_income(session, user)
            logger.debug("Existing user resumed session", extra={"tg_id": tg_id})
        return user, created


async def get_user_by_tg(session: AsyncSession, tg_id: int) -> Optional[User]:
    """Load user entity by Telegram identifier."""

    return await session.scalar(select(User).where(User.tg_id == tg_id))


async def ensure_user_loaded(session: AsyncSession, message: Message) -> Optional[User]:
    """Return user for message or notify user to start the bot."""

    user = await get_user_by_tg(session, message.from_user.id)
    if not user:
        await message.answer(
            "Нажмите /start",
            reply_markup=await main_menu_for_message(message, session=session),
        )
        return None
    return user


@router.message(CommandStart())
@safe_handler
async def cmd_start(message: Message, state: FSMContext):
    user, created = await get_or_create_user(message.from_user.id, message.from_user.first_name or "")
    logger.info(
        "User issued /start",
        extra={"tg_id": message.from_user.id, "user_id": user.id, "is_created": created},
    )
    await message.answer(
        RU.WELCOME,
        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
    )
    if created or (user.tutorial_completed_at is None and user.tutorial_stage < len(TUTORIAL_STEPS)):
        await state.set_state(TutorialState.step)
        await send_tutorial_step_message(message, user.tutorial_stage)


@router.message(TutorialState.step, F.text == RU.BTN_TUTORIAL_NEXT)
@safe_handler
async def tutorial_next(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        next_step = min(user.tutorial_stage + 1, len(TUTORIAL_STEPS))
        user.tutorial_stage = next_step
        if next_step >= len(TUTORIAL_STEPS):
            user.tutorial_completed_at = utcnow()
            user.updated_at = utcnow()
            await state.clear()
            await message.answer(
                RU.TUTORIAL_DONE,
                reply_markup=await main_menu_for_message(message, session=session, user=user),
            )
        else:
            await send_tutorial_step_message(message, next_step)


@router.message(TutorialState.step, F.text == RU.BTN_TUTORIAL_SKIP)
@safe_handler
async def tutorial_skip(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        user.tutorial_stage = len(TUTORIAL_STEPS)
        user.tutorial_completed_at = utcnow()
        user.updated_at = utcnow()
    await state.clear()
    await message.answer(
        RU.TUTORIAL_DONE,
        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
    )


@router.message(F.text.in_({RU.BTN_MENU, RU.BTN_HOME}))
@safe_handler
async def back_to_menu(message: Message):
    async with session_scope() as session:
        user = await get_user_by_tg(session, message.from_user.id)
        if user:
            achievements: List[Tuple[Achievement, UserAchievement]] = []
            await process_offline_income(session, user, achievements)
            await notify_new_achievements(message, achievements)
            active = await get_active_order(session, user)
        else:
            active = None
        markup = await main_menu_for_message(message, session=session, user=user)
    hint = RU.MENU_WITH_ORDER_HINT if active else RU.MENU_HINT
    await message.answer(hint, reply_markup=markup)


# --- Клик ---

@router.message(F.text == RU.BTN_CLICK)
@safe_handler
async def handle_click(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        active = await get_active_order(session, user)
        if not active:
            await message.answer(
                RU.NO_ACTIVE_ORDER,
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            return
        stats = await get_user_stats(session, user)
        base_cp = float(stats["cp"])
        combo_step = stats.get("combo_step", 0.0)
        combo_cap = stats.get("combo_cap", 0.0)
        combo_bonus = 0.0
        if combo_step > 0 and combo_cap > 0:
            tracker = _combo_states.get(user.id)
            now_ts = time.monotonic()
            if tracker and now_ts - tracker.last_ts <= COMBO_RESET_SECONDS:
                tracker.bonus = min(combo_cap, tracker.bonus + combo_step)
                tracker.last_ts = now_ts
            else:
                tracker = ComboTracker(bonus=0.0, last_ts=now_ts)
            _combo_states[user.id] = tracker
            combo_bonus = tracker.bonus
        else:
            _combo_states.pop(user.id, None)
        cp_effective = base_cp + combo_bonus
        crit_triggered = False
        crit_chance = stats.get("crit_chance", 0.0)
        crit_multiplier = stats.get("crit_multiplier", 1.0)
        if crit_chance > 0 and random.random() < crit_chance:
            cp_effective *= crit_multiplier
            crit_triggered = True
        cp = max(1, int(round(cp_effective)))
        user.clicks_total += cp
        achievements.extend(await evaluate_achievements(session, user, {"clicks"}))
        event_message: Optional[str] = None
        if user.clicks_total % RANDOM_EVENT_CLICK_INTERVAL == 0:
            event_message = await trigger_random_event(
                session, user, "click", RANDOM_EVENT_CLICK_PROB, stats
            )
        prev = active.progress_clicks
        active.progress_clicks = min(active.required_clicks, active.progress_clicks + cp)
        if (active.progress_clicks // 10) > (prev // 10) or active.progress_clicks == active.required_clicks:
            pct = int(100 * active.progress_clicks / active.required_clicks)
            await message.answer(
                RU.CLICK_PROGRESS.format(cur=active.progress_clicks, req=active.required_clicks, pct=pct),
                reply_markup=kb_active_order_controls(),
            )
        if crit_triggered:
            await message.answer(
                f"💥 Критический клик! ×{format_stat(crit_multiplier)}", reply_markup=kb_active_order_controls()
            )
        if active.progress_clicks >= active.required_clicks:
            order_entity = await session.scalar(select(Order).where(Order.id == active.order_id))
            reward_base = finish_order_reward(active.required_clicks, active.reward_snapshot_mul)
            reward = reward_base
            high_bonus_pct = 0.0
            if order_entity and order_entity.min_level >= HIGH_ORDER_MIN_LEVEL:
                high_bonus_pct = stats.get("high_order_reward_pct", 0.0)
                if high_bonus_pct > 0:
                    reward = int(round(reward * (1 + high_bonus_pct)))
            xp_gain_base = int(round(active.required_clicks * 0.1))
            xp_gain = int(round(xp_gain_base * (1 + stats.get("xp_pct", 0.0))))
            now = utcnow()
            rush_bonus_pct = stats.get("rush_reward_pct", 0.0)
            rush_applied = False
            if rush_bonus_pct > 0:
                started_at = ensure_naive(active.started_at) or now
                elapsed = max(0.0, (now - started_at).total_seconds())
                if elapsed <= FAST_ORDER_SECONDS:
                    reward = int(round(reward * (1 + rush_bonus_pct)))
                    rush_applied = True
            user.balance += reward
            user.orders_completed += 1
            prev_level = user.level
            levels_gained = await add_xp_and_levelup(user, xp_gain)
            user.updated_at = now
            active.finished = True
            reward_meta: Dict[str, Any] = {"order_id": active.order_id}
            if high_bonus_pct > 0:
                reward_meta["high_order_bonus"] = round(high_bonus_pct, 4)
            if rush_applied:
                reward_meta["rush_bonus"] = round(rush_bonus_pct, 4)
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="order_finish",
                    amount=reward,
                    meta=reward_meta,
                    created_at=now,
                )
            )
            logger.info(
                "Order finished",
                extra={
                    "tg_id": user.tg_id,
                    "user_id": user.id,
                    "order_id": active.order_id,
                    "reward": reward,
                },
            )
            menu_markup = await main_menu_for_message(message, session=session, user=user)
            await message.answer(RU.ORDER_DONE.format(rub=reward, xp=xp_gain), reply_markup=menu_markup)
            await update_campaign_progress(
                session,
                user,
                "order_finish",
                {"order_min_level": order_entity.min_level if order_entity else 0},
            )
            await maybe_prompt_skill_choice(session, message, state, user, prev_level, levels_gained)
            event_order = await trigger_random_event(
                session, user, "order_finish", RANDOM_EVENT_ORDER_PROB, stats
            )
            if event_order:
                await message.answer(event_order, reply_markup=menu_markup)
            achievements.extend(await evaluate_achievements(session, user, {"orders", "level", "balance"}))
        if event_message and not event_message.strip() == "":
            await message.answer(event_message, reply_markup=kb_active_order_controls())
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_TO_MENU)
@safe_handler
async def leave_order_to_menu(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        active = await get_active_order(session, user)
        await notify_new_achievements(message, achievements)
        markup = await main_menu_for_message(message, session=session, user=user)
        hint = RU.MENU_WITH_ORDER_HINT if active else RU.MENU_HINT
    await message.answer(hint, reply_markup=markup)


@router.message(F.text == RU.BTN_RETURN_ORDER)
@safe_handler
async def resume_order_work(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        active = await get_active_order(session, user)
        await notify_new_achievements(message, achievements)
        if not active:
            await message.answer(
                RU.NO_ACTIVE_ORDER,
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            return
        order_entity = await session.scalar(select(Order).where(Order.id == active.order_id))
        title = order_entity.title if order_entity else "заказ"
        pct = int(100 * active.progress_clicks / active.required_clicks)
        progress_line = RU.CLICK_PROGRESS.format(
            cur=active.progress_clicks, req=active.required_clicks, pct=pct
        )
        await message.answer(
            f"{RU.ORDER_RESUME.format(title=title)}\n{progress_line}",
            reply_markup=kb_active_order_controls(),
        )


# --- Заказы ---

def fmt_orders(orders: List[Order]) -> str:
    lines = [RU.ORDERS_HEADER, "Введите номер для выбора:", ""]
    for i, o in enumerate(orders, 1):
        lines.append(
            f"{circled_number(i)} {pick_order_icon(o.title)} {o.title} — мин. ур. {o.min_level}"
        )
    return "\n".join(lines)


@router.message(F.text == RU.BTN_ORDERS)
@safe_handler
async def orders_root(message: Message, state: FSMContext):
    await state.set_state(OrdersState.browsing)
    await state.update_data(page=0)
    await _render_orders_page(message, state)


@router.message(F.text == RU.BTN_UPGRADES)
@safe_handler
async def upgrades_root(message: Message, state: FSMContext):
    await state.clear()
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        include_team = user.level >= 2
        await notify_new_achievements(message, achievements)
    await message.answer(
        RU.UPGRADES_HEADER,
        reply_markup=kb_upgrades_menu(include_team=include_team),
    )


async def _render_orders_page(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        all_orders = (
            await session.execute(
                select(Order)
                .where(Order.min_level <= user.level)
                .order_by(Order.min_level, Order.id)
            )
        ).scalars().all()
        data = await state.get_data()
        page = int(data.get("page", 0))
        sub, has_prev, has_next = slice_page(all_orders, page, 5)
        await message.answer(fmt_orders(sub), reply_markup=kb_numeric_page(has_prev, has_next))
        await state.update_data(order_ids=[o.id for o in sub], page=page)
        await notify_new_achievements(message, achievements)


@router.message(OrdersState.browsing, F.text.in_({"1", "2", "3", "4", "5"}))
@safe_handler
async def choose_order(message: Message, state: FSMContext):
    data = await state.get_data()
    ids = data.get("order_ids", [])
    idx = int(message.text) - 1
    if idx < 0 or idx >= len(ids):
        return
    order_id = ids[idx]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        if not await ensure_no_active_order(session, user):
            await message.answer(RU.ORDER_ALREADY)
            return
        order = await session.scalar(select(Order).where(Order.id == order_id))
        if not order:
            await message.answer("Заказ не найден.")
            await _render_orders_page(message, state)
            return
        stats = await get_user_stats(session, user)
        req = snapshot_required_clicks(order, user.level, stats["req_clicks_pct"])
        await state.set_state(OrdersState.confirm)
        await state.update_data(order_id=order_id, req=req)
        await message.answer(
            f"Взять заказ «{order.title}»?\nТребуемые клики: {req}", reply_markup=kb_confirm(RU.BTN_TAKE)
        )


@router.message(OrdersState.browsing, F.text == RU.BTN_PREV)
@safe_handler
async def orders_prev(message: Message, state: FSMContext):
    data = await state.get_data()
    page = max(0, int(data.get("page", 0)) - 1)
    await state.update_data(page=page)
    await _render_orders_page(message, state)


@router.message(OrdersState.browsing, F.text == RU.BTN_NEXT)
@safe_handler
async def orders_next(message: Message, state: FSMContext):
    data = await state.get_data()
    page = int(data.get("page", 0)) + 1
    await state.update_data(page=page)
    await _render_orders_page(message, state)


@router.message(OrdersState.confirm, F.text == RU.BTN_TAKE)
@safe_handler
async def take_order(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = int(data["order_id"])
    req = int(data["req"])
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        if not await ensure_no_active_order(session, user):
            await message.answer(RU.ORDER_ALREADY)
            await state.clear()
            return
        stats = await get_user_stats(session, user)
        initial_progress = 0
        free_chance = stats.get("free_order_chance", 0.0)
        free_triggered = False
        if free_chance > 0 and random.random() < free_chance:
            initial_progress = min(
                req,
                max(1, int(round(req * FREE_ORDER_PROGRESS_PCT))),
            )
            free_triggered = initial_progress > 0
        session.add(
            UserOrder(
                user_id=user.id,
                order_id=order_id,
                progress_clicks=initial_progress,
                required_clicks=req,
                started_at=utcnow(),
                finished=False,
                canceled=False,
                reward_snapshot_mul=stats["reward_mul_total"],
            )
        )
        user.updated_at = utcnow()
        order = await session.scalar(select(Order).where(Order.id == order_id))
        if order:
            await message.answer(
                RU.ORDER_TAKEN.format(title=order.title), reply_markup=kb_active_order_controls()
            )
            if free_triggered:
                await message.answer(
                    f"📦 Бесплатный старт! Прогресс: {initial_progress}/{req}",
                    reply_markup=kb_active_order_controls(),
                )
        logger.info(
            "Order taken",
            extra={"tg_id": user.tg_id, "user_id": user.id, "order_id": order_id},
        )
    await state.clear()


@router.message(OrdersState.confirm, F.text == RU.BTN_CANCEL)
@safe_handler
async def take_cancel(message: Message, state: FSMContext):
    await state.clear()
    await orders_root(message, state)


# --- Магазин ---

@router.message(F.text == RU.BTN_SHOP)
@safe_handler
async def shop_root(message: Message, state: FSMContext):
    await state.set_state(ShopState.root)
    await message.answer(RU.SHOP_HEADER, reply_markup=kb_shop_menu())


BOOST_TYPE_META: Dict[str, Tuple[str, str, str]] = {
    "cp": ("⚡️", "Клик", "за нажатие"),
    "reward": ("🎯", "Награда", "к наградам"),
    "passive": ("💼", "Пассивный доход", "к пассивному доходу"),
}

ITEM_BONUS_LABELS: Dict[str, str] = {
    "cp_pct": "к силе клика",
    "passive_pct": "к пассивному доходу",
    "req_clicks_pct": "к требуемым кликам",
    "reward_pct": "к наградам",
    "ratelimit_plus": "к лимиту кликов",
    "cp_add": "к силе клика",
}

ITEM_SLOT_EMOJI: Dict[str, str] = {
    "chair": "🪑",
    "laptop": "💻",
    "monitor": "🖥️",
    "phone": "📱",
    "tablet": "📲",
    "charm": "📜",
}


def _boost_display(boost: Boost) -> Tuple[str, str, str]:
    """Return icon, label and effect description for a boost."""

    icon, label, suffix = BOOST_TYPE_META.get(
        boost.type, ("✨", boost.name, "к характеристике")
    )
    step = boost.step_value
    if boost.type == "cp":
        effect = f"+{int(round(step))} {suffix}"
    elif boost.type in {"reward", "passive", "xp", "team_income", "rush_reward", "equipment_eff", "night_passive", "high_order_reward"}:
        effect = f"+{int(round(step * 100))}% {suffix}"
    elif boost.type == "crit":
        extra = BOOST_EXTRA_META.get(boost.code, {})
        multiplier = extra.get("crit_multiplier", 1.5)
        effect = f"+{int(round(step * 100))}% шанс, ×{format_stat(multiplier)} крит"
    elif boost.type == "event_protection":
        effect = f"−{int(round(step * 100))}% к негативу"
    elif boost.type == "combo":
        effect = f"+{format_stat(step)} {suffix}"
    elif boost.type == "ratelimit":
        effect = f"+{int(round(step))} {suffix}"
    elif boost.type == "req_clicks":
        effect = f"−{int(round(step * 100))}% {suffix}"
    elif boost.type == "free_order":
        effect = f"+{int(round(step * 100))}% {suffix}"
    elif boost.type == "team_discount":
        effect = f"−{int(round(step * 100))}% {suffix}"
    elif boost.type == "offline_cap":
        hours = step / 3600.0
        effect = f"+{format_stat(hours)} ч {suffix}"
    elif boost.type == "shop_discount":
        effect = f"−{int(round(step * 100))}% {suffix}"
    else:
        effect = f"+{format_stat(step)} {suffix}"
    return icon, label or boost.name, effect


def _format_item_effect(item: Item) -> str:
    """Human readable representation of an item's bonus."""

    label = ITEM_BONUS_LABELS.get(item.bonus_type, "к характеристике")
    if item.bonus_type.endswith("_pct"):
        value = f"+{int(round(item.bonus_value * 100))}%"
    else:
        value = f"+{int(round(item.bonus_value))}"
    return f"{value} {label}"


def _item_icon(item: Item) -> str:
    """Emoji icon for the given equipment slot."""

    return ITEM_SLOT_EMOJI.get(item.slot, "🎁")


def fmt_boosts(
    user: User, boosts: List[Boost], levels: Dict[int, int], page: int, page_size: int = 5
) -> str:
    """Compose a formatted boost list with balance and pricing."""

    lines = [f"💰 Ваш баланс: {format_price(user.balance)}", ""]
    if not boosts:
        lines.append("Пока нечего прокачать — возвращайтесь позже.")
        return "\n".join(lines)

    start_index = page * page_size
    for offset, boost in enumerate(boosts, 1):
        icon, label, effect = _boost_display(boost)
        lvl_next = levels.get(boost.id, 0) + 1
        cost = format_price(upgrade_cost(boost.base_cost, boost.growth, lvl_next))
        lines.append(
            f"{start_index + offset}. {icon} {label} — {effect} · ур.→{lvl_next} · {cost}"
        )
    return "\n".join(lines)


def format_boost_purchase_prompt(
    boost: Boost, current_level: int, next_level: int, cost: int
) -> str:
    """Pretty confirmation text for a boost upgrade purchase."""

    icon, label, effect = _boost_display(boost)
    return (
        f"{icon} Улучшение «{label}»\n"
        f"Текущий уровень: {current_level}\n"
        f"После покупки: {next_level}\n"
        f"Эффект уровня: {effect}\n"
        f"Стоимость: {format_price(cost)}"
    )


def format_item_purchase_prompt(item: Item, price: int) -> str:
    """Pretty confirmation text for buying an equipment piece."""

    icon = _item_icon(item)
    effect = _format_item_effect(item)
    return (
        f"{icon} Покупка «{item.name}»\n"
        f"Эффект: {effect}\n"
        f"Цена: {format_price(price)}"
    )


def format_item_equip_prompt(item: Item) -> str:
    """Confirmation prompt shown when the user equips an owned item."""

    icon = _item_icon(item)
    effect = _format_item_effect(item)
    return (
        f"{icon} Экипировать «{item.name}»?\n"
        f"Эффект: {effect}"
    )


async def render_boosts(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        boosts = (
            await session.execute(select(Boost).order_by(Boost.id))
        ).scalars().all()
        levels = {
            b_id: lvl
            for b_id, lvl in (
                await session.execute(
                    select(UserBoost.boost_id, UserBoost.level).where(UserBoost.user_id == user.id)
                )
            ).all()
        }
        page = int((await state.get_data()).get("page", 0))
        sub, has_prev, has_next = slice_page(boosts, page, 5)
        await message.answer(
            fmt_boosts(user, sub, levels, page),
            reply_markup=kb_numeric_page(has_prev, has_next),
        )
        await state.update_data(boost_ids=[b.id for b in sub], page=page)
        await notify_new_achievements(message, achievements)


@router.message(ShopState.root, F.text == RU.BTN_BOOSTS)
@safe_handler
async def shop_boosts(message: Message, state: FSMContext):
    await state.set_state(ShopState.boosts)
    await state.update_data(page=0)
    await render_boosts(message, state)


@router.message(ShopState.boosts, F.text.in_({"1", "2", "3", "4", "5"}))
@safe_handler
async def shop_choose_boost(message: Message, state: FSMContext):
    ids = (await state.get_data()).get("boost_ids", [])
    idx = int(message.text) - 1
    if idx < 0 or idx >= len(ids):
        return
    bid = ids[idx]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        boost = await session.scalar(select(Boost).where(Boost.id == bid))
        if not boost:
            await message.answer("Буст не найден.")
            await render_boosts(message, state)
            return
        user_boost = await session.scalar(
            select(UserBoost).where(UserBoost.user_id == user.id, UserBoost.boost_id == bid)
        )
        lvl_next = (user_boost.level if user_boost else 0) + 1
        cost = upgrade_cost(boost.base_cost, boost.growth, lvl_next)
        prompt = format_boost_purchase_prompt(
            boost,
            user_boost.level if user_boost else 0,
            lvl_next,
            cost,
        )
        await message.answer(prompt, reply_markup=kb_confirm(RU.BTN_BUY))
    await state.set_state(ShopState.confirm_boost)
    await state.update_data(boost_id=bid)


@router.message(ShopState.boosts, F.text == RU.BTN_PREV)
@safe_handler
async def shop_boosts_prev(message: Message, state: FSMContext):
    page = max(0, int((await state.get_data()).get("page", 0)) - 1)
    await state.update_data(page=page)
    await render_boosts(message, state)


@router.message(ShopState.boosts, F.text == RU.BTN_NEXT)
@safe_handler
async def shop_boosts_next(message: Message, state: FSMContext):
    page = int((await state.get_data()).get("page", 0)) + 1
    await state.update_data(page=page)
    await render_boosts(message, state)


@router.message(ShopState.confirm_boost, F.text == RU.BTN_BUY)
@safe_handler
async def shop_buy_boost(message: Message, state: FSMContext):
    bid = int((await state.get_data())["boost_id"])
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        boost = await session.scalar(select(Boost).where(Boost.id == bid))
        if not boost:
            await message.answer("Буст не найден.")
            await state.set_state(ShopState.boosts)
            await render_boosts(message, state)
            return
        user_boost = await session.scalar(
            select(UserBoost).where(UserBoost.user_id == user.id, UserBoost.boost_id == bid)
        )
        lvl_next = (user_boost.level if user_boost else 0) + 1
        cost = upgrade_cost(boost.base_cost, boost.growth, lvl_next)
        if user.balance < cost:
            await message.answer(RU.INSUFFICIENT_FUNDS)
        else:
            now = utcnow()
            user.balance -= cost
            user.updated_at = now
            if not user_boost:
                session.add(UserBoost(user_id=user.id, boost_id=bid, level=1))
            else:
                user_boost.level += 1
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="buy_boost",
                    amount=-cost,
                    meta={"boost": boost.code, "lvl": lvl_next},
                    created_at=now,
                )
            )
            logger.info(
                "Boost upgraded",
                extra={
                    "tg_id": user.tg_id,
                    "user_id": user.id,
                    "boost": boost.code,
                    "level": lvl_next,
                },
            )
            await message.answer(RU.PURCHASE_OK)
        await notify_new_achievements(message, achievements)
    await state.set_state(ShopState.boosts)
    await render_boosts(message, state)


@router.message(ShopState.confirm_boost, F.text == RU.BTN_CANCEL)
@safe_handler
async def shop_cancel_boost(message: Message, state: FSMContext):
    await state.set_state(ShopState.boosts)
    await render_boosts(message, state)


# --- Магазин: экипировка ---

def fmt_items(
    user: User,
    items: List[Item],
    page: int,
    *,
    include_price: bool = True,
    discount_pct: float = 0.0,
) -> str:
    """Format equipment listings with balance, icons and effects."""

    lines: List[str] = []
    if include_price:
        lines.append(f"💰 Ваш баланс: {format_price(user.balance)}")
    lines.append("" if include_price else "")

    if not items:
        lines.append("Пока ничего нет — загляните позже.")
        return "\n".join(lines)

    start_index = page * 5
    for offset, it in enumerate(items, 1):
        icon = _item_icon(it)
        effect = _format_item_effect(it)
        entry = f"{start_index + offset}. {icon} {it.name} — {effect}"
        if include_price:
            price = it.price
            if discount_pct > 0:
                price = apply_percentage_discount(price, discount_pct, cap=SHOP_DISCOUNT_CAP)
            entry = f"{entry} · {format_price(price)}"
        lines.append(entry)
    return "\n".join(lines)


async def render_items(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        stats = await get_user_stats(session, user)
        items = await get_next_items_for_user(session, user)
        page = int((await state.get_data()).get("page", 0))
        sub, has_prev, has_next = slice_page(items, page, 5)
        await message.answer(
            fmt_items(
                user,
                sub,
                page,
                include_price=True,
                discount_pct=stats.get("shop_discount_pct", 0.0),
            ),
            reply_markup=kb_numeric_page(has_prev, has_next),
        )
        await state.update_data(item_ids=[it.id for it in sub], page=page)
        await notify_new_achievements(message, achievements)


@router.message(ShopState.root, F.text == RU.BTN_EQUIPMENT)
@safe_handler
async def shop_equipment(message: Message, state: FSMContext):
    await state.set_state(ShopState.equipment)
    await state.update_data(page=0)
    await render_items(message, state)


@router.message(ShopState.equipment, F.text.in_({"1", "2", "3", "4", "5"}))
@safe_handler
async def shop_choose_item(message: Message, state: FSMContext):
    item_ids = (await state.get_data()).get("item_ids", [])
    idx = int(message.text) - 1
    if idx < 0 or idx >= len(item_ids):
        return
    item_id = item_ids[idx]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        it = await session.scalar(select(Item).where(Item.id == item_id))
        if not it:
            await message.answer("Предмет не найден.")
            await render_items(message, state)
            return
        stats = await get_user_stats(session, user)
        discount_pct = stats.get("shop_discount_pct", 0.0)
        price = apply_percentage_discount(it.price, discount_pct, cap=SHOP_DISCOUNT_CAP)
        prompt = format_item_purchase_prompt(it, price)
        await message.answer(prompt, reply_markup=kb_confirm(RU.BTN_BUY))
    await state.set_state(ShopState.confirm_item)
    await state.update_data(item_id=item_id)


@router.message(ShopState.equipment, F.text == RU.BTN_PREV)
@safe_handler
async def shop_items_prev(message: Message, state: FSMContext):
    page = max(0, int((await state.get_data()).get("page", 0)) - 1)
    await state.update_data(page=page)
    await render_items(message, state)


@router.message(ShopState.equipment, F.text == RU.BTN_NEXT)
@safe_handler
async def shop_items_next(message: Message, state: FSMContext):
    page = int((await state.get_data()).get("page", 0)) + 1
    await state.update_data(page=page)
    await render_items(message, state)


@router.message(ShopState.confirm_item, F.text == RU.BTN_BUY)
@safe_handler
async def shop_buy_item(message: Message, state: FSMContext):
    item_id = int((await state.get_data())["item_id"])
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        item = await session.scalar(select(Item).where(Item.id == item_id))
        if not item:
            await message.answer("Предмет не найден.")
            await state.set_state(ShopState.equipment)
            await render_items(message, state)
            return
        stats = await get_user_stats(session, user)
        discount_pct = stats.get("shop_discount_pct", 0.0)
        price = apply_percentage_discount(item.price, discount_pct, cap=SHOP_DISCOUNT_CAP)
        has = await session.scalar(
            select(UserItem).where(UserItem.user_id == user.id, UserItem.item_id == item_id)
        )
        if has:
            await message.answer("Уже куплено.")
        elif user.balance < price:
            await message.answer(RU.INSUFFICIENT_FUNDS)
        else:
            now = utcnow()
            user.balance -= price
            user.updated_at = now
            session.add(UserItem(user_id=user.id, item_id=item_id))
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="buy_item",
                    amount=-price,
                    meta={"item": item.code},
                    created_at=now,
                )
            )
            logger.info(
                "Item purchased",
                extra={"tg_id": user.tg_id, "user_id": user.id, "item": item.code},
            )
            await update_campaign_progress(session, user, "item_purchase", {})
            achievements.extend(await evaluate_achievements(session, user, {"items"}))
            next_item = await session.scalar(
                select(Item).where(Item.slot == item.slot, Item.tier == item.tier + 1)
            )
            if next_item:
                next_hint = (
                    f"Следующий уровень: {next_item.name} за {format_price(next_item.price)}."
                )
            else:
                proj_bonus, proj_price = project_next_item_params(item)
                if "_pct" in item.bonus_type:
                    bonus_str = f"≈+{int(proj_bonus * 100)}%"
                else:
                    bonus_str = f"≈+{int(proj_bonus)}"
                next_hint = (
                    f"Следующий уровень (по формуле): {format_price(proj_price)}, {bonus_str}."
                )
            await message.answer(f"{RU.PURCHASE_OK}\n{next_hint}")
        await notify_new_achievements(message, achievements)
    await state.set_state(ShopState.equipment)
    await render_items(message, state)


@router.message(ShopState.confirm_item, F.text == RU.BTN_CANCEL)
@safe_handler
async def shop_cancel_item(message: Message, state: FSMContext):
    await state.set_state(ShopState.equipment)
    await render_items(message, state)


# --- Команда ---

def fmt_team(sub: List[TeamMember], levels: Dict[int, int], costs: Dict[int, int]) -> str:
    lines = [RU.TEAM_HEADER]
    for i, m in enumerate(sub, 1):
        lvl = levels.get(m.id, 0)
        income = team_income_per_min(m.base_income_per_min, lvl)
        lines.append(f"[{i}] {m.name}: {income:.0f}/мин, ур. {lvl}, цена повышения {costs[m.id]} {RU.CURRENCY}")
    return "\n".join(lines)


async def render_team(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        stats = await get_user_stats(session, user)
        members_all = (
            await session.execute(select(TeamMember).order_by(TeamMember.base_cost, TeamMember.id))
        ).scalars().all()
        unlocked = max(0, min(len(members_all), user.level - 1))
        members = members_all[:unlocked]
        if not members:
            await state.clear()
            await message.answer(
                RU.TEAM_LOCKED,
                reply_markup=kb_upgrades_menu(include_team=False),
            )
            return
        levels = {
            mid: lvl
            for mid, lvl in (
                await session.execute(
                    select(UserTeam.member_id, UserTeam.level).where(UserTeam.user_id == user.id)
                )
            ).all()
        }
        discount_pct = stats.get("team_upgrade_discount_pct", 0.0)
        costs = {}
        for m in members:
            lvl = max(0, levels.get(m.id, 0))
            base_cost = m.base_cost * (1.22 ** lvl)
            costs[m.id] = apply_percentage_discount(base_cost, discount_pct, cap=TEAM_DISCOUNT_CAP)
        page = int((await state.get_data()).get("page", 0))
        sub, has_prev, has_next = slice_page(members, page, 5)
        await message.answer(fmt_team(sub, levels, costs), reply_markup=kb_numeric_page(has_prev, has_next))
        await state.update_data(member_ids=[m.id for m in sub], page=page)
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_TEAM)
@safe_handler
async def team_root(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        if user.level < 2:
            await state.clear()
            await message.answer(
                RU.TEAM_LOCKED,
                reply_markup=kb_upgrades_menu(include_team=False),
            )
            return
    await state.set_state(TeamState.browsing)
    await state.update_data(page=0)
    await render_team(message, state)


@router.message(TeamState.browsing, F.text.in_({"1", "2", "3", "4", "5"}))
@safe_handler
async def team_choose(message: Message, state: FSMContext):
    ids = (await state.get_data()).get("member_ids", [])
    idx = int(message.text) - 1
    if idx < 0 or idx >= len(ids):
        return
    mid = ids[idx]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        member = await session.scalar(select(TeamMember).where(TeamMember.id == mid))
        if not member:
            await message.answer("Сотрудник не найден.")
            await render_team(message, state)
            return
        await message.answer(f"Повысить «{member.name}»?", reply_markup=kb_confirm(RU.BTN_UPGRADE))
    await state.set_state(TeamState.confirm)
    await state.update_data(member_id=mid)


@router.message(TeamState.browsing, F.text == RU.BTN_PREV)
@safe_handler
async def team_prev(message: Message, state: FSMContext):
    page = max(0, int((await state.get_data()).get("page", 0)) - 1)
    await state.update_data(page=page)
    await render_team(message, state)


@router.message(TeamState.browsing, F.text == RU.BTN_NEXT)
@safe_handler
async def team_next(message: Message, state: FSMContext):
    page = int((await state.get_data()).get("page", 0)) + 1
    await state.update_data(page=page)
    await render_team(message, state)


@router.message(TeamState.confirm, F.text == RU.BTN_UPGRADE)
@safe_handler
async def team_upgrade(message: Message, state: FSMContext):
    mid = int((await state.get_data())["member_id"])
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        member = await session.scalar(select(TeamMember).where(TeamMember.id == mid))
        if not member:
            await message.answer("Сотрудник не найден.")
            await state.set_state(TeamState.browsing)
            await render_team(message, state)
            return
        stats = await get_user_stats(session, user)
        team_entry = await session.scalar(
            select(UserTeam).where(UserTeam.user_id == user.id, UserTeam.member_id == mid)
        )
        lvl = team_entry.level if team_entry else 0
        discount_pct = stats.get("team_upgrade_discount_pct", 0.0)
        cost = apply_percentage_discount(
            member.base_cost * (1.22 ** lvl), discount_pct, cap=TEAM_DISCOUNT_CAP
        )
        if user.balance < cost:
            await message.answer(RU.INSUFFICIENT_FUNDS)
        else:
            now = utcnow()
            user.balance -= cost
            user.updated_at = now
            if not team_entry:
                session.add(UserTeam(user_id=user.id, member_id=mid, level=1))
            else:
                team_entry.level += 1
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="team_upgrade",
                    amount=-cost,
                    meta={"member": member.code, "lvl": lvl + 1},
                    created_at=now,
                )
            )
            logger.info(
                "Team upgraded",
                extra={
                    "tg_id": user.tg_id,
                    "user_id": user.id,
                    "member": member.code,
                    "level": lvl + 1,
                },
            )
            await update_campaign_progress(session, user, "team_upgrade", {})
            await message.answer(RU.UPGRADE_OK)
            achievements.extend(await evaluate_achievements(session, user, {"team"}))
        await notify_new_achievements(message, achievements)
    await state.set_state(TeamState.browsing)
    await render_team(message, state)


@router.message(TeamState.confirm, F.text == RU.BTN_CANCEL)
@safe_handler
async def team_upgrade_cancel(message: Message, state: FSMContext):
    await state.set_state(TeamState.browsing)
    await render_team(message, state)


# --- Гардероб ---

def fmt_inventory(user: User, items: List[Item], page: int) -> str:
    """Render wardrobe entries with the same visual style as the shop."""

    text = fmt_items(user, items, page, include_price=False)
    if "Пока ничего нет" in text:
        return text.replace("Пока ничего нет", "Гардероб пуст — загляните в магазин.")
    return text


async def render_inventory(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        items = (
            await session.execute(
                select(Item)
                .join(UserItem, UserItem.item_id == Item.id)
                .where(UserItem.user_id == user.id)
                .order_by(Item.slot, Item.tier)
            )
        ).scalars().all()
        page = int((await state.get_data()).get("page", 0))
        sub, has_prev, has_next = slice_page(items, page, 5)
        await message.answer(
            fmt_inventory(user, sub, page),
            reply_markup=kb_numeric_page(has_prev, has_next),
        )
        await state.update_data(inv_ids=[it.id for it in sub], page=page)
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_WARDROBE)
@safe_handler
async def wardrobe_root(message: Message, state: FSMContext):
    await state.set_state(WardrobeState.browsing)
    await state.update_data(page=0)
    await render_inventory(message, state)


@router.message(WardrobeState.browsing, F.text.in_({"1", "2", "3", "4", "5"}))
@safe_handler
async def wardrobe_choose(message: Message, state: FSMContext):
    ids = (await state.get_data()).get("inv_ids", [])
    idx = int(message.text) - 1
    if idx < 0 or idx >= len(ids):
        return
    item_id = ids[idx]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        it = await session.scalar(select(Item).where(Item.id == item_id))
        if not it:
            await message.answer("Предмет не найден.")
            await render_inventory(message, state)
            return
        prompt = format_item_equip_prompt(it)
        await message.answer(prompt, reply_markup=kb_confirm(RU.BTN_EQUIP))
    await state.set_state(WardrobeState.equip_confirm)
    await state.update_data(item_id=item_id)


@router.message(WardrobeState.browsing, F.text == RU.BTN_PREV)
@safe_handler
async def wardrobe_prev(message: Message, state: FSMContext):
    page = max(0, int((await state.get_data()).get("page", 0)) - 1)
    await state.update_data(page=page)
    await render_inventory(message, state)


@router.message(WardrobeState.browsing, F.text == RU.BTN_NEXT)
@safe_handler
async def wardrobe_next(message: Message, state: FSMContext):
    page = int((await state.get_data()).get("page", 0)) + 1
    await state.update_data(page=page)
    await render_inventory(message, state)


@router.message(WardrobeState.equip_confirm, F.text == RU.BTN_EQUIP)
@safe_handler
async def wardrobe_equip(message: Message, state: FSMContext):
    item_id = int((await state.get_data())["item_id"])
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        item = await session.scalar(select(Item).where(Item.id == item_id))
        if not item:
            await message.answer("Предмет не найден.")
            await state.set_state(WardrobeState.browsing)
            await render_inventory(message, state)
            return
        has = await session.scalar(
            select(UserItem).where(UserItem.user_id == user.id, UserItem.item_id == item_id)
        )
        if not has:
            await message.answer(RU.EQUIP_NOITEM)
        else:
            now = utcnow()
            eq = await session.scalar(
                select(UserEquipment).where(UserEquipment.user_id == user.id, UserEquipment.slot == item.slot)
            )
            if not eq:
                session.add(UserEquipment(user_id=user.id, slot=item.slot, item_id=item.id))
            else:
                eq.item_id = item.id
            user.updated_at = now
            logger.info(
                "Item equipped",
                extra={"tg_id": user.tg_id, "user_id": user.id, "item": item.code},
            )
            await message.answer(RU.EQUIP_OK)
        await notify_new_achievements(message, achievements)
    await state.set_state(WardrobeState.browsing)
    await render_inventory(message, state)


@router.message(WardrobeState.equip_confirm, F.text == RU.BTN_CANCEL)
@safe_handler
async def wardrobe_equip_cancel(message: Message, state: FSMContext):
    await state.set_state(WardrobeState.browsing)
    await render_inventory(message, state)


# --- Профиль ---

@router.message(F.text == RU.BTN_PROFILE)
@safe_handler
async def profile_show(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        stats = await get_user_stats(session, user)
        rate = await calc_passive_income_rate(session, user, stats)
        active = await get_active_order(session, user)
        avg_income = await fetch_user_average_income(session, user.id)
        display_name = user.first_name or message.from_user.full_name or f"Игрок {user.id}"
        order_str = "нет активных заказов"
        if active:
            ord_row = await session.scalar(select(Order).where(Order.id == active.order_id))
            if ord_row:
                order_bar = render_progress_bar(active.progress_clicks, active.required_clicks)
                order_str = (
                    f"{ord_row.title} — {active.progress_clicks}/{active.required_clicks} {order_bar}"
                )
        now = utcnow()
        buffs = (
            await session.execute(
                select(UserBuff).where(UserBuff.user_id == user.id, UserBuff.expires_at > now)
            )
        ).scalars().all()
        buffs_text = (
            ", ".join(
                f"{buff.title} до {ensure_naive(buff.expires_at).strftime('%H:%M')}"
                for buff in buffs
            )
            if buffs
            else "нет"
        )
        campaign = await get_campaign_progress_entry(session, user)
        definition = get_campaign_definition(campaign.chapter)
        if definition:
            pct = percentage(
                campaign_goal_progress(definition.get("goal", {}), campaign.progress or {}),
                1.0,
            )
            status_icon = "✅" if pct >= 100 else ""
            campaign_text = (
                f"{definition['chapter']}/{len(CAMPAIGN_CHAPTERS)} — {pct}% {status_icon}"
            ).strip()
        else:
            campaign_text = "все главы — 100% ✅"
        prestige = await get_prestige_entry(session, user)
        xp_need = max(1, xp_to_level(user.level))
        xp_pct = percentage(user.xp, xp_need)
        xp_bar = render_progress_bar(user.xp, xp_need)
        passive_per_min = format_money(rate * 60)
        text = RU.PROFILE.format(
            name=display_name,
            lvl=user.level,
            xp=user.xp,
            xp_need=xp_need,
            xp_bar=xp_bar,
            xp_pct=xp_pct,
            rub=format_money(user.balance),
            avg=format_money(avg_income),
            cp=format_stat(stats["cp"]),
            passive=f"{passive_per_min} ₽",
            order=order_str,
            buffs=buffs_text,
            campaign=campaign_text,
            rep=prestige.reputation,
        )
        await message.answer(text, reply_markup=kb_profile_menu(has_active_order=bool(active)))
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_DAILY)
@safe_handler
async def profile_daily(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        now = utcnow()
        last_bonus = ensure_naive(user.daily_bonus_at)
        if last_bonus and (now - last_bonus) < timedelta(hours=24):
            await message.answer(
                RU.DAILY_WAIT,
                reply_markup=await main_menu_for_message(message, session=session, user=user),
            )
            return
        user.daily_bonus_at = now
        user.balance += SETTINGS.DAILY_BONUS_RUB
        user.daily_bonus_claims += 1
        user.updated_at = now
        session.add(
            EconomyLog(
                user_id=user.id,
                type="daily_bonus",
                amount=SETTINGS.DAILY_BONUS_RUB,
                meta=None,
                created_at=now,
            )
        )
        logger.info("Daily bonus collected", extra={"tg_id": user.tg_id, "user_id": user.id})
        await message.answer(
            RU.DAILY_OK.format(rub=SETTINGS.DAILY_BONUS_RUB),
            reply_markup=await main_menu_for_message(message, session=session, user=user),
        )
        achievements.extend(await evaluate_achievements(session, user, {"daily", "balance"}))
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_QUEST)
@safe_handler
async def quest_entry(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        if user.level < 2:
            await message.answer(
                RU.QUEST_LOCKED,
                reply_markup=await main_menu_for_message(message, session=session, user=user),
            )
            return
        quest = await get_or_create_quest(session, user, QUEST_CODE_HELL_CLIENT)
        if quest.is_done:
            await message.answer(
                RU.QUEST_ALREADY_DONE,
                reply_markup=await main_menu_for_message(message, session=session, user=user),
            )
            return
        quest.stage = 0
        quest_get_stage_payload(quest)
        await notify_new_achievements(message, achievements)
    await state.set_state(HellClientState.intro)
    await message.answer(RU.QUEST_INTRO)
    await send_hell_client_step(message, "intro")


@router.message(HellClientState.intro)
@safe_handler
async def quest_intro(message: Message, state: FSMContext):
    await process_hell_client_choice(message, state, "intro")


@router.message(HellClientState.step1)
@safe_handler
async def quest_step1(message: Message, state: FSMContext):
    await process_hell_client_choice(message, state, "step1")


@router.message(HellClientState.step2)
@safe_handler
async def quest_step2(message: Message, state: FSMContext):
    await process_hell_client_choice(message, state, "step2")


@router.message(F.text == RU.BTN_SKILLS)
@safe_handler
async def show_skills_menu(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        rows = (
            await session.execute(
                select(Skill.name, Skill.effect, UserSkill.taken_at)
                .join(UserSkill, UserSkill.skill_code == Skill.code)
                .where(UserSkill.user_id == user.id)
                .order_by(UserSkill.taken_at)
            )
        ).all()
        await notify_new_achievements(message, achievements)
    if not rows:
        await message.answer(
            RU.SKILL_LIST_EMPTY,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    lines = [RU.SKILL_LIST_HEADER, ""]
    for idx, (name, effect, taken_at) in enumerate(rows, 1):
        lines.append(f"{idx}. {name} — {describe_effect(effect)}")
    await message.answer(
        "\n".join(lines),
        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
    )


@router.message(F.text == RU.BTN_STATS)
@safe_handler
async def show_global_stats(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        rows = await fetch_average_income_rows(session)
        active = await get_active_order(session, user)
        await notify_new_achievements(message, achievements)
    markup = kb_profile_menu(has_active_order=bool(active))
    ordered = sorted(rows, key=lambda entry: entry[2], reverse=True)
    total_players = len(ordered)
    lines = [RU.STATS_HEADER, ""]
    for idx in range(1, 6):
        if idx <= total_players:
            _, name, income = ordered[idx - 1]
            lines.append(RU.STATS_ROW.format(idx=idx, name=name, value=format_money(income)))
        else:
            lines.append(RU.STATS_EMPTY_ROW.format(idx=idx))
    player_rank = next(
        (idx for idx, (uid, _, _) in enumerate(ordered, start=1) if uid == user.id),
        None,
    )
    lines.append("")
    if player_rank is not None:
        lines.append(RU.STATS_POSITION.format(rank=player_rank, total=total_players or 1))
    else:
        lines.append(RU.STATS_POSITION_MISSING)
    await message.answer("\n".join(lines), reply_markup=markup)


@router.message(F.text.in_({RU.BTN_ACHIEVEMENTS, RU.BTN_SHOW_ACHIEVEMENTS}))
@safe_handler
async def show_achievements(message: Message):
    rows: List[Tuple[Achievement, Optional[UserAchievement]]] = []
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements_new: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements_new)
        rows = (
            await session.execute(
                select(Achievement, UserAchievement)
                .outerjoin(
                    UserAchievement,
                    (UserAchievement.achievement_id == Achievement.id)
                    & (UserAchievement.user_id == user.id),
                )
                .order_by(Achievement.id)
            )
        ).all()
        active = await get_active_order(session, user)
        await notify_new_achievements(message, achievements_new)
    markup = kb_profile_menu(has_active_order=bool(active))
    if not rows:
        await message.answer(RU.ACHIEVEMENTS_EMPTY, reply_markup=markup)
        return
    lines = [RU.ACHIEVEMENTS_TITLE]
    for ach, ua in rows:
        unlocked = bool(ua and ua.unlocked_at)
        current = ua.progress if ua else 0
        target = max(1, ach.threshold)
        if unlocked:
            current = max(current, target)
        pct = percentage(current, target)
        bar = render_progress_bar(current, target, filled_char="█", empty_char="░")
        status_icon = "✅" if unlocked else "⬜️"
        lines.append(
            f"{status_icon} {ach.icon} {ach.name} — [{bar}] {pct}% · {current}/{target}"
        )
    await message.answer("\n".join(lines), reply_markup=markup)


@router.message(F.text == RU.BTN_CAMPAIGN)
@safe_handler
async def show_campaign(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        progress = await get_campaign_progress_entry(session, user)
        definition = get_campaign_definition(progress.chapter)
        active = await get_active_order(session, user)
        goal = definition.get("goal", {}) if definition else {}
        pct = int(campaign_goal_progress(goal, progress.progress or {}) * 100) if definition else 0
        min_level = definition.get("min_level", 1) if definition else 1
        markup_profile = kb_profile_menu(has_active_order=bool(active))
        if not definition:
            await message.answer(RU.CAMPAIGN_EMPTY, reply_markup=markup_profile)
            return
        if user.level < min_level:
            await message.answer(
                RU.CAMPAIGN_HEADER + f"\nДоступ с уровня {min_level}.",
                reply_markup=markup_profile,
            )
            return
        lines = [
            RU.CAMPAIGN_HEADER,
            "",
            RU.CAMPAIGN_STATUS.format(
                chapter=definition["chapter"],
                total=len(CAMPAIGN_CHAPTERS),
                title=definition["title"],
                goal=describe_campaign_goal(goal),
                progress=pct,
            ),
        ]
        if progress.is_done:
            lines.append("")
            lines.append(RU.CAMPAIGN_DONE)
            markup = _reply_keyboard([[RU.BTN_CAMPAIGN_CLAIM], [RU.BTN_BACK]])
        else:
            markup = markup_profile
        await message.answer("\n".join(lines), reply_markup=markup)
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_CAMPAIGN_CLAIM)
@safe_handler
async def claim_campaign_handler(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        result = await claim_campaign_reward(session, user)
        if not result:
            await message.answer(
                RU.CAMPAIGN_EMPTY,
                reply_markup=kb_profile_menu(
                    has_active_order=bool(await get_active_order(session, user))
                ),
            )
            return
        text, prev_level, levels_gained = result
        markup = kb_profile_menu(has_active_order=bool(await get_active_order(session, user)))
        await message.answer(text, reply_markup=markup)
        await maybe_prompt_skill_choice(session, message, state, user, prev_level, levels_gained)


@router.message(Command("studio"))
@router.message(F.text == RU.BTN_STUDIO)
@safe_handler
async def show_studio(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        await process_offline_income(session, user, achievements)
        active = await get_active_order(session, user)
        profile_markup = kb_profile_menu(has_active_order=bool(active))
        if user.level < 20:
            await message.answer(RU.STUDIO_LOCKED, reply_markup=profile_markup)
            return
        prestige = await get_prestige_entry(session, user)
        gain = max(0, user.balance // 1000)
        bonus = (prestige.reputation) * 1
        text = RU.STUDIO_INFO.format(rep=prestige.reputation, resets=prestige.resets, bonus=bonus)
        if gain > 0:
            text += "\n\n" + RU.STUDIO_CONFIRM.format(gain=gain)
            await state.set_state(StudioState.confirm)
            await state.update_data(gain=gain)
            markup = kb_confirm(RU.BTN_STUDIO_CONFIRM)
        else:
            markup = profile_markup
        await message.answer(text, reply_markup=markup)
        await notify_new_achievements(message, achievements)


@router.message(StudioState.confirm, F.text == RU.BTN_STUDIO_CONFIRM)
@safe_handler
async def confirm_studio(message: Message, state: FSMContext):
    data = await state.get_data()
    gain = int(data.get("gain", 0))
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        await perform_prestige_reset(session, user, gain)
        markup = kb_profile_menu(has_active_order=bool(await get_active_order(session, user)))
        await message.answer(RU.STUDIO_DONE.format(gain=gain), reply_markup=markup)
    await state.clear()


@router.message(StudioState.confirm, F.text == RU.BTN_CANCEL)
@safe_handler
async def cancel_studio(message: Message, state: FSMContext):
    await state.clear()
    async with session_scope() as session:
        user = await get_user_by_tg(session, message.from_user.id)
        markup = await main_menu_for_message(message, session=session, user=user)
    await message.answer(RU.MENU_HINT, reply_markup=markup)


@router.message(SkillsState.picking)
@safe_handler
async def pick_skill(message: Message, state: FSMContext):
    data = await state.get_data()
    codes: List[str] = data.get("skill_codes", [])
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer(RU.SKILL_PROMPT)
        return
    idx = int(text) - 1
    if idx < 0 or idx >= len(codes):
        await message.answer(RU.SKILL_PROMPT)
        return
    code = codes[idx]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        skill = await session.scalar(select(Skill).where(Skill.code == code))
        if not skill:
            await message.answer(
                "Навык не найден.",
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            await state.clear()
            return
        existing = await session.scalar(
            select(UserSkill).where(UserSkill.user_id == user.id, UserSkill.skill_code == code)
        )
        if existing:
            await message.answer(
                RU.SKILL_PICKED.format(name=skill.name),
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
        else:
            session.add(UserSkill(user_id=user.id, skill_code=code, taken_at=utcnow()))
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="skill_pick",
                    amount=0.0,
                    meta={"skill": code},
                    created_at=utcnow(),
                )
            )
            await message.answer(
                RU.SKILL_PICKED.format(name=skill.name),
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
    await state.clear()


@router.message(F.text == RU.BTN_CANCEL_ORDER)
@safe_handler
async def profile_cancel_order(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        active = await get_active_order(session, user)
        if not active:
            await message.answer(
                "Нет активного заказа.",
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            return
        now = utcnow()
        active.canceled = True
        user.updated_at = now
        logger.info(
            "Order cancelled",
            extra={"tg_id": user.tg_id, "user_id": user.id, "order_id": active.order_id},
        )
        markup = await build_main_menu_markup(tg_id=message.from_user.id)
        await message.answer(RU.ORDER_CANCELED, reply_markup=markup)


@router.message(F.text == RU.BTN_CANCEL)
@safe_handler
async def cancel_any(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    if current == TutorialState.step.state:
        await tutorial_skip(message, state)
        return
    if current == OrdersState.confirm.state:
        await state.set_state(OrdersState.browsing)
        await _render_orders_page(message, state)
        return
    if current == ShopState.confirm_boost.state:
        await state.set_state(ShopState.boosts)
        await render_boosts(message, state)
        return
    if current == ShopState.confirm_item.state:
        await state.set_state(ShopState.equipment)
        await render_items(message, state)
        return
    if current == TeamState.confirm.state:
        await state.set_state(TeamState.browsing)
        await render_team(message, state)
        return
    if current == WardrobeState.equip_confirm.state:
        await state.set_state(WardrobeState.browsing)
        await render_inventory(message, state)
        return
    if current in {
        OrdersState.browsing.state,
        ShopState.boosts.state,
        ShopState.equipment.state,
        ShopState.root.state,
        TeamState.browsing.state,
        WardrobeState.browsing.state,
        ProfileState.confirm_cancel.state,
        SkillsState.picking.state,
        HellClientState.intro.state,
        HellClientState.step1.state,
        HellClientState.step2.state,
        StudioState.confirm.state,
    }:
        await state.clear()
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    await state.clear()
    await message.answer(
        RU.MENU_HINT,
        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
    )


@router.message(F.text == RU.BTN_BACK)
@safe_handler
async def handle_back(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    if current == TutorialState.step.state:
        await tutorial_skip(message, state)
        return
    if current == OrdersState.confirm.state:
        await state.set_state(OrdersState.browsing)
        await _render_orders_page(message, state)
        return
    if current == OrdersState.browsing.state:
        await state.clear()
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    if current == ShopState.confirm_boost.state:
        await state.set_state(ShopState.boosts)
        await render_boosts(message, state)
        return
    if current == ShopState.confirm_item.state:
        await state.set_state(ShopState.equipment)
        await render_items(message, state)
        return
    if current == ShopState.root.state:
        await state.clear()
        async with session_scope() as session:
            user = await ensure_user_loaded(session, message)
            if not user:
                return
            include_team = user.level >= 2
        await message.answer(
            RU.UPGRADES_HEADER,
            reply_markup=kb_upgrades_menu(include_team=include_team),
        )
        return
    if current in {ShopState.boosts.state, ShopState.equipment.state}:
        await state.set_state(ShopState.root)
        await message.answer(RU.SHOP_HEADER, reply_markup=kb_shop_menu())
        return
    if current == TeamState.confirm.state:
        await state.set_state(TeamState.browsing)
        await render_team(message, state)
        return
    if current == TeamState.browsing.state:
        await state.clear()
        async with session_scope() as session:
            user = await ensure_user_loaded(session, message)
            if not user:
                return
            include_team = user.level >= 2
        await message.answer(
            RU.UPGRADES_HEADER,
            reply_markup=kb_upgrades_menu(include_team=include_team),
        )
        return
    if current == WardrobeState.equip_confirm.state:
        await state.set_state(WardrobeState.browsing)
        await render_inventory(message, state)
        return
    if current == WardrobeState.browsing.state:
        await state.clear()
        async with session_scope() as session:
            user = await ensure_user_loaded(session, message)
            if not user:
                return
            include_team = user.level >= 2
        await message.answer(
            RU.UPGRADES_HEADER,
            reply_markup=kb_upgrades_menu(include_team=include_team),
        )
        return
    if current in {
        SkillsState.picking.state,
        HellClientState.intro.state,
        HellClientState.step1.state,
        HellClientState.step2.state,
        StudioState.confirm.state,
    }:
        await state.clear()
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    await state.clear()
    await message.answer(
        RU.MENU_HINT,
        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
    )


# ----------------------------------------------------------------------------
# Запуск бота
# ----------------------------------------------------------------------------

async def main() -> None:
    """Entry point for running the Telegram bot."""

    if not SETTINGS.BOT_TOKEN or ":" not in SETTINGS.BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден или неверен. Укажите его в .env (BOT_TOKEN=...)")
    await init_models()
    await prepare_database()

    bot = Bot(SETTINGS.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware анти-флуда для всех сообщений (фактически ограничивает только кнопку «Клик»)
    dp.message.middleware(RateLimitMiddleware(get_user_click_limit))

    # Роутер
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Bot started", extra={"event": "startup"})
    await dp.start_polling(bot)


if __name__ == "__main__":
    def _run_startup_checks() -> None:
        """Lightweight assertions to guard critical economic formulas."""

        assert finish_order_reward(100, 1.25) == base_reward_from_required(100, 1.25)
        assert finish_order_reward(100, 0.0) == base_reward_from_required(100, 1.0)


    _run_startup_checks()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped", extra={"event": "shutdown"})
