# -*- coding: utf-8 -*-
"""
Designer Clicker Bot — single-file edition (patched)
===================================================
Полностью рабочий Telegram-кликер «Дизайнер» в одном файле.
Технологии: Python 3.11+ (совместимо с 3.12), aiogram 3.x, SQLAlchemy 2.x (async), SQLite (aiosqlite).

# Changelog: Trend, Shield, Event %, Prestige formula, New content, Unlock hints, Anti-spam

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
from math import floor, sqrt
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
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
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
    and_,
    delete,
    select,
    func,
    update,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.orm.attributes import flag_modified
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
TREND_DURATION_HOURS = 24  # Баланс: сократите при необходимости ускорить ротацию.
TREND_REWARD_MUL = 2.0  # Баланс: снизьте, если доходы растут слишком быстро.
PRESTIGE_GAIN_DIVISOR = 1_000  # Коэффициент K для формулы репутации; подберите под экономику поздней игры.
BOOST_COST_GROWTH = 1.6
BOOSTS_PER_PAGE = 5
BOOST_SELECTION_INPUTS = {str(i) for i in range(1, BOOSTS_PER_PAGE + 1)}
FREE_UPGRADE_PRICE_LABEL = "0 ₽ (первый раз бесплатно)"


def is_tutorial_active(user: Optional["User"]) -> bool:
    """Return True if the user is still going through the tutorial."""

    return bool(
        user
        and user.tutorial_completed_at is None
        and user.tutorial_stage < TUTORIAL_STAGE_DONE
    )


def _append_tutorial_skip(rows: List[List[str]], tutorial: bool) -> None:
    """Append the tutorial skip button row when onboarding is in progress."""

    if tutorial:
        if not rows or rows[-1] != [RU.BTN_TUTORIAL_SKIP]:
            rows.append([RU.BTN_TUTORIAL_SKIP])


# Доп. словари для витрины заказов.
ORDER_DIFFICULTY_LABELS: Dict[str, str] = {
    "easy": "Лёгкий",
    "normal": "Средний",
    "hard": "Сложный",
    "expert": "Эксперт",
}
ORDER_RARITY_ICONS: Dict[str, str] = {
    "common": "",  # Базовые заказы без подсветки
    "rare": "💎",
    "holiday": "🎉",
}
ORDER_RARITY_TITLES: Dict[str, str] = {
    "common": "обычный",
    "rare": "редкий",
    "holiday": "праздничный",
}

# Краткие описания заказов для более цельной витрины.
ORDER_DESCRIPTIONS: Dict[str, str] = {
    "Аватар для соцсетей": "Стильный штрих к твоему онлайн-образу.",
    "Визитка для фрилансера": "Покажи профессионализм с первого взгляда.",
    "Серия сторис для Instagram": "Каждая сторис — шаг к узнаваемости.",
    "Обложка для VK": "Создай визуал, который цепляет с первого взгляда.",
    "Логотип для кафе": "Запомнись гостям фирменным знаком.",
    "Презентация для стартапа": "Собери питч, от которого инвесторы не устоят.",
    "Пакет баннеров для рекламы": "Растяни креатив на все форматы кампании.",
    "Лендинг (1 экран)": "Одноэкранный сайт, который ведёт к заявке.",
    "Контент-план для рассылки": "Сделай письма последовательными и продающими.",
    "Редизайн логотипа": "Освежи бренд без потери узнаваемости.",
    "Новогодний мерч для подписчиков": "Пусть фанаты встретят праздники в твоём стиле.",
    "Хэллоуинская промо-страница": "Окутай проект атмосферой тёмной магии.",
    "Брендбук (мини)": "Собери правила бренда в компактный набор.",
    "UX-аудит мобильного приложения": "Подсвети, где пользователи теряют путь.",
    "Коллекционный NFT-дроп": "Выпусти цифровой артефакт для избранных.",
    "Редизайн приложения (ядро)": "Обнови ключевые экраны и улучшай опыт.",
    "Виртуальный шоурум в VR": "Погрузи клиентов в атмосферу бренда.",
    "Брендинг для корпорации": "Сформируй образ компании мирового уровня.",
    "Сайт компании 5 экранов": "Расскажи историю бизнеса в пяти блоках.",
    "Международная кампания бренда": "Синхронизируй коммуникации на разных рынках.",
    "Глобальный ребрендинг": "Перезапусти бренд с чистого листа.",
    "Особый заказ: Айдентика фестиваля": "Создай фирменный стиль события года.",
}


# Источники пассивного дохода: значения собраны в одном месте для быстрой балансировки.
PASSIVE_SOURCES: List[Dict[str, Any]] = [
    {
        "code": "bank_deposit",
        "title": "🏦 Банковский вклад",
        "min_level": 2,
        "income_per_min": 2,
        "price": 500,
        "description": "Пусть проценты капают, пока ты отдыхаешь.",
    },
    {
        "code": "tech_rental",
        "title": "💻 Аренда техники",
        "min_level": 5,
        "income_per_min": 5,
        "price": 1_500,
        "description": "Отдай ноутбук в аренду — пусть работает за тебя.",
    },
    {
        "code": "dividend_stocks",
        "title": "📊 Дивидендные акции",
        "min_level": 6,
        "income_per_min": 8,
        "price": 3_000,
        "description": "Инвестируй в успех крупных компаний.",
    },
    {
        "code": "royalty_income",
        "title": "🎵 Авторские отчисления",
        "min_level": 8,
        "income_per_min": 10,
        "price": 4_500,
        "description": "Однажды создал — теперь получай роялти вечно.",
    },
    {
        "code": "car_rental",
        "title": "🚗 Сдача автомобиля",
        "min_level": 10,
        "income_per_min": 15,
        "price": 8_000,
        "description": "Пусть твоя машина работает вместо тебя.",
    },
    {
        "code": "coffee_kiosk",
        "title": "☕ Мини-бизнес (ларёк)",
        "min_level": 12,
        "income_per_min": 20,
        "price": 12_000,
        "description": "Малый бизнес — стабильная прибыль каждый день.",
    },
    {
        "code": "property_rent",
        "title": "🏠 Аренда недвижимости",
        "min_level": 14,
        "income_per_min": 30,
        "price": 18_000,
        "description": "Сдавай жильё и получай гарантированный доход.",
    },
    {
        "code": "crypto_mining",
        "title": "💰 Крипто-майнинг",
        "min_level": 16,
        "income_per_min": 35,
        "price": 25_000,
        "description": "Твоя ферма печатает деньги, пока ты отдыхаешь.",
    },
    {
        "code": "startup_investment",
        "title": "🚀 Инвестиция в стартап",
        "min_level": 18,
        "income_per_min": 50,
        "price": 40_000,
        "description": "Поддержи идею — и получай долю от успеха.",
    },
    {
        "code": "franchise_network",
        "title": "🏪 Франшиза (сеть предприятий)",
        "min_level": 20,
        "income_per_min": 70,
        "price": 65_000,
        "description": "Стань владельцем сети и наслаждайся стабильным потоком ₽.",
    },
]

PASSIVE_SOURCE_BY_CODE = {source["code"]: source for source in PASSIVE_SOURCES}


@dataclass
class FreeShopOffer:
    kind: Literal["boost", "item"]
    target_id: int


@dataclass
class OrderCompletionResult:
    order: Optional["Order"]
    reward: int
    xp_gain: int
    prev_level: int
    levels_gained: int
    high_bonus_pct: float
    rush_applied: bool
    rush_bonus_pct: float
    trend_applied: bool
    trend_multiplier: float
    is_special: bool
    event_payload: Optional[Tuple[str, Optional[InlineKeyboardMarkup]]]


@dataclass
class IdleIncomeResult:
    passive_gain: int = 0
    progress_gain: int = 0
    order_completion: Optional[OrderCompletionResult] = None


_extra_phrase_last_sent: Dict[int, float] = {}


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
    BTN_DAILIES = "🗓️ Задания дня"
    BTN_REFERRAL = "🤝 Пригласить друга"
    BTN_STATS = "🏆 Топ"
    BTN_ACHIEVEMENTS = "🏆 Достижения"
    BTN_CAMPAIGN = "📜 Кампания"
    BTN_SKILLS = "🎯 Навыки"
    BTN_QUEST = "😈 Квест"
    BTN_STUDIO = "🏢 Студия"
    BTN_PROFILE_CAT_STATS = "📊 Статистика и задачи"
    BTN_PROFILE_CAT_PROGRESS = "🎯 Прогресс"
    BTN_PROFILE_CAT_LONG_TERM = "🗺️ Долгосрочные цели"
    BTN_PROFILE_CAT_SOCIAL = "🤝 Сообщество"

    PROFILE_CATEGORY_PROMPT = "Выберите раздел профиля:"
    PROFILE_CATEGORY_PROMPT_STATS = "📊 Раздел «Статистика и задачи». Выберите действие:"
    PROFILE_CATEGORY_PROMPT_PROGRESS = "🎯 Раздел «Прогресс». Чем займёмся?"
    PROFILE_CATEGORY_PROMPT_LONG_TERM = "🗺️ Долгосрочные цели — выберите направление:"
    PROFILE_CATEGORY_PROMPT_SOCIAL = "🤝 Сообщество — что хотите открыть?"

    # Общие
    BTN_MENU = "🏠 Меню"
    BTN_TO_MENU = "🏠 Перейти в меню"
    BTN_PREV = "◀️ Предыдущая страница"
    BTN_NEXT = "Следующая страница ▶️"
    BTN_TAKE = "🚀 Взять заказ"
    BTN_CANCEL = "❌ Отмена"
    BTN_CONFIRM = "✅ Подтвердить"
    BTN_EQUIP = "🧩 Экипировать"
    BTN_BUY = "💳 Купить"
    BTN_UPGRADE = "⚙️ Повысить"
    BTN_BOOSTS = "⚡ Усиления"
    BTN_EQUIPMENT = "🧰 Экипировка"
    BTN_DAILY = "🎁 Ежедневный бонус"
    BTN_CANCEL_ORDER = "🛑 Отменить заказ"
    BTN_BACK = "◀️ Назад"
    BTN_RETURN_ORDER = "🔙 Вернуться к заказу"
    BTN_PROFILE_BACK = "◀️ Назад к разделам"
    BTN_HOME = "🏠 Меню"
    BTN_TUTORIAL_NEXT = "➡️ Далее"
    BTN_TUTORIAL_SKIP = "⏭️ Пропустить"
    BTN_TUTORIAL_FINISH = "Готово"
    BTN_SHOW_ACHIEVEMENTS = "🏆 Посмотреть достижения"
    BTN_CAMPAIGN_CLAIM = "🎁 Забрать награду"
    BTN_STUDIO_CONFIRM = "✨ Открыть студию"

    # Сообщения
    BOT_STARTED = "Бот запущен."
    WELCOME = (
        "🎨 Привет, {name}! Я твой менеджер в студии.\n"
        "У нас есть стартовый капитал {capital} ₽ и пара горячих заказов.\n"
        "Нажми «{orders}», чтобы взять первый бриф и начать карьеру!"
    )
    MENU_HINT = "📍 Главное меню: выберите раздел."
    MENU_WITH_ORDER_HINT = "📍 Главное меню: продолжайте заказ или откройте другой раздел."
    TOO_FAST = "⏳ Темп слишком высокий. Дождитесь восстановления лимита."
    NO_ACTIVE_ORDER = "🧾 Пока нет активного заказа. Возьмите новый в разделе «Заказы»."
    CLICK_PROGRESS = "🖱️ Клик ({pct}%): {cur}/{req} кликов."
    ORDER_TAKEN = "🚀 Отлично! Заказ «{title}» теперь ваш. Клиент уже ждёт макеты!"
    ORDER_ALREADY = "⚠️ Сначала завершите текущий заказ — новые выдаём только после сдачи прошлого."
    ORDER_DONE = "✅ Заказ успешно выполнен! Вознаграждение: {rub} ₽ и {xp} XP."
    TEAM_ORDER_DONE = "👥 Команда завершила заказ «{title}». Вознаграждение: {rub} ₽ и {xp} XP."
    ORDER_CANCELED = "↩️ Заказ отменён. Прогресс сброшен."
    ORDER_RESUME = "🧾 Продолжаем заказ «{title}». Кликай, чтобы продвинуться."
    INSUFFICIENT_FUNDS = "💸 Не хватает средств для покупки. Подкопите ещё немного и возвращайтесь!"
    BOOST_LOCKED = "🔒 Это усиление станет доступно с {lvl} уровня."
    PURCHASE_OK = "🛒 Покупка успешна! Улучшение применено."
    UPGRADE_OK = "🔼 Повышение выполнено! Уровень растёт."
    EQUIP_OK = "🧩 Экипировка активирована — стиль и статы на высоте!"
    EQUIP_NOITEM = "🕹️ Сначала купите предмет."
    DAILY_OK = "🎁 Бонус начислен: +{rub} ₽. Можно тут же вложить их в развитие!"
    DAILY_WAIT = "⏰ Бонус уже получен. Загляните позже."
    PROFILE = (
        "🧑‍💼 {name} · 🏅 Ур. {lvl}\n"
        "🏅 Звание: {rank}\n"
        "✨ XP: {xp}/{xp_need} {xp_bar} {xp_pct}%\n"
        "💰 Баланс: {rub} ₽ · 📈 Ср. доход: {avg} ₽\n"
        "🖱️ Сила клика: {cp} · 💤 Пассив: {passive}/мин\n"
        "📌 Заказ: {order}\n"
        "🛡️ Баффы: {buffs}\n"
        "📜 Кампания: {campaign}\n"
        "🏢 Репутация: {rep}\n"
        "🤝 Приглашено друзей: {referrals}"
    )
    PASSIVE_HEADER = "💤 Источники пассивного дохода"
    PASSIVE_PURCHASE_HINT = "Чтобы купить источник, отправьте /buy_passive <номер>."
    PASSIVE_ALREADY = "✅ Этот источник уже куплен."
    PASSIVE_LOCKED = "🔒 Источник откроется с {lvl} уровня."
    PASSIVE_USAGE = "Укажите номер источника: /buy_passive <номер>."
    PASSIVE_UNKNOWN = "Источник с таким номером не найден."
    PASSIVE_PURCHASED = "💤 Источник «{name}» теперь приносит +{income} ₽/мин."
    PROFILE_SHIELD = "🛡️ Защита: {charges}"
    TEAM_HEADER = "👥 Команда (прогресс/мин, уровень, цена повышения):"
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
    ACHIEVEMENT_UNLOCK = "🏆 Поздравляем! Достижение «{title}». {desc}"
    ACHIEVEMENTS_TITLE = "🏆 Достижения"
    ACHIEVEMENTS_EMPTY = "Пока нет достижений — продолжайте играть!"
    ACHIEVEMENTS_ENTRY = "{icon} {name} — {desc}"
    TUTORIAL_DONE = "🎓 Обучение завершено! Главное меню открыто — творим и зарабатываем."
    TUTORIAL_HINT = "⚡ Как будете готовы — нажмите «{button}» на клавиатуре ниже."
    TUTORIAL_LOCKED = "Сейчас идёт обучение. Нажмите «{button}» для продолжения."
    TUTORIAL_FREE_UPGRADE_DONE = "🎓 Первое улучшение бесплатно оформлено! Эффект активирован."
    TUTORIAL_SHOP_HINT_BOOST = (
        "🎯 Бесплатное усиление «{name}» ждёт в категории «{category}». Если список уже открыт, "
        "просто выбери цифру напротив усиления и нажми «{buy}»."
    )
    TUTORIAL_SHOP_HINT_ITEM = (
        "🎯 Бесплатный предмет «{name}» доступен в разделе «{equipment}». Нажми «{equipment}», "
        "затем выбери цифру и кнопку «{buy}»."
    )
    TUTORIAL_SHOP_PRICE_HINT = "👀 Ищи цену {price} — это ваш подарок."
    TUTORIAL_SHOP_LOCK = "🚪 Магазин не отпустит, пока не заберёшь бесплатное улучшение."
    EVENT_POSITIVE = "{title}"
    EVENT_NEGATIVE = "{title}"
    EVENT_BUFF = "{title}"
    EVENT_BUFF_ACTIVE = "🔔 Активен бафф: {title} (до {expires})"
    EVENT_SHIELD_BLOCK = "🛡️ Страховка сработала. Негатив отменён."
    QUEST_LOCKED = "😈 Квесты откроются с {lvl} уровня. Продолжайте прокачку!"
    QUEST_ALREADY_DONE = "😈 Квест «{name}» уже завершён. Выберите другой вызов."
    QUEST_ALL_DONE = "😈 Вы прошли все доступные квесты! Скоро появятся новые испытания."
    QUEST_SELECT = "🔥 Выберите квест для прохождения:"
    QUEST_START = "🔥 Квест «{name}» начался! Сделайте выбор ниже."
    QUEST_OPTION_UNKNOWN = "🤔 Не понимаю этот вариант. Выберите кнопку из списка."
    QUEST_INTRO = "🔥 Клиент из ада появился в чате. Готовы к испытанию?"
    QUEST_STEP = "{text}"
    QUEST_FINISH = "😈 Квест завершён! Награда: {rub} ₽ и {xp} XP."
    QUEST_ITEM_GAIN = "📜 Вы получили талисман клиента — терпение +{pct}%!"
    QUEST_TROPHY_GAIN = "🏆 Новый трофей: {name}! {effect}"
    LEVEL_UP = "🏅 Уровень {lvl}! Вы теперь {rank}."
    DAILIES_HEADER = "🗓️ Задания дня"
    DAILIES_TASK_ROW = "{status} {text} — {progress}/{goal}"
    DAILIES_DONE_REWARD = "🎉 Задание «{text}» выполнено! Награда: {reward}."
    DAILIES_EMPTY = "Сегодня всё сделано! Загляните завтра за новыми задачами."
    REFERRAL_INVITE = (
        "🤝 Пригласите друга по ссылке:\n{link}\n"
        "Каждый новый дизайнер принесёт вам {rub} ₽ и {xp} XP."
    )
    SPECIAL_ORDER_TITLE = "Особый заказ"
    SPECIAL_ORDER_HINT = "🔥 Сегодня трендовый заказ: {title} ×{mul}"
    SPECIAL_ORDER_AVAILABLE = "💡 Сегодня доступен особый заказ с повышенной наградой!"
    TREND_HINT = "🔥 Сегодня трендовый заказ: {title} ×{mul}"
    TREND_BADGE = "🔥 тренд"
    UNLOCK_HINT_TEAM = (
        "👥 Доступна Команда. Наймите первого сотрудника — он поможет выполнять заказы автоматически."
    )
    UNLOCK_HINT_SKILLS = "🎯 Доступны Навыки. Зайдите в Профиль → Навыки."
    UNLOCK_HINT_QUESTS = "😈 Доступны Квесты. Попробуйте «Клиент из ада»."
    UNLOCK_HINT_STUDIO = "🏢 Доступна Студия (престиж)."
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


# --- Дополнительные игровые константы ---

TUTORIAL_STAGE_INTRO = 0
TUTORIAL_STAGE_GO_ORDERS = 1
TUTORIAL_STAGE_ORDER_PICK = 2
TUTORIAL_STAGE_CLICKS = 3
TUTORIAL_STAGE_UPGRADES = 4
TUTORIAL_STAGE_SHOP = 5
TUTORIAL_STAGE_FINISH = 6
TUTORIAL_STAGE_DONE = 7

TUTORIAL_REQUIRED_CLICKS = 3
TUTORIAL_TOTAL_STEPS = TUTORIAL_STAGE_FINISH + 1

TUTORIAL_STAGE_MESSAGES = {
    TUTORIAL_STAGE_INTRO: (
        "👋 Добро пожаловать! Предлагаю быстрый тур — освоишься за минуту.",
    ),
    TUTORIAL_STAGE_GO_ORDERS: (
        "🎯 Цель игры — зарабатывать на дизайне. Начнём с заказа: нажми «{orders}».",
    ),
    TUTORIAL_STAGE_ORDER_PICK: (
        "📋 Выбери любой бриф и подтверди «{take}», чтобы начать работу.",
    ),
    TUTORIAL_STAGE_CLICKS: (
        "🖱️ Жми «{click}», чтобы двигать прогресс. Сделай хотя бы {need} клика.",
    ),
    TUTORIAL_STAGE_UPGRADES: (
        "⚙️ Отлично! Теперь открой «{upgrades}» — покажу, как прокачиваться.",
    ),
    TUTORIAL_STAGE_SHOP: (
        "🛒 Зайди в «{shop}» и возьми первое улучшение. Сейчас оно бесплатное!",
    ),
    TUTORIAL_STAGE_FINISH: (
        "✨ Все готово. Нажми «{finish}», чтобы завершить обучение.",
    ),
}

TUTORIAL_STAGE_HINT_BUTTONS: Dict[int, str] = {
    TUTORIAL_STAGE_INTRO: RU.BTN_TUTORIAL_NEXT,
    TUTORIAL_STAGE_GO_ORDERS: RU.BTN_ORDERS,
    TUTORIAL_STAGE_ORDER_PICK: RU.BTN_TAKE,
    TUTORIAL_STAGE_CLICKS: RU.BTN_CLICK,
    TUTORIAL_STAGE_UPGRADES: RU.BTN_UPGRADES,
    TUTORIAL_STAGE_SHOP: RU.BTN_SHOP,
    TUTORIAL_STAGE_FINISH: RU.BTN_TUTORIAL_FINISH,
}

CLICK_EXTRA_PHRASES = [
    "🎶 Плейлист вдохновения звучит! Креатив кипит.",
    "🧠 Визуал рождается на лету — продолжай!",
    "☕ Латте на столе, кисти готовы. Работает как часы!",
    "📈 Клиент видит прогресс и улыбается.",
]
CLICK_EXTRA_PHRASE_CHANCE = 0.15
CLICK_EXTRA_PHRASE_COOLDOWN = 60.0

ORDER_DONE_EXTRA = [
    "Клиент в восторге!",
    "Портфолио пополнилось стильной работой.",
    "Ваша репутация растёт.",
    "Команда обсуждает успех за чашкой кофе!",
]

RANK_THRESHOLDS = [
    (1, "Новичок"),
    (5, "Дизайнер-стажёр"),
    (10, "Дизайнер"),
    (15, "Старший дизайнер"),
    (20, "Арт-директор"),
]

PRESTIGE_RANK = "Креативный директор"

DAILY_TASKS = [
    {
        "code": "daily_clicks",
        "text": "Совершите 100 кликов",
        "goal": 100,
        "reward": {"xp": 120},
    },
    {
        "code": "daily_orders",
        "text": "Завершите 2 заказа",
        "goal": 2,
        "reward": {"rub": 250},
    },
    {
        "code": "daily_shop",
        "text": "Купите 1 улучшение",
        "goal": 1,
        "reward": {"xp": 80, "rub": 120},
    },
]

REFERRAL_BONUS_RUB = 100
REFERRAL_BONUS_XP = 50

SPECIAL_ORDER_REWARD_MUL = 2.0
SPECIAL_ORDER_MIN_LEVEL = 4

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
    rows.append([RU.BTN_ORDERS])
    rows.append([RU.BTN_UPGRADES, RU.BTN_PROFILE])
    if has_active_order:
        rows.append([RU.BTN_RETURN_ORDER])
    return _reply_keyboard(rows)


def kb_active_order_controls() -> ReplyKeyboardMarkup:
    return _reply_keyboard([[RU.BTN_CLICK, RU.BTN_TO_MENU]])


def kb_numeric_page(
    show_prev: bool, show_next: bool, add_back: bool = True, *, tutorial: bool = False
) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = []
    rows.append([str(i) for i in range(1, 6)])
    nav_row: List[str] = []
    if show_prev:
        nav_row.append(RU.BTN_PREV)
    if add_back:
        nav_row.append(RU.BTN_BACK)
    if show_next:
        nav_row.append(RU.BTN_NEXT)
    if nav_row:
        rows.append(nav_row)
    elif add_back:
        rows.append([RU.BTN_BACK])
    _append_tutorial_skip(rows, tutorial)
    return _reply_keyboard(rows)


def kb_confirm(
    confirm_text: str = RU.BTN_CONFIRM, add_menu: bool = False, *, tutorial: bool = False
) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[confirm_text, RU.BTN_CANCEL]]
    if add_menu:
        rows.append([RU.BTN_BACK])
    _append_tutorial_skip(rows, tutorial)
    return _reply_keyboard(rows)


def kb_upgrades_menu(include_team: bool, *, tutorial: bool = False) -> ReplyKeyboardMarkup:
    if tutorial:
        rows: List[List[str]] = [[RU.BTN_SHOP]]
    else:
        rows = [[RU.BTN_SHOP, RU.BTN_WARDROBE]]
        if include_team:
            rows.append([RU.BTN_TEAM])
        rows.append([RU.BTN_BACK])
    _append_tutorial_skip(rows, tutorial)
    return _reply_keyboard(rows)


def kb_shop_menu(*, tutorial: bool = False) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = [[RU.BTN_BOOSTS, RU.BTN_EQUIPMENT]]
    if not tutorial:
        rows.append([RU.BTN_BACK])
    _append_tutorial_skip(rows, tutorial)
    return _reply_keyboard(rows)

def kb_boost_categories(*, tutorial: bool = False) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = []
    current_row: List[str] = []
    for key, _meta in BOOST_CATEGORY_DEFS:
        current_row.append(BOOST_CATEGORY_BUTTON_TEXT[key])
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([RU.BTN_BACK])
    _append_tutorial_skip(rows, tutorial)
    return _reply_keyboard(rows)


def kb_boosts_controls(
    has_prev: bool, has_next: bool, count: int, *, tutorial: bool = False
) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = []
    if count > 0:
        rows.append([str(i) for i in range(1, count + 1)])
    nav_row: List[str] = []
    if has_prev:
        nav_row.append(RU.BTN_PREV)
    nav_row.append(RU.BTN_BACK)
    if has_next:
        nav_row.append(RU.BTN_NEXT)
    rows.append(nav_row)
    _append_tutorial_skip(rows, tutorial)
    return _reply_keyboard(rows)


PROFILE_MENU_CATEGORY_LABELS: Set[str] = {
    RU.BTN_PROFILE_CAT_STATS,
    RU.BTN_PROFILE_CAT_PROGRESS,
    RU.BTN_PROFILE_CAT_LONG_TERM,
    RU.BTN_PROFILE_CAT_SOCIAL,
}


PROFILE_CATEGORY_LAYOUTS: Dict[str, List[List[str]]] = {
    RU.BTN_PROFILE_CAT_STATS: [[RU.BTN_DAILY, RU.BTN_DAILIES]],
    RU.BTN_PROFILE_CAT_PROGRESS: [[RU.BTN_SKILLS, RU.BTN_ACHIEVEMENTS]],
    RU.BTN_PROFILE_CAT_LONG_TERM: [[RU.BTN_CAMPAIGN, RU.BTN_STUDIO]],
    RU.BTN_PROFILE_CAT_SOCIAL: [[RU.BTN_REFERRAL, RU.BTN_STATS]],
}


PROFILE_CATEGORY_PROMPTS: Dict[str, str] = {
    RU.BTN_PROFILE_CAT_STATS: RU.PROFILE_CATEGORY_PROMPT_STATS,
    RU.BTN_PROFILE_CAT_PROGRESS: RU.PROFILE_CATEGORY_PROMPT_PROGRESS,
    RU.BTN_PROFILE_CAT_LONG_TERM: RU.PROFILE_CATEGORY_PROMPT_LONG_TERM,
    RU.BTN_PROFILE_CAT_SOCIAL: RU.PROFILE_CATEGORY_PROMPT_SOCIAL,
}


def kb_profile_menu(
    has_active_order: bool,
    *,
    category: Optional[str] = None,
) -> ReplyKeyboardMarkup:
    """Return profile keyboard either for root categories or a chosen subgroup."""

    if category and category in PROFILE_CATEGORY_LAYOUTS:
        rows = [list(row) for row in PROFILE_CATEGORY_LAYOUTS[category]]
        rows.append([RU.BTN_PROFILE_BACK])
        return _reply_keyboard(rows)

    rows: List[List[str]] = [
        [RU.BTN_PROFILE_CAT_STATS, RU.BTN_PROFILE_CAT_PROGRESS],
        [RU.BTN_PROFILE_CAT_LONG_TERM, RU.BTN_PROFILE_CAT_SOCIAL],
    ]
    rows.append([RU.BTN_BACK])
    return _reply_keyboard(rows)


def tutorial_keyboard(stage: int) -> Optional[ReplyKeyboardMarkup]:
    """Return a minimal keyboard for the current tutorial stage."""

    rows: Optional[List[List[str]]] = None
    if stage == TUTORIAL_STAGE_INTRO:
        rows = [[RU.BTN_TUTORIAL_NEXT, RU.BTN_TUTORIAL_SKIP]]
    elif stage == TUTORIAL_STAGE_GO_ORDERS:
        rows = [[RU.BTN_ORDERS], [RU.BTN_TUTORIAL_SKIP]]
    elif stage == TUTORIAL_STAGE_ORDER_PICK:
        rows = [
            ["1", "2", "3", "4", "5"],
            [RU.BTN_PREV, RU.BTN_NEXT],
            [RU.BTN_BACK],
            [RU.BTN_TUTORIAL_SKIP],
        ]
    elif stage == TUTORIAL_STAGE_CLICKS:
        rows = [[RU.BTN_CLICK], [RU.BTN_TO_MENU], [RU.BTN_TUTORIAL_SKIP]]
    elif stage == TUTORIAL_STAGE_UPGRADES:
        rows = [[RU.BTN_UPGRADES], [RU.BTN_TUTORIAL_SKIP]]
    elif stage == TUTORIAL_STAGE_SHOP:
        rows = [[RU.BTN_SHOP], [RU.BTN_TUTORIAL_SKIP]]
    elif stage == TUTORIAL_STAGE_FINISH:
        rows = [[RU.BTN_TUTORIAL_FINISH], [RU.BTN_TUTORIAL_SKIP]]
    if not rows:
        return None
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


def kb_event_choice(event_code: str, options: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                text=opt.get("text", str(idx + 1)),
                callback_data=f"event_choice:{event_code}:{idx}",
            )
        ]
        for idx, opt in enumerate(options)
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


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


def ensure_tutorial_payload(user: User) -> Dict[str, Any]:
    """Guarantee that tutorial payload is a mutable dict."""

    payload = user.tutorial_payload or {}
    if not isinstance(payload, dict):
        payload = {}
    user.tutorial_payload = payload
    return payload


def tutorial_stage_text(user: User, stage: int) -> Optional[str]:
    """Return formatted tutorial text for the given stage."""

    template = TUTORIAL_STAGE_MESSAGES.get(stage)
    if not template:
        return None
    payload = ensure_tutorial_payload(user)
    name = user.first_name or "дизайнер"
    if isinstance(template, (list, tuple)):
        template = "\n".join(template)
    text = template.format(
        name=name,
        orders=RU.BTN_ORDERS,
        click=RU.BTN_CLICK,
        upgrades=RU.BTN_UPGRADES,
        take=RU.BTN_TAKE,
        shop=RU.BTN_SHOP,
        finish=RU.BTN_TUTORIAL_FINISH,
        need=TUTORIAL_REQUIRED_CLICKS,
    )
    hint_button = TUTORIAL_STAGE_HINT_BUTTONS.get(stage)
    step_index = min(stage, TUTORIAL_STAGE_FINISH)
    step_prefix = f"🧭 Шаг {step_index + 1} из {TUTORIAL_TOTAL_STEPS}"
    lines = [step_prefix, "", text]
    if stage == TUTORIAL_STAGE_SHOP:
        hint = payload.get("shop_hint", {}) if isinstance(payload, dict) else {}
        lines.append("")
        if hint.get("kind") == "boost":
            category_label = hint.get("category_button", RU.BTN_BOOSTS)
            boost_name = hint.get("name", "улучшение")
            lines.append(
                RU.TUTORIAL_SHOP_HINT_BOOST.format(
                    category=category_label,
                    name=boost_name,
                    buy=RU.BTN_BUY,
                )
            )
        elif hint.get("kind") == "item":
            item_name = hint.get("name", "предмет")
            lines.append(
                RU.TUTORIAL_SHOP_HINT_ITEM.format(
                    equipment=RU.BTN_EQUIPMENT,
                    name=item_name,
                    buy=RU.BTN_BUY,
                )
            )
        lines.append(RU.TUTORIAL_SHOP_PRICE_HINT.format(price=FREE_UPGRADE_PRICE_LABEL))
        lines.append(RU.TUTORIAL_SHOP_LOCK)
    if hint_button:
        lines.append("")
        lines.append(RU.TUTORIAL_HINT.format(button=hint_button))
    if stage < TUTORIAL_STAGE_FINISH:
        lines.append("Если хочешь пропустить — нажми «Пропустить».")
    return "\n".join(lines)


def tutorial_allowed_buttons(stage: int) -> Set[str]:
    """Return a set of reply buttons allowed for the current tutorial stage."""

    allowed: Set[str] = {RU.BTN_TUTORIAL_SKIP}
    if stage == TUTORIAL_STAGE_INTRO:
        allowed.add(RU.BTN_TUTORIAL_NEXT)
    elif stage == TUTORIAL_STAGE_GO_ORDERS:
        allowed.add(RU.BTN_ORDERS)
    elif stage == TUTORIAL_STAGE_ORDER_PICK:
        allowed.update({
            RU.BTN_ORDERS,
            RU.BTN_TAKE,
            RU.BTN_CANCEL,
            RU.BTN_PREV,
            RU.BTN_NEXT,
        })
        allowed.update(str(i) for i in range(1, 6))
    elif stage == TUTORIAL_STAGE_CLICKS:
        allowed.update(
            {
                RU.BTN_CLICK,
                RU.BTN_RETURN_ORDER,
                RU.BTN_TO_MENU,
            }
        )
    elif stage == TUTORIAL_STAGE_UPGRADES:
        allowed.update({RU.BTN_UPGRADES, RU.BTN_TO_MENU})
    elif stage == TUTORIAL_STAGE_SHOP:
        allowed.update(
            {
                RU.BTN_SHOP,
                RU.BTN_BOOSTS,
                RU.BTN_EQUIPMENT,
                RU.BTN_BACK,
                RU.BTN_PREV,
                RU.BTN_NEXT,
                RU.BTN_BUY,
                RU.BTN_CANCEL,
            }
        )
        allowed.update(BOOST_SELECTION_INPUTS)
        allowed.update(str(i) for i in range(1, 6))
        allowed.update(BOOST_CATEGORY_TEXTS)
    elif stage == TUTORIAL_STAGE_FINISH:
        allowed.add(RU.BTN_TUTORIAL_FINISH)
    return allowed


async def send_tutorial_prompt(message: Message, user: User, stage: int) -> None:
    """Send tutorial stage message with skip button."""

    text = tutorial_stage_text(user, stage)
    if not text:
        await message.answer(RU.TUTORIAL_DONE)
        return
    markup = tutorial_keyboard(stage)
    await message.answer(text, reply_markup=markup)


async def tutorial_on_event(
    message: Message, session: AsyncSession, user: User, event: str
) -> bool:
    """Advance tutorial flow when the required action is performed."""

    if user.tutorial_completed_at is not None or user.tutorial_stage >= TUTORIAL_STAGE_DONE:
        return False
    payload = ensure_tutorial_payload(user)
    now = utcnow()
    stage = user.tutorial_stage
    advanced = False
    if stage == TUTORIAL_STAGE_GO_ORDERS and event == "orders_opened":
        user.tutorial_stage = TUTORIAL_STAGE_ORDER_PICK
        advanced = True
    elif stage == TUTORIAL_STAGE_ORDER_PICK and event == "order_taken":
        payload["clicks"] = 0
        user.tutorial_stage = TUTORIAL_STAGE_CLICKS
        advanced = True
    elif stage == TUTORIAL_STAGE_CLICKS and event == "click":
        payload["clicks"] = int(payload.get("clicks", 0)) + 1
        if payload["clicks"] >= TUTORIAL_REQUIRED_CLICKS:
            payload.pop("clicks", None)
            user.tutorial_stage = TUTORIAL_STAGE_UPGRADES
            advanced = True
    elif stage == TUTORIAL_STAGE_CLICKS and event == "order_completed":
        payload.pop("clicks", None)
        user.tutorial_stage = TUTORIAL_STAGE_UPGRADES
        advanced = True
    elif event == "order_completed" and TUTORIAL_STAGE_CLICKS < stage < TUTORIAL_STAGE_DONE:
        user.updated_at = now
        await send_tutorial_prompt(message, user, stage)
        return False
    elif stage == TUTORIAL_STAGE_UPGRADES and event == "upgrades_open":
        await tutorial_prepare_shop_hint(session, user, payload=payload)
        user.tutorial_stage = TUTORIAL_STAGE_SHOP
        advanced = True
    elif stage == TUTORIAL_STAGE_SHOP:
        if event == "shop_open":
            payload["shop_open"] = True
        if event == "upgrade_purchase":
            user.tutorial_stage = TUTORIAL_STAGE_FINISH
            advanced = True
    elif stage == TUTORIAL_STAGE_FINISH and event == "tutorial_complete":
        user.tutorial_stage = TUTORIAL_STAGE_DONE
        user.tutorial_completed_at = now
        user.updated_at = now
        payload.clear()
        await message.answer(
            RU.TUTORIAL_DONE,
            reply_markup=await build_main_menu_markup(session=session, user=user),
        )
        return True
    if advanced:
        user.updated_at = now
        await send_tutorial_prompt(message, user, user.tutorial_stage)
    return advanced


def ensure_daily_task_state(user: User) -> Dict[str, Any]:
    """Ensure that daily tasks state is initialized for today."""

    today = utcnow().date().isoformat()
    state = user.daily_task_state or {}
    if user.daily_task_date != today:
        state = {task["code"]: {"progress": 0, "done": False} for task in DAILY_TASKS}
        user.daily_task_date = today
    elif not isinstance(state, dict):
        state = {task["code"]: {"progress": 0, "done": False} for task in DAILY_TASKS}
    user.daily_task_state = state
    return state


async def daily_task_on_event(
    message: Message,
    session: AsyncSession,
    user: User,
    task_code: str,
    amount: int = 1,
) -> None:
    """Increment progress of a daily task and award reward when completed."""

    state = ensure_daily_task_state(user)
    task_def = next((task for task in DAILY_TASKS if task["code"] == task_code), None)
    if not task_def:
        return
    entry = state.setdefault(task_code, {"progress": 0, "done": False})
    if entry.get("done"):
        return
    modified = False
    current_progress = int(entry.get("progress", 0))
    new_progress = min(task_def["goal"], current_progress + amount)
    if new_progress != current_progress:
        entry["progress"] = new_progress
        modified = True
    if entry["progress"] >= task_def["goal"] and not entry.get("done"):
        entry["done"] = True
        modified = True
        reward = task_def.get("reward", {})
        rub = int(reward.get("rub", 0))
        xp_reward = int(reward.get("xp", 0))
        prev_level = user.level
        levels_gained = 0
        if rub:
            user.balance += rub
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="daily_task",
                    amount=rub,
                    meta={"task": task_code},
                    created_at=utcnow(),
                )
            )
        if xp_reward:
            levels_gained = await add_xp_and_levelup(user, xp_reward)
        user.updated_at = utcnow()
        await message.answer(
            RU.DAILIES_DONE_REWARD.format(
                text=task_def["text"], reward=describe_reward(reward)
            )
        )
        if levels_gained:
            await notify_level_up_message(message, session, user, prev_level, levels_gained)
    if modified:
        flag_modified(user, "daily_task_state")


async def notify_level_up_message(
    message: Message,
    session: AsyncSession,
    user: User,
    prev_level: int,
    levels_gained: int,
) -> None:
    """Send celebration message when user levels up."""

    if levels_gained <= 0:
        return
    prestige = await get_prestige_entry(session, user)
    reputation = prestige.reputation if prestige else 0
    rank = rank_for(user.level, reputation)
    await message.answer(RU.LEVEL_UP.format(lvl=user.level, rank=rank))
    payload = ensure_tutorial_payload(user)
    hints = payload.setdefault("unlock_hints", {})
    unlock_messages: List[str] = []

    def maybe_unlock(flag: str, threshold: int, text: str) -> None:
        if prev_level < threshold <= user.level and not hints.get(flag):
            hints[flag] = True
            unlock_messages.append(text)

    maybe_unlock("team", 2, RU.UNLOCK_HINT_TEAM)
    maybe_unlock("skills", 5, RU.UNLOCK_HINT_SKILLS)
    maybe_unlock("quests", 10, RU.UNLOCK_HINT_QUESTS)
    maybe_unlock("studio", 20, RU.UNLOCK_HINT_STUDIO)
    if unlock_messages:
        user.tutorial_payload = payload
        user.updated_at = utcnow()
        for text in unlock_messages:
            await message.answer(text)


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
    tutorial_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    tutorial_free_boost_used: Mapped[bool] = mapped_column(Boolean, default=False)
    clicks_total: Mapped[int] = mapped_column(Integer, default=0)
    orders_completed: Mapped[int] = mapped_column(Integer, default=0)
    passive_income_collected: Mapped[int] = mapped_column(Integer, default=0)
    daily_bonus_claims: Mapped[int] = mapped_column(Integer, default=0)
    last_special_order_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    daily_task_date: Mapped[Optional[str]] = mapped_column(String(16), default=None)
    daily_task_state: Mapped[dict] = mapped_column(JSON, default=dict)
    referrals_count: Mapped[int] = mapped_column(Integer, default=0)
    referred_by: Mapped[Optional[int]] = mapped_column(Integer, default=None)

    orders: Mapped[List["UserOrder"]] = relationship(back_populates="user")


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    base_clicks: Mapped[int] = mapped_column(Integer)
    min_level: Mapped[int] = mapped_column(Integer, default=1)
    is_special: Mapped[bool] = mapped_column(Boolean, default=False)
    reward_multiplier: Mapped[float] = mapped_column(Float, default=1.0)
    reward_preview: Mapped[int] = mapped_column(Integer, default=0)
    difficulty: Mapped[str] = mapped_column(String(16), default="normal")
    estimated_minutes: Mapped[int] = mapped_column(Integer, default=30)
    rarity: Mapped[str] = mapped_column(String(16), default="common")
    appearance_weight: Mapped[float] = mapped_column(Float, default=0.0)
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
    is_special: Mapped[bool] = mapped_column(Boolean, default=False)
    trend_applied: Mapped[bool] = mapped_column(Boolean, default=False)
    trend_multiplier: Mapped[float] = mapped_column(Float, default=1.0)
    auto_progress_buffer: Mapped[float] = mapped_column(Float, default=0.0)

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
    min_level: Mapped[int] = mapped_column(Integer, default=1)


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
    min_level: Mapped[int] = mapped_column(Integer, default=1)


class UserTeam(Base):
    __tablename__ = "user_team"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("team_members.id", ondelete="CASCADE"))
    level: Mapped[int] = mapped_column(Integer, default=0)  # 0 — не нанят

    __table_args__ = (UniqueConstraint("user_id", "member_id", name="uq_user_team"),)


class UserPassiveSource(Base):
    __tablename__ = "user_passive_sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    source_code: Mapped[str] = mapped_column(String(50))
    level: Mapped[int] = mapped_column(Integer, default=1)  # уровни пригодятся, когда появятся апгрейды
    purchased_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("user_id", "source_code", name="uq_user_passive_source"),)


class Item(Base):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    slot: Mapped[Literal["laptop", "phone", "tablet", "monitor", "chair", "charm"]] = mapped_column(String(20))
    tier: Mapped[int] = mapped_column(Integer)
    bonus_type: Mapped[Literal["passive_pct", "req_clicks_pct", "reward_pct"]] = mapped_column(String(30))
    bonus_value: Mapped[float] = mapped_column(Float)
    price: Mapped[int] = mapped_column(Integer)
    min_level: Mapped[int] = mapped_column(Integer, default=1)
    obtain: Mapped[Optional[str]] = mapped_column(String(30), default=None)
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
    interactive: Mapped[bool] = mapped_column(Boolean, default=False)


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


class GlobalState(Base):
    __tablename__ = "global_state"

    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[dict] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


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
    if "tutorial_payload" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN tutorial_payload JSON DEFAULT '{}'"))
    if "tutorial_free_boost_used" not in user_columns:
        await session.execute(
            text("ALTER TABLE users ADD COLUMN tutorial_free_boost_used BOOLEAN NOT NULL DEFAULT 0")
        )
    if "clicks_total" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN clicks_total INTEGER NOT NULL DEFAULT 0"))
    if "orders_completed" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN orders_completed INTEGER NOT NULL DEFAULT 0"))
    if "passive_income_collected" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN passive_income_collected INTEGER NOT NULL DEFAULT 0"))
    if "daily_bonus_claims" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN daily_bonus_claims INTEGER NOT NULL DEFAULT 0"))
    if "last_special_order_at" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN last_special_order_at DATETIME"))
    if "daily_task_date" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN daily_task_date TEXT"))
    if "daily_task_state" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN daily_task_state JSON DEFAULT '{}'"))
    if "referrals_count" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN referrals_count INTEGER NOT NULL DEFAULT 0"))
    if "referred_by" not in user_columns:
        await session.execute(text("ALTER TABLE users ADD COLUMN referred_by INTEGER"))

    order_columns = await _existing_columns("orders")
    if "is_special" not in order_columns:
        await session.execute(text("ALTER TABLE orders ADD COLUMN is_special BOOLEAN NOT NULL DEFAULT 0"))

    user_order_columns = await _existing_columns("user_orders")
    if "is_special" not in user_order_columns:
        await session.execute(text("ALTER TABLE user_orders ADD COLUMN is_special BOOLEAN NOT NULL DEFAULT 0"))
    if "trend_applied" not in user_order_columns:
        await session.execute(text("ALTER TABLE user_orders ADD COLUMN trend_applied BOOLEAN NOT NULL DEFAULT 0"))
    if "trend_multiplier" not in user_order_columns:
        await session.execute(text("ALTER TABLE user_orders ADD COLUMN trend_multiplier FLOAT NOT NULL DEFAULT 1.0"))
    if "auto_progress_buffer" not in user_order_columns:
        await session.execute(
            text("ALTER TABLE user_orders ADD COLUMN auto_progress_buffer FLOAT NOT NULL DEFAULT 0.0")
        )

    order_columns = await _existing_columns("orders")
    if "reward_multiplier" not in order_columns:
        await session.execute(
            text("ALTER TABLE orders ADD COLUMN reward_multiplier FLOAT NOT NULL DEFAULT 1.0")
        )
    if "reward_preview" not in order_columns:
        await session.execute(
            text("ALTER TABLE orders ADD COLUMN reward_preview INTEGER NOT NULL DEFAULT 0")
        )
    if "difficulty" not in order_columns:
        await session.execute(
            text("ALTER TABLE orders ADD COLUMN difficulty TEXT NOT NULL DEFAULT 'normal'")
        )
    if "estimated_minutes" not in order_columns:
        await session.execute(
            text(
                "ALTER TABLE orders ADD COLUMN estimated_minutes INTEGER NOT NULL DEFAULT 30"
            )
        )
    if "rarity" not in order_columns:
        await session.execute(
            text("ALTER TABLE orders ADD COLUMN rarity TEXT NOT NULL DEFAULT 'common'")
        )
    if "appearance_weight" not in order_columns:
        await session.execute(
            text("ALTER TABLE orders ADD COLUMN appearance_weight FLOAT NOT NULL DEFAULT 0.0")
        )

    boost_columns = await _existing_columns("boosts")
    if "min_level" not in boost_columns:
        await session.execute(
            text("ALTER TABLE boosts ADD COLUMN min_level INTEGER NOT NULL DEFAULT 1")
        )

    item_columns = await _existing_columns("items")
    if "obtain" not in item_columns:
        await session.execute(text("ALTER TABLE items ADD COLUMN obtain TEXT"))

    team_columns = await _existing_columns("team_members")
    if "min_level" not in team_columns:
        await session.execute(text("ALTER TABLE team_members ADD COLUMN min_level INTEGER NOT NULL DEFAULT 1"))

    random_event_columns = await _existing_columns("random_events")
    if "interactive" not in random_event_columns:
        await session.execute(text("ALTER TABLE random_events ADD COLUMN interactive BOOLEAN NOT NULL DEFAULT 0"))

    tables = {
        row[0]
        for row in (await session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))).all()
    }
    if "global_state" not in tables:
        await session.execute(
            text(
                "CREATE TABLE IF NOT EXISTS global_state ("
                "key TEXT PRIMARY KEY, value JSON, updated_at DATETIME)"
            )
        )


# ----------------------------------------------------------------------------
# Сиды данных (встроенные)
# ----------------------------------------------------------------------------

SEED_ORDERS = [
    {
        "title": "Аватар для соцсетей",
        "base_clicks": 80,
        "min_level": 1,
        "difficulty": "easy",
        "estimated_minutes": 5,
    },
    {
        "title": "Визитка для фрилансера",
        "base_clicks": 100,
        "min_level": 1,
        "difficulty": "easy",
        "estimated_minutes": 7,
    },
    {
        "title": "Серия сторис для Instagram",
        "base_clicks": 200,
        "min_level": 1,
        "difficulty": "easy",
        "estimated_minutes": 10,
        "reward_multiplier": 1.05,
    },
    {
        "title": "Обложка для VK",
        "base_clicks": 180,
        "min_level": 1,
        "difficulty": "normal",
        "estimated_minutes": 12,
    },
    {
        "title": "Логотип для кафе",
        "base_clicks": 300,
        "min_level": 2,
        "difficulty": "normal",
        "estimated_minutes": 20,
        "reward_multiplier": 1.1,
    },
    {
        "title": "Презентация для стартапа",
        "base_clicks": 420,
        "min_level": 2,
        "difficulty": "normal",
        "estimated_minutes": 25,
        "reward_multiplier": 1.15,
    },
    {
        "title": "Пакет баннеров для рекламы",
        "base_clicks": 900,
        "min_level": 3,
        "difficulty": "normal",
        "estimated_minutes": 40,
        "reward_multiplier": 1.05,
    },
    {
        "title": "Лендинг (1 экран)",
        "base_clicks": 600,
        "min_level": 3,
        "difficulty": "normal",
        "estimated_minutes": 35,
    },
    {
        "title": "Контент-план для рассылки",
        "base_clicks": 1400,
        "min_level": 4,
        "difficulty": "normal",
        "estimated_minutes": 45,
        "reward_multiplier": 1.05,
    },
    {
        "title": "Редизайн логотипа",
        "base_clicks": 800,
        "min_level": 4,
        "difficulty": "hard",
        "estimated_minutes": 45,
        "reward_multiplier": 1.1,
    },
    {
        "title": "Новогодний мерч для подписчиков",
        "base_clicks": 1600,
        "min_level": 4,
        "difficulty": "normal",
        "estimated_minutes": 50,
        "reward_multiplier": 1.3,
        "rarity": "holiday",
        "appearance_weight": 0.15,
    },
    {
        "title": "Хэллоуинская промо-страница",
        "base_clicks": 1900,
        "min_level": 5,
        "difficulty": "hard",
        "estimated_minutes": 55,
        "reward_multiplier": 1.2,
        "rarity": "holiday",
        "appearance_weight": 0.12,
    },
    {
        "title": "Брендбук (мини)",
        "base_clicks": 1200,
        "min_level": 5,
        "difficulty": "hard",
        "estimated_minutes": 60,
    },
    {
        "title": "UX-аудит мобильного приложения",
        "base_clicks": 2200,
        "min_level": 6,
        "difficulty": "hard",
        "estimated_minutes": 75,
    },
    {
        "title": "Коллекционный NFT-дроп",
        "base_clicks": 2600,
        "min_level": 7,
        "difficulty": "hard",
        "estimated_minutes": 90,
        "reward_multiplier": 1.35,
        "rarity": "rare",
        "appearance_weight": 0.25,
    },
    {
        "title": "Редизайн приложения (ядро)",
        "base_clicks": 3000,
        "min_level": 8,
        "difficulty": "hard",
        "estimated_minutes": 110,
    },
    {
        "title": "Виртуальный шоурум в VR",
        "base_clicks": 4800,
        "min_level": 11,
        "difficulty": "expert",
        "estimated_minutes": 130,
        "reward_multiplier": 1.4,
        "rarity": "rare",
        "appearance_weight": 0.2,
    },
    {
        "title": "Брендинг для корпорации",
        "base_clicks": 4200,
        "min_level": 10,
        "difficulty": "expert",
        "estimated_minutes": 140,
        "reward_multiplier": 1.15,
    },
    {
        "title": "Сайт компании 5 экранов",
        "base_clicks": 5500,
        "min_level": 12,
        "difficulty": "expert",
        "estimated_minutes": 160,
    },
    {
        "title": "Международная кампания бренда",
        "base_clicks": 8000,
        "min_level": 15,
        "difficulty": "expert",
        "estimated_minutes": 210,
        "reward_multiplier": 1.2,
    },
    {
        "title": "Глобальный ребрендинг",
        "base_clicks": 12000,
        "min_level": 18,
        "difficulty": "expert",
        "estimated_minutes": 280,
        "reward_multiplier": 1.25,
    },
    {
        "title": "Особый заказ: Айдентика фестиваля",
        "base_clicks": 1800,
        "min_level": SPECIAL_ORDER_MIN_LEVEL,
        "is_special": True,
        "difficulty": "hard",
        "estimated_minutes": 90,
        "reward_multiplier": 1.8,
    },
]

SEED_BOOSTS = [
    {
        "code": "reward_mastery",
        "name": "🎯 Мастерство гонораров",
        "type": "reward",
        "base_cost": 320,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.15,
        "min_level": 1,
    },
    {
        "code": "passive_income_plus",
        "name": "🌱 Пассивный поток",
        "type": "passive",
        "base_cost": 420,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.12,
        "min_level": 1,
    },
    {
        "code": "accelerated_learning",
        "name": "📚 Спринт обучения",
        "type": "xp",
        "base_cost": 560,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.12,
        "min_level": 1,
    },
    {
        "code": "anti_brak",
        "name": "🧿 Контроль качества",
        "type": "event_protection",
        "base_cost": 760,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.12,
        "min_level": 3,
    },
    {
        "code": "project_insurance",
        "name": "🧯 Подушка безопасности",
        "type": "event_shield",
        "base_cost": 900,
        "growth": BOOST_COST_GROWTH,
        "step_value": 1,
        "min_level": 3,
    },
    {
        "code": "process_optimization",
        "name": "🎛️ Студия на автопилоте",
        "type": "passive",
        "base_cost": 760,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.08,
        "min_level": 3,
    },
    {
        "code": "team_synergy",
        "name": "👥 Синергия команды",
        "type": "team_income",
        "base_cost": 860,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.10,
        "min_level": 3,
    },
    {
        "code": "requirement_relief",
        "name": "🧭 Мягкие брифы",
        "type": "req_clicks",
        "base_cost": 980,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.04,
        "min_level": 5,
    },
    {
        "code": "quick_briefs",
        "name": "📦 Быстрый старт",
        "type": "free_order",
        "base_cost": 1040,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.05,
        "min_level": 5,
    },
    {
        "code": "contractor_discount",
        "name": "🧾 Лояльные подрядчики",
        "type": "team_discount",
        "base_cost": 1080,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.06,
        "min_level": 5,
    },
    {
        "code": "deep_offline",
        "name": "💤 Глубокий офлайн",
        "type": "offline_cap",
        "base_cost": 1140,
        "growth": BOOST_COST_GROWTH,
        "step_value": 10800,
        "min_level": 5,
    },
    {
        "code": "tight_deadlines",
        "name": "⏱️ Бонус за скорость",
        "type": "rush_reward",
        "base_cost": 1200,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.07,
        "min_level": 5,
    },
    {
        "code": "gear_tuning",
        "name": "🧰 Тюнинг студии",
        "type": "equipment_eff",
        "base_cost": 1280,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.06,
        "min_level": 5,
    },
    {
        "code": "night_flow",
        "name": "🌙 Ночной поток",
        "type": "night_passive",
        "base_cost": 1360,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.15,
        "min_level": 5,
    },
    {
        "code": "shop_wholesale",
        "name": "🛍️ Оптовые закупки",
        "type": "shop_discount",
        "base_cost": 1420,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.05,
        "min_level": 5,
    },
    {
        "code": "premium_projects",
        "name": "🎯 Премиум-проекты",
        "type": "high_order_reward",
        "base_cost": 1500,
        "growth": BOOST_COST_GROWTH,
        "step_value": 0.10,
        "min_level": 5,
    },
]

BOOST_EXTRA_META: Dict[str, Dict[str, Any]] = {
    "reward_mastery": {
        "flavor": "Каждый проект приносит больше — ты ловишь золотые инсайты.",
    },
    "passive_income_plus": {
        "flavor": "Команда работает без тебя — прогресс идёт непрерывно.",
    },
    "accelerated_learning": {
        "flavor": "Голова впитывает советы молниеносно — XP льётся рекой.",
    },
    "anti_brak": {
        "flavor": "Контроль качества как лазер — браку не пройти.",
    },
    "project_insurance": {
        "flavor": "Запасной план на месте — провалы не страшны.",
    },
    "process_optimization": {
        "flavor": "Процессы на автопилоте — задачи двигаются сами.",
    },
    "team_synergy": {
        "flavor": "Команда дышит в унисон — макеты летят вперёд.",
    },
    "requirement_relief": {
        "flavor": "Клиенты смягчили условия — меньше кликов до победы.",
    },
    "quick_briefs": {
        "flavor": "Заказы стартуют с фору — часть работы уже сделана.",
    },
    "contractor_discount": {
        "flavor": "Постоянные партнёры дают скидки — бюджет спасён.",
    },
    "deep_offline": {
        "flavor": "Даже во сне студия не стоит — заказы потихоньку закрываются.",
    },
    "tight_deadlines": {
        "flavor": "За скорость теперь платят больше — дедлайны в радость.",
    },
    "gear_tuning": {
        "flavor": "Каждый болтик подкручен — предметы раскрывают потенциал.",
    },
    "night_flow": {
        "flavor": "Ночные идеи превращаются в пассивный доход.",
    },
    "shop_wholesale": {
        "flavor": "Закупки оптом — цены тают на глазах.",
    },
    "premium_projects": {
        "flavor": "Премиальные заказы выстроились в очередь — чеки растут.",
    },
}

BOOST_PURCHASE_FEEDBACK: Dict[str, str] = {
    "reward": "💰 Награды увеличены — клиенты платят больше.",
    "passive": "🌱 Пассивный доход капает быстрее.",
    "xp": "🧠 Обучение ускорилось — опыт льётся рекой.",
    "event_protection": "🛡️ Клиентские факапы теперь менее страшны.",
    "event_shield": "🧯 Запас страховок пополнен — можно рисковать!",
    "team_income": "👥 Команда закрывает задачи ещё быстрее.",
    "req_clicks": "🧭 Брифы смягчены — заказ закрывается быстрее.",
    "free_order": "📦 Заказы стартуют с фору — экономия кликов.",
    "team_discount": "🧾 Закупки для команды стали дешевле.",
    "offline_cap": "💤 Копилка во сне стала глубже.",
    "rush_reward": "⏱️ Бонус за скорость вырос — работай в темпе.",
    "equipment_eff": "🧰 Экипировка раскрывает больше потенциала.",
    "night_passive": "🌙 Ночная смена приносит больше дохода.",
    "shop_discount": "🛍️ Скидки в магазине выросли — закупайся выгоднее.",
    "high_order_reward": "🎯 Премиальные заказы стали прибыльнее.",
}

SEED_TEAM = [
    {"code": "junior", "name": "Junior Designer", "base_income_per_min": 4, "base_cost": 100, "min_level": 2},
    {"code": "middle", "name": "Middle Designer", "base_income_per_min": 10, "base_cost": 300, "min_level": 3},
    {"code": "senior", "name": "Senior Designer", "base_income_per_min": 22, "base_cost": 800, "min_level": 4},
    {"code": "pm", "name": "Project Manager", "base_income_per_min": 35, "base_cost": 1200, "min_level": 5},
    {"code": "director", "name": "Creative Director", "base_income_per_min": 60, "base_cost": 2500, "min_level": 12},
]

SEED_ITEMS = [
    {"code": "phone_t1", "name": "Смартфон «City Lite»", "slot": "phone", "tier": 1, "bonus_type": "passive_pct", "bonus_value": 0.03, "price": 200, "min_level": 1},
    {"code": "phone_t2", "name": "Смартфон «Pulse Max»", "slot": "phone", "tier": 2, "bonus_type": "passive_pct", "bonus_value": 0.06, "price": 400, "min_level": 2},
    {"code": "phone_t3", "name": "Смартфон «Nova Edge»", "slot": "phone", "tier": 3, "bonus_type": "passive_pct", "bonus_value": 0.10, "price": 750, "min_level": 3},

    {"code": "tablet_t1", "name": "Планшет «TabFlow»", "slot": "tablet", "tier": 1, "bonus_type": "req_clicks_pct", "bonus_value": 0.02, "price": 300, "min_level": 1},
    {"code": "tablet_t2", "name": "Планшет «SketchWave»", "slot": "tablet", "tier": 2, "bonus_type": "req_clicks_pct", "bonus_value": 0.04, "price": 600, "min_level": 2},
    {"code": "tablet_t3", "name": "Планшет «FrameMaster»", "slot": "tablet", "tier": 3, "bonus_type": "req_clicks_pct", "bonus_value": 0.06, "price": 950, "min_level": 3},

    {"code": "monitor_t1", "name": "Монитор «PixelWide»", "slot": "monitor", "tier": 1, "bonus_type": "reward_pct", "bonus_value": 0.04, "price": 350, "min_level": 1},
    {"code": "monitor_t2", "name": "Монитор «VisionGrid»", "slot": "monitor", "tier": 2, "bonus_type": "reward_pct", "bonus_value": 0.08, "price": 700, "min_level": 2},
    {"code": "monitor_t3", "name": "Монитор «UltraCanvas»", "slot": "monitor", "tier": 3, "bonus_type": "reward_pct", "bonus_value": 0.12, "price": 1050, "min_level": 3},

    {"code": "client_contract", "name": "Талисман клиента", "slot": "charm", "tier": 1, "bonus_type": "req_clicks_pct", "bonus_value": 0.03, "price": 0, "min_level": 2},
    {
        "code": "talent_badge",
        "name": "Значок таланта",
        "slot": "charm",
        "tier": 1,
        "bonus_type": "reward_pct",
        "bonus_value": 0.02,
        "price": 0,
        "min_level": 1,
        "obtain": "achievement",
    },
    {
        "code": "poster_art",
        "name": "Арт-постер вдохновения",
        "slot": "charm",
        "tier": 2,
        "bonus_type": "reward_pct",
        "bonus_value": 0.03,
        "price": 900,
        "min_level": 8,
    },
    {
        "code": "art_director_trophy",
        "name": "Трофей арт-директора",
        "slot": "charm",
        "tier": 2,
        "bonus_type": "passive_pct",
        "bonus_value": 0.04,
        "price": 0,
        "min_level": 5,
        "obtain": "quest",
    },
    {
        "code": "desk_printer",
        "name": "Командный принтер",
        "slot": "charm",
        "tier": 3,
        "bonus_type": "passive_pct",
        "bonus_value": 0.05,
        "price": 1500,
        "min_level": 12,
    },
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
    {"code": "spill_choice", "title": "☕ Кофе пролился — что делать?", "kind": "penalty", "amount": 0, "duration_sec": None, "weight": 1, "min_level": 1, "interactive": True},
    {"code": "viral_post", "title": "📈 Вирусный пост! +10% к наградам на 10 мин.", "kind": "buff", "amount": 0.10, "duration_sec": 600, "weight": 3, "min_level": 3},
    {"code": "client_tip", "title": "🧾 Клиент оставил чаевые — +350₽.", "kind": "bonus", "amount": 350, "duration_sec": None, "weight": 2, "min_level": 2},
    {"code": "deadline_crunch", "title": "🔥 Горящий дедлайн! −10% к наградам на 5 мин.", "kind": "buff", "amount": -0.10, "duration_sec": 300, "weight": 2, "min_level": 4},
    {"code": "agency_feature", "title": "🎤 Про вас написали в блоге — +5% к пассивному доходу на 15 мин.", "kind": "buff", "amount": 0.05, "duration_sec": 900, "weight": 2, "min_level": 5},
    {"code": "software_crash", "title": "💥 Софт упал! −100 XP.", "kind": "penalty", "amount": 100, "duration_sec": None, "weight": 1, "min_level": 3},
    {"code": "mentor_call", "title": "📞 Ментор подсказал лайфхак — +150 XP.", "kind": "bonus", "amount": 150, "duration_sec": None, "weight": 2, "min_level": 2},
]

RANDOM_EVENT_EFFECTS = {
    "idea_spark": {"balance": 200},
    "coffee_spill": {"balance_pct": -0.05, "balance": -150},
    "viral_post": {"buff": {"reward_pct": 0.10}},
    "client_tip": {"balance": 350},
    "deadline_crunch": {"buff": {"reward_pct": -0.10}},
    "agency_feature": {"buff": {"passive_pct": 0.05}},
    "software_crash": {"xp_pct": -0.10, "xp": -100},
    "mentor_call": {"xp": 150},
    "spill_choice": {
        "interactive": [
            {"text": "−150 ₽", "effect": {"balance_pct": -0.05, "balance": -150}},
            {"text": "−50 XP", "effect": {"xp_pct": -0.05, "xp": -50}},
        ]
    },
}

SEED_SKILLS = [
    {"code": "web_master", "name": "Web-мастер", "branch": "web", "effect": {"reward_pct": 0.05}, "min_level": 5},
    {"code": "brand_evangelist", "name": "Бренд-евангелист", "branch": "brand", "effect": {"reward_pct": 0.03, "passive_pct": 0.02}, "min_level": 10},
    {"code": "art_director", "name": "Арт-директор", "branch": "art", "effect": {"passive_pct": 0.05}, "min_level": 5},
    {"code": "speed_runner", "name": "Спидранер", "branch": "web", "effect": {"req_clicks_pct": 0.03}, "min_level": 10},
    {"code": "team_leader", "name": "Лидер команды", "branch": "brand", "effect": {"passive_pct": 0.04}, "min_level": 15},
    {"code": "sales_guru", "name": "Sales-гуру", "branch": "brand", "effect": {"reward_pct": 0.06}, "min_level": 15},
    {"code": "brand_storyteller", "name": "Сторителлер", "branch": "brand", "effect": {"reward_pct": 0.04, "xp_pct": 0.05}, "min_level": 20},
]

CAMPAIGN_CHAPTERS = [
    {"chapter": 1, "title": "Первые заказы", "min_level": 1, "goal": {"orders_total": 3}, "reward": {"rub": 400, "xp": 150, "reward_pct": 0.01}},
    {"chapter": 2, "title": "Первые крупные клиенты", "min_level": 5, "goal": {"orders_min_level": {"count": 2, "min_level": 3}}, "reward": {"rub": 600, "xp": 250, "reward_pct": 0.01}},
    {"chapter": 3, "title": "Маленькая команда", "min_level": 10, "goal": {"team_level": {"members": 2, "level": 1}}, "reward": {"rub": 800, "xp": 350, "passive_pct": 0.02}},
    {"chapter": 4, "title": "Свой бренд", "min_level": 15, "goal": {"items_bought": 2}, "reward": {"rub": 1000, "xp": 500, "reward_pct": 0.015}},
]

QUEST_CODE_HELL_CLIENT = "hell_client"
QUEST_CODE_ART_DIRECTOR = "art_director"
QUEST_CODE_BRAND_SHOW = "brand_show"

QUEST_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    QUEST_CODE_HELL_CLIENT: {
        "name": "Клиент из ада",
        "min_level": 2,
        "payload_keys": ["mood", "budget", "respect", "speed"],
        "flow": {
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
        },
        "rewards": {
            "default": {"rub": 600, "xp": 300, "item_code": "client_contract", "item_template": "client_talisman"},
            "budget": {"rub": 800, "xp": 250, "item_code": "client_contract", "item_template": "client_talisman"},
            "mood": {"rub": 500, "xp": 320, "item_code": "client_contract", "item_template": "client_talisman"},
            "respect": {"rub": 550, "xp": 360, "item_code": "client_contract", "item_template": "client_talisman"},
            "speed": {"rub": 650, "xp": 280, "item_code": "client_contract", "item_template": "client_talisman"},
        },
    },
    QUEST_CODE_ART_DIRECTOR: {
        "name": "Путь арт-директора",
        "min_level": 5,
        "payload_keys": ["vision", "team", "budget"],
        "flow": {
            "intro": {
                "text": "К вам приходит крупный фестиваль. Нужно презентовать концепцию стендов.",
                "options": [
                    {"text": "Показать смелый мудборд", "next": "step1", "delta": {"vision": 1}},
                    {"text": "Начать с расчётов и KPI", "next": "step1", "delta": {"budget": 1}},
                    {"text": "Представить команду", "next": "step1", "delta": {"team": 1}},
                ],
            },
            "step1": {
                "text": "Жюри просит раскрыть детали подачи. Чем удивим?",
                "options": [
                    {"text": "Живой перформанс иллюстратора", "next": "step2", "delta": {"vision": 1}},
                    {"text": "Совместный воркшоп с клиентом", "next": "step2", "delta": {"team": 1}},
                    {"text": "Разложить экономию бюджета", "next": "step2", "delta": {"budget": 1}},
                ],
            },
            "step2": {
                "text": "Финальный созвон: клиент сомневается. Как закрепить решение?",
                "options": [
                    {"text": "Отстоять идею фактами", "next": "finale", "delta": {"vision": 1}},
                    {"text": "Поддержать команду и распределить задачи", "next": "finale", "delta": {"team": 1}},
                    {"text": "Пересобрать смету", "next": "finale", "delta": {"budget": 1}},
                ],
            },
        },
        "rewards": {
            "default": {"rub": 900, "xp": 420, "item_code": "art_director_trophy", "item_template": "trophy"},
            "vision": {"rub": 1020, "xp": 460, "item_code": "art_director_trophy", "item_template": "trophy"},
            "team": {"rub": 940, "xp": 480, "item_code": "art_director_trophy", "item_template": "trophy"},
            "budget": {"rub": 1100, "xp": 390, "item_code": "art_director_trophy", "item_template": "trophy"},
        },
    },
    QUEST_CODE_BRAND_SHOW: {
        "name": "Шоу бренда",
        "min_level": 10,
        "payload_keys": ["network", "creativity", "discipline"],
        "flow": {
            "intro": {
                "text": "Вы запускаете авторское шоу о дизайне. С чего начнём?",
                "options": [
                    {"text": "Позвать громкого гостя", "next": "step1", "delta": {"network": 1}},
                    {"text": "Сделать необычную заставку", "next": "step1", "delta": {"creativity": 1}},
                    {"text": "Прописать план выпусков", "next": "step1", "delta": {"discipline": 1}},
                ],
            },
            "step1": {
                "text": "Первый эфир близко. Что усилим?",
                "options": [
                    {"text": "Интерактив с аудиторией", "next": "step2", "delta": {"network": 1}},
                    {"text": "Экспериментальный формат", "next": "step2", "delta": {"creativity": 1}},
                    {"text": "Чёткий чек-лист задач", "next": "step2", "delta": {"discipline": 1}},
                ],
            },
            "step2": {
                "text": "Финальный выпуск решит судьбу шоу. Ваш ход?",
                "options": [
                    {"text": "Сделать совместный выпуск с лидером мнений", "next": "finale", "delta": {"network": 1}},
                    {"text": "Добавить интерактивное арт-соревнование", "next": "finale", "delta": {"creativity": 1}},
                    {"text": "Строго держать тайминг и сценарий", "next": "finale", "delta": {"discipline": 1}},
                ],
            },
        },
        "rewards": {
            "default": {"rub": 1200, "xp": 520, "item_code": "talent_badge", "item_template": "trophy"},
            "network": {"rub": 1300, "xp": 540, "item_code": "talent_badge", "item_template": "trophy"},
            "creativity": {"rub": 1180, "xp": 580, "item_code": "talent_badge", "item_template": "trophy"},
            "discipline": {"rub": 1250, "xp": 560, "item_code": "talent_badge", "item_template": "trophy"},
        },
    },
}


async def seed_if_needed(session: AsyncSession) -> None:
    """Идемпотентная загрузка сидов при первом старте."""
    # Заказы
    existing_orders = {
        order.title: order for order in (await session.execute(select(Order))).scalars()
    }
    for d in SEED_ORDERS:
        base_clicks = d["base_clicks"]
        min_level = d["min_level"]
        reward_mul = float(d.get("reward_multiplier", 1.0))
        # Обновлено: автоматически рассчитываем предпросмотр награды.
        preview = int(
            d.get(
                "reward_preview",
                base_reward_from_required(required_clicks(base_clicks, min_level), reward_mul),
            )
        )
        payload = {
            "title": d["title"],
            "base_clicks": base_clicks,
            "min_level": min_level,
            "is_special": d.get("is_special", False),
            "reward_multiplier": reward_mul,
            "reward_preview": preview,
            "difficulty": d.get("difficulty", "normal"),
            "estimated_minutes": int(d.get("estimated_minutes", 30)),
            "rarity": d.get("rarity", "common"),
            "appearance_weight": float(d.get("appearance_weight", 0.0)),
        }
        order = existing_orders.get(d["title"])
        if not order:
            session.add(Order(**payload))
        else:
            for key, value in payload.items():
                setattr(order, key, value)
    # Усиления
    existing_boosts = {
        boost.code: boost for boost in (await session.execute(select(Boost))).scalars()
    }
    seed_codes = {d["code"] for d in SEED_BOOSTS}
    for d in SEED_BOOSTS:
        boost = existing_boosts.get(d["code"])
        if not boost:
            session.add(
                Boost(
                    code=d["code"],
                    name=d["name"],
                    type=d["type"],
                    base_cost=d["base_cost"],
                    growth=d["growth"],
                    step_value=d["step_value"],
                    min_level=d.get("min_level", 1),
                )
            )
        else:
            boost.name = d["name"]
            boost.type = d["type"]
            boost.base_cost = d["base_cost"]
            boost.growth = d["growth"]
            boost.step_value = d["step_value"]
            boost.min_level = d.get("min_level", boost.min_level or 1)
    removed_boost_codes = {
        "finger_training",
        "click_overdrive",
        "coffee_break",
        "motivation",
        "focus_playlist",
        "new_devices",
        "software_upgrade",
        "graphic_tablet_pro",
        "designer_team",
    }
    for obsolete_code in removed_boost_codes:
        if obsolete_code in existing_boosts and obsolete_code not in seed_codes:
            await session.execute(delete(Boost).where(Boost.code == obsolete_code))
    # Команда
    team_existing = {
        code: member
        for member, code in (
            await session.execute(select(TeamMember, TeamMember.code))
        ).all()
    }
    for d in SEED_TEAM:
        member = team_existing.get(d["code"])
        if not member:
            session.add(
                TeamMember(
                    code=d["code"],
                    name=d["name"],
                    base_income_per_min=d["base_income_per_min"],
                    base_cost=d["base_cost"],
                    min_level=d.get("min_level", 1),
                )
            )
        else:
            member.min_level = d.get("min_level", member.min_level)
    # Предметы
    existing_items = {
        code: iid for code, iid in (await session.execute(select(Item.code, Item.id))).all()
    }
    for d in SEED_ITEMS:
        if d["code"] not in existing_items:
            session.add(
                Item(
                    code=d["code"],
                    name=d["name"],
                    slot=d["slot"],
                    tier=d["tier"],
                    bonus_type=d["bonus_type"],
                    bonus_value=d["bonus_value"],
                    price=d["price"],
                    min_level=d["min_level"],
                    obtain=d.get("obtain"),
                )
            )
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
    existing_events = {
        code: event
        for event, code in (
            await session.execute(select(RandomEvent, RandomEvent.code))
        ).all()
    }
    for d in SEED_RANDOM_EVENTS:
        event = existing_events.get(d["code"])
        if not event:
            session.add(
                RandomEvent(
                    code=d["code"],
                    title=d["title"],
                    kind=d["kind"],
                    amount=d["amount"],
                    duration_sec=d["duration_sec"],
                    weight=d["weight"],
                    min_level=d["min_level"],
                    interactive=bool(d.get("interactive", False)),
                )
            )
        else:
            if event.interactive != bool(d.get("interactive", False)):
                event.interactive = bool(d.get("interactive", False))
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


TREND_STATE_KEY = "trend_order"


async def set_trend(
    session: AsyncSession,
    order_id: int,
    valid_until: datetime,
    reward_mul: float = TREND_REWARD_MUL,
) -> None:
    state = await session.scalar(select(GlobalState).where(GlobalState.key == TREND_STATE_KEY))
    payload = {
        "order_id": order_id,
        "valid_until": valid_until.replace(microsecond=0).isoformat(),
        "reward_mul": reward_mul,
    }
    now = utcnow()
    if state:
        state.value = payload
        state.updated_at = now
    else:
        session.add(GlobalState(key=TREND_STATE_KEY, value=payload, updated_at=now))


async def get_trend(session: AsyncSession) -> Optional[dict]:
    state = await session.scalar(select(GlobalState).where(GlobalState.key == TREND_STATE_KEY))
    if not state or not state.value:
        return None
    value = dict(state.value)
    raw_until = value.get("valid_until")
    try:
        valid_until = datetime.fromisoformat(raw_until) if raw_until else None
    except ValueError:
        valid_until = None
    if not valid_until or ensure_naive(valid_until) <= utcnow():
        await session.delete(state)
        return None
    return {
        "order_id": int(value.get("order_id", 0)),
        "valid_until": ensure_naive(valid_until),
        "reward_mul": float(value.get("reward_mul", TREND_REWARD_MUL)),
    }


async def roll_new_trend(
    session: AsyncSession, user_level_hint: Optional[int] = None
) -> dict:
    level_cap = max(1, user_level_hint or 1)
    orders = (
        await session.execute(
            select(Order)
            .where(Order.is_special.is_(False), Order.min_level <= level_cap)
            .order_by(Order.id)
        )
    ).scalars().all()
    if not orders:
        orders = (
            await session.execute(select(Order).where(Order.is_special.is_(False)))
        ).scalars().all()
    if not orders:
        raise RuntimeError("No orders available to roll trend")
    current = await get_trend(session)
    candidates = [o for o in orders if not current or o.id != current.get("order_id")]
    if not candidates:
        candidates = orders
    order = random.choice(candidates)
    valid_until = utcnow() + timedelta(hours=TREND_DURATION_HOURS)
    reward_mul = TREND_REWARD_MUL
    await set_trend(session, order.id, valid_until, reward_mul)
    logger.info(
        "Trend rolled",
        extra={
            "order_id": order.id,
            "valid_until": valid_until.isoformat(),
            "reward_mul": reward_mul,
        },
    )
    return {"order_id": order.id, "valid_until": valid_until, "reward_mul": reward_mul}


# ----------------------------------------------------------------------------
# Экономика: формулы и сервисы
# ----------------------------------------------------------------------------

def xp_to_level(n: int) -> int:
    return 100 * n * n


def upgrade_cost(base: int, growth: float, n: int) -> int:
    """Unified exponential cost progression for boost upgrades."""

    level_index = max(0, n - 1)
    return int(round(base * (BOOST_COST_GROWTH ** level_index)))


def required_clicks(base_clicks: int, level: int) -> int:
    return int(round(base_clicks * (1 + 0.15 * floor(level / 5))))


def base_reward_from_required(req: int, reward_mul: float = 1.0) -> int:
    return int(round(req * 0.6 * reward_mul))


async def calc_total_earned(session: AsyncSession, user: User) -> float:
    earning_types = {"order_finish", "quest_reward", "campaign_reward"}
    stmt = (
        select(
            func.coalesce(
                func.sum(
                    case(
                        (
                            and_(
                                EconomyLog.type.in_(earning_types),
                                EconomyLog.amount > 0,
                            ),
                            EconomyLog.amount,
                        ),
                        else_=0.0,
                    )
                ),
                0.0,
            )
        )
        .where(EconomyLog.user_id == user.id)
    )
    return float((await session.execute(stmt)).scalar_one())


async def calc_prestige_gain(
    session: AsyncSession, user: User, *, total_earned: Optional[float] = None
) -> int:
    total = total_earned if total_earned is not None else await calc_total_earned(session, user)
    if total <= 0:
        return 0
    return int(floor(sqrt(total / PRESTIGE_GAIN_DIVISOR)))


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
    reward_add = 0.0
    passive_add = 0.0
    xp_pct = 0.0
    req_clicks_pct_boost = 0.0
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
    event_shield_charges = 0
    for _code, btype, lvl, step in rows:
        if lvl <= 0 or step == 0:
            continue
        value = lvl * step
        if btype == "reward":
            reward_add += value
        elif btype == "passive":
            passive_add += value
        elif btype == "xp":
            xp_pct += value
        elif btype == "event_protection":
            negative_event_reduction += value
        elif btype == "team_income":
            team_income_pct += value
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
        elif btype == "event_shield":
            event_shield_charges += int(lvl)

    equipment_multiplier = 1.0 + equipment_eff_pct
    items = (
        await session.execute(
            select(Item.bonus_type, Item.bonus_value)
            .join(UserEquipment, UserEquipment.item_id == Item.id)
            .where(UserEquipment.user_id == user.id, UserEquipment.item_id.is_not(None))
        )
    ).all()
    passive_pct = 0.0
    req_clicks_pct = 0.0
    reward_pct = 0.0
    for btype, val in items:
        boosted_val = val * equipment_multiplier
        if btype == "passive_pct":
            passive_pct += boosted_val
        elif btype == "req_clicks_pct":
            req_clicks_pct += boosted_val
        elif btype == "reward_pct":
            reward_pct += boosted_val

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
        reward_pct += payload.get("reward_pct", 0.0)
        passive_pct += payload.get("passive_pct", 0.0)
        req_clicks_pct += payload.get("req_clicks_pct", 0.0)
        xp_pct += payload.get("xp_pct", 0.0)
        free_order_chance += payload.get("free_order_chance", 0.0)
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

    negative_event_weight_mul = max(
        0.0,
        min(1.0, 1.0 - min(NEGATIVE_EVENT_REDUCTION_CAP, max(0.0, negative_event_reduction))),
    )
    req_clicks_pct_total = req_clicks_pct + min(REQ_CLICKS_REDUCTION_CAP, max(0.0, req_clicks_pct_boost))
    req_clicks_pct_total = max(0.0, min(0.95, req_clicks_pct_total))
    team_discount_pct = max(0.0, min(TEAM_DISCOUNT_CAP, team_discount_pct))
    shop_discount_pct = max(0.0, min(SHOP_DISCOUNT_CAP, shop_discount_pct))
    free_order_chance = max(0.0, min(0.95, free_order_chance))

    reward_mul_total = 1.0 + user.reward_mul + reward_add + reward_pct
    passive_mul_total = 1.0 + user.passive_mul + passive_add + passive_pct
    return {
        "cp": 1,
        "reward_mul_total": max(0.0, reward_mul_total),
        "passive_mul_total": max(0.0, passive_mul_total),
        "req_clicks_pct": req_clicks_pct_total,
        "xp_pct": max(0.0, xp_pct),
        "prestige_pct": prestige_pct,
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
        "event_shield_charges": max(0, event_shield_charges),
    }


def team_income_per_min(base_per_min: float, level: int) -> float:
    """Calculate per-minute contribution from a team member for the given level."""

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
            select(UserPassiveSource.source_code, UserPassiveSource.level)
            .where(UserPassiveSource.user_id == user.id)
            .order_by(UserPassiveSource.id)
        )
    ).all()
    per_min = 0.0
    for code, level in rows:
        source = PASSIVE_SOURCE_BY_CODE.get(code)
        if not source:
            continue
        base_income = float(source.get("income_per_min", 0))
        lvl = max(1, level or 1)
        per_min += base_income * lvl
    passive_mul_total = stats.get("passive_mul_total", 1.0)
    rate = (per_min / 60.0) * passive_mul_total
    night_bonus = stats.get("night_passive_pct", 0.0)
    if night_bonus > 0 and is_night_now():
        rate *= 1.0 + night_bonus
    return rate


async def calc_team_progress_rate(session: AsyncSession, user: User, stats: Dict[str, Any]) -> float:
    """Return automated order progress per second based on team performance."""

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


async def apply_team_progress(
    session: AsyncSession,
    user: User,
    stats: Dict[str, Any],
    delta_seconds: float,
    achievements: List[Tuple[Achievement, UserAchievement]],
) -> Tuple[int, Optional[OrderCompletionResult]]:
    """Apply automated team progress for the active order."""

    if delta_seconds <= 0:
        return 0, None
    active = await get_active_order(session, user)
    if not active:
        return 0, None
    rate = await calc_team_progress_rate(session, user, stats)
    if rate <= 0:
        return 0, None
    buffer = getattr(active, "auto_progress_buffer", 0.0)
    total_progress = rate * delta_seconds + buffer
    gained = int(total_progress)
    active.auto_progress_buffer = total_progress - gained
    if gained <= 0:
        return 0, None
    active.progress_clicks = min(active.required_clicks, active.progress_clicks + gained)
    completion: Optional[OrderCompletionResult] = None
    if active.progress_clicks >= active.required_clicks:
        completion = await apply_order_completion(
            session,
            user,
            active,
            stats,
            achievements,
            trigger_events=False,
        )
    return gained, completion


async def process_offline_income(
    session: AsyncSession,
    user: User,
    achievements: List[Tuple[Achievement, UserAchievement]],
    *,
    message: Optional[Message] = None,
    state: Optional[FSMContext] = None,
) -> IdleIncomeResult:
    """Apply passive income and automated progress accumulated since the last action."""

    now = utcnow()
    last_seen = ensure_naive(user.last_seen) or now
    delta_raw = max(0.0, (now - last_seen).total_seconds())
    stats = await get_user_stats(session, user)
    offline_cap = MAX_OFFLINE_SECONDS + stats.get("offline_cap_bonus", 0.0)
    delta = min(delta_raw, offline_cap)
    progress_gain = 0
    completion: Optional[OrderCompletionResult] = None
    if delta > 0:
        progress_gain, completion = await apply_team_progress(
            session, user, stats, delta, achievements
        )
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
        achievements.extend(
            await evaluate_achievements(session, user, {"passive_income", "balance"})
        )
    return IdleIncomeResult(
        passive_gain=amount,
        progress_gain=progress_gain,
        order_completion=completion,
    )


async def handle_idle_completion(
    message: Optional[Message],
    session: AsyncSession,
    user: User,
    state: Optional[FSMContext],
    idle_result: IdleIncomeResult,
) -> None:
    """Notify the user about automated order completion if it occurred."""

    completion = idle_result.order_completion
    if not completion or message is None:
        return
    order = completion.order
    title = order.title if order else "заказ"
    summary_lines = [
        RU.TEAM_ORDER_DONE.format(title=title, rub=completion.reward, xp=completion.xp_gain)
    ]
    badges: List[str] = []
    if completion.trend_applied:
        badges.append(RU.TREND_BADGE)
    if completion.is_special:
        badges.append("⭐ спец")
    if completion.rush_applied:
        badges.append("⏱️ быстро")
    if completion.high_bonus_pct > 0:
        badges.append("🎯 премия")
    summary_line = f"🧾 Заказов всего: {user.orders_completed}"
    if badges:
        summary_line = f"{summary_line} · {' '.join(badges)}"
    summary_lines.append(summary_line)
    await message.answer("\n".join(summary_lines))
    await daily_task_on_event(message, session, user, "daily_orders")
    await maybe_prompt_skill_choice(
        session, message, state, user, completion.prev_level, completion.levels_gained
    )
    if completion.levels_gained:
        await notify_level_up_message(
            message, session, user, completion.prev_level, completion.levels_gained
        )


def snapshot_required_clicks(order: Order, user_level: int, req_clicks_pct: float) -> int:
    """Calculate the effective clicks required for an order based on user stats."""

    base_req = required_clicks(order.base_clicks, user_level)
    reduced = int(round(base_req * (1 - req_clicks_pct)))
    return max(1, reduced)


def finish_order_reward(required_clicks_snapshot: int, reward_snapshot_mul: float) -> int:
    """Return reward amount for completed order based on snapshot multipliers."""

    mul = max(1.0, reward_snapshot_mul)
    return base_reward_from_required(required_clicks_snapshot, mul)


async def apply_order_completion(
    session: AsyncSession,
    user: User,
    active: UserOrder,
    stats: Dict[str, Any],
    achievements: List[Tuple[Achievement, UserAchievement]],
    *,
    trigger_events: bool,
) -> OrderCompletionResult:
    """Finalize an order, applying rewards and returning summary data."""

    order_entity = await session.scalar(select(Order).where(Order.id == active.order_id))
    reward = finish_order_reward(active.required_clicks, active.reward_snapshot_mul)
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
    active.progress_clicks = active.required_clicks
    if hasattr(active, "auto_progress_buffer"):
        active.auto_progress_buffer = 0.0
    reward_meta: Dict[str, Any] = {"order_id": active.order_id}
    if high_bonus_pct > 0:
        reward_meta["high_order_bonus"] = round(high_bonus_pct, 4)
    if rush_applied:
        reward_meta["rush_bonus"] = round(rush_bonus_pct, 4)
    if active.is_special:
        reward_meta["special"] = True
    if getattr(active, "trend_applied", False):
        reward_meta["trend"] = True
        reward_meta["trend_mul"] = round(getattr(active, "trend_multiplier", 1.0), 4)
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
            "trend_mul": getattr(active, "trend_multiplier", 1.0) if getattr(active, "trend_applied", False) else None,
        },
    )
    await update_campaign_progress(
        session,
        user,
        "order_finish",
        {"order_min_level": order_entity.min_level if order_entity else 0},
    )
    achievements.extend(await evaluate_achievements(session, user, {"orders", "level", "balance"}))
    event_payload: Optional[Tuple[str, Optional[InlineKeyboardMarkup]]] = None
    if trigger_events:
        event_payload = await trigger_random_event(
            session, user, "order_finish", RANDOM_EVENT_ORDER_PROB, stats
        )
    return OrderCompletionResult(
        order=order_entity,
        reward=reward,
        xp_gain=xp_gain,
        prev_level=prev_level,
        levels_gained=levels_gained,
        high_bonus_pct=high_bonus_pct,
        rush_applied=rush_applied,
        rush_bonus_pct=rush_bonus_pct,
        trend_applied=getattr(active, "trend_applied", False),
        trend_multiplier=getattr(active, "trend_multiplier", 1.0),
        is_special=active.is_special,
        event_payload=event_payload,
    )


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


PENDING_EVENT_PREFIX = "pending_event_"
EVENT_SHIELD_CODE = "project_insurance"


async def has_pending_interactive_event(session: AsyncSession, user: User) -> bool:
    stmt = (
        select(func.count())
        .select_from(UserBuff)
        .where(
            UserBuff.user_id == user.id,
            UserBuff.code.like(f"{PENDING_EVENT_PREFIX}%"),
        )
    )
    return (await session.execute(stmt)).scalar_one() > 0


async def get_pending_event_buff(
    session: AsyncSession, user: User, event_code: str
) -> Optional[UserBuff]:
    code = f"{PENDING_EVENT_PREFIX}{event_code}"
    return await session.scalar(
        select(UserBuff).where(UserBuff.user_id == user.id, UserBuff.code == code)
    )


def is_negative_event(event: RandomEvent) -> bool:
    """Heuristic to classify events with penalties."""

    effect = RANDOM_EVENT_EFFECTS.get(event.code, {})
    if not effect:
        return event.kind == "penalty"
    if effect.get("balance", 0) < 0 or effect.get("balance_pct", 0) < 0:
        return True
    if effect.get("xp", 0) < 0 or effect.get("xp_pct", 0) < 0:
        return True
    buff = effect.get("buff")
    if isinstance(buff, dict) and any(val < 0 for val in buff.values()):
        return True
    interactive = effect.get("interactive")
    if isinstance(interactive, list):
        for choice in interactive:
            choice_effect = choice.get("effect", {})
            if choice_effect.get("balance", 0) < 0 or choice_effect.get("balance_pct", 0) < 0:
                return True
            if choice_effect.get("xp", 0) < 0 or choice_effect.get("xp_pct", 0) < 0:
                return True
    if event.kind == "penalty":
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


async def apply_event_effect(
    session: AsyncSession,
    user: User,
    event: RandomEvent,
    effect: Dict[str, Any],
    trigger: str,
) -> str:
    now = utcnow()
    message = event.title
    balance_delta = 0
    xp_delta = 0
    meta_base: Dict[str, Any] = {"event": event.code, "trigger": trigger}
    if "balance_pct" in effect:
        pct = float(effect["balance_pct"])
        delta_pct = int(floor(user.balance * pct))
        if pct < 0 and delta_pct == 0 and user.balance > 0:
            delta_pct = -1
        elif pct > 0 and delta_pct == 0 and user.balance > 0:
            delta_pct = 1
        balance_delta += delta_pct
        meta_base["balance_pct"] = pct
    if "balance" in effect:
        balance_delta += int(effect["balance"])
    if balance_delta != 0:
        new_balance = user.balance + balance_delta
        if balance_delta < 0:
            new_balance = max(0, new_balance)
        user.balance = new_balance
        balance_meta = {**meta_base, "balance": balance_delta}
        log_type = "event_bonus" if balance_delta >= 0 else "event_penalty"
        session.add(
            EconomyLog(
                user_id=user.id,
                type=log_type,
                amount=balance_delta,
                meta=balance_meta,
                created_at=now,
            )
        )
    if "xp_pct" in effect:
        pct = float(effect["xp_pct"])
        base = xp_to_level(user.level)
        delta_pct = int(floor(base * pct))
        if pct < 0 and delta_pct == 0 and user.xp > 0:
            delta_pct = -1
        elif pct > 0 and delta_pct == 0:
            delta_pct = 1
        xp_delta += delta_pct
        meta_base["xp_pct"] = pct
    if "xp" in effect:
        xp_delta += int(effect["xp"])
    levels_gained = 0
    if xp_delta != 0:
        if xp_delta >= 0:
            levels_gained = await add_xp_and_levelup(user, xp_delta)
        else:
            user.xp = max(0, user.xp + xp_delta)
        xp_meta = {**meta_base, "xp": xp_delta}
        log_type = "event_bonus" if xp_delta >= 0 else "event_penalty"
        session.add(
            EconomyLog(
                user_id=user.id,
                type=log_type,
                amount=0.0,
                meta=xp_meta,
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
                meta={**meta_base, "buff": payload, "duration": duration},
                created_at=now,
            )
        )
        message = "\n".join(
            [
                RU.EVENT_BUFF.format(title=event.title),
                RU.EVENT_BUFF_ACTIVE.format(title=event.title, expires=expires.strftime("%H:%M")),
            ]
        )
    elif balance_delta > 0 or xp_delta > 0:
        message = RU.EVENT_POSITIVE.format(title=event.title)
    elif balance_delta < 0 or xp_delta < 0:
        message = RU.EVENT_NEGATIVE.format(title=event.title)
    if levels_gained > 0:
        prestige = await get_prestige_entry(session, user)
        reputation = prestige.reputation if prestige else 0
        rank = rank_for(user.level, reputation)
        message = f"{message}\n{RU.LEVEL_UP.format(lvl=user.level, rank=rank)}"
    return message


async def apply_random_event(session: AsyncSession, user: User, event: RandomEvent, trigger: str) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    """Apply selected random event to the user and return announcement text."""

    effect = RANDOM_EVENT_EFFECTS.get(event.code, {})
    if not effect:
        return event.title, None
    if is_negative_event(event):
        shield_entry = await get_user_boost_by_code(session, user, EVENT_SHIELD_CODE)
        if shield_entry and shield_entry.level > 0:
            shield_entry.level -= 1
            user.updated_at = utcnow()
            logger.info(
                "Event shield used",
                extra={"tg_id": user.tg_id, "user_id": user.id, "event": event.code, "shield_left": shield_entry.level},
            )
            return RU.EVENT_SHIELD_BLOCK, None
    interactive = effect.get("interactive")
    if event.interactive and isinstance(interactive, list):
        await session.execute(
            delete(UserBuff).where(
                UserBuff.user_id == user.id,
                UserBuff.code == f"{PENDING_EVENT_PREFIX}{event.code}",
            )
        )
        expires = utcnow() + timedelta(hours=12)
        session.add(
            UserBuff(
                user_id=user.id,
                code=f"{PENDING_EVENT_PREFIX}{event.code}",
                title=event.title,
                expires_at=expires,
                payload={"event": event.code, "trigger": trigger, "options": interactive},
            )
        )
        logger.info(
            "Interactive event pending",
            extra={"tg_id": user.tg_id, "user_id": user.id, "event": event.code, "options": len(interactive)},
        )
        return event.title, kb_event_choice(event.code, interactive)
    message = await apply_event_effect(session, user, event, effect, trigger)
    return message, None


async def trigger_random_event(
    session: AsyncSession,
    user: User,
    trigger: str,
    probability: float,
    stats: Optional[Dict[str, Any]] = None,
) -> Optional[Tuple[str, Optional[InlineKeyboardMarkup]]]:
    """Roll random event with probability and return announcement if triggered."""

    if random.random() > probability:
        return None
    if await has_pending_interactive_event(session, user):
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
        if key in {"reward_pct", "passive_pct", "req_clicks_pct", "xp_pct", "balance_pct"}:
            parts.append(f"{key.replace('_', ' ')} {int(value * 100)}%")
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


async def maybe_send_trend_hint(message: Message, session: AsyncSession, user: User) -> None:
    trend = await get_trend(session)
    if not trend:
        return
    order = await session.scalar(select(Order).where(Order.id == trend.get("order_id")))
    if not order:
        return
    payload = ensure_tutorial_payload(user)
    today_key = utcnow().date().isoformat()
    if payload.get("trend_hint_date") == today_key:
        return
    payload["trend_hint_date"] = today_key
    user.updated_at = utcnow()
    mul_text = format_stat(float(trend.get("reward_mul", TREND_REWARD_MUL)))
    await message.answer(RU.SPECIAL_ORDER_HINT.format(title=order.title, mul=mul_text))


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


def quest_get_stage_payload(quest: UserQuest, definition: Dict[str, Any]) -> Dict[str, int]:
    payload = quest.payload or {}
    for key in definition.get("payload_keys", []):
        payload.setdefault(key, 0)
    quest.payload = payload
    return payload


def quest_choose_reward_key(payload: Dict[str, int], definition: Dict[str, Any]) -> str:
    best_key = "default"
    best_value = -999
    keys = definition.get("payload_keys", [])
    for key in keys:
        if payload.get(key, 0) > best_value:
            best_value = payload.get(key, 0)
            best_key = key
    return best_key if best_value > 0 else "default"


def quest_stage_keys(definition: Dict[str, Any]) -> List[str]:
    """Return ordered list of stage keys for quest flow."""

    return list(definition.get("flow", {}).keys())


def quest_current_stage_key(quest: UserQuest, definition: Dict[str, Any]) -> Optional[str]:
    """Resolve current stage key based on quest.progress counter."""

    keys = quest_stage_keys(definition)
    if 0 <= quest.stage < len(keys):
        return keys[quest.stage]
    return None


def quest_stage_index(definition: Dict[str, Any], stage_key: str) -> Optional[int]:
    """Return index of stage key within quest flow."""

    keys = quest_stage_keys(definition)
    try:
        return keys.index(stage_key)
    except ValueError:
        return None


async def finalize_quest(
    session: AsyncSession,
    user: User,
    quest: UserQuest,
    message: Message,
    state: FSMContext,
    definition: Dict[str, Any],
) -> None:
    payload = quest_get_stage_payload(quest, definition)
    reward_key = quest_choose_reward_key(payload, definition)
    rewards = definition.get("rewards", {})
    reward_data = rewards.get(reward_key, rewards.get("default", {}))
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
    if levels_gained:
        await notify_level_up_message(message, session, user, prev_level, levels_gained)
    reward_item = reward_data.get("item_code")
    if reward_item:
        item = await session.scalar(select(Item).where(Item.code == reward_item))
        if item:
            has_item = await session.scalar(
                select(UserItem).where(UserItem.user_id == user.id, UserItem.item_id == item.id)
            )
            if not has_item:
                session.add(UserItem(user_id=user.id, item_id=item.id))
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="quest_reward",
                    amount=0.0,
                    meta={"quest": quest.quest_code, "item": item.code},
                    created_at=now,
                )
            )
            template = reward_data.get("item_template", "trophy")
            if template == "client_talisman":
                await message.answer(RU.QUEST_ITEM_GAIN.format(pct=int(item.bonus_value * 100)))
            else:
                await message.answer(
                    RU.QUEST_TROPHY_GAIN.format(
                        name=item.name, effect=_format_item_effect(item)
                    )
                )


async def send_quest_step(message: Message, quest_code: str, stage_key: str) -> None:
    definition = QUEST_DEFINITIONS.get(quest_code)
    if not definition:
        return
    step = definition.get("flow", {}).get(stage_key)
    if not step:
        return
    options = [opt["text"] for opt in step.get("options", [])]
    await message.answer(
        RU.QUEST_STEP.format(text=step["text"]),
        reply_markup=kb_quest_options(options),
    )


async def present_quest_selection(
    message: Message, state: FSMContext, session: AsyncSession, user: User
) -> bool:
    """Show quest selection menu and return True if options were presented."""

    available: List[Tuple[str, Dict[str, Any]]] = []
    unlocked_any = False
    min_required: Optional[int] = None
    for code, definition in QUEST_DEFINITIONS.items():
        min_level = int(definition.get("min_level", 1))
        if user.level < min_level:
            if min_required is None or min_level < min_required:
                min_required = min_level
            continue
        unlocked_any = True
        quest = await get_or_create_quest(session, user, code)
        if quest.is_done:
            continue
        available.append((code, definition))
    if available:
        options = [definition.get("name", code) for code, definition in available]
        mapping = {definition.get("name", code): code for code, definition in available}
        await state.set_state(QuestState.selecting)
        await state.update_data(quest_choices=mapping, active_quest=None)
        await message.answer(RU.QUEST_SELECT, reply_markup=kb_quest_options(options))
        return True
    fallback_lvl = min(
        (int(defn.get("min_level", 1)) for defn in QUEST_DEFINITIONS.values()),
        default=2,
    )
    markup = await main_menu_for_message(message, session=session, user=user)
    if not unlocked_any:
        lvl = min_required or fallback_lvl or 2
        await message.answer(RU.QUEST_LOCKED.format(lvl=lvl), reply_markup=markup)
    else:
        await message.answer(RU.QUEST_ALL_DONE, reply_markup=markup)
    await state.clear()
    return False


async def get_prestige_entry(session: AsyncSession, user: User) -> UserPrestige:
    prestige = await session.scalar(select(UserPrestige).where(UserPrestige.user_id == user.id))
    if not prestige:
        prestige = UserPrestige(user_id=user.id, reputation=0, resets=0)
        session.add(prestige)
        await session.flush()
    return prestige


async def perform_prestige_reset(
    session: AsyncSession, user: User, gain: int, total_earned: float
) -> None:
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
            meta={"gain": gain, "total_earned": round(total_earned, 2)},
            created_at=now,
        )
    )
    logger.info(
        "Prestige reset",
        extra={
            "tg_id": user.tg_id,
            "user_id": user.id,
            "prestige_gain": gain,
            "total_earned": round(total_earned, 2),
        },
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
    user.tutorial_free_boost_used = False
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
    await session.execute(delete(UserQuest).where(UserQuest.user_id == user.id))
    progress = await session.scalar(select(CampaignProgress).where(CampaignProgress.user_id == user.id))
    if progress:
        progress.chapter = 1
        progress.is_done = False
        progress.progress = {}



def project_next_item_params(item: Item) -> Tuple[float, int]:
    """Return projected bonus value and price for the next tier of an item."""

    if item.bonus_type in {"passive_pct", "reward_pct"}:
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
    lines = [
        RU.ACHIEVEMENT_UNLOCK.format(
            title=f"{ach.icon} {ach.name}", desc=ach.description
        )
        for ach, _ in unlocked
    ]
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


def rank_for(level: int, reputation: int) -> str:
    """Return rank title for given level and reputation."""

    title = RANK_THRESHOLDS[0][1]
    for lvl, name in RANK_THRESHOLDS:
        if level >= lvl:
            title = name
    if level >= 20 and reputation > 0:
        title = PRESTIGE_RANK
    return title


def describe_reward(reward: Dict[str, int]) -> str:
    """Turn reward dict into readable text."""

    parts: List[str] = []
    if reward.get("rub"):
        parts.append(f"+{reward['rub']}₽")
    if reward.get("xp"):
        parts.append(f"+{reward['xp']} XP")
    return " и ".join(parts) if parts else "бонус"


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
    ("мерч", "🎁"),
    ("nft", "🪙"),
    ("vr", "🕶️"),
    ("кампан", "🚀"),
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

class TutorialGateMiddleware(BaseMiddleware):
    """Block unrelated buttons during the guided tutorial."""

    async def __call__(self, handler, event, data):
        if not isinstance(event, Message):
            return await handler(event, data)
        if not event.from_user:
            return await handler(event, data)
        text = (event.text or "").strip()
        if not text or text.startswith("/start"):
            return await handler(event, data)
        async with session_scope() as session:
            user = await get_user_by_tg(session, event.from_user.id)
            if (
                not user
                or user.tutorial_completed_at is not None
                or user.tutorial_stage >= TUTORIAL_STAGE_DONE
            ):
                return await handler(event, data)
            allowed = tutorial_allowed_buttons(user.tutorial_stage)
            if text in allowed:
                return await handler(event, data)
            hint_button = TUTORIAL_STAGE_HINT_BUTTONS.get(
                user.tutorial_stage, RU.BTN_TUTORIAL_NEXT
            )
        await event.answer(RU.TUTORIAL_LOCKED.format(button=hint_button))
        return


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
    """Возвращает базовый лимит кликов."""

    async with session_scope() as session:
        user = await session.scalar(select(User).where(User.tg_id == tg_id))
        if not user:
            return BASE_CLICK_LIMIT
        await get_user_stats(session, user)
    return BASE_CLICK_LIMIT


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


class QuestState(StatesGroup):
    selecting = State()
    playing = State()


class SkillsState(StatesGroup):
    picking = State()


class StudioState(StatesGroup):
    confirm = State()


# ----------------------------------------------------------------------------
# Роутер и обработчики
# ----------------------------------------------------------------------------

router = Router()


async def get_or_create_user(
    tg_id: int, first_name: str, *, referrer_tg_id: Optional[int] = None
) -> Tuple[User, bool, Optional[Dict[str, Any]]]:
    """Fetch existing user or create a new record. Returns referral info if applied."""

    async with session_scope() as session:
        user = await session.scalar(select(User).where(User.tg_id == tg_id))
        created = False
        referral_payload: Optional[Dict[str, Any]] = None
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
                return await get_or_create_user(tg_id, first_name, referrer_tg_id=referrer_tg_id)
            for slot in ["laptop", "phone", "tablet", "monitor", "chair", "charm"]:
                session.add(UserEquipment(user_id=user.id, slot=slot, item_id=None))
            session.add(UserPrestige(user_id=user.id))
            session.add(CampaignProgress(user_id=user.id, chapter=1, is_done=False, progress={}))
            logger.info("New user created", extra={"tg_id": tg_id, "user_id": user.id})
            if referrer_tg_id and referrer_tg_id != tg_id:
                referrer = await session.scalar(select(User).where(User.tg_id == referrer_tg_id))
                if referrer and referrer.id != user.id:
                    now = utcnow()
                    user.referred_by = referrer.id
                    user.balance += REFERRAL_BONUS_RUB
                    user.updated_at = now
                    user_bonus_levels = await add_xp_and_levelup(user, REFERRAL_BONUS_XP)
                    session.add(
                        EconomyLog(
                            user_id=user.id,
                            type="referral_bonus",
                            amount=REFERRAL_BONUS_RUB,
                            meta={"from": referrer.tg_id},
                            created_at=now,
                        )
                    )
                    referrer_prev_level = referrer.level
                    referrer.balance += REFERRAL_BONUS_RUB
                    referrer.updated_at = now
                    referrer.referrals_count += 1
                    referrer_bonus_levels = await add_xp_and_levelup(referrer, REFERRAL_BONUS_XP)
                    session.add(
                        EconomyLog(
                            user_id=referrer.id,
                            type="referral_bonus",
                            amount=REFERRAL_BONUS_RUB,
                            meta={"new_user": tg_id},
                            created_at=now,
                        )
                    )
                    referral_payload = {
                        "referrer_tg_id": referrer.tg_id,
                        "referrer_prev_level": referrer_prev_level,
                        "referrer_levels": referrer_bonus_levels,
                        "user_bonus_levels": user_bonus_levels,
                    }
        else:
            await process_offline_income(session, user, [], message=None, state=None)
            logger.debug("Existing user resumed session", extra={"tg_id": tg_id})
        return user, created, referral_payload


async def get_user_by_tg(session: AsyncSession, tg_id: int) -> Optional[User]:
    """Load user entity by Telegram identifier."""

    return await session.scalar(select(User).where(User.tg_id == tg_id))


async def get_user_boost_by_code(
    session: AsyncSession, user: User, code: str
) -> Optional[UserBoost]:
    return await session.scalar(
        select(UserBoost)
        .join(Boost, Boost.id == UserBoost.boost_id)
        .where(UserBoost.user_id == user.id, Boost.code == code)
    )


async def ensure_user_loaded(
    session: AsyncSession, message: Message, *, tg_id: Optional[int] = None
) -> Optional[User]:
    """Return user for message or notify user to start the bot."""

    target_id = tg_id or (message.from_user.id if message.from_user else None)
    if target_id is None:
        return None
    user = await get_user_by_tg(session, target_id)
    if not user:
        await message.answer(
            "Нажмите /start",
            reply_markup=await build_main_menu_markup(session=session, tg_id=target_id),
        )
        return None
    return user


@router.message(CommandStart())
@safe_handler
async def cmd_start(message: Message, state: FSMContext):
    args = (message.text or "").split(maxsplit=1)
    ref_code = args[1].strip() if len(args) > 1 else ""
    referrer_id = None
    if ref_code.isdigit():
        referrer_id = int(ref_code)
    user, created, referral_info = await get_or_create_user(
        message.from_user.id,
        message.from_user.first_name or "",
        referrer_tg_id=referrer_id,
    )
    logger.info(
        "User issued /start",
        extra={"tg_id": message.from_user.id, "user_id": user.id, "is_created": created},
    )
    main_menu = await build_main_menu_markup(tg_id=message.from_user.id)
    needs_tutorial = user.tutorial_completed_at is None and user.tutorial_stage < TUTORIAL_STAGE_DONE
    if needs_tutorial:
        await state.set_state(TutorialState.step)
        await send_tutorial_prompt(message, user, user.tutorial_stage)
    else:
        capital_text = format_money(user.balance)
        welcome = RU.WELCOME.format(
            name=message.from_user.first_name or (message.from_user.username or "дизайнер"),
            capital=capital_text,
            orders=RU.BTN_ORDERS,
        )
        await message.answer(welcome, reply_markup=main_menu)
    async with session_scope() as session:
        user_db = await get_user_by_tg(session, message.from_user.id)
        if user_db:
            achievements: List[Tuple[Achievement, UserAchievement]] = []
            idle_result = await process_offline_income(
                session, user_db, achievements, message=message, state=state
            )
            await handle_idle_completion(message, session, user_db, state, idle_result)
            await notify_new_achievements(message, achievements)
            await maybe_send_trend_hint(message, session, user_db)
    if referral_info:
        await message.answer(
            f"🎉 За приглашение от друга получено +{REFERRAL_BONUS_RUB} ₽ и +{REFERRAL_BONUS_XP} XP!",
        )
        if referral_info.get("referrer_tg_id"):
            try:
                await message.bot.send_message(
                    referral_info["referrer_tg_id"],
                    f"🤝 Ваш друг присоединился! +{REFERRAL_BONUS_RUB} ₽ и +{REFERRAL_BONUS_XP} XP на счёт.",
                )
            except Exception:
                logger.debug("Failed to notify referrer", exc_info=True)
    if not needs_tutorial:
        await state.clear()


@router.message(F.text == RU.BTN_TUTORIAL_NEXT)
@safe_handler
async def tutorial_next_step(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        if user.tutorial_completed_at is not None or user.tutorial_stage >= TUTORIAL_STAGE_DONE:
            return
        if user.tutorial_stage <= TUTORIAL_STAGE_INTRO:
            user.tutorial_stage = TUTORIAL_STAGE_GO_ORDERS
            user.updated_at = utcnow()
            await state.set_state(TutorialState.step)
            await send_tutorial_prompt(message, user, user.tutorial_stage)


@router.message(F.text == RU.BTN_TUTORIAL_SKIP)
@safe_handler
async def tutorial_skip(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        user.tutorial_stage = TUTORIAL_STAGE_DONE
        user.tutorial_completed_at = utcnow()
        user.updated_at = utcnow()
    await state.clear()
    await message.answer(
        RU.TUTORIAL_DONE,
        reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
    )


@router.message(F.text == RU.BTN_TUTORIAL_FINISH)
@safe_handler
async def tutorial_finish(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        completed = await tutorial_on_event(message, session, user, "tutorial_complete")
    if completed:
        await state.clear()


@router.callback_query(F.data.startswith("event_choice:"))
@safe_handler
async def handle_event_choice_callback(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return
    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("Некорректный выбор.")
        return
    _, event_code, idx_str = parts
    if not idx_str.isdigit():
        await callback.answer("Некорректный выбор.")
        return
    choice_idx = int(idx_str)
    async with session_scope() as session:
        user = await ensure_user_loaded(session, callback.message, tg_id=callback.from_user.id)
        if not user:
            await callback.answer()
            return
        pending = await get_pending_event_buff(session, user, event_code)
        if not pending:
            await callback.answer("Событие уже обработано.")
            return
        options = pending.payload.get("options", []) if isinstance(pending.payload, dict) else []
        if choice_idx < 0 or choice_idx >= len(options):
            await callback.answer("Некорректный выбор.")
            return
        option = options[choice_idx]
        event = await session.scalar(select(RandomEvent).where(RandomEvent.code == event_code))
        if not event:
            await callback.answer("Событие не найдено.")
            return
        effect = option.get("effect", {})
        text_result = await apply_event_effect(session, user, event, effect, "choice")
        await session.delete(pending)
        logger.info(
            "Event choice",
            extra={
                "tg_id": user.tg_id,
                "user_id": user.id,
                "event": event_code,
                "choice": option.get("text"),
            },
        )
    await callback.answer("Выбор применён.")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    if text_result and text_result.strip():
        await callback.message.answer(text_result)


@router.message(F.text.in_({RU.BTN_MENU, RU.BTN_HOME}))
@safe_handler
async def back_to_menu(message: Message):
    async with session_scope() as session:
        user = await get_user_by_tg(session, message.from_user.id)
        if user:
            achievements: List[Tuple[Achievement, UserAchievement]] = []
            idle_result = await process_offline_income(
                session, user, achievements, message=message, state=None
            )
            await handle_idle_completion(message, session, user, None, idle_result)
            await notify_new_achievements(message, achievements)
            active = await get_active_order(session, user)
            await maybe_send_trend_hint(message, session, user)
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
        idle_result = await process_offline_income(
            session, user, achievements, message=message, state=state
        )
        await handle_idle_completion(message, session, user, state, idle_result)
        active = await get_active_order(session, user)
        if not active:
            await message.answer(
                RU.NO_ACTIVE_ORDER,
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            return
        order_completed = False
        stats = await get_user_stats(session, user)
        cp = max(1, int(stats.get("cp", 1)))
        user.clicks_total += cp
        achievements.extend(await evaluate_achievements(session, user, {"clicks"}))
        # Обновлено: учитываем фактическую силу клика в задании дня.
        await daily_task_on_event(message, session, user, "daily_clicks", amount=cp)
        if await tutorial_on_event(message, session, user, "click"):
            await state.clear()
        event_payload: Optional[Tuple[str, Optional[InlineKeyboardMarkup]]] = None
        if user.clicks_total % RANDOM_EVENT_CLICK_INTERVAL == 0:
            event_payload = await trigger_random_event(
                session, user, "click", RANDOM_EVENT_CLICK_PROB, stats
            )
        prev = active.progress_clicks
        active.progress_clicks = min(active.required_clicks, active.progress_clicks + cp)
        progress_lines: List[str] = []
        progress_markup: Optional[ReplyKeyboardMarkup] = None
        extra_phrase: Optional[str] = None
        if random.random() < CLICK_EXTRA_PHRASE_CHANCE:
            last_extra = _extra_phrase_last_sent.get(user.id, 0.0)
            now_extra = time.monotonic()
            if now_extra - last_extra >= CLICK_EXTRA_PHRASE_COOLDOWN:
                extra_phrase = random.choice(CLICK_EXTRA_PHRASES)
                _extra_phrase_last_sent[user.id] = now_extra
        pct = int(round(100 * active.progress_clicks / active.required_clicks))
        progress_lines.append(
            RU.CLICK_PROGRESS.format(cur=active.progress_clicks, req=active.required_clicks, pct=pct)
        )
        progress_markup = kb_active_order_controls()
        if progress_lines and extra_phrase:
            progress_lines.append(extra_phrase)
            extra_phrase = None
        if progress_lines:
            await message.answer(
                "\n".join(progress_lines),
                reply_markup=progress_markup or kb_active_order_controls(),
            )
        if extra_phrase:
            await message.answer(extra_phrase)
        if active.progress_clicks >= active.required_clicks:
            completion_result = await apply_order_completion(
                session,
                user,
                active,
                stats,
                achievements,
                trigger_events=True,
            )
            menu_markup = await main_menu_for_message(message, session=session, user=user)
            extra_line = random.choice(ORDER_DONE_EXTRA) if ORDER_DONE_EXTRA else ""
            summary_lines = [
                RU.ORDER_DONE.format(rub=completion_result.reward, xp=completion_result.xp_gain)
            ]
            badges: List[str] = []
            if completion_result.trend_applied:
                badges.append(RU.TREND_BADGE)
            if completion_result.is_special:
                badges.append("⭐ спец")
            if completion_result.rush_applied:
                badges.append("⏱️ быстро")
            summary_line = f"🧾 Заказов всего: {user.orders_completed}"
            if badges:
                summary_line = f"{summary_line} · {' '.join(badges)}"
            summary_lines.append(summary_line)
            if extra_line:
                summary_lines.append(extra_line)
            await message.answer("\n".join(summary_lines), reply_markup=menu_markup)
            await maybe_prompt_skill_choice(
                session,
                message,
                state,
                user,
                completion_result.prev_level,
                completion_result.levels_gained,
            )
            if completion_result.levels_gained:
                await notify_level_up_message(
                    message,
                    session,
                    user,
                    completion_result.prev_level,
                    completion_result.levels_gained,
                )
            await daily_task_on_event(message, session, user, "daily_orders")
            if completion_result.event_payload:
                text_event, event_markup = completion_result.event_payload
                if text_event and text_event.strip():
                    if event_markup is not None:
                        await message.answer(text_event, reply_markup=event_markup)
                    else:
                        await message.answer(text_event, reply_markup=menu_markup)
            achievements.extend(await evaluate_achievements(session, user, {"orders", "level", "balance"}))
            order_completed = True
        if event_payload:
            text, inline_markup = event_payload
            if text and text.strip():
                if inline_markup is not None:
                    await message.answer(text, reply_markup=inline_markup)
                else:
                    await message.answer(text, reply_markup=kb_active_order_controls())
        await notify_new_achievements(message, achievements)
        if order_completed:
            advanced_after_finish = await tutorial_on_event(
                message, session, user, "order_completed"
            )
            if advanced_after_finish:
                await state.clear()


@router.message(F.text == RU.BTN_TO_MENU)
@safe_handler
async def leave_order_to_menu(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
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

def fmt_orders(
    orders: List[Order],
    *,
    user_level: int,
    special_hint: bool = False,
    trend: Optional[Dict[str, Any]] = None,
) -> str:
    lines = [RU.ORDERS_HEADER, "Выбирайте цифрой внизу, чтобы оформить заказ."]
    if special_hint:
        lines.append("")
        lines.append(RU.SPECIAL_ORDER_AVAILABLE)
    if orders:
        lines.append("")
    trend_order_id = int(trend.get("order_id")) if trend else None
    trend_mul = float(trend.get("reward_mul", TREND_REWARD_MUL)) if trend else TREND_REWARD_MUL
    for i, o in enumerate(orders, 1):
        base_icon = pick_order_icon(o.title)
        rarity = getattr(o, "rarity", "common")
        rarity_icon = ORDER_RARITY_ICONS.get(rarity, "")
        prefix = f"{rarity_icon} {base_icon}".strip()
        title = o.title
        difficulty = ORDER_DIFFICULTY_LABELS.get(getattr(o, "difficulty", ""), o.difficulty)
        estimated = int(getattr(o, "estimated_minutes", 30))
        reward_mul = float(getattr(o, "reward_multiplier", 1.0))
        preview = int(
            getattr(
                o,
                "reward_preview",
                base_reward_from_required(
                    required_clicks(o.base_clicks, max(user_level, o.min_level)),
                    reward_mul,
                ),
            )
        )
        if getattr(o, "is_special", False):
            preview = int(round(preview * SPECIAL_ORDER_REWARD_MUL))
        detail_parts = [
            f"📈 Мин. уровень: {o.min_level}",
            f"💰 Оплата: {format_money(preview)}{RU.CURRENCY}",
            f"⚙️ Сложность: {difficulty.lower() if difficulty else difficulty}",
            f"⏱️ Время: {estimated} мин",
        ]
        if getattr(o, "is_special", False):
            prefix = "✨"
            title = f"{RU.SPECIAL_ORDER_TITLE}: {o.title}"
            detail_parts.append("⭐ Награда: ×2")
        if trend_order_id and o.id == trend_order_id:
            prefix = "🔥"
            detail_parts.append(f"🔥 Множитель: ×{format_stat(trend_mul)}")
        if rarity in {"rare", "holiday"}:
            detail_parts.append(f"🎁 Статус: {ORDER_RARITY_TITLES.get(rarity, rarity)}")
        header = f"{i}. {prefix} {title}".strip()
        separator = "\u2003"  # эм-пробел для равномерных интервалов
        details = separator.join(part for part in detail_parts if part)
        description = ORDER_DESCRIPTIONS.get(title, "Готовый способ прокачать портфолио.")
        lines.append(header)
        lines.append(details)
        lines.append(description)
        if i != len(orders):
            lines.append("")
    if lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


@router.message(F.text == RU.BTN_ORDERS)
@safe_handler
async def orders_root(message: Message, state: FSMContext):
    await state.set_state(OrdersState.browsing)
    # Сбрасываем случайные заказы при новом заходе в раздел.
    await state.update_data(page=0, rolled_rares=None)
    await _render_orders_page(message, state)
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if user:
            await tutorial_on_event(message, session, user, "orders_opened")


@router.message(F.text == RU.BTN_UPGRADES)
@safe_handler
async def upgrades_root(message: Message, state: FSMContext):
    await state.clear()
    include_team = False
    tutorial_active = False
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        include_team = user.level >= 2
        tutorial_active = is_tutorial_active(user)
        await tutorial_on_event(message, session, user, "upgrades_open")
        await notify_new_achievements(message, achievements)
    await message.answer(
        RU.UPGRADES_HEADER,
        reply_markup=kb_upgrades_menu(
            include_team=include_team, tutorial=tutorial_active
        ),
    )


async def _render_orders_page(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        all_orders = (
            await session.execute(
                select(Order)
                .where(Order.min_level <= user.level)
                .order_by(Order.min_level, Order.id)
            )
        ).scalars().all()
        data = await state.get_data()
        rolled_rares: Optional[List[int]] = data.get("rolled_rares")  # type: ignore[arg-type]
        if rolled_rares is None:
            rolled_rares = []
            for order in all_orders:
                if order.rarity in {"rare", "holiday"}:
                    chance = max(0.0, min(1.0, float(getattr(order, "appearance_weight", 0.0))))
                    if chance > 0 and random.random() < chance:
                        rolled_rares.append(order.id)
            await state.update_data(rolled_rares=rolled_rares)
        special_orders = [o for o in all_orders if o.is_special]
        regular_orders = [
            o
            for o in all_orders
            if not o.is_special and (o.rarity == "common" or o.id in rolled_rares)
        ]
        special_inserted = False
        trend_info = await get_trend(session)
        today = utcnow().date()
        if special_orders:
            special = special_orders[0]
            last_special = ensure_naive(user.last_special_order_at)
            allow_special = user.level >= special.min_level and (
                last_special is None or last_special.date() < today
            )
            if allow_special:
                regular_orders = [special] + regular_orders
                special_inserted = True
        page = int(data.get("page", 0))
        sub, has_prev, has_next = slice_page(regular_orders, page, 5)
        hint_needed = special_inserted and any(getattr(o, "is_special", False) for o in sub)
        await message.answer(
            fmt_orders(
                sub,
                user_level=user.level,
                special_hint=hint_needed,
                trend=trend_info,
            ),
            reply_markup=kb_numeric_page(has_prev, has_next),
        )
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
        order = await session.scalar(select(Order).where(Order.id == order_id))
        is_special_order = bool(order and order.is_special)
        initial_progress = 0
        free_chance = stats.get("free_order_chance", 0.0)
        free_triggered = False
        if free_chance > 0 and random.random() < free_chance:
            initial_progress = min(
                req,
                max(1, int(round(req * FREE_ORDER_PROGRESS_PCT))),
            )
            free_triggered = initial_progress > 0
        order_mul = 1.0
        if order:
            order_mul = float(getattr(order, "reward_multiplier", 1.0))
        reward_snapshot = stats["reward_mul_total"] * (
            SPECIAL_ORDER_REWARD_MUL if is_special_order else 1.0
        ) * order_mul
        trend_info = await get_trend(session)
        trend_applied = False
        trend_multiplier = 1.0
        if order and trend_info and order.id == trend_info.get("order_id"):
            trend_multiplier = float(trend_info.get("reward_mul", TREND_REWARD_MUL))
            reward_snapshot *= trend_multiplier
            trend_applied = True
        now = utcnow()
        session.add(
            UserOrder(
                user_id=user.id,
                order_id=order_id,
                progress_clicks=initial_progress,
                required_clicks=req,
                started_at=now,
                finished=False,
                canceled=False,
                reward_snapshot_mul=reward_snapshot,
                is_special=is_special_order,
                trend_applied=trend_applied,
                trend_multiplier=trend_multiplier,
                auto_progress_buffer=0.0,
            )
        )
        user.updated_at = now
        if order:
            await message.answer(
                RU.ORDER_TAKEN.format(title=order.title), reply_markup=kb_active_order_controls()
            )
            if free_triggered:
                await message.answer(
                    f"📦 Бесплатный старт! Прогресс: {initial_progress}/{req}",
                    reply_markup=kb_active_order_controls(),
                )
            if is_special_order:
                user.last_special_order_at = now
                await message.answer(RU.SPECIAL_ORDER_AVAILABLE)
        await tutorial_on_event(message, session, user, "order_taken")
        log_extra = {"tg_id": user.tg_id, "user_id": user.id, "order_id": order_id}
        if trend_applied:
            log_extra["trend_mul"] = trend_multiplier
        logger.info("Order taken", extra=log_extra)
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
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        tutorial_active = is_tutorial_active(user)
        await state.set_state(ShopState.root)
        await message.answer(
            RU.SHOP_HEADER, reply_markup=kb_shop_menu(tutorial=tutorial_active)
        )
        if user:
            await tutorial_on_event(message, session, user, "shop_open")


BOOST_TYPE_META: Dict[str, Tuple[str, str, str]] = {
    "reward": ("🎯", "Награда", "к наградам"),
    "passive": ("💼", "Пассивный доход", "к пассивному доходу"),
    "xp": ("🧠", "Опыт", "к опыту"),
    "event_protection": ("🧿", "Антибрак", "к штрафам"),
    "event_shield": ("🧯", "Страховка", "зарядов защиты"),
    "req_clicks": ("🧭", "Снижение требований", "к требуемым кликам"),
    "free_order": ("📦", "Быстрые брифы", "к стартовому прогрессу"),
    "team_discount": ("🧾", "Скидки подрядчикам", "к скидке команды"),
    "offline_cap": ("💤", "Глубокий офлайн", "к лимиту офлайн-дохода"),
    "shop_discount": ("🛍️", "Опт в магазине", "к скидке магазина"),
    "team_income": ("👥", "Слаженная команда", "к доходу команды"),
    "rush_reward": ("⏱️", "Сжатые дедлайны", "к награде за скорость"),
    "equipment_eff": ("🧰", "Тюнинг экипировки", "к бонусу экипировки"),
    "night_passive": ("🌙", "Ночной поток", "к ночному пассиву"),
    "high_order_reward": ("🎯", "Премиум-проекты", "к наградам крупных заказов"),
}

BOOST_CATEGORY_DEFS: List[Tuple[str, Dict[str, str]]] = [
    ("economy", {"icon": "💰", "label": "Экономика"}),
    ("xp", {"icon": "🧠", "label": "Опыт"}),
    ("passive", {"icon": "🌀", "label": "Пассив"}),
]
BOOST_CATEGORY_DESCRIPTIONS: Dict[str, str] = {
    "economy": "улучшает заработок и снижает расходы.",
    "xp": "ускоряет рост уровня и навыков.",
    "passive": "даёт долгосрочные и автоэффекты.",
}
BOOST_CATEGORY_META: Dict[str, Dict[str, str]] = {
    key: meta for key, meta in BOOST_CATEGORY_DEFS
}
BOOST_CATEGORY_BY_TYPE: Dict[str, str] = {
    "reward": "economy",
    "req_clicks": "economy",
    "free_order": "economy",
    "team_discount": "economy",
    "shop_discount": "economy",
    "rush_reward": "economy",
    "high_order_reward": "economy",
    "equipment_eff": "economy",
    "passive": "passive",
    "team_income": "passive",
    "offline_cap": "passive",
    "night_passive": "passive",
    "event_protection": "passive",
    "event_shield": "passive",
    "xp": "xp",
}
BOOST_CATEGORY_BUTTON_TEXT: Dict[str, str] = {
    key: f"{meta['icon']} {meta['label']}" for key, meta in BOOST_CATEGORY_DEFS
}
BOOST_CATEGORY_BY_TEXT: Dict[str, str] = {
    text: key for key, text in BOOST_CATEGORY_BUTTON_TEXT.items()
}
BOOST_CATEGORY_TEXTS: Set[str] = set(BOOST_CATEGORY_BY_TEXT.keys())
BOOST_CATEGORY_DEFAULT = BOOST_CATEGORY_DEFS[0][0]
PERMANENT_BOOST_TYPES: Set[str] = {
    "reward",
    "passive",
    "xp",
    "team_income",
    "rush_reward",
    "equipment_eff",
    "night_passive",
    "high_order_reward",
    "req_clicks",
    "free_order",
    "team_discount",
    "shop_discount",
    "offline_cap",
}

ITEM_BONUS_LABELS: Dict[str, str] = {
    "passive_pct": "к пассивному доходу",
    "req_clicks_pct": "к требуемым кликам",
    "reward_pct": "к наградам",
}

ITEM_SLOT_EMOJI: Dict[str, str] = {
    "chair": "🪑",
    "laptop": "💻",
    "monitor": "🖥️",
    "phone": "📱",
    "tablet": "📲",
    "charm": "📜",
}

ITEM_SLOT_LABELS: Dict[str, str] = {
    "chair": "Стул",
    "laptop": "Ноутбук",
    "monitor": "Монитор",
    "phone": "Смартфон",
    "tablet": "Планшет",
    "charm": "Талисман",
}


def _boost_meta(boost: Boost) -> Tuple[str, str, str]:
    """Return base icon, label and suffix for the boost."""

    icon, short_label, suffix = BOOST_TYPE_META.get(
        boost.type, ("✨", boost.name, "к характеристике")
    )
    label = short_label or boost.name
    if label == boost.name and label.startswith(icon):
        label = label[len(icon) :].strip()
    label = label or boost.name
    return icon, label, suffix


def _format_boost_effect_value(boost: Boost, value: float, suffix: str) -> str:
    """Format boost value according to its type for human readable output."""

    if boost.type in {
        "reward",
        "passive",
        "xp",
        "team_income",
        "rush_reward",
        "equipment_eff",
        "night_passive",
        "high_order_reward",
    }:
        return f"+{int(round(value * 100))}% {suffix}"
    if boost.type == "event_protection":
        return f"−{int(round(value * 100))}% {suffix}"
    if boost.type == "event_shield":
        return f"+{int(round(value))} {suffix}"
    if boost.type == "req_clicks":
        return f"−{int(round(value * 100))}% {suffix}"
    if boost.type == "free_order":
        return f"+{int(round(value * 100))}% {suffix}"
    if boost.type == "team_discount":
        return f"−{int(round(value * 100))}% {suffix}"
    if boost.type == "offline_cap":
        hours = value / 3600.0
        return f"+{format_stat(hours)} ч {suffix}"
    if boost.type == "shop_discount":
        return f"−{int(round(value * 100))}% {suffix}"
    return f"+{format_stat(value)} {suffix}"


def _boost_effect_for_level(boost: Boost, level: int) -> str:
    """Return formatted cumulative bonus for the given boost level."""

    _, _, suffix = _boost_meta(boost)
    total_value = boost.step_value * level
    return _format_boost_effect_value(boost, total_value, suffix)


def _boost_display(boost: Boost) -> Tuple[str, str, str]:
    """Return icon, label and single-level effect description for a boost."""

    icon, _, suffix = _boost_meta(boost)
    effect = _format_boost_effect_value(boost, boost.step_value, suffix)
    return icon, boost.name, effect


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


async def resolve_free_shop_offer(
    session: AsyncSession,
    user: User,
    *,
    boosts: Optional[List[Boost]] = None,
    boost_levels: Optional[Dict[int, int]] = None,
    items: Optional[List[Item]] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> Optional[FreeShopOffer]:
    """Determine the cheapest boost or item eligible for a free tutorial purchase."""

    if user.tutorial_free_boost_used:
        return None

    if stats is None:
        stats = await get_user_stats(session, user)
    discount_pct = stats.get("shop_discount_pct", 0.0)

    if boosts is None:
        boosts = (await session.execute(select(Boost).order_by(Boost.base_cost))).scalars().all()
    if boost_levels is None:
        boost_levels = {
            boost_id: level
            for boost_id, level in (
                await session.execute(
                    select(UserBoost.boost_id, UserBoost.level).where(UserBoost.user_id == user.id)
                )
            ).all()
        }

    candidate: Optional[Tuple[str, int, float]] = None
    for boost in boosts:
        min_level = getattr(boost, "min_level", 1) or 1
        if user.level < min_level:
            continue
        current_level = boost_levels.get(boost.id, 0)
        if current_level > 0:
            continue
        cost = float(upgrade_cost(boost.base_cost, boost.growth, 1))
        if candidate is None or cost < candidate[2] or (cost == candidate[2] and candidate[0] == "item"):
            candidate = ("boost", boost.id, cost)

    owned_items: Set[int] = {
        row[0]
        for row in (
            await session.execute(select(UserItem.item_id).where(UserItem.user_id == user.id))
        ).all()
    }
    if items is None:
        items = await get_next_items_for_user(session, user)

    for item in items:
        min_level = getattr(item, "min_level", 1) or 1
        if user.level < min_level or item.id in owned_items:
            continue
        price = float(apply_percentage_discount(item.price, discount_pct, cap=SHOP_DISCOUNT_CAP))
        if candidate is None or price < candidate[2] or (price == candidate[2] and candidate[0] == "boost"):
            candidate = ("item", item.id, price)

    if candidate:
        return FreeShopOffer(kind=candidate[0], target_id=candidate[1])
    return None


async def tutorial_prepare_shop_hint(
    session: AsyncSession,
    user: User,
    *,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """Store hint data about the free tutorial upgrade in the payload."""

    if payload is None:
        payload = ensure_tutorial_payload(user)
    offer = await resolve_free_shop_offer(session, user)
    if not offer:
        payload.pop("shop_hint", None)
        user.tutorial_payload = payload
        return
    hint: Dict[str, Any] = {"kind": offer.kind}
    if offer.kind == "boost":
        boost = await session.scalar(select(Boost).where(Boost.id == offer.target_id))
        if boost:
            category = _boost_category(boost)
            hint.update(
                {
                    "name": boost.name,
                    "category_key": category,
                    "category_button": BOOST_CATEGORY_BUTTON_TEXT.get(
                        category, RU.BTN_BOOSTS
                    ),
                }
            )
        else:
            hint["name"] = "улучшение"
    else:
        item = await session.scalar(select(Item).where(Item.id == offer.target_id))
        hint["name"] = item.name if item else "предмет"
    payload["shop_hint"] = hint
    user.tutorial_payload = payload


def fmt_boosts(
    user: User,
    boosts: List[Boost],
    levels: Dict[int, int],
    page: int,
    page_size: int = 5,
    *,
    free_offer: Optional[FreeShopOffer] = None,
) -> Tuple[str, List[int]]:
    """Compose a formatted boost list and return text along with selectable boost ids."""

    if not boosts:
        return "Пока нечего прокачать — возвращайтесь позже.", []

    selectable: List[int] = []
    lines: List[str] = []
    for idx, boost in enumerate(boosts, 1):
        current_level = levels.get(boost.id, 0)
        next_level = current_level + 1
        min_level = getattr(boost, "min_level", 1) or 1
        next_bonus = _boost_effect_for_level(boost, next_level)
        cost_value = upgrade_cost(boost.base_cost, boost.growth, next_level)
        is_free_candidate = (
            free_offer is not None
            and free_offer.kind == "boost"
            and free_offer.target_id == boost.id
            and not user.tutorial_free_boost_used
            and next_level == 1
        )
        cost = FREE_UPGRADE_PRICE_LABEL if is_free_candidate else format_price(cost_value)
        extra = BOOST_EXTRA_META.get(boost.code, {})
        permanent = extra.get("permanent")
        if permanent is None:
            permanent = boost.type in PERMANENT_BOOST_TYPES
        parts = [f"{idx}. {boost.name}", "—", next_bonus, "·", cost]
        if permanent:
            parts.append("· постоянный эффект")
        locked = user.level < min_level
        if locked:
            parts.insert(1, "🔒")
            parts.append(f"(доступно с {min_level} уровня)")
        line = " ".join(parts)
        lines.append(line)
        flavor = extra.get("flavor")
        if flavor:
            lines.append(f"   _{flavor}_")
        if current_level > 0 and not locked:
            current_bonus = _boost_effect_for_level(boost, current_level)
            lines.append(
                f"   Текущий уровень: {current_level} — {current_bonus}"
            )
        selectable.append(boost.id)
        if idx != len(boosts):
            lines.append("")
    return "\n".join(lines), selectable


def format_boost_purchase_prompt(
    boost: Boost,
    current_level: int,
    next_level: int,
    cost: int,
    *,
    free_available: bool = False,
) -> str:
    """Pretty confirmation text for a boost upgrade purchase."""

    icon, label, _ = _boost_display(boost)
    step_effect = _boost_effect_for_level(boost, 1)
    if current_level > 0:
        current_effect = _boost_effect_for_level(boost, current_level)
        current_line = f"Текущий уровень: {current_level} — {current_effect}"
    else:
        current_line = "Текущий уровень: 0 — бонус ещё не активен"
    next_effect = _boost_effect_for_level(boost, next_level)
    parts = [
        f"{icon} Улучшение «{label}»",
        current_line,
        f"После покупки: {next_level} — {next_effect}",
        f"Бонус за уровень: {step_effect}",
    ]
    if free_available:
        parts.append(f"Стоимость: {FREE_UPGRADE_PRICE_LABEL}")
        parts.append("🎓 Первое улучшение оформляется без списания монет.")
    else:
        parts.append(f"Стоимость: {format_price(cost)}")
    flavor = BOOST_EXTRA_META.get(boost.code, {}).get("flavor")
    if flavor:
        parts.append(flavor)
    parts.append("Эффект действует постоянно.")
    return "\n".join(parts)


def format_item_purchase_prompt(
    item: Item, price: int, *, free_available: bool = False
) -> str:
    """Pretty confirmation text for buying an equipment piece."""

    icon = _item_icon(item)
    effect = _format_item_effect(item)
    price_text = FREE_UPGRADE_PRICE_LABEL if free_available else format_price(price)
    lines = [
        f"{icon} Покупка «{item.name}»",
        f"Эффект: {effect}",
        f"Цена: {price_text}",
    ]
    if free_available:
        lines.append("🎓 Первое улучшение оформляется без списания монет.")
    return "\n".join(lines)


def format_item_equip_prompt(item: Item, current_equipped: Optional[Item] = None) -> str:
    """Confirmation prompt shown when the user equips an owned item."""

    icon = _item_icon(item)
    effect = _format_item_effect(item)
    slot_icon = ITEM_SLOT_EMOJI.get(item.slot, "🎁")
    slot_label = ITEM_SLOT_LABELS.get(item.slot, "Слот")
    if current_equipped and current_equipped.id == item.id:
        slot_line = f"Этот предмет уже экипирован в слот {slot_icon} {slot_label}."
    elif current_equipped:
        slot_line = (
            f"Этот предмет займёт слот {slot_icon} {slot_label} вместо «{current_equipped.name}»."
        )
    else:
        slot_line = f"Этот предмет займёт слот {slot_icon} {slot_label}."
    return (
        f"{icon} Экипировать «{item.name}»?\n"
        f"Эффект: {effect}\n"
        f"{slot_line}\n"
        "Бонус будет активен постоянно."
    )


def _boost_category(boost: Boost) -> str:
    """Resolve category key for the given boost."""

    return BOOST_CATEGORY_BY_TYPE.get(boost.type, BOOST_CATEGORY_DEFAULT)


async def render_boosts(
    message: Message,
    state: FSMContext,
    *,
    tg_id: Optional[int] = None,
) -> None:
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message, tg_id=tg_id)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        stats = await get_user_stats(session, user)
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
        data = await state.get_data()
        category = data.get("boost_category")
        tutorial_active = is_tutorial_active(user)
        payload = ensure_tutorial_payload(user) if tutorial_active else {}
        hint = payload.get("shop_hint", {}) if isinstance(payload, dict) else {}
        if not category:
            auto_category = None
            if tutorial_active and hint.get("kind") == "boost":
                auto_category = hint.get("category_key")
            if auto_category:
                category = auto_category
                await state.update_data(boost_category=category, page=0, boost_ids=[])
            else:
                counts: Dict[str, int] = defaultdict(int)
                for boost in boosts:
                    counts[_boost_category(boost)] += 1
                lines = [
                    "🛍️ Магазин усилений — выбери направление",
                    f"💰 Баланс: {format_price(user.balance)}",
                    "",
                ]
                for key, meta in BOOST_CATEGORY_DEFS:
                    lines.append(
                        f"{meta['icon']} Категория «{meta['label']}» — {counts.get(key, 0)} улучшений"
                    )
                    desc = BOOST_CATEGORY_DESCRIPTIONS.get(key)
                    if desc:
                        lines.append(f"   {desc}")
                lines.append("")
                lines.append("Нажми категорию, чтобы посмотреть улучшения и открыть покупки.")
                await message.answer(
                    "\n".join(lines), reply_markup=kb_boost_categories(tutorial=tutorial_active)
                )
                await state.update_data(page=0, boost_category=None, boost_ids=[])
                await notify_new_achievements(message, achievements)
                return
        category_boosts = [b for b in boosts if _boost_category(b) == category]
        if not category_boosts:
            await state.update_data(boost_category=None, page=0, boost_ids=[])
            await message.answer(
                "В этой категории пока нет улучшений.",
                reply_markup=kb_boost_categories(tutorial=tutorial_active),
            )
            await notify_new_achievements(message, achievements)
            return
        total = len(category_boosts)
        total_pages = max(1, (total + BOOSTS_PER_PAGE - 1) // BOOSTS_PER_PAGE)
        page = int(data.get("page", 0))
        if page >= total_pages:
            page = max(0, total_pages - 1)
        sub, has_prev, has_next = slice_page(category_boosts, page, BOOSTS_PER_PAGE)
        free_offer = await resolve_free_shop_offer(
            session,
            user,
            boosts=boosts,
            boost_levels=levels,
            stats=stats,
        )
        text_body, selectable = fmt_boosts(
            user, sub, levels, page, page_size=BOOSTS_PER_PAGE, free_offer=free_offer
        )
        meta = BOOST_CATEGORY_META.get(
            category, {"icon": "✨", "label": category.title()}
        )
        header_lines = [
            f"{meta['icon']} Категория «{meta['label']}» — {total} улучшений",
            f"💰 Баланс: {format_price(user.balance)}",
        ]
        if total_pages > 1:
            header_lines.append(f"Страница {page + 1}/{total_pages}")
        header_lines.append("Выбирай цифрой ниже или возвращайся к категориям.")
        if text_body:
            header_lines.extend(["", text_body])
        else:
            header_lines.append("")
            header_lines.append("Пока нечего прокачивать в этой категории.")
        await message.answer(
            "\n".join(header_lines),
            reply_markup=kb_boosts_controls(
                has_prev, has_next, len(sub), tutorial=tutorial_active
            ),
        )
        await state.update_data(
            boost_ids=selectable,
            page=page,
            boost_category=category,
        )
        await notify_new_achievements(message, achievements)


async def _handle_boost_selection(
    message: Message,
    state: FSMContext,
    boost_id: int,
    *,
    tg_id: Optional[int] = None,
) -> None:
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message, tg_id=tg_id)
        if not user:
            await state.clear()
            return
        boost = await session.scalar(select(Boost).where(Boost.id == boost_id))
        if not boost:
            await message.answer("Усиление не найдено.")
            await state.set_state(ShopState.boosts)
            await render_boosts(message, state, tg_id=tg_id)
            return
        min_level = getattr(boost, "min_level", 1) or 1
        if user.level < min_level:
            await message.answer(RU.BOOST_LOCKED.format(lvl=min_level))
            await state.set_state(ShopState.boosts)
            await render_boosts(message, state, tg_id=tg_id)
            return
        user_boost = await session.scalar(
            select(UserBoost).where(
                UserBoost.user_id == user.id, UserBoost.boost_id == boost_id
            )
        )
        lvl_next = (user_boost.level if user_boost else 0) + 1
        cost = upgrade_cost(boost.base_cost, boost.growth, lvl_next)
        free_offer = await resolve_free_shop_offer(session, user)
        free_available = (
            free_offer is not None
            and free_offer.kind == "boost"
            and free_offer.target_id == boost_id
            and not user.tutorial_free_boost_used
            and lvl_next == 1
        )
        prompt = format_boost_purchase_prompt(
            boost,
            user_boost.level if user_boost else 0,
            lvl_next,
            cost,
            free_available=free_available,
        )
        await message.answer(
            prompt,
            reply_markup=kb_confirm(RU.BTN_BUY, tutorial=is_tutorial_active(user)),
        )
        await state.set_state(ShopState.confirm_boost)
        await state.update_data(boost_id=boost_id)


@router.message(ShopState.root, F.text == RU.BTN_BOOSTS)
@safe_handler
async def shop_boosts(message: Message, state: FSMContext):
    await state.set_state(ShopState.boosts)
    await state.update_data(
        page=0,
        boost_category=None,
    )
    await render_boosts(message, state)


@router.message(ShopState.boosts, F.text.in_(BOOST_SELECTION_INPUTS))
@safe_handler
async def shop_choose_boost(message: Message, state: FSMContext):
    ids = (await state.get_data()).get("boost_ids", [])
    idx = int(message.text) - 1
    if idx < 0 or idx >= len(ids):
        return
    bid = ids[idx]
    await _handle_boost_selection(message, state, bid)


@router.message(ShopState.boosts, F.text.in_(BOOST_CATEGORY_TEXTS))
@safe_handler
async def shop_boosts_select_category(message: Message, state: FSMContext):
    category = BOOST_CATEGORY_BY_TEXT.get((message.text or "").strip())
    if not category:
        return
    data = await state.get_data()
    if data.get("boost_category") == category:
        return
    await state.update_data(boost_category=category, page=0)
    await render_boosts(message, state)


@router.message(ShopState.boosts, F.text == RU.BTN_BACK)
@safe_handler
async def shop_boosts_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("boost_category"):
        await state.update_data(boost_category=None, page=0, boost_ids=[])
        await render_boosts(message, state)
        return
    await state.set_state(ShopState.root)
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
    await message.answer(
        RU.SHOP_HEADER,
        reply_markup=kb_shop_menu(tutorial=is_tutorial_active(user)),
    )


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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        boost = await session.scalar(select(Boost).where(Boost.id == bid))
        if not boost:
            await message.answer("Усиление не найдено.")
            await state.set_state(ShopState.boosts)
            await render_boosts(message, state)
            return
        user_boost = await session.scalar(
            select(UserBoost).where(UserBoost.user_id == user.id, UserBoost.boost_id == bid)
        )
        lvl_next = (user_boost.level if user_boost else 0) + 1
        cost = upgrade_cost(boost.base_cost, boost.growth, lvl_next)
        free_offer = await resolve_free_shop_offer(session, user)
        free_available = (
            free_offer is not None
            and free_offer.kind == "boost"
            and free_offer.target_id == bid
            and not user.tutorial_free_boost_used
            and lvl_next == 1
        )
        if not free_available and user.balance < cost:
            await message.answer(RU.INSUFFICIENT_FUNDS)
        else:
            now = utcnow()
            actual_cost = 0 if free_available else cost
            if not free_available:
                user.balance -= cost
            else:
                user.tutorial_free_boost_used = True
            user.updated_at = now
            if not user_boost:
                session.add(UserBoost(user_id=user.id, boost_id=bid, level=1))
            else:
                user_boost.level += 1
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="buy_boost",
                    amount=-actual_cost,
                    meta={
                        "boost": boost.code,
                        "lvl": lvl_next,
                        **({"tutorial_free": True} if free_available else {}),
                    },
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
                    "tutorial_free": free_available,
                },
            )
            if free_available:
                await message.answer(RU.TUTORIAL_FREE_UPGRADE_DONE)
            else:
                await message.answer(RU.PURCHASE_OK)
            feedback = BOOST_PURCHASE_FEEDBACK.get(boost.type)
            if feedback:
                await message.answer(feedback)
            await daily_task_on_event(message, session, user, "daily_shop")
            await tutorial_on_event(message, session, user, "upgrade_purchase")
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
    equipped_ids: Optional[Set[int]] = None,
    free_offer: Optional[FreeShopOffer] = None,
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
        if equipped_ids and it.id in equipped_ids:
            entry = f"{entry} (экипирован)"
        if include_price:
            price = it.price
            if discount_pct > 0:
                price = apply_percentage_discount(price, discount_pct, cap=SHOP_DISCOUNT_CAP)
            is_free_candidate = (
                free_offer is not None
                and free_offer.kind == "item"
                and free_offer.target_id == it.id
                and not user.tutorial_free_boost_used
            )
            price_text = FREE_UPGRADE_PRICE_LABEL if is_free_candidate else format_price(price)
            entry = f"{entry} · {price_text}"
        lines.append(entry)
    return "\n".join(lines)


async def render_items(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        stats = await get_user_stats(session, user)
        tutorial_active = is_tutorial_active(user)
        items = await get_next_items_for_user(session, user)
        free_offer = await resolve_free_shop_offer(
            session, user, items=items, stats=stats
        )
        page = int((await state.get_data()).get("page", 0))
        sub, has_prev, has_next = slice_page(items, page, 5)
        await message.answer(
            fmt_items(
                user,
                sub,
                page,
                include_price=True,
                discount_pct=stats.get("shop_discount_pct", 0.0),
                free_offer=free_offer,
            ),
            reply_markup=kb_numeric_page(
                has_prev, has_next, tutorial=tutorial_active
            ),
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
        free_offer = await resolve_free_shop_offer(session, user, stats=stats)
        free_available = (
            free_offer is not None
            and free_offer.kind == "item"
            and free_offer.target_id == item_id
            and not user.tutorial_free_boost_used
        )
        prompt = format_item_purchase_prompt(it, price, free_available=free_available)
        await message.answer(
            prompt,
            reply_markup=kb_confirm(RU.BTN_BUY, tutorial=is_tutorial_active(user)),
        )
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
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
        free_offer = await resolve_free_shop_offer(session, user, stats=stats)
        free_available = (
            free_offer is not None
            and free_offer.kind == "item"
            and free_offer.target_id == item_id
            and not user.tutorial_free_boost_used
        )
        if has:
            await message.answer("Уже куплено.")
        elif not free_available and user.balance < price:
            await message.answer(RU.INSUFFICIENT_FUNDS)
        else:
            now = utcnow()
            actual_price = 0 if free_available else price
            if not free_available:
                user.balance -= price
            else:
                user.tutorial_free_boost_used = True
            user.updated_at = now
            session.add(UserItem(user_id=user.id, item_id=item_id))
            session.add(
                EconomyLog(
                    user_id=user.id,
                    type="buy_item",
                    amount=-actual_price,
                    meta={
                        "item": item.code,
                        **({"tutorial_free": True} if free_available else {}),
                    },
                    created_at=now,
                )
            )
            logger.info(
                "Item purchased",
                extra={
                    "tg_id": user.tg_id,
                    "user_id": user.id,
                    "item": item.code,
                    "tutorial_free": free_available,
                },
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
            main_text = RU.TUTORIAL_FREE_UPGRADE_DONE if free_available else RU.PURCHASE_OK
            await message.answer(f"{main_text}\n{next_hint}")
            await daily_task_on_event(message, session, user, "daily_shop")
            await tutorial_on_event(message, session, user, "upgrade_purchase")
        await notify_new_achievements(message, achievements)
    await state.set_state(ShopState.equipment)
    await render_items(message, state)


@router.message(ShopState.confirm_item, F.text == RU.BTN_CANCEL)
@safe_handler
async def shop_cancel_item(message: Message, state: FSMContext):
    await state.set_state(ShopState.equipment)
    await render_items(message, state)


# --- Пассивный доход ---

def render_passive_sources(owned: Dict[str, int]) -> str:
    """Return formatted list of passive income sources with purchase markers."""

    entries: List[str] = []
    for idx, source in enumerate(PASSIVE_SOURCES, 1):
        income = f"{format_money(source['income_per_min'])}{RU.CURRENCY}/мин"
        price = f"{format_money(source['price'])}{RU.CURRENCY}"
        lines = [
            f"{idx}. {source['title']}",
            f"📈 Мин. уровень: {source['min_level']} 💰 Доход: {income} 💵 Цена: {price}",
            source['description'],
        ]
        level_owned = owned.get(source["code"])
        if level_owned:
            lines.append(f"✅ Куплено (ур. {level_owned})")
        entries.append("\n".join(lines))
    body = "\n\n".join(entries)
    return f"{RU.PASSIVE_HEADER}\n\n{body}\n\n{RU.PASSIVE_PURCHASE_HINT}"


async def get_user_passive_levels(session: AsyncSession, user: User) -> Dict[str, int]:
    """Return dictionary of passive source code to level for the user."""

    rows = (
        await session.execute(
            select(UserPassiveSource.source_code, UserPassiveSource.level)
            .where(UserPassiveSource.user_id == user.id)
            .order_by(UserPassiveSource.id)
        )
    ).all()
    return {code: max(1, lvl or 1) for code, lvl in rows}


@router.message(Command("passive"))
@safe_handler
async def passive_overview(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        await notify_new_achievements(message, achievements)
        owned = await get_user_passive_levels(session, user)
        overview = render_passive_sources(owned)
        await message.answer(overview)


@router.message(Command("buy_passive"))
@safe_handler
async def passive_buy(message: Message, state: FSMContext):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer(RU.PASSIVE_USAGE)
        return
    try:
        index = int(args[1])
    except ValueError:
        await message.answer(RU.PASSIVE_USAGE)
        return
    if index < 1 or index > len(PASSIVE_SOURCES):
        await message.answer(RU.PASSIVE_UNKNOWN)
        return
    source = PASSIVE_SOURCES[index - 1]
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        await notify_new_achievements(message, achievements)
        existing = await session.scalar(
            select(UserPassiveSource)
            .where(
                UserPassiveSource.user_id == user.id,
                UserPassiveSource.source_code == source["code"],
            )
        )
        if existing:
            await message.answer(RU.PASSIVE_ALREADY)
            return
        if user.level < source["min_level"]:
            await message.answer(RU.PASSIVE_LOCKED.format(lvl=source["min_level"]))
            return
        price = int(source["price"])
        if user.balance < price:
            await message.answer(RU.INSUFFICIENT_FUNDS)
            return
        now = utcnow()
        user.balance -= price
        user.updated_at = now
        entry = UserPassiveSource(
            user_id=user.id,
            source_code=source["code"],
            level=1,
            purchased_at=now,
        )
        session.add(entry)
        session.add(
            EconomyLog(
                user_id=user.id,
                type="passive_purchase",
                amount=-price,
                meta={"source": source["code"]},
                created_at=now,
            )
        )
        income_display = format_money(source["income_per_min"])
        await message.answer(
            RU.PASSIVE_PURCHASED.format(name=source["title"], income=income_display)
        )
        owned = await get_user_passive_levels(session, user)
        await message.answer(render_passive_sources(owned))


# --- Команда ---

def fmt_team(sub: List[TeamMember], levels: Dict[int, int], costs: Dict[int, int]) -> str:
    lines = [RU.TEAM_HEADER]
    for i, m in enumerate(sub, 1):
        lvl = levels.get(m.id, 0)
        progress = team_income_per_min(m.base_income_per_min, lvl)
        lines.append(f"[{i}] {m.name}: {progress:.0f}/мин, ур. {lvl}, цена повышения {costs[m.id]} {RU.CURRENCY}")
    return "\n".join(lines)


async def render_team(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        tutorial_active = is_tutorial_active(user)
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        stats = await get_user_stats(session, user)
        members_all = (
            await session.execute(select(TeamMember).order_by(TeamMember.base_cost, TeamMember.id))
        ).scalars().all()
        members = [m for m in members_all if user.level >= max(1, m.min_level)]
        if not members:
            await state.clear()
            await message.answer(
                RU.TEAM_LOCKED,
                reply_markup=kb_upgrades_menu(
                    include_team=False, tutorial=tutorial_active
                ),
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
                reply_markup=kb_upgrades_menu(
                    include_team=False, tutorial=is_tutorial_active(user)
                ),
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
        if user.level < member.min_level:
            await message.answer("Сотрудник ещё не готов присоединиться — прокачайте уровень.")
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
    response_lines: List[str] = []
    next_cost_preview: Optional[int] = None
    member_name: Optional[str] = None
    current_level: Optional[int] = None
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        member = await session.scalar(select(TeamMember).where(TeamMember.id == mid))
        if not member:
            await message.answer("Сотрудник не найден.")
            await state.set_state(TeamState.browsing)
            await render_team(message, state)
            return
        member_name = member.name
        if user.level < member.min_level:
            await message.answer("Сначала достигните нужного уровня, чтобы работать с этим специалистом.")
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
            current_level = lvl
            next_cost_preview = cost
        else:
            now = utcnow()
            user.balance -= cost
            user.updated_at = now
            if not team_entry:
                session.add(UserTeam(user_id=user.id, member_id=mid, level=1))
                current_level = 1
            else:
                team_entry.level += 1
                current_level = team_entry.level
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
                    "level": current_level,
                },
            )
            await update_campaign_progress(session, user, "team_upgrade", {})
            await message.answer(RU.UPGRADE_OK)
            achievements.extend(await evaluate_achievements(session, user, {"team"}))
            next_cost_preview = apply_percentage_discount(
                member.base_cost * (1.22 ** current_level), discount_pct, cap=TEAM_DISCOUNT_CAP
            )
        await notify_new_achievements(message, achievements)
    await state.set_state(TeamState.confirm)
    await state.update_data(member_id=mid)
    if member_name is not None and current_level is not None:
        level_line = f"«{member_name}» — текущий уровень {current_level}."
        response_lines.append(level_line)
    if next_cost_preview:
        response_lines.append(f"Следующее повышение обойдётся в {format_money(next_cost_preview)} ₽.")
    if response_lines:
        # Обновлено: оставляем игрока на карточке сотрудника для повторной прокачки.
        response_lines.append("Нажмите «⚙️ Повысить» для продолжения или «Отмена» для выхода.")
        await message.answer("\n".join(response_lines), reply_markup=kb_confirm(RU.BTN_UPGRADE))


@router.message(TeamState.confirm, F.text == RU.BTN_CANCEL)
@safe_handler
async def team_upgrade_cancel(message: Message, state: FSMContext):
    await state.set_state(TeamState.browsing)
    await render_team(message, state)


# --- Гардероб ---

def fmt_inventory(
    user: User, items: List[Item], page: int, equipped_ids: Optional[Set[int]] = None
) -> str:
    """Render wardrobe entries with the same visual style as the shop."""

    text = fmt_items(
        user,
        items,
        page,
        include_price=False,
        equipped_ids=equipped_ids,
    )
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        items = (
            await session.execute(
                select(Item)
                .join(UserItem, UserItem.item_id == Item.id)
                .where(UserItem.user_id == user.id)
                .order_by(Item.slot, Item.tier)
            )
        ).scalars().all()
        equipped_ids = {
            row
            for row in (
                await session.execute(
                    select(UserEquipment.item_id).where(UserEquipment.user_id == user.id)
                )
            ).scalars()
            if row
        }
        page = int((await state.get_data()).get("page", 0))
        sub, has_prev, has_next = slice_page(items, page, 5)
        await message.answer(
            fmt_inventory(user, sub, page, equipped_ids),
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
        equipped_item: Optional[Item] = None
        current_eq = await session.scalar(
            select(UserEquipment).where(UserEquipment.user_id == user.id, UserEquipment.slot == it.slot)
        )
        if current_eq:
            equipped_item = await session.scalar(select(Item).where(Item.id == current_eq.item_id))
        prompt = format_item_equip_prompt(it, equipped_item)
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
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
        rank = rank_for(user.level, prestige.reputation)
        shield_charges = stats.get("event_shield_charges", 0)
        profile_lines = [
            f"👤 {display_name}",
            f"🏅 Ур. {user.level} · {rank}",
            "",
            "📊 Статистика",
            # Обновлено: секции с короткими маркерами вместо сплошного полотна.
            f"• XP: {user.xp}/{xp_need} {xp_bar} ({xp_pct}%)",
            f"• Баланс: {format_money(user.balance)} ₽ · Ср. доход: {format_money(avg_income)} ₽",
            f"• Сила клика: {format_stat(stats['cp'])} · Пассив: {passive_per_min} ₽/мин",
            "",
            "🎯 Прогресс",
            f"• Заказ: {order_str}",
            f"• Кампания: {campaign_text}",
            f"• Репутация: {prestige.reputation}",
            "",
            "🤝 Сообщество",
            f"• Баффы: {buffs_text}",
            f"• Приглашено друзей: {user.referrals_count}",
        ]
        if shield_charges > 0:
            profile_lines.append(f"• {RU.PROFILE_SHIELD.format(charges=shield_charges)}")
        await message.answer(
            "\n".join(profile_lines),
            reply_markup=kb_profile_menu(has_active_order=bool(active)),
        )
        if await tutorial_on_event(message, session, user, "profile_open"):
            await state.clear()
        await notify_new_achievements(message, achievements)


@router.message(F.text.in_(PROFILE_MENU_CATEGORY_LABELS))
@safe_handler
async def profile_category_header(message: Message, state: FSMContext):
    """Open a dedicated keyboard for the selected profile category."""

    category = (message.text or "").strip()
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        active = await get_active_order(session, user)
    prompt = PROFILE_CATEGORY_PROMPTS.get(category, RU.PROFILE_CATEGORY_PROMPT)
    await message.answer(
        prompt,
        reply_markup=kb_profile_menu(has_active_order=bool(active), category=category),
    )


@router.message(F.text == RU.BTN_PROFILE_BACK)
@safe_handler
async def profile_back_to_categories(message: Message, state: FSMContext):
    """Return to the root profile categories keyboard."""

    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        active = await get_active_order(session, user)
    await message.answer(
        RU.PROFILE_CATEGORY_PROMPT,
        reply_markup=kb_profile_menu(has_active_order=bool(active)),
    )


@router.message(F.text == RU.BTN_DAILY)
@safe_handler
async def profile_daily(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
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
        await tutorial_on_event(message, session, user, "daily_claim")


@router.message(F.text == RU.BTN_DAILIES)
@safe_handler
async def show_daily_tasks_menu(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
        state = ensure_daily_task_state(user)
        lines = [RU.DAILIES_HEADER, ""]
        all_done = True
        for task in DAILY_TASKS:
            entry = state.get(task["code"], {"progress": 0, "done": False})
            done = bool(entry.get("done"))
            if not done:
                all_done = False
            status = "✅" if done else "🔸"
            progress = int(entry.get("progress", 0))
            lines.append(
                RU.DAILIES_TASK_ROW.format(
                    status=status,
                    text=task["text"],
                    progress=progress,
                    goal=task["goal"],
                )
            )
        if all_done:
            lines.append("")
            lines.append(RU.DAILIES_EMPTY)
        markup = kb_profile_menu(
            has_active_order=bool(await get_active_order(session, user)),
            category=RU.BTN_PROFILE_CAT_STATS,
        )
        await message.answer("\n".join(lines), reply_markup=markup)
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_REFERRAL)
@safe_handler
async def show_referral_link(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
        me = await message.bot.get_me()
        username = me.username or ""
        if username:
            link = f"https://t.me/{username}?start={message.from_user.id}"
        else:
            link = f"https://t.me/share/url?url={message.from_user.id}"
        markup = kb_profile_menu(
            has_active_order=bool(await get_active_order(session, user)),
            category=RU.BTN_PROFILE_CAT_SOCIAL,
        )
        await message.answer(
            RU.REFERRAL_INVITE.format(
                link=link, rub=REFERRAL_BONUS_RUB, xp=REFERRAL_BONUS_XP
            ),
            reply_markup=markup,
        )
        await notify_new_achievements(message, achievements)


@router.message(F.text == RU.BTN_QUEST)
@safe_handler
async def quest_entry(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        await present_quest_selection(message, state, session, user)
        await notify_new_achievements(message, achievements)


@router.message(QuestState.selecting)
@safe_handler
async def quest_select(message: Message, state: FSMContext):
    if (message.text or "").strip() == RU.BTN_BACK:
        await state.clear()
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    data = await state.get_data()
    mapping: Dict[str, str] = data.get("quest_choices", {}) or {}
    quest_code = mapping.get((message.text or "").strip())
    if not quest_code:
        await message.answer(RU.QUEST_OPTION_UNKNOWN)
        return
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        definition = QUEST_DEFINITIONS.get(quest_code)
        if not definition:
            await message.answer(RU.QUEST_OPTION_UNKNOWN)
            return
        quest = await get_or_create_quest(session, user, quest_code)
        if quest.is_done:
            await message.answer(
                RU.QUEST_ALREADY_DONE.format(name=definition.get("name", quest_code))
            )
            await present_quest_selection(message, state, session, user)
            return
        quest_get_stage_payload(quest, definition)
        stage_key = quest_current_stage_key(quest, definition)
        if not stage_key:
            await state.update_data(active_quest=None)
            await finalize_quest(session, user, quest, message, state, definition)
            current = await state.get_state()
            if current in {QuestState.selecting.state, QuestState.playing.state}:
                await state.clear()
            return
        await state.set_state(QuestState.playing)
        await state.update_data(active_quest=quest_code, quest_choices=mapping)
        await message.answer(
            RU.QUEST_START.format(name=definition.get("name", quest_code))
        )
        await send_quest_step(message, quest_code, stage_key)


@router.message(QuestState.playing)
@safe_handler
async def quest_playing(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == RU.BTN_BACK:
        async with session_scope() as session:
            user = await ensure_user_loaded(session, message)
            if not user:
                await state.clear()
                return
            await present_quest_selection(message, state, session, user)
        return
    data = await state.get_data()
    quest_code = data.get("active_quest")
    if not quest_code:
        await state.clear()
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    definition = QUEST_DEFINITIONS.get(quest_code)
    if not definition:
        await state.clear()
        await message.answer(
            RU.MENU_HINT,
            reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
        )
        return
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        quest = await get_or_create_quest(session, user, quest_code)
        if quest.is_done:
            await state.update_data(active_quest=None)
            await message.answer(
                RU.QUEST_ALREADY_DONE.format(name=definition.get("name", quest_code))
            )
            await present_quest_selection(message, state, session, user)
            return
        stage_key = quest_current_stage_key(quest, definition)
        if not stage_key:
            await state.update_data(active_quest=None)
            await finalize_quest(session, user, quest, message, state, definition)
            current = await state.get_state()
            if current in {QuestState.selecting.state, QuestState.playing.state}:
                await state.clear()
            return
        step = definition.get("flow", {}).get(stage_key, {})
        options = step.get("options", [])
        choice = next((opt for opt in options if opt.get("text") == text), None)
        if not choice:
            await message.answer(RU.QUEST_OPTION_UNKNOWN)
            return
        payload = quest_get_stage_payload(quest, definition)
        for key, delta in (choice.get("delta") or {}).items():
            payload[key] = payload.get(key, 0) + int(delta)
        quest.payload = payload
        next_stage = choice.get("next")
        if next_stage == "finale":
            await state.update_data(active_quest=None)
            await finalize_quest(session, user, quest, message, state, definition)
            current = await state.get_state()
            if current in {QuestState.selecting.state, QuestState.playing.state}:
                await state.clear()
            return
        target_key: Optional[str]
        index = quest_stage_index(definition, next_stage or "") if next_stage else None
        if index is not None:
            quest.stage = index
            target_key = next_stage
        else:
            quest.stage = min(quest.stage + 1, max(len(quest_stage_keys(definition)) - 1, 0))
            target_key = quest_current_stage_key(quest, definition)
        await state.update_data(active_quest=quest_code)
        if target_key:
            await send_quest_step(message, quest_code, target_key)
        else:
            await message.answer(
                RU.MENU_HINT,
                reply_markup=await build_main_menu_markup(tg_id=message.from_user.id),
            )
            await state.clear()


@router.message(F.text == RU.BTN_SKILLS)
@safe_handler
async def show_skills_menu(message: Message):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, None, idle_result)
        rows = await fetch_average_income_rows(session)
        active = await get_active_order(session, user)
        await notify_new_achievements(message, achievements)
    markup = kb_profile_menu(
        has_active_order=bool(active),
        category=RU.BTN_PROFILE_CAT_SOCIAL,
    )
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
        idle_result = await process_offline_income(session, user, achievements_new)
        await handle_idle_completion(message, session, user, None, idle_result)
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
    markup = kb_profile_menu(
        has_active_order=bool(active),
        category=RU.BTN_PROFILE_CAT_PROGRESS,
    )
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
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        progress = await get_campaign_progress_entry(session, user)
        definition = get_campaign_definition(progress.chapter)
        active = await get_active_order(session, user)
        goal = definition.get("goal", {}) if definition else {}
        pct = int(campaign_goal_progress(goal, progress.progress or {}) * 100) if definition else 0
        min_level = definition.get("min_level", 1) if definition else 1
        markup_profile = kb_profile_menu(
            has_active_order=bool(active),
            category=RU.BTN_PROFILE_CAT_LONG_TERM,
        )
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
                    has_active_order=bool(await get_active_order(session, user)),
                    category=RU.BTN_PROFILE_CAT_LONG_TERM,
                ),
            )
            return
        text, prev_level, levels_gained = result
        markup = kb_profile_menu(
            has_active_order=bool(await get_active_order(session, user)),
            category=RU.BTN_PROFILE_CAT_LONG_TERM,
        )
        await message.answer(text, reply_markup=markup)
        await maybe_prompt_skill_choice(session, message, state, user, prev_level, levels_gained)
        if levels_gained:
            await notify_level_up_message(message, session, user, prev_level, levels_gained)


@router.message(Command("studio"))
@router.message(F.text == RU.BTN_STUDIO)
@safe_handler
async def show_studio(message: Message, state: FSMContext):
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        achievements: List[Tuple[Achievement, UserAchievement]] = []
        idle_result = await process_offline_income(session, user, achievements)
        await handle_idle_completion(message, session, user, state, idle_result)
        active = await get_active_order(session, user)
        profile_markup = kb_profile_menu(
            has_active_order=bool(active),
            category=RU.BTN_PROFILE_CAT_LONG_TERM,
        )
        if user.level < 20:
            await message.answer(RU.STUDIO_LOCKED, reply_markup=profile_markup)
            return
        prestige = await get_prestige_entry(session, user)
        total_earned = await calc_total_earned(session, user)
        gain = await calc_prestige_gain(session, user, total_earned=total_earned)
        bonus = (prestige.reputation) * 1
        text = RU.STUDIO_INFO.format(rep=prestige.reputation, resets=prestige.resets, bonus=bonus)
        if gain > 0:
            text += "\n\n" + RU.STUDIO_CONFIRM.format(gain=gain)
            await state.set_state(StudioState.confirm)
            await state.update_data(gain=gain, total_earned=total_earned)
            markup = kb_confirm(RU.BTN_STUDIO_CONFIRM)
        else:
            markup = profile_markup
        logger.info(
            "Prestige preview",
            extra={
                "tg_id": user.tg_id,
                "user_id": user.id,
                "prestige_gain": gain,
                "total_earned": round(total_earned, 2),
            },
        )
        await message.answer(text, reply_markup=markup)
        await notify_new_achievements(message, achievements)


@router.message(StudioState.confirm, F.text == RU.BTN_STUDIO_CONFIRM)
@safe_handler
async def confirm_studio(message: Message, state: FSMContext):
    data = await state.get_data()
    gain = int(data.get("gain", 0))
    stored_total = data.get("total_earned")
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            await state.clear()
            return
        total_earned = float(stored_total) if stored_total is not None else await calc_total_earned(session, user)
        gain = await calc_prestige_gain(session, user, total_earned=total_earned)
        await perform_prestige_reset(session, user, gain, total_earned)
        markup = kb_profile_menu(
            has_active_order=bool(await get_active_order(session, user)),
            category=RU.BTN_PROFILE_CAT_LONG_TERM,
        )
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


# --- Админ-команды: тест-план ---


def _is_base_admin(message: Message) -> bool:
    return bool(SETTINGS.BASE_ADMIN_ID) and message.from_user and message.from_user.id == SETTINGS.BASE_ADMIN_ID


@router.message(Command("roll_trend"))
@safe_handler
async def admin_roll_trend(message: Message):
    if not _is_base_admin(message):
        return
    async with session_scope() as session:
        user = await get_user_by_tg(session, message.from_user.id)
        level_hint = user.level if user else None
        trend = await roll_new_trend(session, user_level_hint=level_hint)
        order = await session.scalar(select(Order).where(Order.id == trend["order_id"]))
    title = order.title if order else f"#{trend['order_id']}"
    expires = trend["valid_until"].strftime("%d.%m %H:%M")
    await message.answer(
        f"🔥 Новый тренд: {title} до {expires}, ×{format_stat(trend['reward_mul'])}"
    )


@router.message(Command("give_shield"))
@safe_handler
async def admin_give_shield(message: Message):
    if not _is_base_admin(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /give_shield N")
        return
    amount = max(0, int(parts[1]))
    if amount <= 0:
        await message.answer("Укажите количество > 0.")
        return
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        boost = await session.scalar(select(Boost).where(Boost.code == EVENT_SHIELD_CODE))
        if not boost:
            await message.answer("Усиление страховки не найдено.")
            return
        entry = await get_user_boost_by_code(session, user, EVENT_SHIELD_CODE)
        if not entry:
            entry = UserBoost(user_id=user.id, boost_id=boost.id, level=amount)
            session.add(entry)
        else:
            entry.level += amount
        user.updated_at = utcnow()
        logger.info(
            "Event shield granted",
            extra={"tg_id": user.tg_id, "user_id": user.id, "amount": amount, "total": entry.level},
        )
        await message.answer(f"🛡️ Страховка: теперь {entry.level} заряд(ов).")


@router.message(Command("test_event_choice"))
@safe_handler
async def admin_test_event_choice(message: Message):
    if not _is_base_admin(message):
        return
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        event = await session.scalar(select(RandomEvent).where(RandomEvent.code == "spill_choice"))
        if not event:
            await message.answer("Интерактивное событие не найдено.")
            return
        payload = await apply_random_event(session, user, event, "admin_test")
    if payload:
        text, markup = payload
        if text and text.strip():
            await message.answer(text, reply_markup=markup or kb_active_order_controls())


@router.message(Command("prestige_preview"))
@safe_handler
async def admin_prestige_preview(message: Message):
    if not _is_base_admin(message):
        return
    async with session_scope() as session:
        user = await ensure_user_loaded(session, message)
        if not user:
            return
        total_earned = await calc_total_earned(session, user)
        gain = await calc_prestige_gain(session, user, total_earned=total_earned)
    await message.answer(
        f"📊 Всего заработано: {format_money(total_earned)} ₽ → престиж: {gain}"
    )


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
        if hasattr(active, "auto_progress_buffer"):
            active.auto_progress_buffer = 0.0
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
        QuestState.selecting.state,
        QuestState.playing.state,
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
            tutorial_active = is_tutorial_active(user)
        await message.answer(
            RU.UPGRADES_HEADER,
            reply_markup=kb_upgrades_menu(
                include_team=include_team, tutorial=tutorial_active
            ),
        )
        return
    if current in {ShopState.boosts.state, ShopState.equipment.state}:
        await state.set_state(ShopState.root)
        async with session_scope() as session:
            user = await ensure_user_loaded(session, message)
        await message.answer(
            RU.SHOP_HEADER,
            reply_markup=kb_shop_menu(tutorial=is_tutorial_active(user)),
        )
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
            tutorial_active = is_tutorial_active(user)
        await message.answer(
            RU.UPGRADES_HEADER,
            reply_markup=kb_upgrades_menu(
                include_team=include_team, tutorial=tutorial_active
            ),
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
            tutorial_active = is_tutorial_active(user)
        await message.answer(
            RU.UPGRADES_HEADER,
            reply_markup=kb_upgrades_menu(
                include_team=include_team, tutorial=tutorial_active
            ),
        )
        return
    if current in {
        SkillsState.picking.state,
        QuestState.selecting.state,
        QuestState.playing.state,
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

    # Middleware обучения и анти-флуда
    dp.message.middleware(TutorialGateMiddleware())
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
