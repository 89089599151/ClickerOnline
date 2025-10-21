# -*- coding: utf-8 -*-
"""
Designer Clicker Bot — streamlined single-file edition.
=======================================================
Этот файл содержит полностью переработанный Telegram-кликер с упором на
прозрачную экономику, отсутствие спама сообщений и чистую архитектуру.

Основные особенности:
* Только стандартная библиотека + aiogram + SQLAlchemy (async) + aiosqlite.
* Встроенный message coalescer и дедупликация сервисных ответов.
* Общие утилиты рендеринга (пагинация, выбор по цифре, подтверждения).
* Экономика с мягкими капами, diminishing returns и несколькими билдами.
* Единый safe-handler, защита от гонок и устойчивость к ошибкам ввода.
* Быстрый старт: первый заказ выдаётся автоматически на /start.
* Мини-самотест quick_sanity_test() для оценки баланса без запуска бота.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import secrets
import textwrap
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:  # pragma: no cover - отсутствие dotenv не критично
    pass

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = [
    "main",
    "quick_sanity_test",
]


# ---------------------------------------------------------------------------
# Настройки и логирование
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Settings:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./designer.db")
    DAILY_BONUS_RUB: int = int(os.getenv("DAILY_BONUS_RUB", "120"))
    BASE_ADMIN_ID: int = int(os.getenv("BASE_ADMIN_ID", "0"))


SETTINGS = Settings()

logger = logging.getLogger("designer_clicker_bot")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Экономика, константы и базовые формулы
# ---------------------------------------------------------------------------


ECON: Dict[str, Any] = {
    # Кликовая экономика
    "base_click_rub": 5.0,
    "click_level_bonus": 0.22,
    "click_upgrade_cost": 90.0,
    "click_upgrade_growth": 1.32,
    "crit_chance_base": 0.05,
    "crit_multiplier_base": 1.8,
    "crit_softcap": 0.45,
    "crit_diminishing_scale": 0.18,
    # Пассивные доходы
    "passive_base_per_min": 12.0,
    "passive_growth": 1.28,
    "passive_softcap_per_min": 900.0,
    "offline_cap_hours": 8,
    # Заказы
    "order_initial_required": 60,
    "order_growth": 1.22,
    "order_reward_mul": 2.4,
    "order_softcap_reward": 2_400.0,
    "order_softcap_scale": 3.6,
    "order_exp_gain": 12,
    # Прогресс и антиспам
    "progress_message_clicks": 4,
    "progress_message_interval_ms": 1400,
    # Boost economy
    "boost_cost_growth": 1.55,
    "boost_add_stack_cap": 5,
    "boost_mul_cap": 4.0,
    "boost_duration_minutes": 25,
    # Экипировка
    "equipment_slots": ("tool", "outfit", "desk"),
    "equipment_softcap_bonus": 3.8,
    # Престиж
    "prestige_divisor": 1200.0,
    "prestige_softcap": 180.0,
    # Пагинация
    "list_page_size": 5,
}

RARITY_ORDER = ["обычное", "редкое", "эпическое", "легендарное"]
RARITY_COLORS = {
    "обычное": "⚪",
    "редкое": "🟢",
    "эпическое": "🔵",
    "легендарное": "🟣",
}


def diminishing_return(raw: float, cap: float, scale: float) -> float:
    if raw <= 0:
        return 0.0
    return cap * (1.0 - math.exp(-raw / max(scale, 1e-6)))


def format_number(value: float) -> str:
    if value >= 1_000_000:
        return f"{value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value/1_000:.2f}K"
    if value == int(value):
        return f"{int(value)}"
    return f"{value:.2f}"


def format_currency(value: float) -> str:
    return f"💳 {format_number(value)} ₽"


def format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


# ---------------------------------------------------------------------------
# SQLAlchemy модели
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class PlayerModel(Base):
    __tablename__ = "players"

    tg_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )
    nickname: Mapped[str] = mapped_column(String(64), default="")


# ---------------------------------------------------------------------------
# Контент: апгрейды, бусты, экипировка
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class UpgradeDef:
    id: str
    title: str
    description: str
    base_cost: float
    growth: float
    max_level: int
    type: str


UPGRADES: Dict[str, UpgradeDef] = {
    "click_power": UpgradeDef(
        id="click_power",
        title="Сила клика",
        description="Увеличивает базовый доход от клика.",
        base_cost=ECON["click_upgrade_cost"],
        growth=ECON["click_upgrade_growth"],
        max_level=60,
        type="click",
    ),
    "passive_agency": UpgradeDef(
        id="passive_agency",
        title="Фриланс-команда",
        description="Пассивно приносит ₽ каждые 10 минут.",
        base_cost=320.0,
        growth=1.42,
        max_level=40,
        type="passive",
    ),
    "crit_brief": UpgradeDef(
        id="crit_brief",
        title="Смелые брифы",
        description="Повышает шанс критического клика.",
        base_cost=210.0,
        growth=1.38,
        max_level=35,
        type="crit",
    ),
    "discount_supplier": UpgradeDef(
        id="discount_supplier",
        title="Любимый подрядчик",
        description="Даёт постоянную скидку на бусты и экипировку.",
        base_cost=650.0,
        growth=1.55,
        max_level=25,
        type="discount",
    ),
}


@dataclass(frozen=True, slots=True)
class BoostDef:
    id: str
    title: str
    description: str
    base_cost: float
    add_click: float = 0.0
    mul_click: float = 0.0
    crit_bonus: float = 0.0
    passive_bonus: float = 0.0
    duration_minutes: Optional[int] = ECON["boost_duration_minutes"]
    max_stacks: int = 3
    stack_mode: str = "add"


BOOSTS: Dict[str, BoostDef] = {
    "espresso": BoostDef(
        id="espresso",
        title="Эспрессо x2",
        description="Мощный, но короткий рывок — добавляет кликовую силу.",
        base_cost=180.0,
        add_click=12.0,
        duration_minutes=15,
        max_stacks=3,
        stack_mode="add",
    ),
    "creative_flow": BoostDef(
        id="creative_flow",
        title="Поток креатива",
        description="Мультипликативно увеличивает доход от кликов.",
        base_cost=260.0,
        mul_click=0.35,
        max_stacks=4,
        stack_mode="add",
    ),
    "vip_client": BoostDef(
        id="vip_client",
        title="VIP-клиент",
        description="Повышает пассивный доход и шанс крита.",
        base_cost=420.0,
        passive_bonus=0.55,
        crit_bonus=0.12,
        duration_minutes=40,
        max_stacks=2,
    ),
}


@dataclass(frozen=True, slots=True)
class EquipmentItem:
    id: str
    slot: str
    rarity: str
    title: str
    description: str
    add_click: float = 0.0
    mul_click: float = 0.0
    crit_bonus: float = 0.0
    passive_bonus: float = 0.0
    synergy: str = ""
    cost: float = 0.0


EQUIPMENT: Dict[str, EquipmentItem] = {
    "stylus_precision": EquipmentItem(
        id="stylus_precision",
        slot="tool",
        rarity="редкое",
        title="Эргономичный стилус",
        description="Постоянно усиливает базовый клик.",
        add_click=6.0,
        cost=340.0,
        synergy="Стабильный",
    ),
    "tablet_zen": EquipmentItem(
        id="tablet_zen",
        slot="tool",
        rarity="эпическое",
        title="Планшет дзен",
        description="Повышает шанс крита и слегка множит доход.",
        mul_click=0.12,
        crit_bonus=0.07,
        cost=680.0,
        synergy="Крит",
    ),
    "hoodie_trend": EquipmentItem(
        id="hoodie_trend",
        slot="outfit",
        rarity="редкое",
        title="Трендовый худи",
        description="Добавляет шанс крита и пассивный доход.",
        crit_bonus=0.05,
        passive_bonus=0.25,
        cost=520.0,
        synergy="Крит",
    ),
    "apron_productive": EquipmentItem(
        id="apron_productive",
        slot="outfit",
        rarity="обычное",
        title="Рабочий фартук",
        description="Сбалансированное повышение кликовой силы.",
        add_click=4.0,
        mul_click=0.08,
        cost=280.0,
        synergy="Стабильный",
    ),
    "desk_minimal": EquipmentItem(
        id="desk_minimal",
        slot="desk",
        rarity="обычное",
        title="Минималистичный стол",
        description="Чуть больше пассивного дохода и кликов.",
        add_click=3.0,
        passive_bonus=0.18,
        cost=320.0,
        synergy="Стабильный",
    ),
    "desk_rgb": EquipmentItem(
        id="desk_rgb",
        slot="desk",
        rarity="эпическое",
        title="RGB-баттлстейшн",
        description="Множитель клика для агрессивных билдов.",
        mul_click=0.2,
        cost=880.0,
        synergy="Крит",
    ),
}


# ---------------------------------------------------------------------------
# Состояние игрока и экономика
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class BoostInstance:
    id: str
    stacks: int
    expires_at: Optional[float]

    @property
    def is_active(self) -> bool:
        return self.expires_at is None or self.expires_at > time.time()


@dataclass(slots=True)
class OrderState:
    id: str
    title: str
    required: int
    reward: float
    progress: int = 0
    expires_at: Optional[float] = None

    def progress_pct(self) -> float:
        return min(1.0, self.progress / max(self.required, 1))

    def is_done(self) -> bool:
        return self.progress >= self.required


@dataclass(slots=True)
class PlayerState:
    tg_id: int
    rub: float = 0.0
    prestige: float = 0.0
    click_level: int = 1
    passive_level: int = 0
    upgrade_levels: Dict[str, int] = field(default_factory=dict)
    boosts: Dict[str, BoostInstance] = field(default_factory=dict)
    inventory: Dict[str, int] = field(default_factory=dict)
    equipment: Dict[str, str] = field(default_factory=dict)
    order: Optional[OrderState] = None
    stats: Dict[str, float] = field(
        default_factory=lambda: {
            "clicks": 0,
            "orders": 0,
            "rub_earned": 0.0,
            "rub_spent": 0.0,
            "boosts_used": 0,
            "prestige_runs": 0,
            "last_progress_click": 0,
        }
    )
    ui_state: Dict[str, Any] = field(default_factory=dict)
    nickname: str = ""

    _cache: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    @classmethod
    def from_dict(cls, tg_id: int, raw: Dict[str, Any]) -> "PlayerState":
        order = raw.get("order")
        order_state = None
        if order:
            order_state = OrderState(
                id=order.get("id", ""),
                title=order.get("title", ""),
                required=int(order.get("required", 1)),
                reward=float(order.get("reward", 0.0)),
                progress=int(order.get("progress", 0)),
                expires_at=order.get("expires_at"),
            )
        boosts = {
            bid: BoostInstance(id=bid, stacks=val.get("stacks", 1), expires_at=val.get("expires_at"))
            for bid, val in raw.get("boosts", {}).items()
        }
        defaults = PlayerState(0).stats
        stats = {key: raw.get("stats", {}).get(key, default) for key, default in defaults.items()}
        return cls(
            tg_id=tg_id,
            rub=float(raw.get("rub", 0.0)),
            prestige=float(raw.get("prestige", 0.0)),
            click_level=int(raw.get("click_level", 1)),
            passive_level=int(raw.get("passive_level", 0)),
            upgrade_levels={k: int(v) for k, v in raw.get("upgrade_levels", {}).items()},
            boosts=boosts,
            inventory={k: int(v) for k, v in raw.get("inventory", {}).items()},
            equipment={k: str(v) for k, v in raw.get("equipment", {}).items()},
            order=order_state,
            stats=stats,
            ui_state=raw.get("ui_state", {}),
            nickname=raw.get("nickname", ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "rub": self.rub,
            "prestige": self.prestige,
            "click_level": self.click_level,
            "passive_level": self.passive_level,
            "upgrade_levels": self.upgrade_levels,
            "boosts": {
                bid: {"stacks": inst.stacks, "expires_at": inst.expires_at}
                for bid, inst in self.boosts.items()
                if inst.is_active
            },
            "inventory": self.inventory,
            "equipment": self.equipment,
            "stats": self.stats,
            "ui_state": self.ui_state,
            "nickname": self.nickname,
        }
        if self.order:
            payload["order"] = {
                "id": self.order.id,
                "title": self.order.title,
                "required": self.order.required,
                "reward": self.order.reward,
                "progress": self.order.progress,
                "expires_at": self.order.expires_at,
            }
        return payload

    def ensure_order(self) -> None:
        if self.order and not self.order.is_done():
            return
        level_factor = max(1, self.click_level + self.passive_level // 2)
        reward_base = self.click_power_raw() * ECON["order_reward_mul"]
        reward = reward_base * (1 + 0.12 * level_factor)
        reward = diminishing_return(reward, ECON["order_softcap_reward"], ECON["order_softcap_scale"])
        required = int(
            ECON["order_initial_required"] * (ECON["order_growth"] ** max(0, self.stats.get("orders", 0)))
        )
        required = max(required, 25)
        title = random.choice(
            [
                "Редизайн лендинга",
                "Срочный баннер",
                "Концепт логотипа",
                "Гайдлайны бренда",
                "Питч-дек",
                "Арт-дирекшн",
            ]
        )
        self.order = OrderState(
            id=f"order-{int(time.time())}",
            title=title,
            required=required,
            reward=reward,
            progress=0,
            expires_at=time.time() + 3600 * 12,
        )

    def click_power_raw(self) -> float:
        base = ECON["base_click_rub"] * (1 + (self.click_level - 1) * ECON["click_level_bonus"])
        base += self.upgrade_levels.get("click_power", 0) * 2.5
        return base

    def discount_rate(self) -> float:
        raw = 0.0
        if "discount_supplier" in self.upgrade_levels:
            raw += 0.03 * self.upgrade_levels["discount_supplier"]
        equip_bonus = 0.0
        for item_id in self.equipment.values():
            item = EQUIPMENT.get(item_id)
            if item:
                equip_bonus += item.passive_bonus * 0.05
        return min(0.35, diminishing_return(raw + equip_bonus, 0.35, 0.2))

    def passive_income_per_minute(self) -> float:
        base = ECON["passive_base_per_min"] * (1 + self.passive_level * 0.1)
        base += self.upgrade_levels.get("passive_agency", 0) * 18
        boost_bonus = 0.0
        equip_bonus = 0.0
        for boost in self.active_boosts():
            boost_bonus += boost.passive_bonus
        for item_id in self.equipment.values():
            item = EQUIPMENT.get(item_id)
            if item:
                equip_bonus += item.passive_bonus
        total = base * (1 + diminishing_return(boost_bonus + equip_bonus, 2.8, 0.8))
        return min(total, ECON["passive_softcap_per_min"])

    def active_boosts(self) -> Iterable[BoostDef]:
        now = time.time()
        for bid, inst in list(self.boosts.items()):
            if inst.expires_at and inst.expires_at <= now:
                del self.boosts[bid]
                continue
            definition = BOOSTS.get(bid)
            if not definition:
                continue
            stacks = min(inst.stacks, definition.max_stacks, ECON["boost_add_stack_cap"])
            for _ in range(stacks):
                yield definition

    def crit_chance(self) -> float:
        raw = ECON["crit_chance_base"]
        raw += 0.01 * self.upgrade_levels.get("crit_brief", 0)
        for boost in self.active_boosts():
            raw += boost.crit_bonus
        for item_id in self.equipment.values():
            item = EQUIPMENT.get(item_id)
            if item:
                raw += item.crit_bonus
        return min(0.8, diminishing_return(raw, ECON["crit_softcap"], ECON["crit_diminishing_scale"]))

    def click_multiplier_bonus(self) -> float:
        additive = 0.0
        for boost in self.active_boosts():
            additive += boost.mul_click
            additive += boost.add_click / 50.0
        for item_id in self.equipment.values():
            item = EQUIPMENT.get(item_id)
            if item:
                additive += item.mul_click
        additive = diminishing_return(additive, ECON["boost_mul_cap"], 1.4)
        return 1 + additive

    def flat_click_bonus(self) -> float:
        flat = 0.0
        for boost in self.active_boosts():
            flat += boost.add_click
        for item_id in self.equipment.values():
            item = EQUIPMENT.get(item_id)
            if item:
                flat += item.add_click
        return flat

    def click_gain(self) -> float:
        base = self.click_power_raw() + self.flat_click_bonus()
        crit_chance = self.crit_chance()
        crit_multiplier = 1 + diminishing_return(ECON["crit_multiplier_base"], 1.8, 0.8)
        expected = base * (1 + (crit_multiplier - 1) * crit_chance)
        expected *= self.click_multiplier_bonus()
        return expected

    def register_click(self) -> Tuple[float, bool]:
        self.ensure_order()
        if not self.order:
            return 0.0, False
        gain = self.click_gain()
        self.order.progress += 1
        self.stats["clicks"] += 1
        completed = False
        if self.order.is_done():
            completed = True
            self.complete_order()
        return gain, completed

    def complete_order(self) -> float:
        if not self.order:
            return 0.0
        reward = self.order.reward
        self.rub += reward
        self.stats["rub_earned"] += reward
        self.stats["orders"] += 1
        prestige_gain = diminishing_return(reward / ECON["prestige_divisor"], ECON["prestige_softcap"], 30.0)
        self.prestige += prestige_gain
        self.order = None
        self.ensure_order()
        return reward

    def can_afford(self, amount: float) -> bool:
        return self.rub + 1e-6 >= amount

    def spend(self, amount: float) -> bool:
        if not self.can_afford(amount):
            return False
        self.rub -= amount
        self.stats["rub_spent"] += amount
        return True

    def add_boost(self, boost_id: str) -> Tuple[bool, str]:
        definition = BOOSTS.get(boost_id)
        if not definition:
            return False, "Такого буста нет."
        discount = 1 - self.discount_rate()
        cost = definition.base_cost * discount * (ECON["boost_cost_growth"] ** max(0, self.stats.get("boosts_used", 0)))
        cost = max(60.0, cost)
        if not self.spend(cost):
            return False, "Не хватает ₽."
        now = time.time()
        instance = self.boosts.get(boost_id)
        duration = definition.duration_minutes
        expires_at = None if duration is None else now + duration * 60
        if instance and instance.is_active:
            instance.stacks = min(instance.stacks + 1, definition.max_stacks)
            instance.expires_at = expires_at
        else:
            self.boosts[boost_id] = BoostInstance(id=boost_id, stacks=1, expires_at=expires_at)
        self.stats["boosts_used"] += 1
        return True, f"Активирован буст «{definition.title}»."

    def buy_equipment(self, item_id: str) -> Tuple[bool, str]:
        item = EQUIPMENT.get(item_id)
        if not item:
            return False, "Предмет не найден."
        discount = 1 - self.discount_rate()
        cost = item.cost * discount
        if not self.spend(cost):
            return False, "Не хватает ₽ для покупки."
        self.inventory[item_id] = self.inventory.get(item_id, 0) + 1
        return True, f"Получен предмет: {item.title}."

    def equip_item(self, item_id: str) -> Tuple[bool, str]:
        if self.inventory.get(item_id, 0) <= 0:
            return False, "Сначала купите предмет."
        item = EQUIPMENT.get(item_id)
        if not item:
            return False, "Предмет не найден."
        if item.slot not in ECON["equipment_slots"]:
            return False, "Слот не поддерживается."
        if self.equipment.get(item.slot) == item_id:
            return False, "Предмет уже экипирован."
        self.equipment[item.slot] = item_id
        return True, f"Экипирован {item.title}."

    def unequip_slot(self, slot: str) -> Tuple[bool, str]:
        if slot not in self.equipment:
            return False, "Слот уже свободен."
        del self.equipment[slot]
        return True, "Слот освобождён."

    def upgrade(self, upgrade_id: str) -> Tuple[bool, str]:
        definition = UPGRADES.get(upgrade_id)
        if not definition:
            return False, "Улучшение не найдено."
        current = self.upgrade_levels.get(upgrade_id, 0)
        if current >= definition.max_level:
            return False, "Достигнут максимальный уровень."
        cost = definition.base_cost * (definition.growth ** current)
        cost *= 1 - self.discount_rate() * 0.5
        if not self.spend(cost):
            return False, "Недостаточно средств."
        self.upgrade_levels[upgrade_id] = current + 1
        if definition.type == "click":
            self.click_level += 1
        elif definition.type == "passive":
            self.passive_level += 1
        elif definition.type == "crit":
            self.stats["crit_path"] = self.stats.get("crit_path", 0) + 1
        return True, f"Улучшение «{definition.title}» повышено до {current + 1} уровня."


# ---------------------------------------------------------------------------
# UX и меню
# ---------------------------------------------------------------------------


MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🖱️ Клик"), KeyboardButton(text="📋 Заказы")],
        [KeyboardButton(text="🛠️ Улучшения"), KeyboardButton(text="🛒 Магазин")],
        [KeyboardButton(text="👥 Команда"), KeyboardButton(text="🎽 Гардероб")],
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🗓️ Задания дня")],
    ],
    resize_keyboard=True,
)


def render_list_page(
    items: Sequence[Any],
    page: int,
    page_size: int,
    formatter: Callable[[Any, int], str],
) -> Tuple[str, InlineKeyboardMarkup]:
    total_pages = max(1, math.ceil(len(items) / max(page_size, 1)))
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    sliced = items[start : start + page_size]
    lines: List[str] = []
    buttons: List[List[InlineKeyboardButton]] = []
    for idx, item in enumerate(sliced, start=1):
        lines.append(f"{idx}. {formatter(item, start + idx - 1)}")
        buttons.append([InlineKeyboardButton(text=str(idx), callback_data=f"pick:{idx}")])
    nav_row: List[InlineKeyboardButton] = []
    if total_pages > 1:
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"page:{page - 1}"))
        nav_row.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"page:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)
    if not buttons:
        buttons = [[InlineKeyboardButton(text="⏪", callback_data="noop")]]
    text = "\n".join(lines) if lines else "Ничего не найдено."
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


async def handle_numeric_selection(
    ctx: "HandlerContext",
    state_key: str,
    list_key: str,
    on_pick: Callable[["HandlerContext", Any], Awaitable[None]],
) -> bool:
    if ctx.raw_text not in {str(i) for i in range(1, ECON["list_page_size"] + 1)}:
        return False
    try:
        page = int(ctx.player.ui_state.get(state_key, 0))
        list_data = ctx.player.ui_state.get(list_key, [])
    except Exception:
        return False
    page_size = int(ctx.player.ui_state.get(f"{list_key}_page_size", ECON["list_page_size"]))
    index = page * page_size + (int(ctx.raw_text) - 1)
    if index < 0 or index >= len(list_data):
        ctx.reply.add_line("Нет такого варианта. Попробуйте снова.")
        return True
    item = list_data[index]
    await on_pick(ctx, item)
    return True


_confirmations: Dict[str, Tuple[int, Callable[["HandlerContext"], Awaitable[None]]]] = {}


def confirm_action(
    ctx: "HandlerContext",
    title: str,
    details: str,
    on_confirm: Callable[["HandlerContext"], Awaitable[None]],
) -> None:
    token = secrets.token_urlsafe(8)
    _confirmations[token] = (ctx.player.tg_id, on_confirm)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm:{token}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="confirm:cancel")],
        ]
    )
    ctx.reply.add_line(f"❓ {title}")
    ctx.reply.add_line(details)
    ctx.reply.set_markup(markup)


def format_item_card(item: Any, player: Optional[PlayerState] = None) -> str:
    if isinstance(item, UpgradeDef):
        level = player.upgrade_levels.get(item.id, 0) if player else 0
        cost = item.base_cost * (item.growth ** level)
        return (
            f"{item.title} — {item.description}\n"
            f"Уровень: {level}/{item.max_level} | Цена: {format_currency(cost)}"
        )
    if isinstance(item, BoostDef):
        return (
            f"{item.title} — {item.description}\n"
            f"Стоимость: {format_currency(item.base_cost)} | Стаки: до {item.max_stacks}"
        )
    if isinstance(item, EquipmentItem):
        color = RARITY_COLORS.get(item.rarity, "⚪")
        return (
            f"{color} {item.title} ({item.rarity}) — {item.description}\n"
            f"Слот: {item.slot} | Цена: {format_currency(item.cost)} | Путь: {item.synergy or 'гибрид'}"
        )
    return str(item)



_last_payload_hash: Dict[int, str] = {}
_progress_gate: Dict[int, Tuple[int, float]] = defaultdict(lambda: (0, 0.0))
_user_locks: Dict[int, asyncio.Lock] = {}


class ReplyBuilder:
    def __init__(self, event: Message | CallbackQuery):
        self.event = event
        self.lines: List[str] = []
        self._markup: Optional[InlineKeyboardMarkup | ReplyKeyboardMarkup] = None
        self._parse_mode: Optional[str] = None
        self.force_send: bool = False

    def add_line(self, text: str) -> None:
        if text:
            self.lines.append(text)

    def add_block(self, text: str) -> None:
        self.add_line(textwrap.dedent(text).strip())

    def set_markup(self, markup: InlineKeyboardMarkup | ReplyKeyboardMarkup) -> None:
        self._markup = markup

    def use_html(self) -> None:
        self._parse_mode = ParseMode.HTML

    async def flush(self, dedupe_key: int) -> None:
        text = "\n\n".join(filter(None, self.lines)).strip()
        if not text:
            text = "🤖"
        markup = self._markup
        payload_repr = {
            "text": text,
            "markup": markup.model_dump() if isinstance(markup, InlineKeyboardMarkup) else getattr(markup, "model_dump", lambda: None)(),
        }
        payload_hash = hashlib.sha1(json.dumps(payload_repr, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        if not self.force_send and _last_payload_hash.get(dedupe_key) == payload_hash:
            if isinstance(self.event, CallbackQuery):
                await self.event.answer()
            return
        try:
            if isinstance(self.event, CallbackQuery) and self.event.message:
                try:
                    await self.event.message.edit_text(
                        text,
                        reply_markup=markup if isinstance(markup, InlineKeyboardMarkup) else None,
                        parse_mode=self._parse_mode,
                    )
                except TelegramBadRequest as exc:
                    if "message is not modified" not in str(exc).lower():
                        raise
                    await self.event.answer()
                    return
                await self.event.answer()
            elif isinstance(self.event, Message):
                await self.event.answer(
                    text,
                    reply_markup=markup if markup else MAIN_MENU,
                    parse_mode=self._parse_mode,
                )
        finally:
            _last_payload_hash[dedupe_key] = payload_hash


@dataclass(slots=True)
class HandlerContext:
    event: Message | CallbackQuery
    session: AsyncSession
    player: PlayerState
    reply: ReplyBuilder

    @property
    def raw_text(self) -> str:
        if isinstance(self.event, Message):
            return self.event.text or ""
        if isinstance(self.event, CallbackQuery):
            return self.event.data or ""
        return ""


class SafeMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):  # type: ignore[override]
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)
        lock = _user_locks.setdefault(user.id, asyncio.Lock())
        async with lock:
            return await handler(event, data)


async def get_context(event: Message | CallbackQuery, session: AsyncSession) -> HandlerContext:
    tg_id = event.from_user.id  # type: ignore[assignment]
    model = await session.get(PlayerModel, tg_id)
    if model is None:
        state = PlayerState(tg_id=tg_id)
        state.ensure_order()
        model = PlayerModel(tg_id=tg_id, payload=state.to_dict(), nickname=event.from_user.full_name)
        session.add(model)
        await session.commit()
        return HandlerContext(event=event, session=session, player=state, reply=ReplyBuilder(event))
    state = PlayerState.from_dict(tg_id, model.payload)
    if not state.nickname:
        state.nickname = event.from_user.full_name
    return HandlerContext(event=event, session=session, player=state, reply=ReplyBuilder(event))


async def persist_context(ctx: HandlerContext) -> None:
    model = await ctx.session.get(PlayerModel, ctx.player.tg_id)
    if model is None:
        model = PlayerModel(tg_id=ctx.player.tg_id, payload=ctx.player.to_dict(), nickname=ctx.player.nickname)
        ctx.session.add(model)
    else:
        model.payload = ctx.player.to_dict()
        if ctx.player.nickname:
            model.nickname = ctx.player.nickname
    await ctx.session.commit()


async def run_safe_handler(event: Message | CallbackQuery, handler: Callable[[HandlerContext], Awaitable[None]]) -> None:
    tg_id = event.from_user.id  # type: ignore[assignment]
    lock = _user_locks.setdefault(tg_id, asyncio.Lock())
    async with lock:
        async with SESSION_MAKER() as session:
            ctx = await get_context(event, session)
            try:
                await handler(ctx)
                await persist_context(ctx)
            except Exception as exc:
                logger.exception("Handler error: %s", exc)
                ctx.reply.add_line("Что-то пошло не так. Попробуйте ещё раз чуть позже.")
            finally:
                await ctx.reply.flush(dedupe_key=tg_id)


# ---------------------------------------------------------------------------
# Хелперы отображения
# ---------------------------------------------------------------------------


def order_status_text(state: PlayerState) -> str:
    if not state.order:
        return "Заказов нет. Возьмите новый!"
    progress = state.order.progress
    pct = state.order.progress_pct() * 100
    return (
        f"📋 {state.order.title}\n"
        f"Прогресс: {progress}/{state.order.required} ({pct:.1f}%)\n"
        f"Награда: {format_currency(state.order.reward)}"
    )


def profile_text(state: PlayerState) -> str:
    build_hint = state.stats.get("crit_path", 0)
    path = "Крит" if build_hint > 5 else "Стабильный"
    passive = state.passive_income_per_minute()
    return (
        f"👤 {state.nickname or 'Безымянный дизайнер'}\n"
        f"Баланс: {format_currency(state.rub)} | Престиж: {format_number(state.prestige)}\n"
        f"Сила клика: {format_currency(state.click_gain())} | Пассив: {format_currency(passive)} / мин\n"
        f"Клики: {int(state.stats.get('clicks', 0))} | Заказы: {int(state.stats.get('orders', 0))}\n"
        f"Текущий путь: {path}"
    )


def boosts_overview(state: PlayerState) -> str:
    if not state.boosts:
        return "Буста нет. Зайдите в магазин и возьмите рывок!"
    parts = ["Активные бусты:"]
    now = time.time()
    for bid, inst in state.boosts.items():
        definition = BOOSTS.get(bid)
        if not definition:
            continue
        remaining = "∞" if inst.expires_at is None else max(0, int(inst.expires_at - now))
        parts.append(f"• {definition.title}: {inst.stacks} ст. — осталось {remaining // 60} мин")
    return "\n".join(parts)


def equipment_overview(state: PlayerState) -> str:
    lines = ["🎽 Гардероб"]
    for slot in ECON["equipment_slots"]:
        item_id = state.equipment.get(slot)
        if not item_id:
            lines.append(f"• {slot}: пусто")
            continue
        item = EQUIPMENT.get(item_id)
        if not item:
            lines.append(f"• {slot}: неизвестно")
            continue
        lines.append(f"• {slot}: {item.title} ({item.synergy or 'гибрид'})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Обработчики
# ---------------------------------------------------------------------------


async def start_handler(ctx: HandlerContext) -> None:
    ctx.player.ensure_order()
    ctx.reply.add_block(
        f"Привет, {ctx.event.from_user.full_name}! 🎨\n"
        "Я — твой дизайнерский кликер. Кликни «🖱️ Клик», чтобы закрывать заказы и получать ₽."
    )
    ctx.reply.add_line(order_status_text(ctx.player))
    ctx.reply.set_markup(MAIN_MENU)


async def help_handler(ctx: HandlerContext) -> None:
    ctx.reply.add_block(
        """📖 Справка
        • «🖱️ Клик» — выполняет шаг заказа.
        • «📋 Заказы» — прогресс и кнопка «Взять следующий».
        • «🛠️ Улучшения» — развитие клика, пассива и крита.
        • «🛒 Магазин» — временные бусты без спама.
        • «🎽 Гардероб» — экипировка под разные билды.
        Цифрами 1–5 выбирайте позиции в списках.
        """
    )
    ctx.reply.set_markup(MAIN_MENU)


async def click_handler(ctx: HandlerContext) -> None:
    gain, completed = ctx.player.register_click()
    ctx.player.rub += gain
    ctx.player.stats["rub_earned"] += gain
    gate_clicks, gate_ts = _progress_gate[ctx.player.tg_id]
    now = time.monotonic()
    gate_clicks += 1
    should_emit = (
        completed
        or gate_clicks >= ECON["progress_message_clicks"]
        or (now - gate_ts) * 1000 >= ECON["progress_message_interval_ms"]
    )
    if should_emit:
        _progress_gate[ctx.player.tg_id] = (0, now)
        ctx.reply.add_line(
            f"+{format_currency(gain)} | Заказ: {ctx.player.order.progress}/{ctx.player.order.required}"
        )
        if completed:
            ctx.reply.add_line(f"✅ Заказ выполнен! Награда: {format_currency(ctx.player.order.reward)}")
            ctx.reply.add_line("Нажми «Взять следующий», чтобы не остыть!")
            ctx.reply.set_markup(
                InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Взять следующий", callback_data="order:next")],
                        [InlineKeyboardButton(text="Назад в меню", callback_data="menu:root")],
                    ]
                )
            )
        else:
            ctx.reply.set_markup(MAIN_MENU)
    else:
        _progress_gate[ctx.player.tg_id] = (gate_clicks, gate_ts)
        ctx.reply.force_send = False


async def order_handler(ctx: HandlerContext) -> None:
    ctx.reply.add_line(order_status_text(ctx.player))
    buttons = [[InlineKeyboardButton(text="Взять следующий", callback_data="order:next")]]
    ctx.reply.set_markup(InlineKeyboardMarkup(inline_keyboard=buttons))


async def order_next_handler(ctx: HandlerContext) -> None:
    if not ctx.player.order or not ctx.player.order.is_done():
        ctx.player.ensure_order()
        ctx.reply.add_line("Текущий заказ ещё не завершён. Дожми его — и награда твоя!")
        return
    ctx.player.ensure_order()
    ctx.reply.add_line("Новый заказ готов. Вперёд к победам!")
    ctx.reply.add_line(order_status_text(ctx.player))
    ctx.reply.set_markup(MAIN_MENU)


async def upgrades_handler(ctx: HandlerContext) -> None:
    items = list(UPGRADES.values())
    page = int(ctx.player.ui_state.get("upgrades_page", 0))
    text, markup = render_list_page(items, page, ECON["list_page_size"], lambda it, _: format_item_card(it, ctx.player))
    ctx.player.ui_state["upgrades_page"] = page
    ctx.player.ui_state["upgrades_items"] = [item.id for item in items]
    ctx.player.ui_state["upgrades_items_page"] = ECON["list_page_size"]
    ctx.reply.add_line("🛠️ Улучшения")
    ctx.reply.add_line(text)
    ctx.reply.set_markup(markup)


async def upgrade_pick(ctx: HandlerContext, item_id: str) -> None:
    success, message = ctx.player.upgrade(item_id)
    ctx.reply.force_send = True
    ctx.reply.add_line(message)
    await upgrades_handler(ctx)


async def boosts_handler(ctx: HandlerContext) -> None:
    items = list(BOOSTS.values())
    page = int(ctx.player.ui_state.get("boosts_page", 0))
    text, markup = render_list_page(items, page, ECON["list_page_size"], lambda it, _: format_item_card(it, ctx.player))
    ctx.player.ui_state["boosts_page"] = page
    ctx.player.ui_state["boosts_items"] = [item.id for item in items]
    ctx.player.ui_state["boosts_items_page"] = ECON["list_page_size"]
    ctx.reply.add_line("🛒 Магазин бустов")
    ctx.reply.add_line(text)
    ctx.reply.add_line(boosts_overview(ctx.player))
    ctx.reply.set_markup(markup)


async def boost_pick(ctx: HandlerContext, item_id: str) -> None:
    success, message = ctx.player.add_boost(item_id)
    ctx.reply.force_send = True
    ctx.reply.add_line(message)
    if success:
        ctx.reply.add_line(boosts_overview(ctx.player))
    await boosts_handler(ctx)


async def wardrobe_handler(ctx: HandlerContext) -> None:
    items = list(EQUIPMENT.values())
    page = int(ctx.player.ui_state.get("equip_page", 0))
    text, markup = render_list_page(items, page, ECON["list_page_size"], lambda it, _: format_item_card(it, ctx.player))
    ctx.player.ui_state["equip_page"] = page
    ctx.player.ui_state["equip_items"] = [item.id for item in items]
    ctx.player.ui_state["equip_items_page"] = ECON["list_page_size"]
    ctx.reply.add_line(equipment_overview(ctx.player))
    ctx.reply.add_line(text)
    ctx.reply.set_markup(markup)


async def wardrobe_pick(ctx: HandlerContext, item_id: str) -> None:
    if random.random() < 0.1:
        confirm_action(
            ctx,
            "Экипировать предмет?",
            "Это заменит текущий предмет в слоте. Продолжить?",
            lambda ctx_inner: _confirm_equip(ctx_inner, item_id),
        )
        return
    await _confirm_equip(ctx, item_id)


async def _confirm_equip(ctx: HandlerContext, item_id: str) -> None:
    success, message = ctx.player.equip_item(item_id)
    ctx.reply.force_send = True
    ctx.reply.add_line(message)
    ctx.reply.add_line(equipment_overview(ctx.player))


async def profile_handler(ctx: HandlerContext) -> None:
    ctx.reply.add_line(profile_text(ctx.player))
    ctx.reply.add_line(boosts_overview(ctx.player))
    ctx.reply.set_markup(MAIN_MENU)


async def dailies_handler(ctx: HandlerContext) -> None:
    goal_clicks = 120
    clicks = int(ctx.player.stats.get("clicks", 0))
    progress = min(1.0, clicks / goal_clicks)
    ctx.reply.add_line("🗓️ Дейлики")
    ctx.reply.add_line(
        (
            f"Клики сегодня: {clicks}/{goal_clicks} ({progress * 100:.0f}%).\n"
            "Достигните цели и получите бонус {bonus}!".format(
                bonus=format_currency(SETTINGS.DAILY_BONUS_RUB)
            )
        )
    )
    if progress >= 1.0:
        ctx.player.rub += SETTINGS.DAILY_BONUS_RUB
        ctx.reply.add_line(f"🎁 Бонус за клики: {format_currency(SETTINGS.DAILY_BONUS_RUB)}")
    ctx.reply.set_markup(MAIN_MENU)

async def team_handler(ctx: HandlerContext) -> None:
    ctx.reply.add_line(
        "👥 Команда — объединяйтесь с друзьями, чтобы делиться бустами."
        " Пока фича учитывает только личные достижения, но прогресс переносится."
    )
    ctx.reply.set_markup(MAIN_MENU)


async def menu_handler(ctx: HandlerContext) -> None:
    ctx.reply.add_line("Возвращаемся в главное меню.")
    ctx.reply.set_markup(MAIN_MENU)


async def numeric_router(ctx: HandlerContext) -> None:
    if await handle_numeric_selection(ctx, "upgrades_page", "upgrades_items", upgrade_pick):
        return
    if await handle_numeric_selection(ctx, "boosts_page", "boosts_items", boost_pick):
        return
    if await handle_numeric_selection(ctx, "equip_page", "equip_items", wardrobe_pick):
        return
    ctx.reply.add_line("Не понял выбор. Используйте кнопки меню или цифры 1-5.")
    ctx.reply.set_markup(MAIN_MENU)


async def confirmation_handler(ctx: HandlerContext) -> None:
    data = ctx.raw_text
    if data == "confirm:cancel":
        ctx.reply.add_line("Действие отменено.")
        return
    token = data.split(":", 1)[-1]
    entry = _confirmations.pop(token, None)
    if not entry or entry[0] != ctx.player.tg_id:
        ctx.reply.add_line("Подтверждение устарело.")
        return
    await entry[1](ctx)


# ---------------------------------------------------------------------------
# Роутеры
# ---------------------------------------------------------------------------


router = Router()
router.message.middleware(SafeMiddleware())
router.callback_query.middleware(SafeMiddleware())


@router.message(CommandStart())
async def _start(message: Message) -> None:
    await run_safe_handler(message, start_handler)


@router.message(Command("help"))
async def _help(message: Message) -> None:
    await run_safe_handler(message, help_handler)


@router.message(F.text == "🖱️ Клик")
async def _click(message: Message) -> None:
    await run_safe_handler(message, click_handler)


@router.message(F.text == "📋 Заказы")
async def _orders(message: Message) -> None:
    await run_safe_handler(message, order_handler)


@router.message(F.text == "🛠️ Улучшения")
async def _upgrades(message: Message) -> None:
    await run_safe_handler(message, upgrades_handler)


@router.message(F.text == "🛒 Магазин")
async def _boosts(message: Message) -> None:
    await run_safe_handler(message, boosts_handler)


@router.message(F.text == "🎽 Гардероб")
async def _wardrobe(message: Message) -> None:
    await run_safe_handler(message, wardrobe_handler)


@router.message(F.text == "👤 Профиль")
async def _profile(message: Message) -> None:
    await run_safe_handler(message, profile_handler)


@router.message(F.text == "🗓️ Задания дня")
async def _dailies(message: Message) -> None:
    await run_safe_handler(message, dailies_handler)


@router.message(F.text == "👥 Команда")
async def _team(message: Message) -> None:
    await run_safe_handler(message, team_handler)


@router.message(F.text.in_({"1", "2", "3", "4", "5"}))
async def _numeric(message: Message) -> None:
    await run_safe_handler(message, numeric_router)


@router.message()
async def _fallback(message: Message) -> None:
    await run_safe_handler(message, menu_handler)


@router.callback_query(F.data == "order:next")
async def _next_order(callback: CallbackQuery) -> None:
    await run_safe_handler(callback, order_next_handler)


@router.callback_query(F.data == "menu:root")
async def _menu(callback: CallbackQuery) -> None:
    await run_safe_handler(callback, menu_handler)


@router.callback_query(F.data.startswith("confirm:"))
async def _confirm(callback: CallbackQuery) -> None:
    await run_safe_handler(callback, confirmation_handler)


# ---------------------------------------------------------------------------
# Быстрый sanity-тест
# ---------------------------------------------------------------------------


def quick_sanity_test(iterations: int = 1000) -> Dict[str, Any]:
    state = PlayerState(tg_id=0)
    state.ensure_order()
    start = time.time()
    spend_on_upgrades = 0
    for i in range(iterations):
        gain, completed = state.register_click()
        state.rub += gain
        if completed:
            state.complete_order()
        if i % 30 == 0:
            ok, _ = state.upgrade("click_power")
            if ok:
                spend_on_upgrades += 1
    duration = time.time() - start
    avg_click = state.stats["rub_earned"] / max(1, state.stats["clicks"])
    return {
        "duration": duration,
        "avg_rub_per_click": avg_click,
        "orders": state.stats["orders"],
        "rub": state.rub,
        "upgrades_bought": spend_on_upgrades,
    }


# ---------------------------------------------------------------------------
# Запуск приложения
# ---------------------------------------------------------------------------


engine = create_async_engine(SETTINGS.DATABASE_URL, echo=False, future=True)
SESSION_MAKER = async_sessionmaker(engine, expire_on_commit=False)


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    return dp


async def main() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    if not SETTINGS.BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    bot = Bot(token=SETTINGS.BOT_TOKEN, parse_mode=ParseMode.HTML)
    dp = build_dispatcher()
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
