import asyncio
import logging
from datetime import datetime

import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon import TelegramClient, errors
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

# ========== НАСТРОЙКИ ==========
API_ID = 39028303
API_HASH = 'b20f712549c389f606799536d32b3df1'
PHONE = '+972547036850'
BOT_TOKEN = '8097274880:AAFjyJLHaCvQoCPFneEirqmZsDqTL4COKWs'
OWNER_ID = 5883796026
SECOND_ADMIN_ID = 7654150854
SESSION_STRING = '1BJWap1sBu0gFEqPXEuTYUGYQ4feRyTF6GqCWMyVHNKYAuXGrqWRZFxc5vN39R1uGKNAMq6qrHt_hkq5TA3CIGqWZDHKZkwFZwROXjVffJwAZOSDnIireqJIy3dvxCiDfwsYqKLUnYhvar_FDN-a3Tc5IbGd_HWe96Sl_gn5pDWVaNoUdkI2yQW5ywFHn7LMnwQcOg0dK4rDu1E3oMzETQJXAAlNRCADN8EWDd8PHte5f2DGbEW60ZrUuL8lFBm1Ch_uCsjAtXqicp7-YpKRzZoHwPydh3arGzD1qZVL0eGevHRE3LTNUPpNMC0FwiHVAc61rrXpLyD0QX7a7PpfLkkCQs2kFfUY='
# =================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
DB_PATH = 'autopost.db'

# ========== Премиум эмодзи ==========
def tge(eid, fb=''):
    return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>'

EM_HELLO    = tge('5257963315258204021', '👋')
EM_LIST     = tge('5257969839313526622', '📋')
EM_ADD      = tge('5452165780579843515', '➕')
EM_TEXT     = tge('5258328383183396223', '✏️')
EM_START    = tge('5260221883940347555', '▶️')
EM_STOP     = tge('5260249440450520061', '⏸️')
EM_STATUS   = tge('5258391025281408576', '📊')
EM_INTERVAL = tge('5258215635996908355', '⏱')
EM_BANNED   = tge('5258362429389152256', '⛔')
EM_OK       = tge('5870633910337015697', '✅')
EM_ERR      = tge('5870657884844462243', '❌')
EM_INFO     = tge('6028435952299413210', 'ℹ️')
EM_BOT      = tge('6030400221232501136', '🤖')
EM_PEOPLE   = tge('5870772616305839506', '👥')

BTN_ADD      = '5452165780579843515'
BTN_LIST     = '5257969839313526622'
BTN_TEXT     = '5258328383183396223'
BTN_START    = '5260221883940347555'
BTN_STOP     = '5260249440450520061'
BTN_STATUS   = '5258391025281408576'
BTN_INTERVAL = '5258215635996908355'
BTN_DELETE   = '5258130763148172425'
BTN_3MIN     = '5199457120428249992'
BTN_BACK     = '5260450573768990626'

WELCOME_TEXT = (
    f'{EM_HELLO} <b>Добро пожаловать в Авторассылку!</b>\n\n'
    f'{EM_BOT} <b>Что умеет этот бот?</b>\n'
    f'Бот автоматически рассылает ваше сообщение во все нужные группы '
    f'по индивидуальным таймерам — каждый чат работает независимо.\n\n'
    f'<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n\n'
    f'{EM_ADD} <b>Добавить чат</b> — ссылка, юзернейм или ID + интервал\n\n'
    f'{EM_PEOPLE} <b>Мои чаты</b> — ваши группы, добавить одним нажатием\n\n'
    f'{EM_LIST} <b>Список рассылки</b> — управление добавленными чатами\n\n'
    f'{EM_TEXT} <b>Текст сообщения</b> — что рассылается по всем чатам\n\n'
    f'{EM_START} <b>Запустить</b> / {EM_STOP} <b>Остановить</b> — управление рассылкой\n\n'
    f'{EM_STATUS} <b>Статус</b> — активные задачи и текущий текст\n\n'
    f'<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n\n'
    f'{EM_INFO} <i>Каждый чат — отдельная независимая задача со своим таймером.</i>'
)

# ========== База данных ==========
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                chat_id   INTEGER PRIMARY KEY,
                title     TEXT,
                interval_minutes INTEGER DEFAULT 60,
                added_date TEXT,
                banned    INTEGER DEFAULT 0
            )
        ''')
        cur = await db.execute("PRAGMA table_info(chats)")
        cols = [c[1] for c in await cur.fetchall()]
        if 'interval_minutes' not in cols:
            await db.execute("ALTER TABLE chats ADD COLUMN interval_minutes INTEGER DEFAULT 60")
        if 'banned' not in cols:
            await db.execute("ALTER TABLE chats ADD COLUMN banned INTEGER DEFAULT 0")
        await db.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER, message TEXT, date TEXT
            )
        ''')
        await db.execute("INSERT OR IGNORE INTO settings VALUES ('message_text','Привет! Это автосообщение.')")
        await db.execute("INSERT OR IGNORE INTO settings VALUES ('posting_active','0')")
        await db.commit()

async def get_chats(include_banned=False):
    async with aiosqlite.connect(DB_PATH) as db:
        sql = 'SELECT chat_id, title, interval_minutes FROM chats'
        if not include_banned:
            sql += ' WHERE banned = 0'
        return await (await db.execute(sql)).fetchall()

async def add_chat(chat_id, title, interval_minutes):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                'INSERT INTO chats (chat_id,title,interval_minutes,added_date,banned) VALUES (?,?,?,?,0)',
                (chat_id, title, interval_minutes, datetime.now().isoformat())
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def remove_chat(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM chats WHERE chat_id=?', (chat_id,))
        await db.commit()

async def set_chat_interval(chat_id, minutes):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE chats SET interval_minutes=? WHERE chat_id=?', (minutes, chat_id))
        await db.commit()

async def mark_chat_banned(chat_id, banned=True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE chats SET banned=? WHERE chat_id=?', (1 if banned else 0, chat_id))
        await db.commit()

async def get_chat_info(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT title, interval_minutes, banned FROM chats WHERE chat_id=?', (chat_id,))
        return await cur.fetchone()

async def get_setting(key):
    async with aiosqlite.connect(DB_PATH) as db:
        row = await (await db.execute('SELECT value FROM settings WHERE key=?', (key,))).fetchone()
        return row[0] if row else None

async def set_setting(key, value):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('REPLACE INTO settings VALUES (?,?)', (key, value))
        await db.commit()

async def save_post(chat_id, message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO posts (chat_id,message,date) VALUES (?,?,?)',
                         (chat_id, message, datetime.now().isoformat()))
        await db.commit()

# ========== Клиенты ==========
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())
user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

def is_owner(uid):
    return uid == OWNER_ID or bool(SECOND_ADMIN_ID and uid == SECOND_ADMIN_ID)

# ========== Кеш диалогов ==========
_dialogs_cache: dict[int, object] = {}

async def warm_dialogs_cache():
    global _dialogs_cache
    try:
        dialogs = await user_client.get_dialogs()
        _dialogs_cache = {d.id: d.entity for d in dialogs}
        logger.info(f"Кеш диалогов обновлён: {len(_dialogs_cache)} записей")
        return dialogs
    except Exception as ex:
        logger.warning(f"Не удалось обновить кеш диалогов: {ex}")
        return []

# ========== Ошибки реального бана ==========
# ChatWriteForbiddenError означает нет прав на отправку (канал/нет прав) — НЕ бан
# Только эти ошибки реально означают бан аккаунта:
REAL_BAN_ERRORS = (
    errors.UserBannedInChannelError,   # аккаунт забанен в чате
    errors.ChannelPrivateError,        # чат стал приватным
    errors.ChatForbiddenError,         # выгнали из чата
    errors.UserNotParticipantError,    # больше не участник
)

# Ошибки прав — не баним, просто пропускаем итерацию
PERMISSION_ERRORS = (
    errors.ChatWriteForbiddenError,    # нет права писать (канал / slow mode / ограничения)
    errors.ChatAdminRequiredError,     # нужны права админа
    errors.BannedRightsInvalidError,   # ограничены права на отправку
    errors.SlowModeWaitError,          # slow mode
    errors.UserIsBlockedError,         # заблокировал в лс
)

# ========== Получение entity ==========
async def get_entity_safe(full_chat_id: int):
    """
    full_chat_id — полный Telegram ID как хранится в БД.
    Сначала ищет в кеше, потом через Telethon, потом обновляет кеш.
    Возвращает None если не найден (не баним!).
    """
    if full_chat_id in _dialogs_cache:
        return _dialogs_cache[full_chat_id]

    try:
        entity = await user_client.get_entity(full_chat_id)
        _dialogs_cache[full_chat_id] = entity
        return entity
    except REAL_BAN_ERRORS:
        raise
    except PERMISSION_ERRORS:
        raise
    except Exception as ex:
        logger.warning(f"get_entity({full_chat_id}) не удался: {ex}")

    # Обновляем кеш и пробуем снова
    await warm_dialogs_cache()
    if full_chat_id in _dialogs_cache:
        return _dialogs_cache[full_chat_id]

    logger.warning(f"Entity не найден даже после обновления кеша: {full_chat_id}")
    return None

# ========== Менеджер задач ==========
chat_tasks: dict[int, asyncio.Task] = {}

async def single_chat_worker(chat_id: int, title: str, interval_minutes: int):
    logger.info(f"▶ [{title}] Задача запущена, интервал={interval_minutes} мин.")
    resolve_fails = 0

    while True:
        try:
            if await get_setting('posting_active') != '1':
                await asyncio.sleep(15)
                continue

            info = await get_chat_info(chat_id)
            if not info:
                logger.info(f"[{title}] Удалён из БД.")
                break
            _, current_interval, banned = info
            if banned:
                logger.info(f"[{title}] Помечен banned.")
                break

            message_text = await get_setting('message_text')

            # Получаем entity
            try:
                entity = await get_entity_safe(chat_id)
            except REAL_BAN_ERRORS as ex:
                logger.error(f"[{title}] Аккаунт забанен/выгнан: {type(ex).__name__} — помечаю banned.")
                await mark_chat_banned(chat_id, True)
                break
            except PERMISSION_ERRORS as ex:
                # Нет прав на запись — пропускаем итерацию, НЕ баним
                logger.warning(f"[{title}] Нет прав на запись: {type(ex).__name__}. "
                                f"Это канал или аккаунт не имеет прав. Жду {current_interval} мин.")
                # Ждём интервал и пробуем снова (права могут измениться)
                await _sleep_interval(current_interval)
                continue

            if entity is None:
                resolve_fails += 1
                logger.warning(f"[{title}] Entity не найден ({resolve_fails}). Жду 60с.")
                await asyncio.sleep(60)
                continue

            resolve_fails = 0

            # Отправка
            try:
                await user_client.send_message(entity, message_text)
                await save_post(chat_id, message_text)
                logger.info(f"✅ [{title}] Отправлено. Следующая через {current_interval} мин.")

            except FloodWaitError as ex:
                logger.warning(f"[{title}] FloodWait {ex.seconds}с")
                await asyncio.sleep(ex.seconds + 5)
                continue

            except REAL_BAN_ERRORS as ex:
                logger.error(f"[{title}] Аккаунт забанен/выгнан при отправке: {type(ex).__name__} — помечаю banned.")
                await mark_chat_banned(chat_id, True)
                break

            except PERMISSION_ERRORS as ex:
                # Нет прав — не баним, просто пишем в лог
                logger.warning(f"[{title}] Нет прав на отправку: {type(ex).__name__}. "
                                f"Проверьте что аккаунт может писать в этот чат. Жду {current_interval} мин.")
                await _sleep_interval(current_interval)
                continue

            except Exception as ex:
                logger.error(f"[{title}] Временная ошибка: {ex}. Жду 60с.")
                await asyncio.sleep(60)
                continue

            # Ждём интервал
            await _sleep_interval(current_interval)

        except asyncio.CancelledError:
            logger.info(f"⏹ [{title}] Задача отменена.")
            break
        except Exception as ex:
            logger.exception(f"[{title}] Неожиданная ошибка: {ex}")
            await asyncio.sleep(30)

    chat_tasks.pop(chat_id, None)
    logger.info(f"🔴 [{title}] Задача завершена.")


async def _sleep_interval(minutes: int):
    """Ждёт минуты, разбивая на куски по 10с чтобы реагировать на остановку."""
    total = minutes * 60
    waited = 0
    while waited < total:
        chunk = min(10, total - waited)
        await asyncio.sleep(chunk)
        waited += chunk
        if await get_setting('posting_active') != '1':
            break


def start_chat_task(chat_id, title, interval_minutes):
    if chat_id not in chat_tasks or chat_tasks[chat_id].done():
        chat_tasks[chat_id] = asyncio.create_task(
            single_chat_worker(chat_id, title, interval_minutes)
        )

def stop_chat_task(chat_id):
    task = chat_tasks.pop(chat_id, None)
    if task and not task.done():
        task.cancel()

def stop_all_tasks():
    for task in list(chat_tasks.values()):
        if not task.done():
            task.cancel()
    chat_tasks.clear()

async def refresh_tasks():
    chats = await get_chats(include_banned=False)
    for chat_id, title, interval in chats:
        start_chat_task(chat_id, title, interval)
    logger.info(f"🔄 Задач запущено: {len(chat_tasks)} для {len(chats)} чатов")

# ========== Клавиатуры ==========
def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить чат",    callback_data="add_chat",      icon_custom_emoji_id=BTN_ADD),
         InlineKeyboardButton(text="Список рассылки", callback_data="list_chats",    icon_custom_emoji_id=BTN_LIST)],
        [InlineKeyboardButton(text="Мои чаты",        callback_data="my_chats",      icon_custom_emoji_id=BTN_LIST),
         InlineKeyboardButton(text="Текст сообщения", callback_data="set_text",      icon_custom_emoji_id=BTN_TEXT)],
        [InlineKeyboardButton(text="Запустить",       callback_data="start_posting", icon_custom_emoji_id=BTN_START),
         InlineKeyboardButton(text="Остановить",      callback_data="stop_posting",  icon_custom_emoji_id=BTN_STOP)],
        [InlineKeyboardButton(text="Статус",          callback_data="status",        icon_custom_emoji_id=BTN_STATUS)],
    ])

def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=BTN_BACK)],
    ])

def chat_manage_keyboard(chat_id: int):
    # Кодируем chat_id в hex чтобы избежать проблем с отрицательными числами и разделителями
    cid_hex = format(chat_id & 0xFFFFFFFFFFFFFFFF, 'x')
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить интервал", callback_data=f"setint|{cid_hex}",  icon_custom_emoji_id=BTN_INTERVAL)],
        [InlineKeyboardButton(text="Установить 3 мин",  callback_data=f"set3|{cid_hex}",    icon_custom_emoji_id=BTN_3MIN)],
        [InlineKeyboardButton(text="Удалить чат",       callback_data=f"delchat|{cid_hex}", icon_custom_emoji_id=BTN_DELETE)],
        [InlineKeyboardButton(text="Снять бан",         callback_data=f"unban|{cid_hex}",   icon_custom_emoji_id=BTN_START)],
        [InlineKeyboardButton(text="Назад к списку",    callback_data="list_chats",          icon_custom_emoji_id=BTN_BACK)],
    ])

def decode_cid(cid_hex: str) -> int:
    """Декодирует hex chat_id обратно в int со знаком."""
    val = int(cid_hex, 16)
    # Восстанавливаем знак (64-bit two's complement)
    if val >= (1 << 63):
        val -= (1 << 64)
    return val

# ========== FSM ==========
class AddChat(StatesGroup):
    waiting_for_link     = State()
    waiting_for_interval = State()

class SetText(StatesGroup):
    waiting = State()

class SetInterval(StatesGroup):
    waiting_for_minutes = State()

# ========== Показ меню чата (общая функция) ==========
async def show_chat_menu(message_or_callback, chat_id: int):
    """Показывает меню управления конкретным чатом."""
    info = await get_chat_info(chat_id)
    if not info:
        if hasattr(message_or_callback, 'answer'):
            await message_or_callback.answer("Чат не найден", show_alert=True)
        return
    title, interval, banned = info
    running = chat_id in chat_tasks and not chat_tasks[chat_id].done()
    if banned:
        st = f"{EM_BANNED} Нет прав / выгнан"
    elif running:
        st = f"{EM_OK} Работает"
    else:
        st = f"{EM_OK} Активен (пауза)"

    text = (
        f'{EM_LIST} <b>{title}</b>\n\n'
        f'<b>ID:</b> <code>{chat_id}</code>\n'
        f'<b>Статус:</b> {st}\n'
        f'{EM_INTERVAL} <b>Интервал:</b> {interval} мин.'
    )
    kb = chat_manage_keyboard(chat_id)

    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await message_or_callback.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

# ========== Список групп ==========
async def show_my_chats(callback, page):
    try:
        dialogs = await user_client.get_dialogs()
        for d in dialogs:
            _dialogs_cache[d.id] = d.entity

        groups = [d for d in dialogs if d.is_group]
        if not groups:
            await callback.message.edit_text(
                f'{EM_INFO} Вы не состоите ни в одной группе.',
                reply_markup=back_keyboard(), parse_mode=ParseMode.HTML)
            return

        per_page    = 10
        total_pages = (len(groups) + per_page - 1) // per_page
        page        = max(0, min(page, total_pages - 1))
        rows = []
        for d in groups[page * per_page:(page + 1) * per_page]:
            title   = d.name or 'Без названия'
            full_id = d.id
            cid_hex = format(full_id & 0xFFFFFFFFFFFFFFFF, 'x')
            rows.append([InlineKeyboardButton(
                text=title,
                callback_data=f"afl|{page}|{cid_hex}"
            )])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="Предыдущая",
                                            callback_data=f"my_chats_page_{page-1}",
                                            icon_custom_emoji_id=BTN_BACK))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton(text="Следующая",
                                            callback_data=f"my_chats_page_{page+1}",
                                            icon_custom_emoji_id=BTN_START))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton(text="Назад", callback_data="back_to_main",
                                          icon_custom_emoji_id=BTN_BACK)])
        await callback.message.edit_text(
            f'{EM_PEOPLE} <b>Ваши группы</b>  <i>(стр. {page+1}/{total_pages})</i>\n'
            f'Нажмите — добавится с интервалом 60 мин.',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode=ParseMode.HTML)
    except Exception as ex:
        logger.exception("show_my_chats error")
        await callback.message.edit_text(f'{EM_ERR} Ошибка: {ex}',
                                         reply_markup=back_keyboard(), parse_mode=ParseMode.HTML)

# ========== Handlers ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("⛔ Доступ запрещён.")
    await message.answer(WELCOME_TEXT, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    await callback.message.edit_text(WELCOME_TEXT, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

# --- Добавление чата по ссылке ---
@dp.callback_query(F.data == "add_chat")
async def add_chat_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback.from_user.id): return await callback.answer()
    await callback.message.edit_text(
        f'{EM_ADD} <b>Добавление чата</b>\n\nОтправьте ссылку, юзернейм или ID:\n'
        f'<i>• @chat\n• https://t.me/chat\n• -100123456789</i>',
        parse_mode=ParseMode.HTML)
    await state.set_state(AddChat.waiting_for_link)

@dp.message(AddChat.waiting_for_link)
async def add_chat_link(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id): return await state.clear()
    try:
        entity  = await user_client.get_entity(message.text.strip())
        full_id = entity.id
        title   = getattr(entity, 'title', None) or getattr(entity, 'username', str(full_id))
        _dialogs_cache[full_id] = entity
        await state.update_data(chat_id=full_id, title=title)
        await message.answer(
            f'{EM_OK} Найден: <b>{title}</b>\n<code>ID: {full_id}</code>\n\n'
            f'{EM_INTERVAL} Укажите <b>интервал в минутах</b>:', parse_mode=ParseMode.HTML)
        await state.set_state(AddChat.waiting_for_interval)
    except Exception as ex:
        await message.answer(f'{EM_ERR} Ошибка: <code>{ex}</code>', parse_mode=ParseMode.HTML)
        await state.clear()

@dp.message(AddChat.waiting_for_interval)
async def add_chat_interval(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id): return await state.clear()
    try:
        interval = int(message.text.strip())
        assert interval >= 1
    except:
        return await message.answer(f'{EM_ERR} Введите целое положительное число.', parse_mode=ParseMode.HTML)
    d = await state.get_data()
    chat_id, title = d['chat_id'], d['title']
    if await add_chat(chat_id, title, interval):
        await message.answer(
            f'{EM_OK} <b>Добавлено!</b>\n{title}\n{EM_INTERVAL} Интервал: <b>{interval} мин.</b>',
            parse_mode=ParseMode.HTML)
        if await get_setting('posting_active') == '1':
            start_chat_task(chat_id, title, interval)
    else:
        await message.answer(f'{EM_ERR} Чат уже в списке.', parse_mode=ParseMode.HTML)
    await state.clear()
    await message.answer(f'{EM_HELLO} Главное меню', reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

# --- Мои чаты ---
@dp.callback_query(F.data == "my_chats")
async def my_chats_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    await show_my_chats(callback, 0)

@dp.callback_query(F.data.startswith("my_chats_page_"))
async def my_chats_page(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    await show_my_chats(callback, int(callback.data.split("_")[3]))

@dp.callback_query(F.data.startswith("afl|"))
async def add_from_list(callback: types.CallbackQuery):
    """callback_data: afl|{page}|{cid_hex}"""
    if not is_owner(callback.from_user.id): return await callback.answer()
    _, page_s, cid_hex = callback.data.split("|")
    page    = int(page_s)
    full_id = decode_cid(cid_hex)

    entity = _dialogs_cache.get(full_id)
    if entity is None:
        await callback.answer("Не удалось найти чат в кеше.", show_alert=True)
        return await show_my_chats(callback, page)

    title = getattr(entity, 'title', None) or getattr(entity, 'username', str(full_id))
    if await add_chat(full_id, title, 60):
        await callback.answer(f"✅ Добавлено! Интервал: 60 мин.")
        if await get_setting('posting_active') == '1':
            start_chat_task(full_id, title, 60)
    else:
        await callback.answer("Группа уже в списке.", show_alert=True)
    await show_my_chats(callback, page)

# --- Список рассылки ---
@dp.callback_query(F.data == "list_chats")
async def list_chats(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    chats = await get_chats(include_banned=True)
    if not chats:
        return await callback.message.edit_text(
            f'{EM_INFO} Список пуст. Добавьте чаты.',
            reply_markup=back_keyboard(), parse_mode=ParseMode.HTML)
    rows = []
    for cid, title, _ in chats:
        async with aiosqlite.connect(DB_PATH) as db:
            row    = await (await db.execute('SELECT banned FROM chats WHERE chat_id=?', (cid,))).fetchone()
            banned = row[0] if row else 0
        cid_hex = format(cid & 0xFFFFFFFFFFFFFFFF, 'x')
        label   = f"⛔ {title}" if banned else title
        rows.append([InlineKeyboardButton(text=label, callback_data=f"chat|{cid_hex}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=BTN_BACK)])
    await callback.message.edit_text(
        f'{EM_LIST} <b>Список рассылки</b>\n<i>Нажмите на чат для управления</i>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("chat|"))
async def chat_menu_cb(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    chat_id = decode_cid(callback.data.split("|")[1])
    await show_chat_menu(callback, chat_id)

@dp.callback_query(F.data.startswith("unban|"))
async def unban_chat(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    chat_id = decode_cid(callback.data.split("|")[1])
    await mark_chat_banned(chat_id, False)
    info = await get_chat_info(chat_id)
    if info and await get_setting('posting_active') == '1':
        start_chat_task(chat_id, info[0], info[1])
    await callback.answer("✅ Бан снят, задача перезапущена")
    await show_chat_menu(callback, chat_id)

@dp.callback_query(F.data.startswith("set3|"))
async def set_3min(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    chat_id = decode_cid(callback.data.split("|")[1])
    await set_chat_interval(chat_id, 3)
    info = await get_chat_info(chat_id)
    if info:
        stop_chat_task(chat_id)
        if await get_setting('posting_active') == '1':
            start_chat_task(chat_id, info[0], 3)
    await callback.answer("⚡ Интервал 3 мин — задача перезапущена")
    await show_chat_menu(callback, chat_id)

@dp.callback_query(F.data.startswith("delchat|"))
async def delete_chat(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    chat_id = decode_cid(callback.data.split("|")[1])
    stop_chat_task(chat_id)
    await remove_chat(chat_id)
    await callback.answer("Чат удалён")
    await list_chats(callback)

@dp.callback_query(F.data.startswith("setint|"))
async def set_interval_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback.from_user.id): return await callback.answer()
    chat_id = decode_cid(callback.data.split("|")[1])
    await state.update_data(chat_id=chat_id)
    await callback.message.edit_text(
        f'{EM_INTERVAL} <b>Новый интервал</b>\n\nВведите количество минут (целое число):',
        parse_mode=ParseMode.HTML)
    await state.set_state(SetInterval.waiting_for_minutes)

@dp.message(SetInterval.waiting_for_minutes)
async def set_interval_minutes(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id): return await state.clear()
    try:
        minutes = int(message.text.strip())
        assert minutes >= 1
    except:
        return await message.answer(f'{EM_ERR} Введите положительное число.', parse_mode=ParseMode.HTML)
    chat_id = (await state.get_data())['chat_id']
    await set_chat_interval(chat_id, minutes)
    info = await get_chat_info(chat_id)
    if info:
        stop_chat_task(chat_id)
        if await get_setting('posting_active') == '1':
            start_chat_task(chat_id, info[0], minutes)
    await message.answer(
        f'{EM_OK} Интервал <b>{minutes} мин.</b> установлен для <code>{chat_id}</code>',
        parse_mode=ParseMode.HTML)
    await state.clear()
    await message.answer(f'{EM_HELLO} Главное меню', reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "set_text")
async def set_text_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback.from_user.id): return await callback.answer()
    current = await get_setting('message_text')
    await callback.message.edit_text(
        f'{EM_TEXT} <b>Текст рассылки</b>\n\n<b>Текущий:</b>\n<blockquote>{current}</blockquote>\n\nОтправьте новый текст:',
        parse_mode=ParseMode.HTML)
    await state.set_state(SetText.waiting)

@dp.message(SetText.waiting)
async def set_text_input(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id): return await state.clear()
    await set_setting('message_text', message.text.strip())
    await message.answer(f'{EM_OK} Текст обновлён!', parse_mode=ParseMode.HTML)
    await state.clear()
    await message.answer(f'{EM_HELLO} Главное меню', reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "start_posting")
async def start_posting(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    await set_setting('posting_active', '1')
    await refresh_tasks()
    chats = await get_chats(include_banned=False)
    await callback.message.edit_text(
        f'{EM_START} <b>Рассылка запущена!</b>\n\n'
        f'{EM_OK} Запущено задач: <b>{len(chats)}</b>\n'
        f'<i>Каждый чат работает независимо по своему таймеру.</i>',
        reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "stop_posting")
async def stop_posting(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    await set_setting('posting_active', '0')
    stop_all_tasks()
    await callback.message.edit_text(
        f'{EM_STOP} <b>Рассылка остановлена.</b>\n\n'
        f'<i>Все задачи отменены. Нажмите «Запустить» для возобновления.</i>',
        reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "status")
async def status(callback: types.CallbackQuery):
    if not is_owner(callback.from_user.id): return await callback.answer()
    active = await get_setting('posting_active')
    text   = await get_setting('message_text')
    chats  = await get_chats(include_banned=False)
    async with aiosqlite.connect(DB_PATH) as db:
        banned_cnt = (await (await db.execute('SELECT COUNT(*) FROM chats WHERE banned=1')).fetchone())[0]
    running = sum(1 for t in chat_tasks.values() if not t.done())
    st = f"{EM_START} Активна" if active == '1' else f"{EM_STOP} Остановлена"
    await callback.message.edit_text(
        f'{EM_STATUS} <b>Статус рассылки</b>\n\n'
        f'<b>Состояние:</b> {st}\n\n'
        f'{EM_OK} <b>Чатов в рассылке:</b> {len(chats)}\n'
        f'<b>Активных задач:</b> {running}\n'
        f'{EM_BANNED} <b>Нет прав/выгнан:</b> {banned_cnt}\n\n'
        f'{EM_TEXT} <b>Текст:</b>\n<blockquote>{text}</blockquote>',
        reply_markup=back_keyboard(), parse_mode=ParseMode.HTML)

# ========== Запуск ==========
async def main():
    await init_db()
    logger.info("БД инициализирована")

    await user_client.start()
    me = await user_client.get_me()
    logger.info(f"Аккаунт: {me.first_name} (@{me.username})")

    logger.info("Загружаем диалоги для прогрева кеша...")
    await warm_dialogs_cache()

    if await get_setting('posting_active') == '1':
        logger.info("Восстанавливаем задачи рассылки...")
        await refresh_tasks()

    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())