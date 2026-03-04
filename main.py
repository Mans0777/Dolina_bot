import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
import pytz
import os
import asyncio
import re
from datetime import datetime, time, timedelta
import psycopg2
from aiogram import Bot, Dispatcher, F, types
from aiogram.types import BotCommand, InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from openpyxl import Workbook

import google.generativeai as genai
# Настройка часового пояса
TASHKENT_TZ = pytz.timezone('Asia/Tashkent')

def get_now():
    """Возвращает текущее время в часовом поясе Ташкента"""
    return datetime.now(TASHKENT_TZ)
# ==========================================================
# 1. НАСТРОЙКИ И КОНФИГУРАЦИЯ
# ==========================================================
TOKEN = os.getenv("TOKEN")
ADMIN_IDS = [878423396, 276477340]
GEMINI_KEY = os.getenv("GEMINI_KEY")
GROUP_CHAT_ID = -1001174920470
# Находим это место в коде:
DATABASE_URL = os.getenv("DATABASE_URL")

# Заменяем создание подключения на более надежное:
try:
    # Добавляем параметр sslmode для работы с облачными БД
    if DATABASE_URL and "sslmode" not in DATABASE_URL:
        # Если в ссылке нет параметров, добавляем sslmode
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        conn = psycopg2.connect(DATABASE_URL)
    
    cursor = conn.cursor()
    print("✅ Успешное подключение к базе данных Railway")
except Exception as e:
    print(f"❌ Ошибка подключения к БД: {e}")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
# Создаем базовые таблицы
cursor.execute("""
CREATE TABLE IF NOT EXISTS problems (
    id TEXT PRIMARY KEY,
    store_code TEXT,
    created_at TEXT,
    fixed INTEGER DEFAULT 0,
    fixed_at TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS late_openings (
    id SERIAL PRIMARY KEY,
    store_code TEXT,
    date TEXT
)
""")

# --- Безопасно добавляем новые колонки ---
def add_column_if_not_exists(column_name, column_type):
    try:
        cursor.execute(f"ALTER TABLE problems ADD COLUMN {column_name} {column_type}")
        conn.commit()
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()

cursor.execute("ALTER TABLE problems ADD COLUMN IF NOT EXISTS group_id TEXT")
cursor.execute("ALTER TABLE problems ADD COLUMN IF NOT EXISTS description TEXT")
cursor.execute("ALTER TABLE problems ADD COLUMN IF NOT EXISTS registration_message_id INTEGER")
conn.commit()

# Настройка ИИ
genai.configure(api_key=GEMINI_KEY, transport='rest')

print("Доступные модели:")
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(m.name)
except:
    print("Ошибка получения списка моделей")

# Инициализация модели
model = genai.GenerativeModel("gemini-flash-latest")

# Список магазинов
STORES = {
    "023": "Андижон", "126": "Афсона", "071": "Наманган", 
    "174": "Шимолий", "148": "Гранд", "177": "Азия", 
    "191": "Чорток", "164": "Узбегим", "109": "Турт куча", "054": "Навруз"
}

# Добавьте это здесь:
LATE_STORES = ["177", "164", "054"]

# Список менеджеров (username -> код магазина)
MANAGERS = {
    "JasurKazakov": "109",
    "Az1mov_K109": "109",
    "Umarov_K109_SV": "109",
    "Obidov_7700": "109",
    "Vazi_ra8": "109",
    # Турт куча
    "VahobjonovAbdulaziz": "054",
    "delphi1007": "054", 
    "Sherzodbek_Ruziev": "054",
    "QALAMPIRMUNCOQ": "054",
    "Kholmatov_21": "054",
    # Навруз
    "Samijonov_Azizbek": "148",
    "NasoyiddinovHusainbek": "148",
    "yoldoshev_038": "148",
    "Oybek_Ol1mjonov": "148",
    # Гранд
    "Kamolov_K164": "164",
    "KamolovRustambek": "164",
    "Jasurbek9770": "164",
    "Akmadjonov_95": "164",
    "Akbarov_1806": "164",
    # Узбеким
    "Qurbonov_Qodiriy": "191",
    "ulugbek_k191": "191",
    "Namangan_K191": "191",
    # Чорток
    "AbdurashidovDavron": "071",
    "Sultonbek_K071": "071",
    "Cash2oo2": "071",
    # Наманган
    "DilobarParmonova": "023",
    "Jalilovak023": "023",
    "NurulloK023": "023",
    # Андижан
    "Ibrohimjon0526": "177",
    "IAlievK177": "177",
    "Norkholikova": "177",
    # Азия
    "AbdumutalXudoyberdiyevK126": "126",
    "rustamovicho1": "126",
    "Nur1ddinov_K126": "126",
    "Islombek_D": "126",
    # Афсона
    "1655383135": "174", #Шимолий
    "m_gulamjanov": "174",
    "diyorbek1577": "174",
    "ALISHER_94_02_09": "174",
    "Asad_Axmedov": "174",
}

# Варианты написания для поиска (все в нижнем регистре)
STORE_VARIANTS = {
    "023": ["андижан", "андижон", "andijan", "andijon"],
    "174": ["шимолий", "shimoliy", "северный"],
    "071": ["наманган", "namangan"],
    "054": ["навруз", "navruz"],
    "126": ["афсона", "afsona"],
    "148": ["гранд", "grand"],
    "177": ["азия", "asia", "azia"],
    "191": ["чорток", "chortoq"],
    "164": ["узбегим", "uzbegim"],
    "109": ["турт куча", "торт куча", "4 куча", "turt kucha"]
}

# Список тем (Topics) — ОБЯЗАТЕЛЬНО ВПИШИТЕ ID ВАШЕЙ ТЕМЫ ВМЕСТО 000000
TOPICS = {
    "ХО": 123265, "Олов Таклиф": 200633, "Книга Жалоб": 53786, 
    "Открытие и Закрытие": 53684, "Алея и Промо": 123264, "Уборка": 84070, 
    "Планограмма": 142337, 
    "Проблемы": 140937,
    "Лого": 295526    # <--- УЗНАЙТЕ ID ТЕМЫ И ВПИШИТЕ СЮДА
}

# Базы данных
admin_selection = {}  
problems_db = {}      
store_kpi = {code: {"total_problems": 0, "fixed_problems": 0, "late_openings": 0} for code in STORES}
db = {code: {t: [] for t in TOPICS} for code in STORES}
db_times = {code: {"open": None, "close": None} for code in STORES}
awaiting_reason = {}

bot = Bot(token=TOKEN)
dp = Dispatcher()
# Теперь все задачи в cron будут работать по времени Ташкента
scheduler = AsyncIOScheduler(timezone=TASHKENT_TZ)

# ==========================================================
# 2. ИИ ФУНКЦИИ (ИСПРАВЛЕНО ДЛЯ НОВЫХ ВЕРСИЙ GEMINI)
# ==========================================================

async def ask_gemini_intent(text):
    """Определяет: Открытие, Закрытие или Другое"""
    try:
        prompt = f"Анализируй текст: '{text}'. Ответь ОДНИМ словом: OPEN, CLOSE или OTHER."
        # Используем синхронный метод .generate_content через asyncio.to_thread
        # Это предотвратит путаницу с 'coroutine object' и не заблокирует бота
        response = await asyncio.to_thread(model.generate_content, prompt)
        intent = response.text.strip().upper()
        return intent
    except Exception as e:
        print(f"AI Error (Intent): {e}")
        t = text.lower()
        if any(word in t for word in ["откр", "open", "начал"]): return "OPEN"
        if any(word in t for word in ["закр", "close", "заверш"]): return "CLOSE"
        return "OTHER"

async def check_is_complaint(text):
    """Проверяет: жалоба или просто отчет о пустоте"""
    try:
        prompt = f"Текст: '{text}'. Если в тексте написано, что жалоб или записей НЕТ — ответь NO. Если есть жалоба или новая запись — ответь YES."
        response = await asyncio.to_thread(model.generate_content, prompt)
        result = response.text.strip().upper()
        return "YES" in result
    except Exception as e:
        print(f"AI Error (Complaint): {e}")
        t = text.lower()
        if re.search(r'(нет|пусто|не име|0).*(запис|жалоб)', t) or re.search(r'(запис|жалоб).*(нет|пусто|0)', t):
            return False
        return any(word in t for word in ["жалоба", "запись", "кж", "претензия"])

# ==========================================================
# 3. ОТЧЕТНОСТЬ
# ==========================================================

async def send_actual_report(chat_id):
    now = get_now()
    today = now.date()
    # Фикс Лого: утром смотрим вчерашний вечер
    check_date_logo = today if now.hour >= 17 else (today - timedelta(days=1))
    clean_chat_id = str(GROUP_CHAT_ID).replace("-100", "")
    
    # 1. ОТКРЫТИЕ И ЗАКРЫТИЕ (Дизайн изменен на буллиты)
    report_open_close = f"🔑 **ОТКРЫТИЕ И ЗАКРЫТИЕ ({now.strftime('%d.%m %H:%M')})**\n"
    for c in STORES:
        op_time = db_times[c].get("open") or "❌"
        cl_time = db_times[c].get("close") or "❌"
        report_open_close += f"• {c}: {cl_time} | {op_time}\n"
        
    kb_oc = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Перейти в Открытие/Закрытие", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Открытие и Закрытие']}") ]])
    await bot.send_message(chat_id, report_open_close, reply_markup=kb_oc)

    # 2. ХО (Дизайн изменен на буллиты)
    report_xo = f"❄️ **ХО ({now.strftime('%d.%m %H:%M')})**\n"
    for c in STORES:
        data_xo = db[c].get("ХО", [])
        xo_check = "✅" if any((d["time"].date() if isinstance(d, dict) else d.date()) == today for d in data_xo) else "❌"
        report_xo += f"• {c}: {xo_check}\n"
        
    kb_xo = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Перейти в ХО", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['ХО']}") ]])
    await bot.send_message(chat_id, report_xo, reply_markup=kb_xo)

    # 3. ЛОГО (Дизайн изменен на буллиты)
    report_logo = f"💡 **ЛОГО ({now.strftime('%d.%m %H:%M')})**\n"
    for c in STORES:
        data_logo = db[c].get("Лого", [])
        e_check = "❌"
        for item in data_logo:
            dt = item["time"] if isinstance(item, dict) else item
            if dt.date() == check_date_logo and 17 <= dt.hour < 22:
                e_check = "✅"
                break
        report_logo += f"• {c}: {e_check}\n"
        
    kb_logo = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Перейти в Лого", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Лого']}") ]])
    await bot.send_message(chat_id, report_logo, reply_markup=kb_logo)

    # 4. КНИГА ЖАЛОБ (Без изменений, уже по стандарту)
    report_kj = f"📕 **КНИГА ЖАЛОБ (Утро | Новые)**\n"
    for c in STORES:
        data = db[c].get("Книга Жалоб", [])
        today_data = [item for item in data if isinstance(item, dict) and item["time"].date() == today]
        morning_check = "✅" if any(item.get("type") == "MORNING" for item in today_data) else "❌"
        new_complaint = "🔔" if any(item.get("type") == "NEW" for item in today_data) else "❌"
        report_kj += f"• {c}: {morning_check} | {new_complaint}\n"
    
    kb_kj = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Перейти в Книгу Жалоб", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Книга Жалоб']}") ]])
    await bot.send_message(chat_id, report_kj, reply_markup=kb_kj)

    # 5. ОСТАЛЬНЫЕ ТЕМЫ (Алея, Олов Таклиф, Планограмма)
    check_list = [("🌳 Алея и Промо", "Алея и Промо"), ("🔥 Олов Таклиф", "Олов Таклиф"), ("📐 Планограмма", "Планограмма")]
    for title, db_key in check_list:
        msg = f"**{title}**\n"
        for c in STORES:
            data = db[c].get(db_key, [])
            today_records = [d for d in data if (d["time"].date() if isinstance(d, dict) else d.date()) == today]
            if db_key == "Планограмма":
                status = f"{len(today_records)} фото/отчет" if today_records else "❌"
            else:
                status = "✅" if today_records else "❌"
            msg += f"• {c}: {status}\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"➡️ Перейти в {db_key}", url=f"https://t.me/c/{clean_chat_id}/{TOPICS[db_key]}") ]])
        await bot.send_message(chat_id, msg, reply_markup=kb)

# ==========================================================
# 4. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================================

def get_store_code(user: types.User):

    # 1️⃣ СНАЧАЛА ищем 3 цифры в имени (как раньше)
    if user.full_name:
        match = re.search(r'\b\d{3}\b', user.full_name)
        if match and match.group(0) in STORES:
            return match.group(0)

    # 2️⃣ Потом Telegram ID
    if user.id in MANAGERS:
        return MANAGERS[user.id]

    # 3️⃣ Потом username (без @)
    if user.username:
        username = user.username.lstrip("@")
        if username in MANAGERS:
            return MANAGERS[username]

    return None
    
def normalize(text: str) -> str:
    return text.lower().replace("ё", "е").strip()


def detect_intent(text: str) -> str:
    t = normalize(text)

    open_words = ["откр", "открыт", "начал", "работаем"]
    close_words = ["закр", "закрыт", "закончили", "уходим"]

    if any(w in t for w in open_words):
        return "OPEN"
    if any(w in t for w in close_words):
        return "CLOSE"

    return "OTHER"
    
async def get_store_code_safe(message: types.Message):
    user = message.from_user

    # --- 1. СТАРАЯ ЛОГИКА ---
    code = get_store_code(user)
    if code:
        return code

    # --- 2. ПЫТАЕМСЯ ВЗЯТЬ BIO ---
    try:
        chat_member = await bot.get_chat_member(message.chat.id, user.id)
        bio = (chat_member.user.bio or "").lower()
    except Exception as e:
        print("BIO ERROR:", e)
        return None

    # --- 3. ИЩЕМ КОД В BIO ---
    import re
    match = re.search(r'\b\d{3}\b', bio)
    if match:
        found = match.group(0)
        if found in STORES:
            print(f"DEBUG | code from bio: {found}")
            return found

    return None

# ==========================================================
# 5. ЕДИНЫЙ ОБРАБОТЧИК СООБЩЕНИЙ
# ==========================================================

@dp.message()
async def master_handler(message: types.Message):
    global awaiting_reason
    
    # 1. КОМАНДЫ (Сначала проверяем команды админа)
    if message.from_user.id in ADMIN_IDS:
        if message.text == "/report":
            await send_actual_report(message.chat.id)
            return
        if message.text == "/problems":
            await send_problems_report(message.chat.id)
            return
        if message.text == "/rating":
            await send_daily_rating(message.chat.id)
            return

    tid = message.message_thread_id
    now = datetime.now()
    content = message.text or message.caption 

# 2. СПЕЦИАЛЬНАЯ ЛОГИКА: ТЕМА "ПРОБЛЕМЫ"
    if tid == TOPICS.get('Проблемы'):
        text_clean = (message.text or message.caption or "").strip().lower()

        # --- 1️⃣ ИСПРАВЛЕНИЕ (reply "исправлено") ---
        if message.reply_to_message and "исправлено" in text_clean:
            parent = message.reply_to_message
            problem_id = str(parent.media_group_id or parent.message_id)

            cursor.execute("SELECT store_code FROM problems WHERE id = %s AND fixed = 0", (problem_id,))
            row = cursor.fetchone()

            if row:
                fixed_time = now.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute("UPDATE problems SET fixed = 1, fixed_at = %s WHERE id = %s", (fixed_time, problem_id))
                conn.commit()
                store_code = row[0]
                if store_code in store_kpi:
                    store_kpi[store_code]["fixed_problems"] += 1
                await message.reply("✅ Исправлено!")
            return

        # --- 2️⃣ ВЫБОР МАГАЗИНА И РЕГИСТРАЦИЯ ---
        if message.from_user.id in ADMIN_IDS:
            found_code = None
            raw_text = (message.text or message.caption or "").strip()

            # Ищем код или название в тексте
            if raw_text.upper() in STORES:
                found_code = raw_text.upper()
            if not found_code:
                for code_key, variants in STORE_VARIANTS.items():
                    if any(v.lower() in text_clean for v in variants):
                        found_code = code_key
                        break

            # Если нашли магазин — запоминаем выбор
            if found_code:
                admin_selection[message.from_user.id] = found_code
                await message.reply(f"🎯 Магазин: {STORES[found_code]}")
                # ❗ ВАЖНО: Если фото НЕТ, выходим. Если фото ЕСТЬ — идем дальше сохранять его!
                if not (message.photo or message.video):
                    return
                    
            # --- СОХРАНЕНИЕ ПРОБЛЕМЫ ---
            selected_store = admin_selection.get(message.from_user.id)
            if selected_store and (message.photo or message.video):
                group_id = str(message.media_group_id) if message.media_group_id else None
                problem_id = group_id if group_id else str(message.message_id)

                # В альбомах берем только фото с текстом
                if group_id and not message.caption:
                    return

                description = (message.caption or "").strip()

                # 1️⃣ Отправляем сообщение и получаем message_id
                sent_msg = await message.reply(f"❌ Зарегистрировано ({STORES[selected_store]})")
                registration_message_id = sent_msg.message_id

                # 2️⃣ Вставляем запись в БД уже с registration_message_id
                cursor.execute("SELECT 1 FROM problems WHERE id = %s", (problem_id,))
                if cursor.fetchone() is None:
                    cursor.execute("""
                        INSERT INTO problems 
                        (id, store_code, created_at, fixed, group_id, description, registration_message_id)
                        VALUES (%s, %s, %s, 0, %s, %s, %s)
                    """, (problem_id, selected_store, now.strftime("%Y-%m-%d %H:%M:%S"), group_id, description, registration_message_id))
                    conn.commit()

                # 3️⃣ Обновляем статистику
                if selected_store in store_kpi:
                    store_kpi[selected_store]["total_problems"] += 1

            elif not selected_store and (message.photo or message.video):
                await message.reply("⚠️ Сначала напишите название магазина (Шимолий, Азия и т.д.)")

    # 3. ЛОГИКА ПО ТЕМАМ
    # 3. ОБЫЧНАЯ ЛОГИКА (Теперь проверяет и список менеджеров, и имя профиля)
    if not message.from_user: return 
    code = get_store_code(message.from_user)

    if not code:
        print("⚠️ Не удалось определить магазин:",
              message.from_user.id,
              message.from_user.username,
              message.from_user.full_name)
        return
    # --- ОТКРЫТИЕ И ЗАКРЫТИЕ ---
    if tid == TOPICS['Открытие и Закрытие']:
        if content:
            # Если ждем объяснительную
            if code in awaiting_reason:
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, f"📝 **ОБЪЯСНИТЕЛЬНАЯ {code}:**")
                        await bot.copy_message(admin_id, message.chat.id, message.message_id)
                    except: pass
                
                await message.reply("✅ Объяснительная передана на рассмотрение РМ.")
                del awaiting_reason[code]
                return

            # Проверка намерения
            intent = await ask_gemini_intent(content)
            
            # Внутри master_handler, где OPEN
            if intent == "OPEN":
                db_times[code]["open"] = now.strftime("%H:%M")
                
                # Пользуемся глобальным LATE_STORES
                deadline_str = "07:31" if code in LATE_STORES else "06:46"
                deadline_time = datetime.strptime(deadline_str, "%H:%M").time()

                # Проверка на опоздание
                if now.time() > deadline_time:
                    awaiting_reason[code] = True
                    store_kpi[code]["late_openings"] += 1
                    cursor.execute("""
                    INSERT INTO late_openings (store_code, date)
                    VALUES (%s, %s)
                    """, (code, now.strftime("%Y-%m-%d")))
                    conn.commit()
                    await message.reply(f"⚠️ {code}: Вы открылись в {now.strftime('%H:%M')} (Дедлайн: {deadline_str}).\n❗️ Напишите причину опоздания в ответном сообщении.")
                    
                    for admin_id in ADMIN_IDS:
                        try:
                            await bot.send_message(admin_id, f"🚨 **ОПОЗДАНИЕ {code}:**\nМагазин открылся в {now.strftime('%H:%M')} (Поздно!)")
                        except: pass
            elif intent == "CLOSE":
                db_times[code]["close"] = now.strftime("%H:%M")

# --- КНИГА ЖАЛОБ (ОБНОВЛЕНО) ---
    elif tid == TOPICS['Книга Жалоб']:
        if "Книга Жалоб" not in db[code]: 
            db[code]["Книга Жалоб"] = []

        if content:
            is_complaint = await check_is_complaint(content)
            if is_complaint:
                # Сохраняем как новую жалобу
                db[code]["Книга Жалоб"].append({"time": now, "type": "NEW"})
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, f"🔔 **НОВАЯ ЗАПИСЬ В КЖ ({code}):**")
                        await bot.copy_message(admin_id, message.chat.id, message.message_id)
                    except: pass
            else:
                # Сохраняем как отчет об отсутствии (Утро)
                db[code]["Книга Жалоб"].append({"time": now, "type": "MORNING"})

    # --- ПЛАНОГРАММА (ОБНОВЛЕНО: Суббота/Воскресенье) ---
    elif tid == TOPICS['Планограмма']:
        if "Планограмма" not in db[code]: 
            db[code]["Планограмма"] = []
            
        is_sunday = now.weekday() == 6  # 6 — это воскресенье
        content_lower = content.lower() if content else ""

        if is_sunday:
            # В ВОСКРЕСЕНЬЕ: ждем строго "все планограммы выполнены"
            if "все планограммы выполнены" in content_lower:
                db[code]["Планограмма"].append({"time": now, "type": "FINAL_DONE"})
                await message.reply("✅ Принято! Финальный отчет за неделю зафиксирован.")
            elif message.photo:
                # Фото в воскресенье просто сохраняем в базу, но это не "финал"
                db[code]["Планограмма"].append({"time": now, "type": "PHOTO"})
        else:
            # В ОСТАЛЬНЫЕ ДНИ: фото или текст со словом "выполн"
            if message.photo or "выполн" in content_lower:
                db[code]["Планограмма"].append({"time": now, "type": "DAILY"})

    # Найти в master_handler блок для ХО и заменить на это:
    elif tid == TOPICS['ХО']:
        if "ХО" not in db[code]: db[code]["ХО"] = []
        db[code]["ХО"].append(now) # Фиксируем время отправки
    else:
        # Проверяем остальные темы (Алея, Олов, Уборка)
        for t_name, t_id in TOPICS.items():
            if tid == t_id:
                # Если ID темы совпал с одной из наших тем
                if t_name not in db[code]: db[code][t_name] = []
                
                # Сохраняем время (И фото, и текст считаются отчетом)
                db[code][t_name].append(now)
                break

async def export_weekly_excel():
    wb = Workbook()
    ws = wb.active
    ws.title = "Weekly KPI"

    ws.append(["Store", "Total Problems", "Fixed", "Percent"])

    cursor.execute("""
    SELECT store_code, COUNT(*), SUM(fixed)
    FROM problems
    GROUP BY store_code
    """)

    rows = cursor.fetchall()

    for row in rows:
        code = row[0]
        total = row[1]
        fixed = row[2] or 0
        percent = round((fixed / total) * 100) if total else 0

        ws.append([STORES[code], total, fixed, percent])

    file_name = "weekly_kpi.xlsx"
    wb.save(file_name)

    await bot.send_document(GROUP_CHAT_ID, types.FSInputFile(file_name))
                
async def send_weekly_rating():
    today = get_now()
    start_of_week = today - timedelta(days=today.weekday())
    last_monday = start_of_week - timedelta(days=7)
    last_sunday = start_of_week - timedelta(seconds=1)

    start = last_monday.strftime("%Y-%m-%d 00:00:00")
    end = last_sunday.strftime("%Y-%m-%d 23:59:59")

    stats = {}

    cursor.execute("""
    SELECT store_code, COUNT(*),
           SUM(fixed)
    FROM problems
    WHERE created_at BETWEEN %s AND %s
    GROUP BY store_code
    """, (start, end))

    rows = cursor.fetchall()

    for row in rows:
        code = row[0]
        total = row[1]
        fixed = row[2] or 0
        percent = round((fixed / total) * 100) if total else 0
        stats[code] = percent

    # 🔥 Штраф за опоздания
    cursor.execute("""
    SELECT store_code, COUNT(*)
    FROM late_openings
    WHERE date BETWEEN %s AND %s
    GROUP BY store_code
    """, (last_monday.strftime("%Y-%m-%d"),
          last_sunday.strftime("%Y-%m-%d")))

    late_rows = cursor.fetchall()

    for row in late_rows:
        code = row[0]
        late_count = row[1]
        penalty = late_count * 5
        if code in stats:
            stats[code] = max(0, stats[code] - penalty)

    if not stats:
        return

    rating = sorted(stats.items(), key=lambda x: x[1], reverse=True)

    text = f"🏆 НЕДЕЛЬНЫЙ KPI ({last_monday.strftime('%d.%m')} - {last_sunday.strftime('%d.%m')})\n\n"

    medals = ["🥇", "🥈", "🥉"]

    for i, (code, percent) in enumerate(rating):
        medal = medals[i] if i < 3 else "•"
        text += f"{medal} {STORES[code]} ({code}) — {percent}%\n"

    await bot.send_message(GROUP_CHAT_ID, text)

# ==========================================================
# ОБНОВЛЕННЫЕ ФУНКЦИИ ОТЧЕТНОСТИ (SQLite)
# ==========================================================

# ==========================================================
# БЛОК ОТЧЕТНОСТИ И АРХИВАЦИИ (SQLite)
# ==========================================================

def archive_old_problems():
    """Удаляет из базы данных проблемы старше 30 дней"""
    limit_date = (get_now() - timedelta(days=30)).strftime("%Y-%m-%d")
    try:
        cursor.execute("DELETE FROM problems WHERE date(created_at) < %s", (limit_date,))
        conn.commit()
        print(f"🧹 База очищена: старые записи до {limit_date} удалены.")
    except Exception as e:
        print(f"Ошибка при архивации: {e}")

async def send_daily_rating(chat_id):
    """Генерирует рейтинг магазинов за вчера на основе SQLite"""
    yesterday = (get_now() - timedelta(days=1)).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT store_code, COUNT(*), SUM(fixed)
        FROM problems
        WHERE date(created_at) = %s
        GROUP BY store_code
    """, (yesterday,))
    
    rows = cursor.fetchall()
    if not rows:
        await bot.send_message(chat_id, f"📊 Рейтинг за {yesterday}: замечаний не зафиксировано.")
        return

    rating_list = []
    for code, total, fixed in rows:
        fixed = fixed or 0
        percent = round((fixed / total) * 100)
        rating_list.append((code, percent, total, fixed))

    rating_list.sort(key=lambda x: x[1], reverse=True)
    text = f"🏆 **РЕЙТИНГ МАГАЗИНОВ ЗА {yesterday}**\n\n"
    for i, (code, percent, total, fixed) in enumerate(rating_list, 1):
        store_name = STORES.get(code, code)
        text += f"{i}. {store_name} — {percent}% ({fixed}/{total})\n"
    await bot.send_message(chat_id, text)

async def send_problems_report(chat_id, only_yesterday=False):
    """Детальный отчет по замечаниям с группировкой альбомов (LIVE и вчера)"""

    yesterday_str = (get_now() - timedelta(days=1)).strftime("%Y-%m-%d")
    store_issues = {code: [] for code in STORES}
    processed_groups = set()

    # --- Запрос из базы ---
    if only_yesterday:
        cursor.execute("""
            SELECT id, store_code, created_at, fixed, group_id, description, registration_message_id
            FROM problems
            WHERE date(created_at) = %s
        """, (yesterday_str,))
    else:
        cursor.execute("""
            SELECT id, store_code, created_at, fixed, group_id, description, registration_message_id
            FROM problems
        """)

    rows = cursor.fetchall()

    if not rows:
        await bot.send_message(chat_id, "✅ Замечаний нет.")
        return

    for msg_id, store_code, created_at, fixed, group_id, description, registration_message_id in rows:
        if store_code not in store_issues:
            continue

        unique_id = group_id if group_id else msg_id

        if unique_id in processed_groups:
            continue

        processed_groups.add(unique_id)

        issue_data = {
            "time": datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S"),
            "fixed": bool(fixed),
            "description": description or "Без описания",
            "is_album": bool(group_id),
            "registration_message_id": registration_message_id,
            "message_id": msg_id   # 👈 добавили
        }

        store_issues[store_code].append(issue_data)

    # --- Заголовок ---
    header = (
        "🌅 <b>ИТОГИ ПО ЗАМЕЧАНИЯМ (ВЧЕРА)</b>"
        if only_yesterday
        else "🛠 <b>ТЕКУЩИЕ ЗАМЕЧАНИЯ (LIVE)</b>"
    )

    # --- КНОПКА ПЕРЕХОДА В ТЕМУ ---
    problems_thread_id = TOPICS.get('Проблемы')

        # Убираем -100 у supergroup chat_id
    clean_chat_id = str(GROUP_CHAT_ID).replace("-100", "")

    # --- КНОПКА ПЕРЕХОДА В ТЕМУ ---
    topic_link = f"https://t.me/c/{clean_chat_id}/{problems_thread_id}"

    header_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="🔎 Перейти в тему проблем",
                url=topic_link
            )]
        ]
    )

    await bot.send_message(
        chat_id,
        header,
        parse_mode="HTML",
        reply_markup=header_keyboard
    )

    # --- Формирование отчета по магазинам ---
    for code, issues in store_issues.items():
        if not issues:
            continue

        issues.sort(key=lambda x: x["time"])

        for i, issue in enumerate(issues, 1):
            icon = "✅" if issue["fixed"] else "❌"
            label = "Альбом (несколько фото)" if issue["is_album"] else "Фото/Медиа"

            text = (
                f"🏪 <b>{STORES.get(code, code)}</b>\n\n"
                f"{i}. {icon} {label} ({issue['time'].strftime('%H:%M')})\n"
                f"📝 {issue['description']}"
            )

            # Если НЕ исправлено — добавляем кнопку к конкретному сообщению
            if not issue["fixed"]:
                message_link = f"tg://privatepost?channel={clean_chat_id}&post={issue['registration_message_id']}"

                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(
                            text="📎 Открыть замечание",
                            url=message_link
                        )]
                    ]
                )

                await bot.send_message(
                    chat_id,
                    text,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            else:
                await bot.send_message(
                    chat_id,
                    text,
                    parse_mode="HTML"
                )

async def job_daily_problems_report():
    """Ежедневная утренняя рассылка (Админам общая, Менеджерам персональная)"""
    yesterday_dt = get_now() - timedelta(days=1)
    yesterday_str = yesterday_dt.strftime("%Y-%m-%d")

    # 1. Админам (используем уже обновленную функцию с группировкой)
    for admin_id in ADMIN_IDS:
        try:
            await send_problems_report(admin_id, only_yesterday=True)
            await send_daily_rating(admin_id)
        except: pass

    # 2. Менеджерам персонально
    for m_id, store_code in MANAGERS.items():
        # Добавляем group_id в запрос
        cursor.execute("""
            SELECT fixed, created_at, group_id 
            FROM problems 
            WHERE store_code = %s AND date(created_at) = %s
        """, (store_code, yesterday_str))
        
        rows = cursor.fetchall()
        if not rows: continue

        processed_groups = set()
        unique_issues = []

        # Группируем данные перед отправкой менеджеру
        for is_fixed, created_at, group_id in rows:
            if group_id:
                if group_id not in processed_groups:
                    unique_issues.append({"fixed": is_fixed, "time": created_at, "is_album": True})
                    processed_groups.add(group_id)
            else:
                unique_issues.append({"fixed": is_fixed, "time": created_at, "is_album": False})

        total = len(unique_issues)
        fixed = sum(1 for issue in unique_issues if issue["fixed"] == 1)
        percent = round((fixed/total)*100) if total > 0 else 0
        
        text = f"📅 **ОТЧЕТ ЗА {yesterday_dt.strftime('%d.%m.%Y')}**\n"
        text += f"Магазин: {STORES.get(store_code, store_code)}\n"
        text += f"✅ Исправлено: {fixed}/{total} ({percent}%)\n\n"
        
        for i, issue in enumerate(unique_issues, 1):
            icon = "✅" if issue["fixed"] else "❌"
            time_str = issue["time"].split(" ")[1][:5] if " " in issue["time"] else ""
            label = "Альбом" if issue["is_album"] else "Замечание"
            text += f"{i}. {icon} {label} от {time_str}\n"
        
        try: 
            await bot.send_message(m_id, text)
        except: 
            pass

    archive_old_problems()

async def job_send_admin_report():
    """Отдельный технический отчет админам в 8:00"""
    for admin_id in ADMIN_IDS:
        try:
            # Убедитесь, что функция send_actual_report определена в коде
            await send_actual_report(admin_id)
        except Exception as e:
            print(f"Ошибка в job_send_admin_report: {e}")



async def job_9am_check_aleya_olov():
    """Проверка Алеи и Олов (только Обычные магазины)"""
    late_aleya = [c for c in STORES if c not in LATE_STORES and not db[c].get('Алея и Промо')]
    if late_aleya: await bot.send_message(GROUP_CHAT_ID, f"⚠️ **АЛЕЯ**: Нет фото от {', '.join(late_aleya)}", message_thread_id=TOPICS['Алея и Промо'])
    
    late_olov = [c for c in STORES if c not in LATE_STORES and not db[c].get('Олов Таклиф')]
    if late_olov: await bot.send_message(GROUP_CHAT_ID, f"⚠️ **Олов Таклиф**: Нет фото от {', '.join(late_olov)}", message_thread_id=TOPICS['Олов Таклиф'])

async def job_check_standard_opening():
    """Проверка открытия 06:50"""
    late = [c for c in STORES if c not in LATE_STORES and not db_times[c].get("open")]
    if late:
        stores_str = ", ".join(late)
        await bot.send_message(GROUP_CHAT_ID, f"🚨 **06:50:** {stores_str} не открылись!", message_thread_id=TOPICS['Открытие и Закрытие'])

async def job_check_late_stores_0835():
    """Единая проверка для 177, 164, 054 ровно в 07:35 (Сразу все утренние задачи)"""
    clean_chat_id = str(GROUP_CHAT_ID).replace("-100", "")
    
    # 1. Открытие
    late_open = [c for c in LATE_STORES if not db_times[c].get("open")]
    if late_open:
        stores_str = ", ".join(late_open)
        for c in late_open: awaiting_reason[c] = True
        await bot.send_message(GROUP_CHAT_ID, f"🚨 **07:35:** {stores_str} не открылись!", message_thread_id=TOPICS['Открытие и Закрытие'])
        kb_open = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✉️ Написать им", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Открытие и Закрытие']}")]])
        for admin_id in ADMIN_IDS:
            try: await bot.send_message(admin_id, f"🚨 **ОПОЗДАНИЕ 07:35 (177, 164, 054):**\n📍 {stores_str} не открылись!", reply_markup=kb_open)
            except: pass

    # 2. ХО
    late_xo = [c for c in LATE_STORES if not db[c].get('ХО')]
    if late_xo: await bot.send_message(GROUP_CHAT_ID, f"⚠️ **ХО (08:00)**: Нет фото от {', '.join(late_xo)}", message_thread_id=TOPICS['ХО'])

    # 3. КЖ
    late_kj = [c for c in LATE_STORES if not db[c].get('Книга Жалоб')]
    if late_kj:
        stores_str = ", ".join(late_kj)
        await bot.send_message(GROUP_CHAT_ID, f"⚠️ **Книга Жалоб (08:00)**: Нет отчета от {stores_str}", message_thread_id=TOPICS['Книга Жалоб'])

    # 4. Алея и Олов
    late_aleya = [c for c in LATE_STORES if not db[c].get('Алея и Промо')]
    if late_aleya: await bot.send_message(GROUP_CHAT_ID, f"⚠️ **АЛЕЯ (08:00)**: Нет фото от {', '.join(late_aleya)}", message_thread_id=TOPICS['Алея и Промо'])

    late_olov = [c for c in LATE_STORES if not db[c].get('Олов Таклиф')]
    if late_olov: await bot.send_message(GROUP_CHAT_ID, f"⚠️ **Олов Таклиф (08:00)**: Нет фото от {', '.join(late_olov)}", message_thread_id=TOPICS['Олов Таклиф'])

async def job_8am_check_kj():
    """Проверка КЖ (только Обычные магазины)"""
    late = [c for c in STORES if c not in LATE_STORES and not db[c].get('Книга Жалоб')]
    if late:
        stores_str = ", ".join(late)
        clean_chat_id = str(GROUP_CHAT_ID).replace("-100", "")
        await bot.send_message(GROUP_CHAT_ID, f"⚠️ **Книга Жалоб**: Нет отчета от {stores_str}", message_thread_id=TOPICS['Книга Жалоб'])
        kb_kj = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✉️ Написать в КЖ", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Книга Жалоб']}")]])
        for admin_id in ADMIN_IDS:
            try: await bot.send_message(admin_id, f"📕 **НЕТ ОТЧЕТА КЖ (08:10):**\n📍 {stores_str}", reply_markup=kb_kj)
            except: pass

async def job_wednesday_2100_cleaning():
    if get_now().weekday() == 2:
        late = [c for c in STORES if not db[c].get('Уборка')]
        if late: await bot.send_message(GROUP_CHAT_ID, f"🧹 **УБОРКА**: Где фото%s {', '.join(late)}", message_thread_id=TOPICS['Уборка'])

async def job_wednesday_night_aleya_check():
    late = []
    yesterday = get_now() - timedelta(days=1)
    for c in STORES:
        was_sent = any(dt.date() == yesterday.date() and 21 <= dt.hour <= 23 for dt in db[c].get('Алея и Промо', []))
        if not was_sent: late.append(c)
    if late:
        await bot.send_message(GROUP_CHAT_ID, f"🚨 **АЛЕЯ (ВЕЧЕР)**: Нет отчета от {', '.join(late)}", message_thread_id=TOPICS['Алея и Промо'])

async def job_sunday_1800_planogram():
    """Проверка в воскресенье 18:00: ищем фразу и уведомляем админов с кнопкой"""
    if get_now().weekday() == 6:  # 6 — воскресенье
        late = []
        for c in STORES:
            data = db[c].get('Планограмма', [])
            # Ищем запись типа 'FINAL_DONE'
            has_final_report = any(
                isinstance(item, dict) and item.get("type") == "FINAL_DONE" 
                for item in data
            )
            
            if not has_final_report:
                late.append(c)
        
        if late:
            stores_str = ", ".join(late)
            clean_chat_id = str(GROUP_CHAT_ID).replace("-100", "")
            
            # 1. Сообщение в общую группу (в тему Планограмма)
            text_group = f"📐 **ПЛАНОГРАММА (ДЕДЛАЙН)**: Магазины не прислали фразу 'все планограммы выполнены': {stores_str}"
            await bot.send_message(GROUP_CHAT_ID, text_group, message_thread_id=TOPICS['Планограмма'])

            # 2. Сообщение каждому админу с КНОПКОЙ
            kb_admin = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✉️ Написать им", 
                    url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Планограмма']}"
                )
            ]])

            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id, 
                        f"🚨 **ОТЧЕТ ДЛЯ АДМИНА (ВОСКРЕСЕНЬЕ)**\n\nСледующие магазины НЕ завершили планограммы:\n📍 {stores_str}",
                        reply_markup=kb_admin
                    )
                except Exception as e:
                    print(f"Ошибка отправки админу {admin_id}: {e}")

async def job_check_logo_2100():
    """Проверка Лого в 21:00 (проверяем тему 'Лого')"""
    late = []
    today = get_now().date()

    for c in STORES:
        # Берем данные из НОВОЙ темы
        data = db[c].get('Лого', [])
        # Проверяем: было ли фото сегодня с 17 до 21
        if not any(d.date() == today and 17 <= d.hour < 21 for d in data):
            late.append(c)
    
    if late:
        stores_str = ", ".join(late)
        clean_chat_id = str(GROUP_CHAT_ID).replace("-100", "")
        
        # Сообщение в НОВУЮ тему
        await bot.send_message(
            GROUP_CHAT_ID, 
            f"💡 **ВНИМАНИЕ (ЛОГО)!**\nМагазины: {stores_str}\nПожалуйста, включите Лого и отправьте фото СЮДА!", 
            message_thread_id=TOPICS['Лого']
        )
        
        # Кнопка для админов ведет в НОВУЮ тему
        kb_logo = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✉️ Написать в Лого", url=f"https://t.me/c/{clean_chat_id}/{TOPICS['Лого']}")
        ]])
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"🚨 **НЕТ ЛОГО (21:00):**\n📍 {stores_str}", reply_markup=kb_logo)
            except: pass

async def job_check_night_xo():
    """Проверка ХО с 21:00 до 00:00"""
    late = []
    for c in STORES:
        data = db[c].get('ХО', [])
        if not any(21 <= d.hour < 24 for d in data):
            late.append(c)
    
    if late:
        await bot.send_message(
            GROUP_CHAT_ID, 
            f"🌙 **ХО (НОЧЬ):** Нет отчета за период 21:00-00:00 от: {', '.join(late)}", 
            message_thread_id=TOPICS['ХО']
        )

async def job_midnight_reset_open_times():
    """Сброс времени открытия в полночь"""
    for code in STORES:
        db_times[code]["open"] = None

async def job_noon_reset_close_times():
    """Сброс времени закрытия днем (в 14:00)"""
    for code in STORES:
        db_times[code]["close"] = None

async def job_8am_check_xo():
    """Проверка ХО (только для обычных магазинов, кроме 177, 164, 054)"""
    # LATE_STORES должен быть определен в начале файла
    late = [c for c in STORES if c not in LATE_STORES and not db[c].get('ХО')]
    if late:
        stores_str = ", ".join(late)
        await bot.send_message(
            GROUP_CHAT_ID, 
            f"⚠️ **ХО (УТРО)**: Нет фото от {stores_str}", 
            message_thread_id=TOPICS['ХО']
        )

# --- ОБРАБОТЧИК РЕАКЦИЙ (Чтобы крестик менялся на галочку) ---
@dp.message_reaction()
async def on_reaction_changed(reaction: types.MessageReactionUpdated):
    # 1. Проверяем, что поставили именно ✅
    is_fixed = any(getattr(r, "emoji", None) == "✅" for r in reaction.new_reaction)
    
    if is_fixed:
        msg_id = str(reaction.message_id)
        now_str = get_now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 2. Обновляем статус в базе данных SQLite
        cursor.execute("""
            UPDATE problems 
            SET fixed = 1, fixed_at = %s 
            WHERE id = %s AND fixed = 0
        """, (now_str, msg_id))
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✅ Проблема {msg_id} отмечена как исправленная через реакцию.")
        
        # 3. Также проверяем по group_id (если это был альбом)
        cursor.execute("SELECT group_id FROM problems WHERE id = %s", (msg_id,))
        row = cursor.fetchone()
        if row and row[0]:
            cursor.execute("""
                UPDATE problems 
                SET fixed = 1, fixed_at = %s 
                WHERE group_id = %s AND fixed = 0
            """, (now_str, row[0]))
            conn.commit()

# ==========================================================
# 7. ЗАПУСК
# ==========================================================
cursor.execute("SELECT * FROM problems")
print(cursor.fetchall())

async def main():
    await bot.set_my_commands([
    BotCommand(command="report", description="Полный отчет"),
    BotCommand(command="problems", description="Текущие проблемы"),
    BotCommand(command="rating", description="Рейтинг магазинов")
])
    
    # --- ОБНОВЛЕННОЕ РАСПИСАНИЕ ---
    scheduler.add_job(send_weekly_rating, 'cron', day_of_week='mon', hour=10, minute=0)
    scheduler.add_job(export_weekly_excel, 'cron', day_of_week='mon', hour=9, minute=5)
    # Сброс времени открытия ночью (00:00) и закрытия днем (14:00)
    scheduler.add_job(job_midnight_reset_open_times, 'cron', hour=0, minute=0)
    scheduler.add_job(job_noon_reset_close_times, 'cron', hour=14, minute=0)
    # 1. Проверка обычных магазинов (дедлайн 6:45, проверяем в 6:50)
    scheduler.add_job(job_check_standard_opening, 'cron', hour=6, minute=50)
        # 3. Отчет админам ровно в 8:00
    # 8:05, 8:10, 9:00 — Проверки ХО, КЖ и Промо для ОБЫЧНЫХ
    scheduler.add_job(job_8am_check_xo, 'cron', hour=8, minute=5) 
    scheduler.add_job(job_8am_check_kj, 'cron', hour=8, minute=10)
    scheduler.add_job(job_9am_check_aleya_olov, 'cron', hour=9, minute=0)

    # 8:35 — Проверка 177, 164, 054 (ВСЕ ЗАДАЧИ СРАЗУ)
    scheduler.add_job(job_check_late_stores_0835, 'cron', hour=7, minute=35)

    # 8:40 — ОБЩИЙ ОТЧЕТ АДМИНАМ (Когда все данные уже есть)
    scheduler.add_job(job_send_admin_report, 'cron', hour=8, minute=40)
    scheduler.add_job(job_wednesday_2100_cleaning, 'cron', hour=18, minute=0)
    scheduler.add_job(job_wednesday_night_aleya_check, 'cron', day_of_week='thu', hour=0, minute=5)
    scheduler.add_job(job_sunday_1800_planogram, 'cron', day_of_week='sun', hour=18, minute=0)
    # Проверка Лого (Вечер) в 21:00
    scheduler.add_job(job_check_logo_2100, 'cron', hour=21, minute=0)
    # Отчет по проблемам за вчера в 09:00
    scheduler.add_job(job_daily_problems_report, 'cron', hour=9, minute=0)
    # Проверка ХО (Ночь) ровно в полночь за период 21-00
    scheduler.add_job(job_check_night_xo, 'cron', hour=0, minute=5)
    scheduler.start()
    print("🚀 БОТ ЗАПУЩЕН! (Режим: Разные дедлайны + Отчет в 8:00)")
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):

        pass













