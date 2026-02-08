import os
import json
import time
import logging
import subprocess
from typing import Dict, Set, Optional, Tuple, List

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from mcrcon import MCRcon

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mc-tg-bot")


# -----------------------------
# .env loader
# -----------------------------
def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")


def parse_int_set(raw: str) -> Set[int]:
    raw = (raw or "").strip()
    if not raw:
        return set()
    parts = [p.strip() for p in raw.replace(" ", "").split(",") if p.strip()]
    return {int(p) for p in parts}


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# -----------------------------
# Config
# -----------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
ADMIN_IDS = parse_int_set(os.getenv("TG_ADMIN_IDS", ""))

MC_RCON_HOST = os.getenv("MC_RCON_HOST", "127.0.0.1").strip()
MC_RCON_PORT = int(os.getenv("MC_RCON_PORT", "25575").strip())
MC_RCON_PASSWORD = os.getenv("MC_RCON_PASSWORD", "").strip()

MC_START_SCRIPT = os.getenv("MC_START_SCRIPT", "./start.sh").strip()
RATE_LIMIT_SECONDS = float(os.getenv("TG_RATE_LIMIT_SECONDS", "1.5").strip())

ALLOWED_CHAT_IDS = parse_int_set(os.getenv("TG_ALLOWED_CHAT_IDS", ""))

BOT_COMMANDS_FILE = os.getenv("BOT_COMMANDS_FILE", "./botcomand.conf").strip()
LINKS_DB_FILE = os.getenv("LINKS_DB_FILE", "./links.json").strip()
TG_USERS_DB_FILE = os.getenv("TG_USERS_DB_FILE", "./tg_users.json").strip()

LINK_CODE_TTL = int(os.getenv("LINK_CODE_TTL", "300").strip())
TG_OBJECTIVE = os.getenv("TG_TRIGGER_OBJECTIVE", "tgauth").strip()

START_STDOUT_LOG = os.path.join(BASE_DIR, "bot_start_stdout.log")
START_STDERR_LOG = os.path.join(BASE_DIR, "bot_start_stderr.log")

if not TG_BOT_TOKEN:
    raise SystemExit("TG_BOT_TOKEN is empty in .env")
if not MC_RCON_PASSWORD:
    raise SystemExit("MC_RCON_PASSWORD is empty in .env")


# -----------------------------
# Helpers
# -----------------------------
def abs_path(p: str) -> str:
    return p if os.path.isabs(p) else os.path.join(BASE_DIR, p)


def rcon_exec(cmd: str) -> str:
    with MCRcon(MC_RCON_HOST, MC_RCON_PASSWORD, port=MC_RCON_PORT) as mcr:
        resp = mcr.command(cmd)
    return (resp or "").strip()


_last_call: Dict[int, float] = {}


def rate_limited(user_id: int) -> bool:
    now = time.time()
    last = _last_call.get(user_id, 0.0)
    if now - last < RATE_LIMIT_SECONDS:
        return True
    _last_call[user_id] = now
    return False


def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    return uid is not None and uid in ADMIN_IDS


def is_chat_allowed(update: Update) -> bool:
    if not ALLOWED_CHAT_IDS:
        return True
    cid = update.effective_chat.id if update.effective_chat else None
    return cid is not None and cid in ALLOWED_CHAT_IDS


def allowed_admin(update: Update) -> bool:
    return is_admin(update) and is_chat_allowed(update)


def trim_telegram(text: str, limit: int = 3500) -> str:
    text = (text or "").strip()
    if not text:
        return "Пустой ответ."
    if len(text) > limit:
        return text[:limit] + "\n...(обрезано)"
    return text


def tail_file(path: str, lines: int) -> str:
    path = abs_path(path)
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "rb") as f:
            data = f.read()
        text = data.decode("utf-8", errors="replace")
        return "\n".join(text.splitlines()[-lines:])
    except Exception:
        return ""


# -----------------------------
# Start script
# -----------------------------
def run_start_script_detached() -> None:
    script_path = abs_path(MC_START_SCRIPT)
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Start script not found: {script_path}")

    out = open(abs_path(START_STDOUT_LOG), "ab", buffering=0)
    err = open(abs_path(START_STDERR_LOG), "ab", buffering=0)

    subprocess.Popen(
        ["bash", script_path],
        cwd=BASE_DIR,
        stdout=out,
        stderr=err,
        start_new_session=True,
    )


# -----------------------------
# botcomand.conf
# -----------------------------
_user_allowed_cmds: Set[str] = set()


def load_user_commands() -> None:
    global _user_allowed_cmds
    path = abs_path(BOT_COMMANDS_FILE)

    cmds: Set[str] = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                cmds.add(line.lower())

    _user_allowed_cmds = cmds


def user_cmd_allowed(cmd: str) -> bool:
    return cmd.lower() in _user_allowed_cmds


load_user_commands()


# -----------------------------
# Links DB: tg_user_id -> mc_name
# -----------------------------
_links: Dict[str, str] = {}


def load_links() -> None:
    global _links
    p = abs_path(LINKS_DB_FILE)
    if not os.path.exists(p):
        return
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _links = {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass


def save_links() -> None:
    p = abs_path(LINKS_DB_FILE)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_links, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)


def get_linked_mc(tg_user_id: int) -> Optional[str]:
    return _links.get(str(tg_user_id))


def set_link(tg_user_id: int, mc_name: str) -> None:
    _links[str(tg_user_id)] = mc_name
    save_links()


def unlink(tg_user_id: int) -> bool:
    k = str(tg_user_id)
    if k in _links:
        del _links[k]
        save_links()
        return True
    return False


load_links()


# -----------------------------
# Users DB: who uses bot
# -----------------------------
_users: Dict[str, dict] = {}


def load_users() -> None:
    global _users
    p = abs_path(TG_USERS_DB_FILE)
    if not os.path.exists(p):
        return
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _users = data
    except Exception:
        pass


def save_users() -> None:
    p = abs_path(TG_USERS_DB_FILE)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_users, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)


def touch_user(update: Update) -> None:
    try:
        u = update.effective_user
        if not u:
            return
        uid = str(u.id)
        now = int(time.time())

        rec = _users.get(uid, {})
        rec["id"] = u.id
        rec["username"] = u.username or ""
        rec["first_name"] = u.first_name or ""
        rec["last_name"] = u.last_name or ""
        rec["last_seen"] = now

        mc = get_linked_mc(u.id)
        rec["mc_name"] = mc or ""

        _users[uid] = rec
        save_users()
    except Exception:
        pass


load_users()


# -----------------------------
# Auth codes
# -----------------------------
_codes: Dict[str, Tuple[int, float]] = {}


def gen_code() -> str:
    return f"{int(time.time() * 1000) % 1000000:06d}"


def create_link_code(tg_user_id: int) -> str:
    code = gen_code()
    exp = time.time() + LINK_CODE_TTL
    _codes[code] = (tg_user_id, exp)
    return code


def consume_code(code: str) -> Optional[int]:
    item = _codes.get(code)
    if not item:
        return None
    tg_user_id, exp = item
    if exp < time.time():
        del _codes[code]
        return None
    del _codes[code]
    return tg_user_id


# -----------------------------
# Vanilla trigger helpers
# -----------------------------
def ensure_trigger_objective() -> None:
    rcon_exec(f"scoreboard objectives add {TG_OBJECTIVE} trigger")


def enable_trigger_for_player(mc_name: str) -> None:
    rcon_exec(f"scoreboard players enable {mc_name} {TG_OBJECTIVE}")


def get_player_score(mc_name: str) -> Optional[int]:
    out = rcon_exec(f"scoreboard players get {mc_name} {TG_OBJECTIVE}")
    parts = out.split()
    for token in reversed(parts):
        try:
            return int(token)
        except Exception:
            continue
    return None


def reset_player_score(mc_name: str) -> None:
    rcon_exec(f"scoreboard players reset {mc_name} {TG_OBJECTIVE}")


# -----------------------------
# Online parsing
# -----------------------------
def rcon_list_raw() -> str:
    return rcon_exec("list")


def parse_list_players(list_output: str) -> List[str]:
    # Typical: "There are X of a max of Y players online: a, b"
    if ":" not in list_output:
        return []
    after = list_output.split(":", 1)[1].strip()
    if not after:
        return []
    players = [p.strip() for p in after.split(",") if p.strip()]
    return players


def online_text() -> str:
    try:
        out = rcon_list_raw()
        return out or "Пустой ответ."
    except Exception as e:
        return f"Сервер оффлайн или RCON недоступен.\nОшибка: {e}"


# -----------------------------
# Reply keyboards (bottom panel)
# -----------------------------
BTN_ONLINE = "Онлайн"
BTN_TP = "ТП"
BTN_GM = "Режим"
BTN_LINK = "Привязать"
BTN_UNLINK = "Отвязать"
BTN_HELP = "Помощь"
BTN_ADMIN = "Админка"

BTN_BACK = "Назад"
BTN_COORDS = "Координаты"

BTN_GM_SURV = "Survival"
BTN_GM_CREA = "Creative"
BTN_GM_ADVE = "Adventure"
BTN_GM_SPEC = "Spectator"

A_USERS = "Пользователи"
A_BACKUP = "Бэкап"
A_START = "Старт"
A_STOP = "Стоп"
A_RESTART = "Рестарт"
A_LOGS = "Логи старта"


def kb_user(update: Update) -> ReplyKeyboardMarkup:
    uid = update.effective_user.id if update.effective_user else 0
    linked = bool(get_linked_mc(uid))

    row1 = [KeyboardButton(BTN_ONLINE), KeyboardButton(BTN_TP)]
    row2 = [KeyboardButton(BTN_GM), KeyboardButton(BTN_HELP)]
    row3 = [KeyboardButton(BTN_UNLINK if linked else BTN_LINK)]

    if allowed_admin(update):
        row3.append(KeyboardButton(BTN_ADMIN))

    return ReplyKeyboardMarkup(
        [row1, row2, row3],
        resize_keyboard=True,
        is_persistent=True,
    )


def kb_tp_players(players: List[str]) -> ReplyKeyboardMarkup:
    rows = []
    row = []
    for p in players[:24]:
        row.append(KeyboardButton(p))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([KeyboardButton(BTN_COORDS), KeyboardButton(BTN_BACK)])

    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)


def kb_gamemode() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_GM_SURV), KeyboardButton(BTN_GM_CREA)],
            [KeyboardButton(BTN_GM_ADVE), KeyboardButton(BTN_GM_SPEC)],
            [KeyboardButton(BTN_BACK)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def kb_admin() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_ONLINE), KeyboardButton(A_USERS)],
            [KeyboardButton(A_BACKUP)],
            [KeyboardButton(A_STOP), KeyboardButton(A_START), KeyboardButton(A_RESTART)],
            [KeyboardButton(A_LOGS), KeyboardButton(BTN_BACK)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


# -----------------------------
# State helpers
# -----------------------------
def set_state(context: ContextTypes.DEFAULT_TYPE, state: str) -> None:
    context.user_data["state"] = state


def get_state(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("state", "main")


def clear_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
    context.user_data.pop("tp_players", None)


# -----------------------------
# Admin report
# -----------------------------
def format_users_report(limit: int = 3500) -> str:
    items = list(_users.values())
    items.sort(key=lambda x: int(x.get("last_seen", 0)), reverse=True)

    lines = []
    lines.append(f"Пользователей в базе: {len(items)}")
    lines.append("Формат: MC | @username | имя | id | last_seen")
    lines.append("")

    for rec in items:
        mc = rec.get("mc_name", "") or "-"
        username = rec.get("username", "")
        uname = ("@" + username) if username else "-"
        name = (rec.get("first_name", "") + " " + rec.get("last_name", "")).strip() or "-"
        uid = rec.get("id", "-")
        last_seen = rec.get("last_seen", 0)
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(last_seen)))
        except Exception:
            ts = str(last_seen)

        lines.append(f"{mc} | {uname} | {name} | {uid} | {ts}")

        if len("\n".join(lines)) > limit:
            lines.append("...(обрезано)")
            break

    return "\n".join(lines)


# -----------------------------
# Commands
# -----------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.")
        return

    linked = get_linked_mc(uid)
    if linked:
        msg = (
            f"Аккаунт привязан: {linked}\n"
            "Нажимай кнопки снизу."
        )
    else:
        code = create_link_code(uid)
        msg = (
            "Привязка аккаунта:\n\n"
            f"1) В Minecraft введи:\n"
            f"/trigger {TG_OBJECTIVE} set {code}\n\n"
            f"2) Затем в Telegram:\n"
            f"/check <твой_nick>\n\n"
            f"Код действует {LINK_CODE_TTL} секунд."
        )

    clear_state(context)
    await update.message.reply_text(msg, reply_markup=kb_user(update))


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.")
        return

    if not context.args:
        await update.message.reply_text("Использование: /check <твой_nick>", reply_markup=kb_user(update))
        return

    mc_name = context.args[0].strip()
    if not mc_name:
        await update.message.reply_text("Укажи ник.", reply_markup=kb_user(update))
        return

    try:
        try:
            ensure_trigger_objective()
        except Exception:
            pass

        try:
            enable_trigger_for_player(mc_name)
        except Exception:
            pass

        score = get_player_score(mc_name)
        if score is None:
            await update.message.reply_text(
                "Не смог прочитать значение. Убедись, что ты на сервере и ввел /trigger.",
                reply_markup=kb_user(update),
            )
            return

        code = f"{score:06d}"
        tg_from_code = consume_code(code)

        if tg_from_code is None:
            await update.message.reply_text("Код неверный или истек. Сделай новый через /start.", reply_markup=kb_user(update))
            return

        if tg_from_code != uid:
            await update.message.reply_text("Этот код выдан не тебе. Получи свой через /start.", reply_markup=kb_user(update))
            return

        set_link(uid, mc_name)
        touch_user(update)

        try:
            reset_player_score(mc_name)
        except Exception:
            pass

        await update.message.reply_text(f"Привязка успешна: {mc_name}", reply_markup=kb_user(update))
    except Exception as e:
        await update.message.reply_text(f"Ошибка проверки: {e}", reply_markup=kb_user(update))


async def unlink_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if unlink(uid):
        touch_user(update)
        await update.message.reply_text("Отвязал аккаунт.", reply_markup=kb_user(update))
    else:
        await update.message.reply_text("У тебя не было привязки.", reply_markup=kb_user(update))


async def online_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.", reply_markup=kb_user(update))
        return

    await update.message.reply_text(trim_telegram(online_text()), reply_markup=kb_user(update))


async def tp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # оставим команду, но основной UX через кнопки
    touch_user(update)
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.", reply_markup=kb_user(update))
        return

    if not user_cmd_allowed("tp"):
        await update.message.reply_text("Команда tp отключена админом.", reply_markup=kb_user(update))
        return

    mc_name = get_linked_mc(uid)
    if not mc_name:
        await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
        return

    if not context.args:
        await update.message.reply_text("Использование: /tp <ник> или /tp <x y z>", reply_markup=kb_user(update))
        return

    args = context.args
    try:
        if len(args) == 1:
            target = args[0]
            cmdline = f"tp {mc_name} {target}"
        elif len(args) == 3:
            x, y, z = args
            float(x); float(y); float(z)
            cmdline = f"tp {mc_name} {x} {y} {z}"
        else:
            await update.message.reply_text("Использование: /tp <ник> или /tp <x y z>", reply_markup=kb_user(update))
            return

        resp = rcon_exec(cmdline)
        await update.message.reply_text(resp if resp else "Готово.", reply_markup=kb_user(update))
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}", reply_markup=kb_user(update))


async def gamemode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # оставим команду, но основной UX через кнопки
    touch_user(update)
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.", reply_markup=kb_user(update))
        return

    if not user_cmd_allowed("gamemode"):
        await update.message.reply_text("Команда gamemode отключена админом.", reply_markup=kb_user(update))
        return

    mc_name = get_linked_mc(uid)
    if not mc_name:
        await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
        return

    if not context.args:
        await update.message.reply_text("Использование: /gamemode <survival|creative|adventure|spectator>", reply_markup=kb_user(update))
        return

    mode = context.args[0].lower()
    allowed_modes = {"survival", "creative", "adventure", "spectator", "s", "c", "a", "sp"}
    if mode not in allowed_modes:
        await update.message.reply_text("Разрешено: survival, creative, adventure, spectator", reply_markup=kb_user(update))
        return

    try:
        cmdline = f"gamemode {mode} {mc_name}"
        resp = rcon_exec(cmdline)
        await update.message.reply_text(resp if resp else "Готово.", reply_markup=kb_user(update))
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}", reply_markup=kb_user(update))


# Admin command: raw RCON
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    if not allowed_admin(update):
        await update.message.reply_text("Доступ запрещен.", reply_markup=kb_user(update))
        return

    uid = update.effective_user.id
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.", reply_markup=kb_admin())
        return

    if not context.args:
        await update.message.reply_text("Использование: /cmd <команда>", reply_markup=kb_admin())
        return

    cmdline = " ".join(context.args).strip()
    try:
        resp = rcon_exec(cmdline)
        await update.message.reply_text(trim_telegram(resp), reply_markup=kb_admin())
    except Exception as e:
        await update.message.reply_text(f"Ошибка RCON: {e}", reply_markup=kb_admin())


async def reload_user_cmds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    if not allowed_admin(update):
        await update.message.reply_text("Доступ запрещен.", reply_markup=kb_user(update))
        return
    load_user_commands()
    await update.message.reply_text("botcomand.conf перечитан.", reply_markup=kb_admin())


# -----------------------------
# Text handler for bottom buttons
# -----------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    touch_user(update)
    if not update.message or not update.message.text:
        return

    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return

    text = update.message.text.strip()
    state = get_state(context)

    # Back always works
    if text == BTN_BACK:
        clear_state(context)
        await update.message.reply_text("Ок.", reply_markup=kb_user(update))
        return

    # Admin panel switch
    if text == BTN_ADMIN and allowed_admin(update):
        clear_state(context)
        set_state(context, "admin")
        await update.message.reply_text("Админка.", reply_markup=kb_admin())
        return

    # If in admin panel
    if state == "admin" and allowed_admin(update):
        if rate_limited(uid):
            await update.message.reply_text("Слишком часто, подожди.", reply_markup=kb_admin())
            return

        if text == BTN_ONLINE:
            await update.message.reply_text(trim_telegram(online_text()), reply_markup=kb_admin())
            return

        if text == A_USERS:
            await update.message.reply_text(trim_telegram(format_users_report()), reply_markup=kb_admin())
            return

        if text == A_BACKUP:
            try:
                rcon_exec("backuper backup local")
                await update.message.reply_text("Бэкап запущен.", reply_markup=kb_admin())
            except Exception as e:
                await update.message.reply_text(f"Ошибка бэкапа: {e}", reply_markup=kb_admin())
            return

        if text == A_STOP:
            try:
                rcon_exec("stop")
                await update.message.reply_text("Отправил stop.", reply_markup=kb_admin())
            except Exception as e:
                await update.message.reply_text(f"Ошибка stop: {e}", reply_markup=kb_admin())
            return

        if text == A_START:
            try:
                run_start_script_detached()
                await update.message.reply_text("Запустил start.sh.", reply_markup=kb_admin())
            except Exception as e:
                await update.message.reply_text(f"Ошибка старта: {e}", reply_markup=kb_admin())
            return

        if text == A_RESTART:
            try:
                try:
                    rcon_exec("stop")
                except Exception:
                    pass
                await update.message.reply_text("Рестарт: через 10 секунд запуск.", reply_markup=kb_admin())
                context.job_queue.run_once(job_start, when=10, data={"chat_id": update.effective_chat.id})
            except Exception as e:
                await update.message.reply_text(f"Ошибка рестарта: {e}", reply_markup=kb_admin())
            return

        if text == A_LOGS:
            out_tail = tail_file(START_STDOUT_LOG, 40)
            err_tail = tail_file(START_STDERR_LOG, 80)
            msg = "stdout:\n" + (out_tail or "(пусто)") + "\n\nstderr:\n" + (err_tail or "(пусто)")
            await update.message.reply_text(trim_telegram(msg), reply_markup=kb_admin())
            return

        # Unknown in admin
        await update.message.reply_text("Не понял кнопку. Нажми Назад.", reply_markup=kb_admin())
        return

    # Public flow
    if rate_limited(uid):
        await update.message.reply_text("Слишком часто, подожди.", reply_markup=kb_user(update))
        return

    # Help
    if text == BTN_HELP:
        msg = (
            "Кнопки снизу:\n"
            "- Онлайн: онлайн и игроки\n"
            "- ТП: телепорт к игроку или по координатам\n"
            "- Режим: смена режима себе\n"
            "- Привязать/Отвязать\n\n"
            "Команды:\n"
            "/start, /online, /check <nick>, /unlink\n"
        )
        await update.message.reply_text(msg, reply_markup=kb_user(update))
        return

    # Link/unlink
    if text == BTN_LINK:
        if get_linked_mc(uid):
            await update.message.reply_text("У тебя уже есть привязка.", reply_markup=kb_user(update))
            return
        code = create_link_code(uid)
        msg = (
            "Привязка аккаунта:\n\n"
            f"1) В Minecraft введи:\n"
            f"/trigger {TG_OBJECTIVE} set {code}\n\n"
            f"2) Затем в Telegram:\n"
            f"/check <твой_nick>\n\n"
            f"Код действует {LINK_CODE_TTL} секунд."
        )
        await update.message.reply_text(msg, reply_markup=kb_user(update))
        return

    if text == BTN_UNLINK:
        if unlink(uid):
            touch_user(update)
            await update.message.reply_text("Отвязал аккаунт.", reply_markup=kb_user(update))
        else:
            await update.message.reply_text("У тебя не было привязки.", reply_markup=kb_user(update))
        return

    # Online
    if text == BTN_ONLINE:
        await update.message.reply_text(trim_telegram(online_text()), reply_markup=kb_user(update))
        return

    # TP flow
    if text == BTN_TP:
        if not user_cmd_allowed("tp"):
            await update.message.reply_text("Команда tp отключена админом.", reply_markup=kb_user(update))
            return
        mc_name = get_linked_mc(uid)
        if not mc_name:
            await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
            return

        try:
            raw = rcon_list_raw()
            players = parse_list_players(raw)
        except Exception:
            players = []

        set_state(context, "tp_menu")
        context.user_data["tp_players"] = players

        if players:
            await update.message.reply_text("Выбери игрока или Координаты:", reply_markup=kb_tp_players(players))
        else:
            await update.message.reply_text("Нет игроков онлайн. Нажми Координаты или Назад.", reply_markup=kb_tp_players([]))
        return

    if state == "tp_menu":
        mc_name = get_linked_mc(uid)
        if not mc_name:
            clear_state(context)
            await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
            return

        if text == BTN_COORDS:
            set_state(context, "tp_coords")
            await update.message.reply_text("Введи координаты: x y z", reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True, is_persistent=True))
            return

        players = context.user_data.get("tp_players", [])
        if text in players:
            try:
                resp = rcon_exec(f"tp {mc_name} {text}")
                clear_state(context)
                await update.message.reply_text(resp if resp else "Готово.", reply_markup=kb_user(update))
            except Exception as e:
                clear_state(context)
                await update.message.reply_text(f"Ошибка: {e}", reply_markup=kb_user(update))
            return

        await update.message.reply_text("Выбери игрока из списка или Координаты, либо Назад.", reply_markup=kb_tp_players(players))
        return

    if state == "tp_coords":
        mc_name = get_linked_mc(uid)
        if not mc_name:
            clear_state(context)
            await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
            return

        parts = text.split()
        if len(parts) != 3:
            await update.message.reply_text("Нужно 3 числа: x y z (или Назад)", reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True, is_persistent=True))
            return

        try:
            x, y, z = parts
            float(x); float(y); float(z)
            resp = rcon_exec(f"tp {mc_name} {x} {y} {z}")
            clear_state(context)
            await update.message.reply_text(resp if resp else "Готово.", reply_markup=kb_user(update))
        except Exception as e:
            await update.message.reply_text(f"Ошибка: {e}", reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True, is_persistent=True))
        return

    # Gamemode flow
    if text == BTN_GM:
        if not user_cmd_allowed("gamemode"):
            await update.message.reply_text("Команда gamemode отключена админом.", reply_markup=kb_user(update))
            return
        mc_name = get_linked_mc(uid)
        if not mc_name:
            await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
            return
        set_state(context, "gm_menu")
        await update.message.reply_text("Выбери режим:", reply_markup=kb_gamemode())
        return

    if state == "gm_menu":
        mc_name = get_linked_mc(uid)
        if not mc_name:
            clear_state(context)
            await update.message.reply_text("Сначала привяжи аккаунт через /start.", reply_markup=kb_user(update))
            return

        mapping = {
            BTN_GM_SURV: "survival",
            BTN_GM_CREA: "creative",
            BTN_GM_ADVE: "adventure",
            BTN_GM_SPEC: "spectator",
        }
        if text in mapping:
            try:
                resp = rcon_exec(f"gamemode {mapping[text]} {mc_name}")
                clear_state(context)
                await update.message.reply_text(resp if resp else "Готово.", reply_markup=kb_user(update))
            except Exception as e:
                clear_state(context)
                await update.message.reply_text(f"Ошибка: {e}", reply_markup=kb_user(update))
            return

        await update.message.reply_text("Выбери режим кнопкой или Назад.", reply_markup=kb_gamemode())
        return

    # Fallback
    await update.message.reply_text("Не понял. Нажми кнопку снизу или /start.", reply_markup=kb_user(update))


async def job_start(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = int(ctx.job.data["chat_id"])
    try:
        run_start_script_detached()
        await ctx.bot.send_message(chat_id=chat_id, text="start.sh запущен.")
    except Exception as e:
        await ctx.bot.send_message(chat_id=chat_id, text=f"Ошибка старта: {e}")


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    app = Application.builder().token(TG_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("check", check_cmd))
    app.add_handler(CommandHandler("unlink", unlink_cmd))
    app.add_handler(CommandHandler("online", online_cmd))
    app.add_handler(CommandHandler("tp", tp_cmd))
    app.add_handler(CommandHandler("gamemode", gamemode_cmd))

    app.add_handler(CommandHandler("cmd", admin_cmd))
    app.add_handler(CommandHandler("reloadcmds", reload_user_cmds))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    log.info("Bot started. objective=%s admins=%s", TG_OBJECTIVE, sorted(ADMIN_IDS))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
