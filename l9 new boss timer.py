# -*- coding: utf-8 -*-
# ===== L9 Boss Timer (Hybrid: interval from sheet, fixed & world fixed times) =====

import os, re, time, asyncio
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List

import json
from aiohttp import web

import pytz
import gspread
import discord
from discord.ext import commands, tasks
from discord.ext.commands import has_permissions
from discord.ui import View, Button
from discord import Interaction
from oauth2client.service_account import ServiceAccountCredentials

# ========= ตั้งค่า Discord =========
app_prefix = "!"
TOKEN = os.getenv("DISCORD_TOKEN") or "MTQwNDE2MTcxMTYyMzI0MTc1OQ.GIHFcE.sJ71c7bkJd5PR3SOhp1e0KYaS5tI1daASeYOIw"   # แนะนำตั้งเป็น ENV

# ========= ตั้งค่า Google Sheet =========
SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID", "1Ps_sLYIA3j9WWyrP7kLN4g-kPaN_rPBTqSmsxzxzarQ")  # <- ไอดีไฟล์ใหม่ของคุณ
SPREADSHEET_URL  = ""        # ปล่อยว่าง เพื่อตัดสลับไปไฟล์อื่นโดยไม่ตั้งใจ
SPREADSHEET_NAME = ""        # ปล่อยว่าง (เราใช้ ID เปิดไฟล์)

# ใช้ “ชื่อแท็บแบบตายตัว”
PRIMARY_WS   = "boss_timer"
FALLBACK_WS  = "Boss Tracker"
READ_RANGE   = "A1:K2000"   # A..K = 11 คอลัมน์ตามชีต

# ห้องปลายทาง
CHANNEL_ID_DEFAULT = 1404159701750382673
SHEET_CHANNEL_MAP = {
    PRIMARY_WS: CHANNEL_ID_DEFAULT,
    FALLBACK_WS: CHANNEL_ID_DEFAULT,
    "Fixed": CHANNEL_ID_DEFAULT,   # เอาไว้ส่ง world/fixed ที่คำนวณในโค้ด
}

# แจ้งเตือน
ROLE_ID_1 = 1402706627886321724
ROLE_ID_2 = 1402025046850932856

ALERT_THRESHOLDS_MIN = [60, 30, 5]   # เวิลด์บอสจะใช้แค่ T-5 แบบรวมบรรทัด
ALERT_WINDOW_SEC = 75

# UI
DEFAULT_EMBED_COLOR = 0xffcc00
NORMAL_EMOJI = "💠"
WORLD_EMOJI  = "🌍"

# เวิลด์บอส
WORLD_BOSSES = {"ลาตัน", "พาร์โต", "เนดร้า"}
NAME_ALIASES = {"พาร์โต้": "พาร์โต"}  # กันสะกด

def normalize_name(n: str) -> str:
    n = (n or "").strip()
    return NAME_ALIASES.get(n, n)

def is_world_boss(name: str) -> bool:
    return normalize_name(name.replace(" (พรุ่งนี้)", "").strip()) in WORLD_BOSSES

tz = pytz.timezone("Asia/Bangkok")

# === Mini Web for health/status on Koyeb ===
STARTED_AT = datetime.now(tz)

async def handle_root(request):
    return web.Response(text="L9 Boss Timer Bot is running.")

async def handle_healthz(request):
    now = datetime.now(tz)
    payload = {
        "ok": True,
        "now": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "uptime_sec": int((now - STARTED_AT).total_seconds()),
        "spreadsheet_id": os.getenv("SPREADSHEET_ID", ""),
        "worksheet": PRIMARY_WS,
    }
    return web.json_response(payload)

async def start_http_server():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/healthz", handle_healthz)
    # Koyeb จะกำหนด PORT ให้ใน ENV เสมอ
    port = int(os.getenv("PORT", "8080"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()





# ========= Discord client =========
intents = discord.Intents.default()
intents.message_content = True
allowed_mentions = discord.AllowedMentions(roles=True)
bot = commands.Bot(command_prefix=app_prefix, intents=intents, allowed_mentions=allowed_mentions)

def log(msg: str):
    print(datetime.now(tz).strftime("[%H:%M:%S]"), msg)

# ========= Google Sheets helper =========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

def gspread_client():
    # เขียนไฟล์ credentials.json จาก ENV ถ้ามี
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        with open("credentials.json", "w", encoding="utf-8") as f:
            f.write(creds_json)

    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPES)
    return gspread.authorize(creds), creds


def open_spreadsheet(client):
    if SPREADSHEET_ID:   return client.open_by_key(SPREADSHEET_ID), "ID"
    if SPREADSHEET_URL:  return client.open_by_url(SPREADSHEET_URL), "URL"
    if SPREADSHEET_NAME: return client.open(SPREADSHEET_NAME), "NAME"
    raise RuntimeError("ยังไม่ได้กำหนด SPREADSHEET_ID / URL / NAME")

def values_get_ws(ss, ws_title, a1_range=READ_RANGE):
    rng = f"'{ws_title}'!{a1_range}"
    return ss.values_get(rng).get("values", [])

# ========= Parsing / utils =========
BAD_TOKENS = {"", "-", "#VALUE!", "NA", "N/A", "None", "null", "NULL"}
TIME_RE   = re.compile(r"^\s*(\d{1,2})[:\.](\d{2})(?::(\d{2}))?\s*$")   # HH:MM[:SS]
WINDOW_RE = re.compile(r"(\d{1,2}:\d{2})\s*[–\-]\s*(\d{1,2}:\d{2})")
HOURS_RE  = re.compile(r"(\d+)\s*ชั่วโมง", re.I)

WEEKDAY_THAI = ["จันทร์","อังคาร","พุธ","พฤหัสบดี","ศุกร์","เสาร์","อาทิตย์"]
WEEKDAY_MAP  = {"จันทร์":0,"อังคาร":1,"พุธ":2,"พฤหัส":3,"พฤหัสบดี":3,"ศุกร์":4,"เสาร์":5,"อาทิตย์":6}

def weekday_th(dt: datetime) -> str:
    return WEEKDAY_THAI[dt.weekday()]

def parse_time_of_day(raw) -> Optional[Tuple[int,int,int]]:
    if raw is None: return None
    s = str(raw).strip()
    if s in BAD_TOKENS: return None
    m = TIME_RE.match(s)
    if not m: return None
    hh, mm = int(m.group(1)), int(m.group(2))
    ss = int(m.group(3) or 0)
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59): return None
    return hh, mm, ss

def parse_gsheet_date(raw) -> Optional[date]:
    if raw is None: return None
    s = str(raw).strip()
    if s in BAD_TOKENS: return None
    # serial number
    try:
        if isinstance(raw, (int, float)):
            base = datetime(1899,12,30)
            return (base + timedelta(days=float(raw))).date()
    except Exception:
        pass
    # text date
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def parse_kill_dt(raw) -> Optional[datetime]:
    """แปลงค่า kill_dt จากชีตเป็น datetime (รองรับ serial และสตริงหลายฟอร์แม็ต)"""
    if raw is None:
        return None
    s = str(raw).strip()
    if s in BAD_TOKENS:
        return None
    # serial (Google Sheets)
    try:
        if isinstance(raw, (int, float)):
            base = datetime(1899, 12, 30)
            return tz.localize(base + timedelta(days=float(raw)))
    except Exception:
        pass
    # หลายฟอร์แม็ตที่พบบ่อย
    for fmt in (
        "%d/%m/%Y, %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y, %H:%M",    "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",  "%Y-%m-%d %H:%M",
    ):
        try:
            return tz.localize(datetime.strptime(s, fmt))
        except Exception:
            continue
    return None

def near_same_minute(a: Optional[datetime], b: Optional[datetime], tol_min: int = 2) -> bool:
    """ตรวจว่าเวลาสองค่าใกล้กันภายใน tol_min นาที (ใช้จับกรณี next/date = เวลา kill)"""
    if not a or not b:
        return False
    return abs(int((a - b).total_seconds()) // 60) <= tol_min

def next_from_weekday_time(day_name: str, hh: int, mm: int, now_dt: datetime) -> datetime:
    wd = WEEKDAY_MAP.get(day_name)
    if wd is None: raise ValueError(f"ไม่รู้จักชื่อวัน: {day_name}")
    base = tz.localize(datetime(now_dt.year, now_dt.month, now_dt.day, hh, mm))
    cand = base + timedelta(days=(wd - now_dt.weekday()) % 7)
    if cand <= now_dt: cand += timedelta(days=7)
    return cand

def compute_next_interval(kill_dt: Optional[datetime], hours: Optional[int], now_dt: datetime) -> Optional[datetime]:
    """คำนวณรอบถัดไปจาก kill_dt + hours (ทบให้ > ตอนนี้)"""
    if not kill_dt or not hours:
        return None
    dt = kill_dt + timedelta(hours=hours)
    if dt <= now_dt:
        diff_h = (now_dt - kill_dt).total_seconds() / 3600.0
        steps = int(diff_h // hours) + 1
        dt = kill_dt + timedelta(hours=hours * steps)
    return dt

def parse_weekly_pairs(detail: str) -> List[Tuple[str,str]]:
    """อ่านรูปแบบ 'อังคาร 10:30; พฤหัส 18:00' หรือ 'อาทิตย์ 16:00-19:00' (ใช้ต้นหน้าต่าง)"""
    if not detail: return []
    parts = [p.strip() for p in str(detail).split(";") if p.strip()]
    out = []
    for p in parts:
        for day_th in WEEKDAY_MAP.keys():
            if p.startswith(day_th):
                rest = p[len(day_th):].strip()
                m = WINDOW_RE.search(rest)
                t = (m.group(1) if m else rest).strip()
                out.append((day_th, t))
                break
    return out

def _norm_header(s: str) -> str:
    # ล้างช่องว่างแปลก ๆ แล้วแปลงเป็นตัวพิมพ์เล็ก
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()

def build_col_idx(headers: list) -> dict:
    norm = [_norm_header(h) for h in headers]

    def find(*cands):
        for c in cands:
            c = _norm_header(c)
            if c in norm:
                return norm.index(c)
        return None

    # ชุดหัวคอลัมน์ของตารางใหม่
    return {
        "level":        find("level", "lvl", "เลเวล"),
        "name":         find("name", "ชื่อ"),
        "location":     find("location", "สถานที่", "จุดเกิด", "แผนที่"),
        "kill_time":    find("kill_time", "เวลาตาย"),
        "next_spawn":   find("next_spawn", "เวลาถัดไป", "next"),
        "date_kill":    find("date_kill", "วันที่ตาย"),
        "date_spawn":   find("date_spawn", "วันที่ถัดไป", "วันที่เกิดถัดไป"),
        "spawn_type":   find("spawn_type", "ประเภท"),
        "spawn_detail": find("spawn_detail", "รายละเอียด"),
        "note":         find("note", "หมายเหตุ"),
        "kill_dt":      find("kill_dt", "kill datetime", "เวลาตายเต็ม"),
    }


# ====== mapping คงที่ ======
# A..K = [level, name, location, kill_time, next_spawn, date_kill, date_spawn, spawn_type, spawn_detail, note, kill_dt]
IDX = {
    "level":0, "name":1, "location":2, "kill_time":3, "next_spawn":4,
    "date_kill":5, "date_spawn":6, "spawn_type":7, "spawn_detail":8, "note":9, "kill_dt":10
}

# fixed รายสัปดาห์ (กรณีชื่อเหล่านี้ให้มีอย่างน้อยตามนี้)
FIXED_WEEKLY_TIMES = {
    "คลาแมนทีส": [("จันทร์","10:30"), ("พฤหัส","18:00")],
    "ซาฟิรัส":   [("อาทิตย์","16:00"), ("อังคาร","10:30")],
    "นิวโทร":    [("อังคาร","18:00"), ("พฤหัส","10:30")],
    "ไธเมล":     [("จันทร์","18:00"), ("พุธ","10:30")],
    "มิลลาวี":   [("เสาร์","14:00")],
    "ริงกอร์":   [("เสาร์","16:00")],
    "โรเดอริก":  [("ศุกร์","18:00")],
    "ออรัค":     [("อาทิตย์","20:00"), ("พุธ","20:00")],
}

# เวิลด์บอส: ทุกวัน 10:00 และ 19:00
WORLD_DAILY_TIMES = ["10:00", "19:00"]

def fixed_bosses_next(now_dt: datetime) -> List[Tuple[str,str,datetime,str,str]]:
    """คืนรายการ fixed + world รอบถัดไป"""
    out = []
    # รายสัปดาห์ตามแม็พ
    for name, pairs in FIXED_WEEKLY_TIMES.items():
        cands = []
        for day_th, hhmm in pairs:
            t = parse_time_of_day(hhmm)
            if not t: continue
            cands.append(next_from_weekday_time(day_th, t[0], t[1], now_dt))
        if cands:
            out.append(("Fixed", name, min(cands), "", ""))

    # เวิลด์บอส
    wcands = []
    for t in WORLD_DAILY_TIMES:
        hh, mm, _ = parse_time_of_day(t)
        dt = tz.localize(datetime(now_dt.year, now_dt.month, now_dt.day, hh, mm))
        if dt <= now_dt: dt += timedelta(days=1)
        wcands.append(dt)
    w_dt = min(wcands)
    for name in WORLD_BOSSES:
        out.append(("Fixed", name, w_dt, "", ""))
    return out

def choose_ws(ss) -> str:
    """เลือกแท็บที่หัวคอลัมน์ครบของตารางใหม่เป็นอันดับแรก"""
    titles = [ws.title for ws in ss.worksheets()]

    # สแกนทุกแท็บหาแท็บที่มีหัวคอลัมน์ครบ
    for ws in ss.worksheets():
        try:
            head_vals = ss.values_get(f"'{ws.title}'!A1:Z1").get("values", [])
            if not head_vals:
                continue
            col = build_col_idx(head_vals[0])
            needed = ("name", "spawn_type", "next_spawn", "date_spawn")
            if all(col.get(k) is not None for k in needed):
                log(f"[AUTO] เลือกแท็บ '{ws.title}' (หัวคอลัมน์ครบ)")
                return ws.title
        except Exception:
            continue

    # ไม่พบ → fallback (พร้อมเตือน)
    if PRIMARY_WS in titles:
        log(f"[WARN] ไม่พบแท็บหัวคอลัมน์ครบ ใช้ '{PRIMARY_WS}' แทน")
        return PRIMARY_WS
    if FALLBACK_WS in titles:
        log(f"[WARN] ไม่พบแท็บหัวคอลัมน์ครบ ใช้ '{FALLBACK_WS}' (interval จะไม่ถูกอ่าน)")
        return FALLBACK_WS

    raise RuntimeError("ไม่พบแท็บที่รองรับตารางใหม่ (ต้องมี name/spawn_type/next_spawn/date_spawn)")


# ========= อ่านข้อมูล + คำนวณรอบถัดไป =========
def get_boss_from_sheet(max_retry=3):
    """
    คืน [(ws_name, name, spawn_dt, level, location)]

    กติกา:
    - เติม Fixed (รายสัปดาห์ + เวิลด์บอส 10:00/19:00) จาก fixed_bosses_next() ก่อน
    - interval: ใช้ next_spawn + date_spawn เป็นหลัก
        * ถ้าเวลาที่ได้ใกล้ kill_dt (±2 นาที) หรือได้เวลาย้อนอดีต → fallback เป็น kill_dt + X ชั่วโมง
        * ถ้าไม่มี next/date เลย → fallback เป็น kill_dt + X ชั่วโมง (ถ้ามี)
    - fixed: คำนวณจาก spawn_detail (คู่วัน/เวลา) เพื่อรองรับ fixed ที่ไม่อยู่ในแม็พ
    - อื่นๆ: ถ้ามี date+time ให้ใช้ได้เลย
    """
    bosses, seen = [], set()
    now_dt = datetime.now(tz)

    # 1) เติม fixed รายสัปดาห์ + worldboss จากโค้ด
    for ws_name, name, dt, level, location in fixed_bosses_next(now_dt):
        k = f"{name}|{dt.strftime('%Y%m%d%H%M')}"
        if k not in seen:
            seen.add(k)
            bosses.append((ws_name, name, dt, level, location))

    # 2) อ่านจากชีต (map ด้วยหัวคอลัมน์จริง)
    client, _ = gspread_client()
    ss, _ = open_spreadsheet(client)
    ws_name = choose_ws(ss)

    for i in range(max_retry):
        try:
            rows = values_get_ws(ss, ws_name, READ_RANGE)
            if not rows or len(rows) < 2:
                log(f"[WARN] '{ws_name}' ว่าง")
                break

            header = rows[0]
            col = build_col_idx(header)
            log(f"[DEBUG] ใช้แท็บ: {ws_name}")
            log(f"[DEBUG] header: {header}")
            log(f"[DEBUG] colmap: {col}")

            def cell(row, key):
                idx = col.get(key)
                if idx is None or idx >= len(row):
                    return ""
                return row[idx]

            # ข้ามหัวตารางแถวแรก
            for r in rows[1:]:
                name = (cell(r, "name") or "").strip()
                if not name or name in BAD_TOKENS:
                    continue

                # worldboss ถูกคำนวณจาก fixed_bosses_next แล้ว — ข้ามเพื่อกันซ้ำ
                if is_world_boss(name):
                    continue

                level     = (str(cell(r, "level")) or "").strip()
                location  = (cell(r, "location") or "").strip()
                sp_type   = (str(cell(r, "spawn_type")) or "").strip().lower()
                detail    = (cell(r, "spawn_detail") or "").strip()
                raw_date  = cell(r, "date_spawn")
                raw_next  = cell(r, "next_spawn")
                kill_dt   = parse_kill_dt(cell(r, "kill_dt"))

                d  = parse_gsheet_date(raw_date)
                t  = parse_time_of_day(raw_next)
                dt = None

                if sp_type == "fixed":
                    # fixed: ใช้คู่วัน/เวลาใน spawn_detail
                    pairs = parse_weekly_pairs(detail)
                    cands = []
                    for day_th, hhmm in pairs:
                        tm = parse_time_of_day(hhmm)
                        if not tm:
                            continue
                        cands.append(next_from_weekday_time(day_th, tm[0], tm[1], now_dt))
                    if cands:
                        dt = min(cands)

                elif sp_type == "interval":
                    # interval: ใช้ next_spawn + date_spawn เป็นหลัก
                    m = HOURS_RE.search(detail or "")
                    hours = int(m.group(1)) if m else None

                    if d and t:
                        cand = tz.localize(datetime(d.year, d.month, d.day, t[0], t[1], t[2]))
                        # ถ้า cand ใกล้ kill_dt หรือ cand ย้อนอดีต → fallback เป็น kill_dt + ชั่วโมง
                        if ((kill_dt and near_same_minute(cand, kill_dt)) or cand <= now_dt) and hours:
                            dt = compute_next_interval(kill_dt, hours, now_dt)
                        else:
                            dt = cand

                    elif t and not d:
                        cand = tz.localize(datetime(now_dt.year, now_dt.month, now_dt.day, t[0], t[1], t[2]))
                        if cand <= now_dt:
                            cand += timedelta(days=1)
                        if kill_dt and near_same_minute(cand, kill_dt) and hours:
                            dt = compute_next_interval(kill_dt, hours, now_dt)
                        else:
                            dt = cand

                    # ถ้ายังไม่มี dt และมี kill_dt+hours → ใช้เป็น fallback
                    if dt is None and hours and kill_dt:
                        dt = compute_next_interval(kill_dt, hours, now_dt)

                else:
                    # ประเภทอื่น: ถ้ามี date+time ก็ใช้เลย
                    if d and t:
                        dt = tz.localize(datetime(d.year, d.month, d.day, t[0], t[1], t[2]))
                    elif t and not d:
                        cand = tz.localize(datetime(now_dt.year, now_dt.month, now_dt.day, t[0], t[1], t[2]))
                        if cand <= now_dt:
                            cand += timedelta(days=1)
                        dt = cand

                # กรองค่าไม่ได้/ย้อนหลัง
                if not dt or dt <= now_dt:
                    continue

                k = f"{name}|{dt.strftime('%Y%m%d%H%M')}"
                if k in seen:
                    continue
                seen.add(k)
                bosses.append((ws_name, name, dt, level, location))
            break

        except Exception as e:
            log(f"[ERROR] อ่าน '{ws_name}' ล้มเหลวครั้งที่ {i+1}: {e}")
            time.sleep(2)

    bosses.sort(key=lambda x: x[2])
    log(f"โหลด {len(bosses)} รายการ จากแท็บ: ['{ws_name}', 'Fixed']")
    return bosses



# ========= วนลูปแจ้งเตือน =========
alerted = set()

@tasks.loop(seconds=60)
async def check_alerts():
    try:
        now = datetime.now(tz)
        bosses = get_boss_from_sheet()

        # เวิลด์บอส: รวมเวลาตรงกัน แจ้งเฉพาะ T-5
        world_groups = {}
        for ws, name, spawn_dt, *_ in bosses:
            if is_world_boss(name) and ws == "Fixed":
                key = spawn_dt.strftime("%Y%m%d-%H%M")
                world_groups.setdefault(key, {"dt": spawn_dt, "names": set(), "ws": ws})
                world_groups[key]["names"].add(normalize_name(name))

        for key, info in world_groups.items():
            if info["names"] == WORLD_BOSSES:
                spawn_dt = info["dt"]
                delta = (spawn_dt - now).total_seconds()
                th_sec = 5 * 60
                alert_key = f"group:{key}_T5"
                if (th_sec - ALERT_WINDOW_SEC) < delta <= (th_sec + ALERT_WINDOW_SEC) and alert_key not in alerted:
                    alerted.add(alert_key)
                    channel_id = SHEET_CHANNEL_MAP.get(info["ws"], CHANNEL_ID_DEFAULT)
                    ch = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                    embed = discord.Embed(
                        title=f"{WORLD_EMOJI} Worldboss: ลาตัน, พาร์โต, เนดร้า",
                        description="⏰ จะเกิดในอีก 5 นาที",
                        color=DEFAULT_EMBED_COLOR
                    )
                    mention = f"<@&{ROLE_ID_1}> <@&{ROLE_ID_2}>"
                    try:
                        await ch.send(content=mention, embed=embed)
                        log("[WorldBoss] แจ้งรวม T-5m")
                    except Exception as e:
                        log(f"[ERROR] ส่งแจ้งเตือนรวมบอสโลก T-5: {e}")

        # รายตัว 60/30/5 (ยกเว้นเวิลด์บอส)
        for ws, name, spawn_dt, level, location in bosses:
            if is_world_boss(name):
                continue
            delta = (spawn_dt - now).total_seconds()
            if delta <= 0:
                continue
            for th_min in ALERT_THRESHOLDS_MIN:
                th_sec = th_min * 60
                key = f"{ws}:{name}_{spawn_dt.strftime('%Y%m%d-%H%M')}_T{th_min}"
                if (th_sec - ALERT_WINDOW_SEC) < delta <= (th_sec + ALERT_WINDOW_SEC) and key not in alerted:
                    alerted.add(key)
                    channel_id = SHEET_CHANNEL_MAP.get(ws, CHANNEL_ID_DEFAULT)
                    ch = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                    title = f"📅 ตารางแน่นอน: {name}" + (f" Lv.{level}" if level else "")
                    desc = []
                    if location: desc.append(f"📍 {location}")
                    desc.append(f"🕘 รอบต่อไป: {spawn_dt.strftime('%H:%M')} ({weekday_th(spawn_dt)} {spawn_dt.strftime('%d/%m')})")
                    desc.append(f"⏰ แจ้งเตือนล่วงหน้า {th_min} นาที")
                    embed = discord.Embed(title=title, description="\n".join(desc), color=DEFAULT_EMBED_COLOR)
                    try:
                        if th_min == 5:
                            mention = f"<@&{ROLE_ID_1}> <@&{ROLE_ID_2}>"
                            await ch.send(content=mention, embed=embed)
                        else:
                            await ch.send(embed=embed)
                        log(f"[{ws}] แจ้ง {name} T-{th_min}m")
                    except Exception as e:
                        log(f"[ERROR] ส่งแจ้งเตือน {name} T-{th_min}: {e}")
    except Exception as e:
        log(f"[ERROR] check_alerts crash: {e}")

# ========= Commands =========
@bot.command()
async def ping(ctx):
    await ctx.send("✅ บอทยังทำงานอยู่")

@bot.command()
async def boss(ctx):
    """แสดงเฉพาะ 'รอบถัดไป' แบบข้ามวัน (รวม Fixed + ชีตตามกติกาใหม่)"""
    now = datetime.now(tz)
    bosses = get_boss_from_sheet()
    embed = discord.Embed(title="🕒 ตารางเกิดถัดไป", color=0x00ccff)

    # รวม worldboss เวลาเดียวกันให้เหลือ 1 บรรทัด
    world_groups, used = {}, set()
    for ws, name, spawn_dt, *_ in bosses:
        if is_world_boss(name) and ws == "Fixed":
            key = spawn_dt.strftime("%Y%m%d-%H%M")
            world_groups.setdefault(key, {"dt": spawn_dt, "names": set()})
            world_groups[key]["names"].add(normalize_name(name))

    count = 0
    for ws, name, spawn_dt, level, location in bosses:
        delta_m = int((spawn_dt - now).total_seconds()//60)
        if delta_m <= 0:
            continue

        if is_world_boss(name) and ws == "Fixed":
            key = spawn_dt.strftime("%Y%m%d-%H%M")
            if key in used: 
                continue
            if world_groups.get(key, {}).get("names") == WORLD_BOSSES:
                embed.add_field(
                    name=f"{WORLD_EMOJI} Worldboss",
                    value=f"{spawn_dt.strftime('%H:%M')} ({weekday_th(spawn_dt)} {spawn_dt.strftime('%d/%m')}) • ลาตัน • พาร์โต • เนดร้า • อีก {delta_m} นาที",
                    inline=False
                )
                used.add(key)
                count += 1
            continue

        title = f"{NORMAL_EMOJI} {name}" + (f" • Lv.{level}" if level else "")
        line  = f"{spawn_dt.strftime('%H:%M')} ({weekday_th(spawn_dt)} {spawn_dt.strftime('%d/%m')})"
        if location: line += f" • {location}"
        line += f" • อีก {delta_m} นาที"
        embed.add_field(name=title, value=line, inline=False)
        count += 1
        if count >= 25:
            break

    if count == 0:
        embed.description = "❌ ยังไม่มีบอสที่จะเกิดถัดไป"
    embed.set_footer(text="อัปเดตเมื่อ " + now.strftime("%d/%m/%Y %H:%M"))
    await ctx.send(embed=embed)

# === ลบข้อความทั้งหมดในห้อง ===
@bot.command()
@has_permissions(manage_messages=True)
async def deleteall(ctx):
    allowed_channels = set(SHEET_CHANNEL_MAP.values()) | {CHANNEL_ID_DEFAULT}
    if ctx.channel.id not in allowed_channels:
        await ctx.send("❌ ใช้ได้เฉพาะในห้องแจ้งเตือนบอสเท่านั้น", delete_after=10)
        return

    class ConfirmDelete(View):
        @discord.ui.button(label="ยืนยันลบทั้งหมด", style=discord.ButtonStyle.danger)
        async def confirm(self, interaction: Interaction, button: Button):
            if interaction.user != ctx.author:
                await interaction.response.send_message("⛔ คุณไม่ใช่ผู้สั่งลบ", ephemeral=True)
                return
            deleted = 0
            async for msg in ctx.channel.history(limit=None):
                try:
                    await msg.delete()
                    deleted += 1
                    await asyncio.sleep(0.3)
                except:
                    pass
            await interaction.channel.send(f"🧹 ลบข้อความทั้งหมดแล้ว ({deleted} ข้อความ)", delete_after=10)
            self.stop()

        @discord.ui.button(label="ยกเลิก", style=discord.ButtonStyle.secondary)
        async def cancel(self, interaction: Interaction, button: Button):
            if interaction.user != ctx.author:
                await interaction.response.send_message("⛔ ไม่ใช่คนสั่ง", ephemeral=True)
                return
            await interaction.response.edit_message(content="❌ ยกเลิกแล้ว", view=None)
            self.stop()

    await ctx.send("⚠️ ต้องการลบข้อความทั้งหมดในห้องนี้ใช่ไหม?", view=ConfirmDelete())

# ========= Debug =========
def debug_list_tabs():
    try:
        client, creds = gspread_client()
        ss, how = open_spreadsheet(client)
        tabs = [ws.title for ws in ss.worksheets()]
        sa_email = getattr(creds, "_service_account_email", "(unknown)")
        log(f"[DEBUG] Opened by {how}. Worksheets: {tabs}")
        log(f"[DEBUG] Service Account email (แชร์สิทธิ์ไฟล์ให้บัญชีนี้): {sa_email}")
    except gspread.exceptions.SpreadsheetNotFound:
        log("[DEBUG] SpreadsheetNotFound: เปิดไฟล์ไม่สำเร็จ — ตรวจ ID/URL/NAME และการแชร์สิทธิ์")
    except Exception as e:
        log(f"[DEBUG] list tabs error: {type(e).__name__}: {e}")

# ========= Lifecycle =========
@bot.event
async def on_ready():
    log(f"✅ Logged in as {bot.user}")
    debug_list_tabs()
    if not check_alerts.is_running():
        check_alerts.start()

# ========= Run =========
if __name__ == "__main__":
    bot.run(TOKEN)
