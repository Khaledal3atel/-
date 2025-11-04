import random
import time
import re
import requests
import json
import os
import asyncio
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from collections import defaultdict

BOT_TOKEN = "8494424963:AAEWupeNLhnLu5uOnoGx36N1gdqYK60Pf9s" 
ADMIN_IDS = {5562144078}

message_cache = {}
last_save_time = time.time()
save_interval = 2.0

chat_locks = defaultdict(asyncio.Lock)

URLS = {
    "جمم": "https://raw.githubusercontent.com/AL3ATEL/TXT-bot-telegram-/refs/heads/main/sentences.txt",
    "شرط": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-2/refs/heads/main/conditions.txt",
    "فكك": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-3/refs/heads/main/FKK.txt",
    "صج": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-4/refs/heads/main/arabic_sentences.json",
    "جش": "https://raw.githubusercontent.com/BoulahiaAhmed/Arabic-Quotes-Dataset/main/Arabic_Quotes.csv",
    "شك": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-5/refs/heads/main/3amh.txt",
    "ويكي": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/Wekebedea.txt"
}

REPEAT_WORDS = ["صمت", "صوف", "سين", "عين", "جيم", "كتب", "خبر", "حلم", "جمل", "تعب", "عرب", "نار", "برد", "نسر", "حبل", "شك", "درب", "خسر", "علم", "صوت"]

CONDITIONS = [
    "كرر أول كلمة",
    "كرر ثاني كلمة",
    "كرر آخر كلمة",
    "كرر أول كلمة وآخر كلمة",
    "فكك أول كلمة",
    "فكك آخر كلمة",
    "بدل أول كلمتين",
    "بدل آخر كلمتين",
    "بدل ثاني كلمة والكلمة الأخيرة"
]

CHAR_MAP = {'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ى': 'ي', 'ة': 'ه', 'ئ': 'ي', 'ؤ': 'و', 'ٱ': 'ا', 'ٳ': 'ا'}
NUM_WORDS = {'0': 'صفر', '1': 'واحد', '2': 'اثنان', '3': 'ثلاثة', '4': 'أربعة', '5': 'خمسة', '6': 'ستة', '7': 'سبعة', '8': 'ثمانية', '9': 'تسعة', '10': 'عشرة', '11': 'احدى عشر', '12': 'اثنا عشر', '13': 'ثلاثة عشر', '14': 'أربعة عشر', '15': 'خمسة عشر', '16': 'ستة عشر', '17': 'سبعة عشر', '18': 'ثمانية عشر', '19': 'تسعة عشر', '20': 'عشرون', '30': 'ثلاثون', '40': 'أربعون', '50': 'خمسون', '60': 'ستون', '70': 'سبعون', '80': 'ثمانون', '90': 'تسعون', '100': 'مائة', '1000': 'ألف'}

class Storage:
    def __init__(self):
        self.file = "bot_data.json"
        self.data = self.load()
        self.dirty = False

    def load(self):
        try:
            with open(self.file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {
                "users": {},
                "chats": {},
                "banned": [],
                "scores": {},
                "patterns": {},
                "sessions": {},
                "awards": {},
                "weekly_awards": {},
                "stats": {},
                "broadcast_mode": {},
                "rounds": {},
                "round_mode": {}
            }

    def save(self, force=False):
        global last_save_time
        current_time = time.time()

        if not self.dirty and not force:
            return

        if not force and (current_time - last_save_time) < save_interval:
            return

        try:
            with open(self.file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            self.dirty = False
            last_save_time = current_time
            print(f"[STORAGE] Data saved successfully at {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Error saving data: {e}")

    def mark_dirty(self):
        self.dirty = True

    def add_user(self, uid, usr, name):
        self.data["users"][str(uid)] = {
            "username": usr,
            "first_name": name,
            "created_at": datetime.now().isoformat()
        }
        self.mark_dirty()

    def add_chat(self, cid, title):
        self.data["chats"][str(cid)] = {
            "title": title,
            "created_at": datetime.now().isoformat()
        }
        self.mark_dirty()

    def is_banned(self, uid):
        uid_str = str(uid)
        is_in_list = uid_str in self.data["banned"]
        print(f"[BAN_CHECK] User {uid} - Banned: {is_in_list} - Banned list: {self.data['banned']}")
        return is_in_list

    def ban_user(self, uid):
        uid_str = str(uid)
        if uid_str not in self.data["banned"]:
            self.data["banned"].append(uid_str)
            print(f"[BAN] User {uid} has been banned. Updated banned list: {self.data['banned']}")

        sessions_to_remove = []
        for key, session in self.data["sessions"].items():
            if session.get("starter_uid") == uid:
                sessions_to_remove.append(key)

        for key in sessions_to_remove:
            self.data["sessions"].pop(key, None)

        self.mark_dirty()
        self.save(force=True)

    def unban_user(self, uid):
        uid_str = str(uid)
        print(f"[UNBAN] Attempting to unban user {uid}. Current banned list: {self.data['banned']}")

        if uid_str in self.data["banned"]:
            self.data["banned"].remove(uid_str)
            self.mark_dirty()
            self.save(force=True)
            print(f"[UNBAN] User {uid} has been unbanned. Updated banned list: {self.data['banned']}")
            return True
        else:
            print(f"[UNBAN] User {uid} was not in banned list")
            return False

    def update_score(self, uid, typ, wpm):
        key = f"{uid}_{typ}"
        self.data["scores"][key] = max(self.data["scores"].get(key, 0), wpm)
        self.mark_dirty()

    def get_score(self, uid, typ):
        return self.data["scores"].get(f"{uid}_{typ}", 0)

    def add_pattern(self, uid, key):
        if str(uid) not in self.data["patterns"]:
            self.data["patterns"][str(uid)] = []
        if key not in self.data["patterns"][str(uid)]:
            self.data["patterns"][str(uid)].append(key)
            self.mark_dirty()

    def is_pattern_used(self, uid, key):
        return key in self.data["patterns"].get(str(uid), [])

    def clear_patterns(self, uid):
        self.data["patterns"][str(uid)] = []
        self.mark_dirty()

    def save_session(self, uid, cid, typ, txt, tm, sent=False):
        key = f"{cid}_{typ}"
        self.data["sessions"][key] = {
            "type": typ,
            "text": txt,
            "time": tm,
            "starter_uid": uid,
            "sent": sent
        }
        self.mark_dirty()

    def mark_session_sent(self, cid, typ):
        key = f"{cid}_{typ}"
        if key in self.data["sessions"]:
            self.data["sessions"][key]["sent"] = True
            self.mark_dirty()

    def get_session(self, cid, typ):
        return self.data["sessions"].get(f"{cid}_{typ}")

    def get_all_active_sessions(self, cid):
        expired_keys = []
        active_sessions = []

        for key, session in list(self.data["sessions"].items()):
            if key.startswith(f"{cid}_"):
                elapsed = time.time() - session.get("time", 0)
                if elapsed <= 60:
                    active_sessions.append(session)
                else:
                    expired_keys.append(key)

        for key in expired_keys:
            self.data["sessions"].pop(key, None)
        if expired_keys:
            self.mark_dirty()

        return active_sessions

    def del_session(self, cid, typ):
        self.data["sessions"].pop(f"{cid}_{typ}", None)
        self.mark_dirty()

    def cancel_user_session_in_type(self, uid, cid, typ):
        key = f"{cid}_{typ}"
        session = self.data["sessions"].get(key)
        if session and session.get("starter_uid") == uid:
            self.data["sessions"].pop(key, None)
            self.mark_dirty()
            return True
        return False

    def get_leaderboard(self, typ):
        scores = []
        for k, v in self.data["scores"].items():
            if k.endswith(f"_{typ}"):
                uid = k.split('_')[0]
                user_data = self.data["users"].get(uid, {})
                username = user_data.get("username")
                first_name = user_data.get("first_name", "مستخدم")
                scores.append((uid, username, first_name, v))

        scores.sort(key=lambda x: x[3], reverse=True)
        return scores[:3]

    def add_award(self, uid, name, wpm, typ):
        if str(uid) not in self.data["weekly_awards"]:
            self.data["weekly_awards"][str(uid)] = []

        self.data["weekly_awards"][str(uid)].append({
            "name": name,
            "wpm": wpm,
            "type": typ,
            "date": datetime.now().isoformat()
        })
        self.mark_dirty()

    def get_awards(self, uid):
        return self.data["weekly_awards"].get(str(uid), [])

    def log_cmd(self, cmd):
        dt = datetime.now().strftime("%Y-%m-%d")
        if dt not in self.data["stats"]:
            self.data["stats"][dt] = {}
        if cmd not in self.data["stats"][dt]:
            self.data["stats"][dt][cmd] = 0
        self.data["stats"][dt][cmd] += 1
        self.mark_dirty()

    def set_broadcast_mode(self, uid, status):
        self.data["broadcast_mode"][str(uid)] = status
        self.mark_dirty()
        self.save(force=True)

    def get_broadcast_mode(self, uid):
        return self.data["broadcast_mode"].get(str(uid), False)

    def start_round(self, cid, target):
        self.data["rounds"][str(cid)] = {
            "target": target,
            "wins": {},
            "started_at": datetime.now().isoformat(),
            "last_activity": time.time()
        }
        self.mark_dirty()

    def update_round_activity(self, cid):
        if str(cid) in self.data["rounds"]:
            self.data["rounds"][str(cid)]["last_activity"] = time.time()
            self.mark_dirty()

    def get_round(self, cid):
        return self.data["rounds"].get(str(cid))

    def end_round(self, cid):
        self.data["rounds"].pop(str(cid), None)
        self.mark_dirty()

    def add_win(self, cid, uid):
        if str(cid) not in self.data["rounds"]:
            return False

        if str(uid) not in self.data["rounds"][str(cid)]["wins"]:
            self.data["rounds"][str(cid)]["wins"][str(uid)] = 0

        self.data["rounds"][str(cid)]["wins"][str(uid)] += 1
        self.mark_dirty()
        return self.data["rounds"][str(cid)]["wins"][str(uid)]

    def set_round_mode(self, cid, status):
        self.data["round_mode"][str(cid)] = status
        self.mark_dirty()

    def get_round_mode(self, cid):
        return self.data["round_mode"].get(str(cid), False)

    def cleanup(self):
        now = time.time()
        to_del = []
        for k, v in self.data["sessions"].items():
            if now - v["time"] > 3600:
                to_del.append(k)

        for k in to_del:
            del self.data["sessions"][k]

        month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        for dt in list(self.data["stats"].keys()):
            if dt < month_ago:
                del self.data["stats"][dt]

        if to_del:
            self.mark_dirty()

    def cleanup_inactive_rounds(self):
        now = time.time()
        rounds_to_remove = []

        for cid, round_data in list(self.data["rounds"].items()):
            last_activity = round_data.get("last_activity", 0)
            if now - last_activity > 60:
                rounds_to_remove.append(cid)

        for cid in rounds_to_remove:
            self.data["rounds"].pop(cid, None)

        if rounds_to_remove:
            self.mark_dirty()

        return rounds_to_remove

storage = Storage()

class RemoteManager:
    def __init__(self, url, min_words=5, max_words=25, disasm=False):
        self.url = url
        self.min_words = min_words
        self.max_words = max_words
        self.disasm = disasm
        self.sentences = []
        self.last_update = 0

    def load(self):
        try:
            r = requests.get(self.url, timeout=10)
            if r.status_code == 200:
                if self.url.endswith('.json'):
                    data = r.json()
                    self.sentences = [
                        clean(s) for s in data
                        if s.strip() and self.min_words <= len(clean(s).split()) <= self.max_words
                    ]
                else:
                    self.sentences = [
                        clean(s) for s in r.text.split('\n')
                        if s.strip() and self.min_words <= len(clean(s).split()) <= self.max_words
                    ]
                self.last_update = time.time()
        except Exception as e:
            print(f"Error loading from {self.url}: {e}")

    def get(self):
        if not self.sentences or time.time() - self.last_update > 3600:
            self.load()
        return random.choice(self.sentences) if self.sentences else "لا توجد جمل حالياً"

class CSVQuotesManager:
    def __init__(self, url, min_words=3, max_words=30):
        self.url = url
        self.min_words = min_words
        self.max_words = max_words
        self.quotes = []
        self.last_update = 0

    def load(self):
        try:
            r = requests.get(self.url, timeout=10)
            if r.status_code == 200:
                lines = r.text.strip().split('\n')[1:]
                self.quotes = []
                for line in lines:
                    if '","' in line or ',' in line:
                        parts = line.split('","')
                        if len(parts) >= 1:
                            quote = parts[0].strip('"').strip()
                            quote = clean(quote)
                            if quote and self.min_words <= len(quote.split()) <= self.max_words:
                                self.quotes.append(quote)
                self.last_update = time.time()
        except Exception as e:
            print(f"Error loading quotes: {e}")

    def get(self):
        if not self.quotes or time.time() - self.last_update > 3600:
            self.load()
        return random.choice(self.quotes) if self.quotes else "لا توجد اقتباسات حالياً"

def clean(txt):
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt.replace(' ≈ ', ' ').replace('≈', ' '))
    txt = re.sub(r'\([^)]*[a-zA-Z]+[^)]*\)', '', txt)
    txt = re.sub(r'\[[^\]]*\]', '', txt)
    txt = re.sub(r'\([^)]*\)', '', txt)
    txt = ' '.join([w for w in txt.split() if not re.search(r'[a-zA-Z]', w)])

    def rep_num(m):
        n = m.group()
        return NUM_WORDS.get(n, ' '.join(NUM_WORDS.get(d, d) for d in n) if len(n) > 1 else n)

    txt = re.sub(r'\d+', rep_num, txt)
    txt = re.sub(r'[،,:;؛\-–—\.\!؟\?\(\)\[\]\{\}""''«»…]', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()

def normalize(txt):
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt)
    return re.sub(r'\s+', ' ', ''.join(CHAR_MAP.get(c, c) for c in txt)).strip()

def format_display(s):
    return ' ، '.join(s.split())

def count_words_for_wpm(text):
    cleaned_text = re.sub(r'[≈=\-~_|/\\،,:;؛\.\!؟\?\(\)\[\]\{\}""''«»…]+', ' ', text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    return len(cleaned_text.split())

def match_text(orig, usr):
    orig_normalized = normalize(orig)
    usr_normalized = normalize(usr)

    if orig_normalized == usr_normalized:
        return True

    usr_with_spaces = re.sub(r'[≈=\-~_|/\\]+', ' ', usr)
    usr_with_spaces_normalized = normalize(usr_with_spaces)

    if orig_normalized == usr_with_spaces_normalized:
        return True

    words = usr_with_spaces_normalized.split()
    if len(words) >= 2:
        reversed_text = ' '.join(reversed(words))
        if orig_normalized == reversed_text:
            return True

    return False

def norm_spaces(txt):
    return re.sub(r'\s+', ' ', txt).strip()

def disassemble_word(word):
    return ' '.join(list(word))

def assemble_word(disassembled_word):
    return disassembled_word.replace(' ', '')

def disassemble_sentence(sentence):
    words = sentence.split()
    return ' '.join([disassemble_word(word) for word in words])

def assemble_sentence(disassembled_sentence):
    words = disassembled_sentence.split()
    assembled_words = []
    current_word = []

    for char in words:
        if char.strip():
            current_word.append(char)
        if len(current_word) > 0 and (not char.strip() or char == words[-1]):
            assembled_words.append(assemble_word(' '.join(current_word)))
            current_word = []

    return ' '.join(assembled_words)

def is_correct_disassembly(original, user_disassembly):
    expected = disassemble_sentence(original)
    return normalize(user_disassembly) == normalize(expected)

def is_correct_assembly(original_disassembled, user_assembly):
    expected = assemble_sentence(original_disassembled)
    return normalize(user_assembly) == normalize(expected)

def apply_condition(cond, sent):
    words = sent.split()
    if not words:
        return sent

    if cond == "كرر أول كلمة":
        return f"{words[0]} {sent}"

    elif cond == "كرر ثاني كلمة" and len(words) >= 2:
        return f"{words[1]} {sent}"

    elif cond == "كرر آخر كلمة":
        return f"{sent} {words[-1]}"

    elif cond == "كرر أول كلمة وآخر كلمة":
        return f"{words[0]} {sent} {words[-1]}"

    elif cond == "فكك أول كلمة":
        return f"{' '.join(words[0])} {' '.join(words[1:])}" if len(words) > 1 else ' '.join(words[0])

    elif cond == "فكك آخر كلمة":
        return f"{' '.join(words[:-1])} {' '.join(words[-1])}" if len(words) > 1 else ' '.join(words[-1])

    elif cond == "بدل أول كلمتين" and len(words) >= 2:
        words[0], words[1] = words[1], words[0]
        return ' '.join(words)

    elif cond == "بدل آخر كلمتين" and len(words) >= 2:
        words[-1], words[-2] = words[-2], words[-1]
        return ' '.join(words)

    elif cond == "بدل ثاني كلمة والكلمة الأخيرة" and len(words) >= 3:
        words[1], words[-1] = words[-1], words[1]
        return ' '.join(words)

    return sent

def validate_condition(cond, orig, usr):
    expected = apply_condition(cond, orig)
    return normalize(usr) == normalize(expected), expected

def validate_repeat(exp, usr):
    matches = re.findall(r'(\S+)\((\d+)\)', exp)
    user_words = usr.split()
    total = sum(int(c) for _, c in matches)

    if len(user_words) != total:
        return False, f"عدد الكلمات غير صحيح. المفترض: {total}"

    idx = 0
    for word, count in matches:
        for j in range(idx, idx + int(count)):
            if normalize(user_words[j]) != normalize(word):
                return False, f"الكلمة '{user_words[j]}' يجب أن تكون '{word}'"
        idx += int(count)

    return True, ""

def gen_pattern(uid):
    for _ in range(100):
        words = random.sample(REPEAT_WORDS, 4)
        pattern = []
        key_parts = []

        for w in words:
            c = random.randint(2, 4)
            pattern.append(f"{w}({c})")
            key_parts.append(f"{w}_{c}")

        key = '_'.join(key_parts)
        if not storage.is_pattern_used(uid, key):
            storage.add_pattern(uid, key)
            return ' '.join(pattern)

    storage.clear_patterns(uid)
    return gen_pattern(uid)

def arabic_to_num(txt):
    txt = txt.strip()
    nums = {
        'صفر': 0, 'واحد': 1, 'اثنان': 2, 'اثنين': 2, 'ثلاثة': 3, 'ثلاث': 3, 'أربعة': 4, 'أربع': 4,
        'خمسة': 5, 'خمس': 5, 'ستة': 6, 'ست': 6, 'سبعة': 7, 'سبع': 7, 'ثمانية': 8, 'ثماني': 8, 'ثمان': 8,
        'تسعة': 9, 'تسع': 9, 'عشرة': 10, 'عشر': 10,
        'احدى عشر': 11, 'احد عشر': 11, 'اثنا عشر': 12, 'اثني عشر': 12,
        'ثلاثة عشر': 13, 'ثلاث عشر': 13, 'أربعة عشر': 14, 'أربع عشر': 14,
        'خمسة عشر': 15, 'خمس عشر': 15, 'ستة عشر': 16, 'ست عشر': 16,
        'سبعة عشر': 17, 'سبع عشر': 17, 'ثمانية عشر': 18, 'ثماني عشر': 18, 'ثمان عشر': 18,
        'تسعة عشر': 19, 'تسع عشر': 19, 'عشرون': 20, 'عشرين': 20,
        'ثلاثون': 30, 'ثلاثين': 30, 'أربعون': 40, 'أربعين': 40,
        'خمسون': 50, 'خمسين': 50, 'ستون': 60, 'ستين': 60,
        'سبعون': 70, 'سبعين': 70, 'ثمانون': 80, 'ثمانين': 80,
        'تسعون': 90, 'تسعين': 90, 'مئة': 100, 'مائة': 100, 'مية': 100
    }

    if txt in nums:
        return nums[txt]

    try:
        return int(txt)
    except ValueError:
        return None

def is_admin(uid):
    return uid in ADMIN_IDS

managers = {
    "جمم": RemoteManager(URLS["جمم"]),
    "ويكي": RemoteManager(URLS["ويكي"]),
    "شرط": RemoteManager(URLS["شرط"]),
    "فكك": RemoteManager(URLS["فكك"], disasm=True),
    "صج": RemoteManager(URLS["صج"]),
    "شك": RemoteManager(URLS["شك"]),
    "جش": CSVQuotesManager(URLS["جش"])
}

async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    msg = (
        " ارررحب في بوت иĸℓ للطباعة \n\n"
        "الأقسام المتاحة:\n"
        "- (جمم) - جمل عربية\n"
        "- (ويكي) - جمل ويكيبيديا\n"
        "- (صج) - مولد الكلمات العربية\n"
        "- (شك) - جمل عامية\n"
        "- (جش) - اقتباسات\n"
        "- (كرر) - تكرار الكلمات\n"
        "- (شرط) - جمل بالشروط\n"
        "- (فكك) - فك وتركيب\n\n"
        " أوامر البوت : \n\n"
        "-(الصدارة) - المتصدرين \n" 
        "- (جوائزي) - عرض جوائزك\n"
        "- (فتح جولة) - حسبة نقاط\n\n"
        "دزيت في البوت دزة أسطورية حياك هنا\n\n"
        "https://t.me/nklnklnklnklnkl\n"
    )
    await u.message.reply_text(msg)

async def cmd_leaderboard(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    types = ['جمم', 'ويكي', 'صج', 'شك', 'جش', 'كرر', 'شرط', 'فكك']
    msg = "لوحة الصدارة:\n\n"

    for typ in types:
        leaders = storage.get_leaderboard(typ)
        if leaders:
            msg += f"━━ {typ} ━━\n"
            for i, (uid, username, first_name, wpm) in enumerate(leaders, 1):
                mention = f"@{username}" if username else first_name
                msg += f"{i}. {mention}: {wpm:.2f} WPM\n"
            msg += "\n"

    await u.message.reply_text(msg)

async def cmd_awards(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    awards = storage.get_awards(uid)

    if not awards:
        await u.message.reply_text("ليس لديك جوائز بعد!")
        return

    msg = "جوائزك:\n\n"
    for award in awards:
        msg += f"- {award['type']} - {award['name']}\n"
        msg += f"   السرعة: {award['wpm']:.2f} WPM\n"
        msg += f"   التاريخ: {award['date'][:10]}\n\n"

    await u.message.reply_text(msg)

async def cmd_stats(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if not is_admin(uid):
        await u.message.reply_text("هذا الأمر للمشرفين فقط!")
        return

    stats = storage.data["stats"]
    total_users = len(storage.data["users"])
    total_chats = len(storage.data["chats"])
    total_banned = len(storage.data["banned"])

    msg = f"📊 إحصائيات البوت:\n\n"
    msg += f"👥 عدد المستخدمين: {total_users}\n"
    msg += f"💬 عدد المجموعات: {total_chats}\n"
    msg += f"🚫 عدد المحظورين: {total_banned}\n\n"
    msg += "📈 الأوامر المستخدمة (آخر 7 أيام):\n"

    last_week = []
    for i in range(7):
        dt = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        if dt in stats:
            for cmd, count in stats[dt].items():
                last_week.append((cmd, count))

    if last_week:
        cmd_totals = defaultdict(int)
        for cmd, count in last_week:
            cmd_totals[cmd] += count

        sorted_cmds = sorted(cmd_totals.items(), key=lambda x: x[1], reverse=True)
        for cmd, count in sorted_cmds[:10]:
            msg += f"  • {cmd}: {count}\n"
    else:
        msg += "  لا توجد بيانات متاحة\n"

    await u.message.reply_text(msg)

async def broadcast_message(c: ContextTypes.DEFAULT_TYPE, text):
    success = 0
    failed = 0

    for uid in storage.data["users"].keys():
        try:
            await c.bot.send_message(chat_id=int(uid), text=text)
            success += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1

    for cid in storage.data["chats"].keys():
        try:
            await c.bot.send_message(chat_id=int(cid), text=text)
            success += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1

    return success, failed

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message or not u.message.text:
        return

    text = u.message.text.strip()
    uid = u.effective_user.id
    usr = u.effective_user.username
    name = u.effective_user.first_name or "مستخدم"
    cid = u.effective_chat.id
    is_private = u.effective_chat.type == "private"

    print(f"[MSG] User {uid} ({name}) in chat {cid}: '{text}'")

    if is_admin(uid) and u.message.reply_to_message:
        replied_user_id = u.message.reply_to_message.from_user.id

        if text in ['حظر']:
            if replied_user_id in ADMIN_IDS:
                await u.message.reply_text("❌ لا يمكن حظر المشرفين!")
                return

            storage.ban_user(replied_user_id)
            replied_user_name = u.message.reply_to_message.from_user.first_name or "المستخدم"
            await u.message.reply_text(f"✅ تم حظر {replied_user_name} بنجاح")
            return

        elif text in ['فك حظر', 'فك الحظر', 'الغاء حظر', 'الغاء الحظر']:
            if storage.unban_user(replied_user_id):
                replied_user_name = u.message.reply_to_message.from_user.first_name or "المستخدم"
                await u.message.reply_text(f"✅ تم فك الحظر عن {replied_user_name} بنجاح")
            else:
                await u.message.reply_text("⚠️ هذا المستخدم غير محظور")
            return

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    storage.add_user(uid, usr, name)

    if not is_private:
        storage.add_chat(cid, u.effective_chat.title or "مجموعة")

    if storage.get_broadcast_mode(uid):
        if text in ["إلغاء", "الغاء"]:
            storage.set_broadcast_mode(uid, False)
            await u.message.reply_text("❌ تم إلغاء وضع الإذاعة")
            return

        success, failed = await broadcast_message(c, text)
        storage.set_broadcast_mode(uid, False)
        await u.message.reply_text(
            f"✅ تم إرسال الإذاعة بنجاح\n"
            f"📤 نجح: {success} مستخدم/مجموعة\n"
            f"📛 فشل: {failed} مستخدم/مجموعة"
        )
        return

    if storage.get_round_mode(cid):
        target_num = arabic_to_num(text)
        if target_num and target_num > 0 and target_num <= 100:
            storage.start_round(cid, target_num)
            storage.set_round_mode(cid, False)

            keyboard = [[InlineKeyboardButton("إنهاء الجولة", callback_data=f"end_round_{cid}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await u.message.reply_text(
                f"تم فتح جولة جديدة\nالهدف: {target_num} فوز\nابدأوا اللعب الآن!",
                reply_markup=reply_markup
            )
            return
        else:
            await u.message.reply_text("الرجاء إدخال رقم صحيح من 1 إلى 100")
            return

    if text in ['فتح جولة', 'فتح جوله']:
        storage.set_round_mode(cid, True)
        keyboard = [[InlineKeyboardButton("إنهاء الجولة", callback_data=f"end_round_{cid}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await u.message.reply_text(
            "كم عدد الانتصارات المطلوبة للفوز في الجولة؟\nأدخل الرقم من 1 إلى 100 (مثال: 5 أو خمسة)",
            reply_markup=reply_markup
        )
        return

    if text in ['إنهاء الجولة', 'انهاء الجولة', 'إنهاء جولة', 'انهاء جوله', 'إنهاء الجوله', 'انهاء الجوله']:
        round_data = storage.get_round(cid)
        if round_data:
            wins_list = round_data.get('wins', {})
            if wins_list:
                msg = "نتائج الجولة:\n\n"
                sorted_wins = sorted(wins_list.items(), key=lambda x: x[1], reverse=True)
                for i, (user_id, wins) in enumerate(sorted_wins, 1):
                    user_data = storage.data["users"].get(str(user_id), {})
                    user_name = user_data.get("first_name", "مستخدم")
                    user_username = user_data.get("username")
                    mention = f"@{user_username}" if user_username else user_name
                    msg += f"{i}. {mention}: {wins} فوز\n"
                await u.message.reply_text(msg)

            storage.end_round(cid)
            await u.message.reply_text("تم إنهاء الجولة")
        else:
            await u.message.reply_text("لا توجد جولة نشطة حالياً")
        return

    if text in ['جولة', 'الجولة', 'جوله']:
        round_data = storage.get_round(cid)
        if round_data:
            target = round_data['target']
            wins_list = round_data.get('wins', {})
            msg = f"الجولة الحالية - الهدف: {target} فوز\n\n"
            if wins_list:
                sorted_wins = sorted(wins_list.items(), key=lambda x: x[1], reverse=True)
                for i, (user_id, wins) in enumerate(sorted_wins, 1):
                    user_data = storage.data["users"].get(str(user_id), {})
                    user_name = user_data.get("first_name", "مستخدم")
                    user_username = user_data.get("username")
                    mention = f"@{user_username}" if user_username else user_name
                    msg += f"{i}. {mention}: {wins}/{target}\n"
            else:
                msg += "لا توجد انتصارات بعد"
            await u.message.reply_text(msg)
        else:
            await u.message.reply_text("لا توجد جولة نشطة حالياً\nاكتب 'فتح جولة' لبدء جولة جديدة")
        return

    if text in ['جمم', 'ويكي', 'صج', 'شك', 'جش', 'كرر', 'شرط', 'فكك', 'الصدارة', 'جوائزي', 'عرض', 'مقالات', 'احصاء', 'اذاعة', 'إذاعة']:
        storage.log_cmd(text)

    if is_admin(uid) and text in ['اذاعة', 'إذاعة', 'اذاعه', 'إذاعه']:
        storage.set_broadcast_mode(uid, True)
        await u.message.reply_text(
            "📢 وضع الإذاعة مفعل\n\n"
            "أرسل الرسالة التي تريد إذاعتها إلى جميع المستخدمين والمجموعات\n"
            "أو اكتب 'إلغاء' للإلغاء"
        )
        return

    async with chat_locks[cid]:
        if text == 'جمم':
            existing_session = storage.get_session(cid, 'جمم')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'جمم')
            t = managers["جمم"].get()
            storage.save_session(uid, cid, 'جمم', t, time.time(), sent=False)
            await u.message.reply_text(format_display(t))
            storage.mark_session_sent(cid, 'جمم')
        elif text == 'ويكي':
            existing_session = storage.get_session(cid, 'ويكي')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'ويكي')
            t = managers["ويكي"].get()
            storage.save_session(uid, cid, 'ويكي', t, time.time(), sent=False)
            await u.message.reply_text(format_display(t))
            storage.mark_session_sent(cid, 'ويكي')
        elif text == 'صج':
            existing_session = storage.get_session(cid, 'صج')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'صج')
            t = managers["صج"].get()
            storage.save_session(uid, cid, 'صج', t, time.time(), sent=False)
            await u.message.reply_text(format_display(t))
            storage.mark_session_sent(cid, 'صج')
        elif text == 'شك':
            existing_session = storage.get_session(cid, 'شك')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'شك')
            t = managers["شك"].get()
            storage.save_session(uid, cid, 'شك', t, time.time(), sent=False)
            await u.message.reply_text(format_display(t))
            storage.mark_session_sent(cid, 'شك')
        elif text == 'جش':
            existing_session = storage.get_session(cid, 'جش')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'جش')
            t = managers["جش"].get()
            storage.save_session(uid, cid, 'جش', t, time.time(), sent=False)
            await u.message.reply_text(format_display(t))
            storage.mark_session_sent(cid, 'جش')
        elif text == 'كرر':
            existing_session = storage.get_session(cid, 'كرر')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'كرر')
            p = gen_pattern(uid)
            storage.save_session(uid, cid, 'كرر', p, time.time(), sent=False)
            await u.message.reply_text(p)
            storage.mark_session_sent(cid, 'كرر')
        elif text == 'شرط':
            existing_session = storage.get_session(cid, 'شرط')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'شرط')
            s = managers["شرط"].get()
            cond = random.choice(CONDITIONS)
            storage.save_session(uid, cid, 'شرط', f"{s}||{cond}", time.time(), sent=False)

            await u.message.reply_text(cond)
            await asyncio.sleep(2)
            await u.message.reply_text(format_display(s))
            storage.mark_session_sent(cid, 'شرط')
        elif text == 'فكك':
            existing_session = storage.get_session(cid, 'فكك_تفكيك')
            if existing_session and not existing_session.get('sent', False):
                return
            storage.cancel_user_session_in_type(uid, cid, 'فكك_تفكيك')
            s = managers["فكك"].get()
            storage.save_session(uid, cid, 'فكك_تفكيك', s, time.time(), sent=False)
            msg = f"فكك الجملة التالية (افصل بين حروف كل كلمة):\n\n{format_display(s)}"
            await u.message.reply_text(msg)
            storage.mark_session_sent(cid, 'فكك_تفكيك')
        elif text in ['الصدارة']:
            await cmd_leaderboard(u, c)
        elif text in ['جوائزي']:
            await cmd_awards(u, c)
        elif text in ['احصاء']:
            await cmd_stats(u, c)
        elif text in ['عرض', 'مقالات']:
            await cmd_start(u, c)
        else:
            sessions = storage.get_all_active_sessions(cid)
            if not sessions:
                return

            for session in sessions:
                typ = session["type"]
                orig = session["text"]
                tm = session["time"]
                elapsed = time.time() - tm
                matched = False

                if typ in ['جمم', 'ويكي', 'صج', 'شك', 'جش'] and match_text(orig, text):
                    matched = True
                elif typ == 'كرر':
                    valid, err = validate_repeat(orig, text)
                    if valid:
                        matched = True
                elif typ == 'شرط':
                    orig_s, cond = orig.split('||')
                    valid, exp = validate_condition(cond, orig_s, text)
                    if valid:
                        matched = True
                elif typ == 'فكك_تفكيك':
                    if is_correct_disassembly(orig, text):
                        matched = True

                if matched:
                    word_count = count_words_for_wpm(text)
                    wpm = (word_count / elapsed) * 60
                    score_typ = 'فكك' if typ == 'فكك_تفكيك' else typ
                    storage.update_score(uid, score_typ, wpm)

                    round_data = storage.get_round(cid)
                    if round_data:
                        storage.update_round_activity(cid)
                        wins = storage.add_win(cid, uid)
                        target = round_data['target']
                        wins_list = round_data.get('wins', {})
                        mention = f"@{usr}" if usr else name

                        round_stats = "\n\nإحصائيات الجولة:\n"
                        sorted_wins = sorted(wins_list.items(), key=lambda x: x[1], reverse=True)
                        for i, (user_id, user_wins) in enumerate(sorted_wins, 1):
                            if i <= 3:
                                user_data = storage.data["users"].get(str(user_id), {})
                                user_name = user_data.get("first_name", "مستخدم")
                                user_username = user_data.get("username")
                                user_mention = f"@{user_username}" if user_username else user_name
                                round_stats += f"{i}. {user_mention}: {user_wins}/{target}\n"

                        if wins >= target:
                            await u.message.reply_text(
                                f"مبروك {mention}!\n"
                                f"أنت الفائز في الجولة!\n"
                                f"السرعة: {wpm:.2f} WPM\n"
                                f"الفوز رقم: {wins}/{target}"
                                f"{round_stats}"
                            )
                            storage.end_round(cid)
                        else:
                            await u.message.reply_text(
                                f"صحيح {mention}!\n"
                                f"السرعة: {wpm:.2f} WPM\n"
                                f"التقدم: {wins}/{target}"
                                f"{round_stats}"
                            )
                    else:
                        await u.message.reply_text(f"ممتاز سرعتك! {wpm:.2f} WPM")

                    storage.del_session(cid, typ)
                    break

    storage.save()

async def periodic_save():
    while True:
        await asyncio.sleep(save_interval)
        storage.save()

async def periodic_cleanup():
    while True:
        await asyncio.sleep(3600)
        storage.cleanup()
        storage.save(force=True)

async def periodic_round_cleanup():
    while True:
        await asyncio.sleep(30)
        removed = storage.cleanup_inactive_rounds()
        if removed:
            print(f"Removed {len(removed)} inactive rounds")

async def handle_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query = u.callback_query
    await query.answer()

    if query.data.startswith("end_round_"):
        cid = int(query.data.split("_")[2])

        round_data = storage.get_round(cid)
        if round_data:
            wins_list = round_data.get('wins', {})
            if wins_list:
                msg = "نتائج الجولة:\n\n"
                sorted_wins = sorted(wins_list.items(), key=lambda x: x[1], reverse=True)
                for i, (user_id, wins) in enumerate(sorted_wins, 1):
                    user_data = storage.data["users"].get(str(user_id), {})
                    user_name = user_data.get("first_name", "مستخدم")
                    user_username = user_data.get("username")
                    mention = f"@{user_username}" if user_username else user_name
                    msg += f"{i}. {mention}: {wins} فوز\n"
                await query.edit_message_text(msg)

            storage.end_round(cid)
            await query.message.reply_text("تم إنهاء الجولة")
        else:
            await query.edit_message_text("لا توجد جولة نشطة حالياً")

def main():
    if not BOT_TOKEN:
        print("Error: BOT_TOKEN environment variable not set!")
        return

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

    loop = asyncio.get_event_loop()
    loop.create_task(periodic_save())
    loop.create_task(periodic_cleanup())
    loop.create_task(periodic_round_cleanup())

    print("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
