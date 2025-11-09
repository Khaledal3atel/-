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
from telegram.error import BadRequest
from collections import defaultdict

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = 5562144078

message_cache = {}
last_save_time = time.time()
save_interval = 2.0

chat_locks = defaultdict(asyncio.Lock)
last_command_time = defaultdict(lambda: defaultdict(float))
sent_message_tracker = defaultdict(lambda: defaultdict(float))

URLS = {
    "جمم": "https://raw.githubusercontent.com/AL3ATEL/TXT-bot-telegram-/refs/heads/main/sentences.txt",
    "شرط": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-2/refs/heads/main/conditions.txt",
    "فكك": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/3kswdblwtrdl.txt",
    "صج": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-4/refs/heads/main/arabic_sentences.json",
    "جش": "https://raw.githubusercontent.com/BoulahiaAhmed/Arabic-Quotes-Dataset/main/Arabic_Quotes.csv",
    "شك": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-5/refs/heads/main/3amh.txt",
    "ويكي": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/Wekebedea.txt",
    "دبل": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/3kswdblwtrdl.txt",
    "تر": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/3kswdblwtrdl.txt",
    "عكس": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/3kswdblwtrdl.txt",
    "فر": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/Farese.txt",
    "E": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/English.txt"
}

REPEAT_WORDS = ["صمت", "صوف", "سين", "عين", "جيم", "كتب", "خبر", "حلم", "جمل", "تعب", "عرب", "نار", "برد", "نسر", "طرب", "شك", "ثور", "خسر", "علم", "صوت", "سون", "كل", "ثوب", "هات", "همس"]

NUMBER_WORDS = ["واحد", "اثنين", "ثلاثه", "اربعه", "خمسه", "سته", "سبعه", "ثمانيه", "تسعه", "عشرة", "عشرين", "خمسين", "ميه", "الف", "مليون", "ون", "تو", "ثري", "فور", "فايف", "سكس", "سفن", "ايت", "ناين", "تن", "تولف", "ايلفن", "تونتي"]

LETTER_WORDS = ["الف", "باء", "جيم", "دال", "شين", "ضاد", "قاف", "كاف", "لام", "ميم", "نون", "واو", "اكس", "واي", "ام", "جي", "كيو", "باي", "رو", "بيتا", "فاي"]

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

def clean_text_for_word_count(text):
    text = re.sub(r'[،,:;؛\.\!؟\?]+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_number_from_text(text):
    match = re.search(r'\s+(\d+)$', text)
    if match:
        number = int(match.group(1))
        command = text[:match.start()].strip()
        return command, number
    return text, None

def get_text_with_word_count(manager, target_word_count):
    if target_word_count < 1 or target_word_count > 60:
        return None

    combined_words = []
    attempt = 0
    max_attempts = 10

    while len(combined_words) < target_word_count and attempt < max_attempts:
        if hasattr(manager, 'get_multiple'):
            new_sentences = manager.get_multiple(3)
        else:
            new_sentences = [manager.get() for _ in range(3)]

        for sentence in new_sentences:
            cleaned_sentence = clean_text_for_word_count(sentence)
            combined_words.extend(cleaned_sentence.split())
            if len(combined_words) >= target_word_count:
                break

        attempt += 1

    if len(combined_words) >= target_word_count:
        return ' '.join(combined_words[:target_word_count])
    else:
        return ' '.join(combined_words)

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
                "round_mode": {},
                "pending_round_setup": {},
                "admins": [],
                "owners": [],
                "preferences": {},
                "auto_mode": {}
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

    def save_preference(self, uid, section, word_count):
        uid_str = str(uid)
        if "preferences" not in self.data:
            self.data["preferences"] = {}
        if uid_str not in self.data["preferences"]:
            self.data["preferences"][uid_str] = {}
        self.data["preferences"][uid_str][section] = word_count
        self.mark_dirty()
        self.save(force=True)

    def get_preference(self, uid, section):
        uid_str = str(uid)
        if "preferences" not in self.data:
            return None
        if uid_str not in self.data["preferences"]:
            return None
        return self.data["preferences"][uid_str].get(section)

    def clear_preference(self, uid, section):
        uid_str = str(uid)
        if "preferences" not in self.data:
            return False
        if uid_str not in self.data["preferences"]:
            return False
        if section in self.data["preferences"][uid_str]:
            del self.data["preferences"][uid_str][section]
            self.mark_dirty()
            self.save(force=True)
            return True
        return False

    def is_banned(self, uid):
        uid_str = str(uid)
        is_in_list = uid_str in self.data["banned"]
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

    def is_admin(self, uid):
        return str(uid) in self.data["admins"]

    def is_owner(self, uid):
        return str(uid) in self.data["owners"]

    def is_main_owner(self, uid):
        return uid == OWNER_ID

    def add_admin(self, uid):
        uid_str = str(uid)
        if uid_str not in self.data["admins"]:
            self.data["admins"].append(uid_str)
            self.mark_dirty()
            self.save(force=True)

    def add_owner(self, uid):
        uid_str = str(uid)
        if uid_str not in self.data["owners"]:
            self.data["owners"].append(uid_str)
            self.mark_dirty()
            self.save(force=True)

    def remove_admin(self, uid):
        uid_str = str(uid)
        if uid_str in self.data["admins"]:
            self.data["admins"].remove(uid_str)
            self.mark_dirty()
            self.save(force=True)

    def remove_owner(self, uid):
        uid_str = str(uid)
        if uid_str in self.data["owners"]:
            self.data["owners"].remove(uid_str)
            self.mark_dirty()
            self.save(force=True)

    def get_all_admins(self):
        return self.data["admins"]

    def get_all_owners(self):
        return self.data["owners"]

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

    def start_round(self, cid, target, starter_uid=None):
        self.data["rounds"][str(cid)] = {
            "target": target,
            "wins": {},
            "started_at": datetime.now().isoformat(),
            "last_activity": time.time(),
            "starter_uid": starter_uid
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
            if now - last_activity > 120:
                rounds_to_remove.append(cid)

        for cid in rounds_to_remove:
            self.data["rounds"].pop(cid, None)

        if rounds_to_remove:
            self.mark_dirty()

        return rounds_to_remove

    def set_pending_round_setup(self, cid, uid, status):
        key = str(cid)
        if status:
            self.data["pending_round_setup"][key] = uid
        else:
            self.data["pending_round_setup"].pop(key, None)
        self.mark_dirty()

    def get_pending_round_setup(self, cid):
        return self.data["pending_round_setup"].get(str(cid))

    def get_round_stats(self, cid):
        round_data = self.get_round(cid)
        if not round_data:
            return None

        target = round_data.get("target", 0)
        wins = round_data.get("wins", {})
        started_at = round_data.get("started_at", "")

        if not wins:
            return f"إحصائيات الجولة الحالية:\n\nالهدف: {target} انتصار\nالمشاركون: لا يوجد بعد\nبدأت: {started_at[:16]}"

        sorted_wins = sorted(wins.items(), key=lambda x: x[1], reverse=True)

        stats_msg = f"إحصائيات الجولة الحالية:\n\nالهدف: {target} انتصار\n\nالمشاركون:\n"

        for idx, (uid, count) in enumerate(sorted_wins, 1):
            user_info = self.data["users"].get(str(uid), {})
            name = user_info.get("first_name", "مستخدم")
            username = user_info.get("username", "")
            display_name = f"@{username}" if username else name
            stats_msg += f"{idx}. {display_name}: {count} انتصار\n"

        stats_msg += f"\nبدأت: {started_at[:16]}"

        return stats_msg

    def start_auto_mode(self, cid, uid, message_thread_id=None):
        if "auto_mode" not in self.data:
            self.data["auto_mode"] = {}
        
        key = str(cid)
        self.data["auto_mode"][key] = {
            "uid": uid,
            "sections": [],
            "collecting": True,
            "active": False,
            "last_used_section": None,
            "last_activity": time.time(),
            "message_thread_id": message_thread_id
        }
        self.mark_dirty()
        self.save(force=True)

    def add_auto_section(self, cid, section):
        if "auto_mode" not in self.data:
            self.data["auto_mode"] = {}
        
        key = str(cid)
        if key in self.data["auto_mode"]:
            if section not in self.data["auto_mode"][key]["sections"]:
                self.data["auto_mode"][key]["sections"].append(section)
                self.mark_dirty()
                self.save(force=True)
                return True
        return False

    def finish_auto_collection(self, cid):
        key = str(cid)
        if key in self.data.get("auto_mode", {}):
            self.data["auto_mode"][key]["collecting"] = False
            self.data["auto_mode"][key]["active"] = True
            self.mark_dirty()
            self.save(force=True)
            return True
        return False

    def get_auto_mode(self, cid):
        return self.data.get("auto_mode", {}).get(str(cid))

    def update_auto_activity(self, cid):
        key = str(cid)
        if key in self.data.get("auto_mode", {}):
            self.data["auto_mode"][key]["last_activity"] = time.time()
            self.mark_dirty()

    def set_auto_last_section(self, cid, section):
        key = str(cid)
        if key in self.data.get("auto_mode", {}):
            self.data["auto_mode"][key]["last_used_section"] = section
            self.mark_dirty()

    def end_auto_mode(self, cid):
        key = str(cid)
        if "auto_mode" in self.data and key in self.data["auto_mode"]:
            self.data["auto_mode"].pop(key, None)
            self.mark_dirty()
            self.save(force=True)

    def cleanup_inactive_auto_modes(self):
        if "auto_mode" not in self.data:
            return []
        
        now = time.time()
        to_remove = []

        for cid, auto_data in list(self.data["auto_mode"].items()):
            last_activity = auto_data.get("last_activity", 0)
            if now - last_activity > 180:
                to_remove.append(cid)

        for cid in to_remove:
            self.data["auto_mode"].pop(cid, None)

        if to_remove:
            self.mark_dirty()
            self.save(force=True)

        return to_remove

storage = Storage()

class RemoteManager:
    def __init__(self, url, min_words=5, max_words=25, disasm=False, lang="arabic"):
        self.url = url
        self.min_words = min_words
        self.max_words = max_words
        self.disasm = disasm
        self.lang = lang
        self.sentences = []
        self.last_update = 0

        if lang == "english":
            self.clean_func = clean_english
        elif lang == "persian":
            self.clean_func = clean_persian
        else:
            self.clean_func = clean

    def load(self):
        try:
            r = requests.get(self.url, timeout=10)
            if r.status_code == 200:
                if self.url.endswith('.json'):
                    data = r.json()
                    self.sentences = [
                        self.clean_func(s) for s in data
                        if s.strip() and self.min_words <= len(clean_text_for_word_count(self.clean_func(s)).split()) <= self.max_words
                    ]
                else:
                    self.sentences = [
                        self.clean_func(s) for s in r.text.split('\n')
                        if s.strip() and self.min_words <= len(clean_text_for_word_count(self.clean_func(s)).split()) <= self.max_words
                    ]
                self.last_update = time.time()
        except Exception as e:
            print(f"Error loading from {self.url}: {e}")

    def get(self):
        if not self.sentences or time.time() - self.last_update > 3600:
            self.load()
        return random.choice(self.sentences) if self.sentences else "لا توجد جمل حالياً"

    def get_multiple(self, count=2):
        if not self.sentences or time.time() - self.last_update > 3600:
            self.load()
        if self.sentences:
            return random.sample(self.sentences, min(count, len(self.sentences)))
        return []

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
                            cleaned_quote = clean_text_for_word_count(quote)
                            if quote and self.min_words <= len(cleaned_quote.split()) <= self.max_words:
                                self.quotes.append(quote)
                self.last_update = time.time()
        except Exception as e:
            print(f"Error loading quotes: {e}")

    def get(self):
        if not self.quotes or time.time() - self.last_update > 3600:
            self.load()
        return random.choice(self.quotes) if self.quotes else "لا توجد اقتباسات حالياً"

    def get_multiple(self, count=2):
        if not self.quotes or time.time() - self.last_update > 3600:
            self.load()
        if self.quotes:
            return random.sample(self.quotes, min(count, len(self.quotes)))
        return []

def generate_random_sentence(uid, word_list, min_length=7, max_length=20, system_type="رق"):
    for attempt in range(100):
        sentence_length = random.randint(min_length, max_length)

        selected_words = []
        available_indices = list(range(len(word_list)))

        for _ in range(sentence_length):
            if not available_indices:
                available_indices = list(range(len(word_list)))

            if selected_words:
                last_index = word_list.index(selected_words[-1])
                valid_indices = [i for i in available_indices if abs(i - last_index) > 1]

                if not valid_indices:
                    valid_indices = available_indices
            else:
                valid_indices = available_indices

            chosen_index = random.choice(valid_indices)
            selected_words.append(word_list[chosen_index])

            if chosen_index in available_indices:
                available_indices.remove(chosen_index)

        sentence = ' '.join(selected_words)
        key = f"{system_type}_{sentence}"

        if not storage.is_pattern_used(uid, key):
            storage.add_pattern(uid, key)
            return sentence

    storage.clear_patterns(uid)
    return generate_random_sentence(uid, word_list, min_length, max_length, system_type)

def clean(txt):
    txt = txt.replace('\u0640', '')
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

def clean_persian(txt):
    txt = txt.replace('\u0640', '')
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt)
    txt = re.sub(r'\([^)]*\)', '', txt)
    txt = re.sub(r'\[[^\]]*\]', '', txt)
    txt = re.sub(r'[،,:;؛\-–—\.\!؟\?\(\)\[\]\{\}""''«»…]', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()

def clean_english(txt):
    txt = txt.strip()
    txt = txt.replace('\u0640', '')
    txt = re.sub(r'\([^)]*\)', '', txt)
    txt = re.sub(r'\[[^\]]*\]', '', txt)
    txt = re.sub(r'[,;:\-–—\.!\?\(\)\[\]\{\}""''«»…]+', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()

def normalize(txt):
    txt = txt.replace('\u0640', '')
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt)
    return re.sub(r'\s+', ' ', ''.join(CHAR_MAP.get(c, c) for c in txt)).strip()

def normalize_persian(txt):
    txt = txt.strip().lower()
    txt = txt.replace('\u0640', '')
    persian_map = {
        'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ى': 'ي', 'ة': 'ه', 'ئ': 'ي', 'ؤ': 'و', 'ٱ': 'ا', 'ٳ': 'ا',
        'گ': 'ك', 'پ': 'ب', 'ژ': 'ز', 'چ': 'ج'
    }
    txt = ''.join(persian_map.get(c, c) for c in txt)
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt)
    return re.sub(r'\s+', ' ', txt).strip()

def normalize_english(txt):
    txt = txt.strip().lower()
    txt = txt.replace('\u0640', '')
    txt = re.sub(r'[^\w\s]', '', txt)
    return re.sub(r'\s+', ' ', txt).strip()

def format_display(s):
    s = s.replace('\u0640', '')
    return ' ، '.join(s.split())

def count_words_for_wpm(text):
    cleaned_text = re.sub(r'[≈=\-~_|/\\،,:;؛\.\!؟\?\(\)\[\]\{\}""''«»…]+', ' ', text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    words = cleaned_text.split()
    return len(words)

def match_text(orig, usr, lang="arabic"):
    if lang == "persian":
        orig_normalized = normalize_persian(orig)
        usr_normalized = normalize_persian(usr)
    elif lang == "english":
        orig_normalized = normalize_english(orig)
        usr_normalized = normalize_english(usr)
    else:
        orig_normalized = normalize(orig)
        usr_normalized = normalize(usr)

    if orig_normalized == usr_normalized:
        return True

    usr_with_spaces = re.sub(r'[≈=\-~_|/\\]+', ' ', usr)
    if lang == "persian":
        usr_with_spaces_normalized = normalize_persian(usr_with_spaces)
    elif lang == "english":
        usr_with_spaces_normalized = normalize_english(usr_with_spaces)
    else:
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
    usr_cleaned = clean_text_for_word_count(usr)
    user_words = usr_cleaned.split()
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

def validate_double(original_sentence, user_text):
    original_words = original_sentence.split()
    user_text_cleaned = clean_text_for_word_count(user_text)
    user_words = user_text_cleaned.split()

    if len(user_words) != len(original_words) * 2:
        return False, f"عدد الكلمات غير صحيح. المفترض: {len(original_words) * 2}"

    idx = 0
    for word in original_words:
        if normalize(user_words[idx]) != normalize(word):
            return False, f"الكلمة '{user_words[idx]}' يجب أن تكون '{word}'"
        if normalize(user_words[idx + 1]) != normalize(word):
            return False, f"الكلمة '{user_words[idx + 1]}' يجب أن تكون '{word}'"
        idx += 2

    return True, ""

def validate_triple(original_sentence, user_text):
    original_words = original_sentence.split()
    user_text_cleaned = clean_text_for_word_count(user_text)
    user_words = user_text_cleaned.split()

    if len(user_words) != len(original_words) * 3:
        return False, f"عدد الكلمات غير صحيح. المفترض: {len(original_words) * 3}"

    idx = 0
    for word in original_words:
        for i in range(3):
            if normalize(user_words[idx + i]) != normalize(word):
                return False, f"الكلمة '{user_words[idx + i]}' يجب أن تكون '{word}'"
        idx += 3

    return True, ""

def validate_reverse(original_sentence, user_text):
    original_words = original_sentence.split()
    user_text_cleaned = clean_text_for_word_count(user_text)
    user_words = user_text_cleaned.split()

    if len(user_words) != len(original_words):
        return False, f"عدد الكلمات غير صحيح. المفترض: {len(original_words)}"

    reversed_original = list(reversed(original_words))
    for i, word in enumerate(reversed_original):
        if normalize(user_words[i]) != normalize(word):
            return False, f"الكلمة '{user_words[i]}' يجب أن تكون '{word}'"

    return True, ""

def gen_pattern(uid, count=1):
    patterns = []
    for _ in range(count):
        for attempt in range(100):
            words = random.sample(REPEAT_WORDS, 4)
            pattern = []
            key_parts = []

            for w in words:
                w_clean = w.replace('\u0640', '')
                c = random.randint(2, 4)
                pattern.append(f"{w_clean}({c})")
                key_parts.append(f"{w_clean}_{c}")

            key = '_'.join(key_parts)
            if not storage.is_pattern_used(uid, key):
                storage.add_pattern(uid, key)
                patterns.append(' '.join(pattern))
                break
        else:
            storage.clear_patterns(uid)
            return gen_pattern(uid, count)

    return patterns

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

def has_permission(uid, level):
    if storage.is_main_owner(uid):
        return True
    if level == "admin":
        return storage.is_admin(uid) or storage.is_owner(uid)
    if level == "owner":
        return storage.is_owner(uid)
    return False

managers = {
    "جمم": RemoteManager(URLS["جمم"]),
    "ويكي": RemoteManager(URLS["ويكي"]),
    "شرط": RemoteManager(URLS["شرط"]),
    "فكك": RemoteManager(URLS["فكك"], disasm=True),
    "صج": RemoteManager(URLS["صج"]),
    "شك": RemoteManager(URLS["شك"]),
    "جش": CSVQuotesManager(URLS["جش"]),
    "دبل": RemoteManager(URLS["دبل"]),
    "تر": RemoteManager(URLS["تر"]),
    "عكس": RemoteManager(URLS["عكس"]),
    "فر": RemoteManager(URLS["فر"], min_words=3, max_words=30, lang="persian"),
    "E": RemoteManager(URLS["E"], min_words=3, max_words=30, lang="english")
}

async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    msg = (
        " ارررحب في بوت иĸℓ للطباعة \n\n"
        "الأقسام المتاحة:\n"
        "- (جمم) - جمل عادية\n"
        "- (ويكي) - جمل ويكيبيديا\n"
        "- (صج) - كلمات عشوائية\n"
        "- (شك) - جمل عامية\n"
        "- (جش) - اقتباسات\n"
        "- (كرر) - تكرار الكلمات\n"
        "- (شرط) - جمل بالشروط\n"
        "- (فكك) - فك كلمات\n"
        "- (دبل) - تكرار كل كلمة مرتين\n"
        "- (تر) - تكرار كل كلمة ثلاث مرات\n"
        "- (عكس) - كتابة الجملة بالعكس\n"
        "- (فر) - جمل باللغة الفارسية\n"
        "- (E) - جمل باللغة الإنجليزية\n"
        "- (رق) - جمل أرقام\n"
        "- (حر) - جمل أحرف\n\n"
        " - - - - أوامر البوت - - - -\n\n"
        "-(الصدارة) - المتصدرين\n" 
        "- (جوائزي) - عرض جوائزك\n"
        "- (فتح جولة) - فتح جولة جديدة\n"
        "- (جولة) - عرض إحصائيات الجولة\n"
        "- (تلقائي) - وضع الجمل التلقائية المتتالية\n"
        "اكتب اسم قسم بعده رقم يحدد عدد الكلمات - (*اي قسم* 40)\n"
        "(*ريست *اي قسم) - البوت يرسل لك مقالات متفاوته الاحجام\n" 
    )
    await u.message.reply_text(msg)

async def show_bot_sections(u: Update, c: ContextTypes.DEFAULT_TYPE, is_callback=False):
    uid = u.effective_user.id if not is_callback else u.callback_query.from_user.id
    
    if storage.is_banned(uid):
        if is_callback:
            await u.callback_query.answer("انت محظور تواصل مع @XXVV_99")
        else:
            await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return
    
    msg = (
        "الأقسام المتاحة:\n\n"
        "- (جمم) - جمل عادية\n"
        "- (ويكي) - جمل ويكيبيديا\n"
        "- (صج) - كلمات عشوائية\n"
        "- (شك) - جمل عامية\n"
        "- (جش) - اقتباسات\n"
        "- (كرر) - تكرار الكلمات\n"
        "- (شرط) - جمل بالشروط\n"
        "- (فكك) - فك كلمات\n"
        "- (دبل) - تكرار كل كلمة مرتين\n"
        "- (تر) - تكرار كل كلمة ثلاث مرات\n"
        "- (عكس) - كتابة الجملة بالعكس\n"
        "- (فر) - جمل باللغة الفارسية\n"
        "- (E) - جمل باللغة الإنجليزية\n"
        "- (رق) - جمل أرقام\n"
        "- (حر) - جمل أحرف\n\n"
        "مميزات البوت وأوامره في زر أوامر البوت\n\n"
    )
    
    keyboard = [
        [InlineKeyboardButton("أوامر البوت", callback_data="show_commands")],
        [InlineKeyboardButton("أقسام البوت", callback_data="show_sections")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if is_callback:
        try:
            await u.callback_query.edit_message_text(msg, reply_markup=reply_markup)
        except BadRequest:
            pass
    else:
        await u.message.reply_text(msg, reply_markup=reply_markup)

async def show_bot_commands(u: Update, c: ContextTypes.DEFAULT_TYPE, is_callback=False):
    uid = u.effective_user.id if not is_callback else u.callback_query.from_user.id
    
    if storage.is_banned(uid):
        if is_callback:
            await u.callback_query.answer("انت محظور تواصل مع @XXVV_99")
        else:
            await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return
    
    msg = (
        "أوامر البوت:\n\n"
        "- (الصدارة) - المتصدرين\n" 
        "- (جوائزي) - عرض جوائزك\n"
        "- (فتح جولة) - فتح جولة جديدة\n"
        "- (جولة) - عرض إحصائيات الجولة\n"
        "- (انهاء جولة) - إنهاء الجولة الحالية\n"
        "- (تلقائي) - وضع الجمل التلقائية المتتالية\n\n"
        "اكتب اسم قسم بعده رقم يحدد عدد كلمات المقالة - (*اي قسم* 40)\n"
        "(*ريست *اي قسم) - البوت يرسل لك مقالات متفاوته الاحجام\n\n"
    )
    keyboard = [
        [InlineKeyboardButton("أوامر البوت", callback_data="show_commands")],
        [InlineKeyboardButton("أقسام البوت", callback_data="show_sections")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if is_callback:
        try:
            await u.callback_query.edit_message_text(msg, reply_markup=reply_markup)
        except BadRequest:
            pass
    else:
        await u.message.reply_text(msg, reply_markup=reply_markup)

async def cmd_leaderboard(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    types = ["جمم", "ويكي", "صج", "شك", "جش", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]
    sections = []

    for typ in types:
        lb = storage.get_leaderboard(typ)
        if lb:
            s = f"{typ}\n"
            for i, (uid_str, username, first_name, wpm) in enumerate(lb, 1):
                display = f"@{username}" if username else first_name
                s += f"{i}. {display}: {wpm:.2f} WPM\n"
            sections.append(s)

    if sections:
        await u.message.reply_text("\n".join(sections))
    else:
        await u.message.reply_text("لا توجد نتائج بعد")

async def cmd_awards(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    awards = storage.get_awards(uid)
    if not awards:
        await u.message.reply_text("لا توجد جوائز لديك بعد")
        return

    msg = "جوائزك:\n\n"
    for aw in awards:
        dt = datetime.fromisoformat(aw['date']).strftime('%Y-%m-%d')
        msg += f"• {aw['name']} - {aw['type']}: {aw['wpm']:.2f} WPM ({dt})\n"

    await u.message.reply_text(msg)

async def cmd_round(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    cid = u.effective_chat.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    existing_round = storage.get_round(cid)
    if existing_round:
        await u.message.reply_text("فيه جولة شغالة يا قفلها ب انهاء جولة او اختار رقم لفظا او رقما")
        return

    storage.set_pending_round_setup(cid, uid, True)
    await u.message.reply_text("كم عدد الانتصارات المطلوبة للفوز في الجولة؟\nأدخل الرقم من 1 إلى 100 (مثال: 5 أو خمسة)")

    await asyncio.sleep(20)

    if storage.get_pending_round_setup(cid) == uid:
        storage.set_pending_round_setup(cid, uid, False)
        await c.bot.send_message(chat_id=cid, text="لم يختر منشئ الجولة اي عدد لذا افتحوا جولة جديدة")

async def cmd_show_round(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    cid = u.effective_chat.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    existing_round = storage.get_round(cid)
    if not existing_round:
        await u.message.reply_text("لا توجد جولة مفتوحة حالياً")
        return

    stats = storage.get_round_stats(cid)
    await u.message.reply_text(stats)

async def cmd_end_round(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    cid = u.effective_chat.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    round_data = storage.get_round(cid)
    if not round_data:
        await u.message.reply_text("لا توجد جولة مفتوحة حالياً")
        return

    starter_uid = round_data.get("starter_uid")
    is_starter = (starter_uid == uid)

    is_admin = False
    try:
        chat_member = await c.bot.get_chat_member(cid, uid)
        is_admin = chat_member.status in ['creator', 'administrator']
    except:
        pass

    if not is_starter and not is_admin:
        await u.message.reply_text("منشئ الجولة ينهي الجولة فقط او مشرفين المجموعة")
        return

    storage.end_round(cid)
    await u.message.reply_text("تم إنهاء الجولة")

async def cmd_ban(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    if not has_permission(uid, "admin"):
        await u.message.reply_text("هذا الأمر للمشرفين فقط")
        return

    if u.message.reply_to_message:
        target_uid = u.message.reply_to_message.from_user.id

        if storage.is_main_owner(target_uid):
            await u.message.reply_text("لا يمكنك حظر المالك الأساسي")
            return

        storage.ban_user(target_uid)
        await u.message.reply_text(f"تم حظر المستخدم")
    else:
        await u.message.reply_text("رد على رسالة المستخدم لحظره")

async def cmd_unban(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    if not has_permission(uid, "admin"):
        await u.message.reply_text("هذا الأمر للمشرفين فقط")
        return

    if u.message.reply_to_message:
        target_uid = u.message.reply_to_message.from_user.id
        success = storage.unban_user(target_uid)
        if success:
            await u.message.reply_text(f"تم إلغاء حظر المستخدم")
        else:
            await u.message.reply_text(f"المستخدم غير محظور")
    else:
        await u.message.reply_text("رد على رسالة المستخدم لإلغاء حظره")

async def cmd_broadcast_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    if not has_permission(uid, "owner"):
        await u.message.reply_text("هذا الأمر للمالكين فقط")
        return

    storage.set_broadcast_mode(uid, True)
    await u.message.reply_text("تم تفعيل وضع الإذاعة. أرسل الرسالة التي تريد إذاعتها الآن.")

async def cmd_stats(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    if not storage.is_main_owner(uid):
        await u.message.reply_text("هذا الأمر للمالك الأساسي فقط")
        return

    total_users = len(storage.data["users"])
    total_chats = len(storage.data["chats"])
    banned = len(storage.data["banned"])

    stats_details = "\n\nإحصائيات الأقسام:\n"
    types = ["جمم", "ويكي", "صج", "شك", "جش", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]

    total_usage = {}
    for date, commands in storage.data["stats"].items():
        for cmd, count in commands.items():
            if cmd in types:
                total_usage[cmd] = total_usage.get(cmd, 0) + count

    for typ in types:
        usage = total_usage.get(typ, 0)
        stats_details += f"• {typ}: {usage} مرة\n"

    msg = (
        f"📊 إحصائيات البوت:\n\n"
        f"المستخدمين: {total_users}\n"
        f"المجموعات: {total_chats}\n"
        f"المحظورين: {banned}"
        f"{stats_details}"
    )

    await u.message.reply_text(msg)

async def cmd_supervision(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    msg = "👥 قائمة الإشراف:\n\n"

    msg += "🔰 المالك الأساسي:\n"
    owner_data = storage.data["users"].get(str(OWNER_ID), {})
    owner_username = owner_data.get("username")
    if owner_username:
        msg += f"• @{owner_username}\n"
    else:
        msg += f"• {owner_data.get('first_name', 'المالك الأساسي')}\n"

    owners = storage.get_all_owners()
    if owners:
        msg += "\n👑 الملاك:\n"
        for owner_id in owners:
            user_data = storage.data["users"].get(str(owner_id), {})
            username = user_data.get("username")
            if username:
                msg += f"• @{username}\n"
            else:
                msg += f"• {user_data.get('first_name', 'مالك')}\n"

    admins = storage.get_all_admins()
    if admins:
        msg += "\n⭐ الادمنز:\n"
        for admin_id in admins:
            user_data = storage.data["users"].get(str(admin_id), {})
            username = user_data.get("username")
            if username:
                msg += f"• @{username}\n"
            else:
                msg += f"• {user_data.get('first_name', 'ادمن')}\n"

    if not owners and not admins:
        msg += "\nلا يوجد ملاك أو ادمنز حالياً."

    await u.message.reply_text(msg)

async def send_auto_sentence(c: ContextTypes.DEFAULT_TYPE, cid, auto_data):
    sections = auto_data["sections"]
    last_section = auto_data.get("last_used_section")
    message_thread_id = auto_data.get("message_thread_id")
    
    available_sections = [s for s in sections if s != last_section] if len(sections) > 1 and last_section else sections
    if not available_sections:
        available_sections = sections
    
    selected_section = random.choice(available_sections)
    storage.set_auto_last_section(cid, selected_section)
    
    if selected_section in ["جمم", "ويكي", "صج", "شك", "جش", "فر", "E"]:
        sent = managers[selected_section].get()
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", sent, time.time(), sent=True)
        display = format_display(sent)
        await c.bot.send_message(chat_id=cid, text=display, message_thread_id=message_thread_id)
    elif selected_section == "شرط":
        sent = managers["شرط"].get()
        cond = random.choice(CONDITIONS)
        full = f"{sent}||{cond}"
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", full, time.time(), sent=True)
        await c.bot.send_message(chat_id=cid, text=cond, message_thread_id=message_thread_id)
        await asyncio.sleep(2)
        await c.bot.send_message(chat_id=cid, text=format_display(sent), message_thread_id=message_thread_id)
    elif selected_section == "فكك":
        sent = managers["فكك"].get()
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", sent, time.time(), sent=True)
        await c.bot.send_message(chat_id=cid, text=format_display(sent), message_thread_id=message_thread_id)
    elif selected_section in ["دبل", "تر", "عكس"]:
        sent = managers[selected_section].get()
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", sent, time.time(), sent=True)
        display = format_display(sent)
        await c.bot.send_message(chat_id=cid, text=display, message_thread_id=message_thread_id)
    elif selected_section == "كرر":
        pattern = gen_pattern(auto_data["uid"], 1)[0]
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", pattern, time.time(), sent=True)
        await c.bot.send_message(chat_id=cid, text=pattern, message_thread_id=message_thread_id)
    elif selected_section == "رق":
        sent = generate_random_sentence(auto_data["uid"], NUMBER_WORDS, 7, 20, "رق")
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", sent, time.time(), sent=True)
        display = format_display(sent)
        await c.bot.send_message(chat_id=cid, text=display, message_thread_id=message_thread_id)
    elif selected_section == "حر":
        sent = generate_random_sentence(auto_data["uid"], LETTER_WORDS, 7, 20, "حر")
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", sent, time.time(), sent=True)
        display = format_display(sent)
        await c.bot.send_message(chat_id=cid, text=display, message_thread_id=message_thread_id)

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message or not u.message.text:
        return

    uid = u.effective_user.id
    cid = u.effective_chat.id
    text = u.message.text.strip()
    usr = u.effective_user.username
    name = u.effective_user.first_name
    message_thread_id = u.message.message_thread_id if u.effective_chat.is_forum else None

    storage.add_user(uid, usr, name)

    if u.effective_chat.type in ['group', 'supergroup']:
        chat_title = u.effective_chat.title
        storage.add_chat(cid, chat_title)

    if u.message.reply_to_message and storage.is_main_owner(uid):
        replied_user = u.message.reply_to_message.from_user
        replied_uid = replied_user.id
        replied_username = replied_user.username
        replied_name = replied_user.first_name

        auto_reply_commands = ["باند", "تقييد", "كتم", "الغاء تقييد", "الغاء باند", "طرد", "الغاء كتم"]

        if text in auto_reply_commands:
            await u.message.reply_to_message.reply_text(text)
            return

        if text == "رفع ادمن":
            storage.add_admin(replied_uid)
            mention = f"@{replied_username}" if replied_username else replied_name
            await u.message.reply_text(f"تم رفع {mention} إلى رتبة ادمن")
            return

        if text == "رفع مالك":
            storage.add_owner(replied_uid)
            mention = f"@{replied_username}" if replied_username else replied_name
            await u.message.reply_text(f"تم رفع {mention} إلى رتبة مالك")
            return

    if storage.is_banned(uid):
        commands = ["الصدارة", "جوائزي", "جولة", "فتح جولة", "باند", "الغاء باند", "إذاعة", "احصاء", "الإشراف",
                   "جمم", "ويكي", "صج", "شك", "جش", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "عرض", "مقالات", "فر", "E", "e", "رق", "حر", "ريست", "تلقائي"]
        is_command = any(text.startswith(cmd) for cmd in commands)

        if is_command:
            await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    if storage.get_broadcast_mode(uid):
        storage.set_broadcast_mode(uid, False)
        sent = 0
        failed = 0
        for chat_id in storage.data["chats"].keys():
            try:
                await c.bot.send_message(chat_id=int(chat_id), text=text)
                sent += 1
            except Exception as e:
                failed += 1
                print(f"Failed to send to {chat_id}: {e}")

        await u.message.reply_text(f"تم الإرسال إلى {sent} مجموعة. فشل: {failed}")
        return

    pending_setup = storage.get_pending_round_setup(cid)
    if pending_setup == uid:
        target = arabic_to_num(text)
        if target is None:
            try:
                target = int(text)
            except ValueError:
                await u.message.reply_text("الرجاء إدخال رقم صحيح من 1 إلى 100")
                return

        if target < 1 or target > 100:
            await u.message.reply_text("الرجاء إدخال رقم من 1 إلى 100")
            return

        storage.set_pending_round_setup(cid, uid, False)
        storage.start_round(cid, target, uid)
        storage.set_round_mode(cid, True)
        await u.message.reply_text(f"تم فتح جولة جديدة من {target} انتصار!")
        return

    auto_mode = storage.get_auto_mode(cid)
    if auto_mode:
        if message_thread_id is None or auto_mode.get("message_thread_id") == message_thread_id:
            if text == "قف":
                storage.end_auto_mode(cid)
                await u.message.reply_text("تم إيقاف نظام تلقائي")
                storage.log_cmd("قف")
                return
            
            if auto_mode["collecting"]:
                valid_sections = ["جمم", "ويكي", "صج", "شك", "جش", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]
                
                if text in valid_sections:
                    if storage.add_auto_section(cid, text):
                        await u.message.reply_text(f"الحين انت أضفت قسم {text} تبي تضيف زيادة او تبي تكتب انهاء؟")
                        return
                elif text == "انهاء":
                    if auto_mode["sections"]:
                        storage.finish_auto_collection(cid)
                        await u.message.reply_text("3")
                        await asyncio.sleep(1)
                        await u.message.reply_text("2")
                        await asyncio.sleep(1)
                        await u.message.reply_text("1")
                        await asyncio.sleep(1)
                        
                        await send_auto_sentence(c, cid, auto_mode)
                        storage.update_auto_activity(cid)
                    else:
                        await u.message.reply_text("لم تختر أي قسم. اكتب اسم قسم أو أكثر ثم اكتب انهاء")
                    return
            elif auto_mode["active"]:
                auto_sessions = {}
                for section in auto_mode["sections"]:
                    session_key = f"تلقائي_{section}"
                    session = storage.get_session(cid, session_key)
                    if session:
                        auto_sessions[section] = session
                
                matched = False
                for section, session in auto_sessions.items():
                    typ = session.get("type")
                    orig = session.get("text")
                    start_time = session.get("time")
                    elapsed = time.time() - start_time

                    if elapsed > 180:
                        continue

                    section_type = typ.replace("تلقائي_", "")
                    
                    if section_type in ["جمم", "ويكي", "صج", "شك", "جش"]:
                        if match_text(orig, text, "arabic"):
                            matched = True
                    elif section_type == "فر":
                        if match_text(orig, text, "persian"):
                            matched = True
                    elif section_type == "E":
                        if match_text(orig, text, "english"):
                            matched = True
                    elif section_type in ["رق", "حر"]:
                        if match_text(orig, text, "arabic"):
                            matched = True
                    elif section_type == "كرر":
                        valid, err = validate_repeat(orig, text)
                        if valid:
                            matched = True
                    elif section_type == "شرط":
                        orig_s, cond = orig.split('||')
                        valid, exp = validate_condition(cond, orig_s, text)
                        if valid:
                            matched = True
                    elif section_type == "فكك":
                        if is_correct_disassembly(orig, text):
                            matched = True
                    elif section_type == "دبل":
                        valid, err = validate_double(orig, text)
                        if valid:
                            matched = True
                    elif section_type == "تر":
                        valid, err = validate_triple(orig, text)
                        if valid:
                            matched = True
                    elif section_type == "عكس":
                        valid, err = validate_reverse(orig, text)
                        if valid:
                            matched = True

                    if matched:
                        word_count = count_words_for_wpm(text)
                        wpm = (word_count / elapsed) * 60 + 10
                        storage.update_score(uid, section_type, wpm)
                        
                        storage.del_session(cid, typ)
                        storage.update_auto_activity(cid)
                        
                        await u.message.reply_text(f"⚡ سرعتك: {wpm:.1f} كلمة/دقيقة")
                        await asyncio.sleep(0.5)
                        await send_auto_sentence(c, cid, auto_mode)
                        return

    current_time = time.time()
    if current_time - last_command_time[cid][text] < 1:
        return
    last_command_time[cid][text] = current_time

    if text == "تلقائي":
        storage.start_auto_mode(cid, uid, message_thread_id)
        await u.message.reply_text("اختر انواع الأقسام اللتي تريدها حين الإنتهاء اكتب انهاء")
        storage.log_cmd("تلقائي")
        return

    if text == "الصدارة":
        await cmd_leaderboard(u, c)
        storage.log_cmd("الصدارة")
        return

    if text == "جوائزي":
        await cmd_awards(u, c)
        storage.log_cmd("جوائزي")
        return

    if text == "جولة":
        await cmd_show_round(u, c)
        storage.log_cmd("جولة")
        return

    if text.startswith("فتح جولة"):
        await cmd_round(u, c)
        storage.log_cmd("فتح جولة")
        return

    if text == "انهاء جولة":
        await cmd_end_round(u, c)
        storage.log_cmd("انهاء جولة")
        return

    if text == "باند":
        await cmd_ban(u, c)
        storage.log_cmd("باند")
        return

    if text == "الغاء باند":
        await cmd_unban(u, c)
        storage.log_cmd("الغاء باند")
        return

    if text == "إذاعة":
        await cmd_broadcast_start(u, c)
        storage.log_cmd("إذاعة")
        return

    if text == "احصاء":
        await cmd_stats(u, c)
        storage.log_cmd("احصاء")
        return

    if text == "الإشراف":
        await cmd_supervision(u, c)
        storage.log_cmd("الإشراف")
        return

    if text in ["عرض", "مقالات"]:
        await show_bot_sections(u, c)
        storage.log_cmd(text)
        return

    if text.startswith("ريست "):
        section = text[5:].strip()
        if section in ["جمم", "ويكي", "صج", "شك", "جش", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E"]:
            if storage.clear_preference(uid, section):
                await u.message.reply_text(f"تم إعادة تعيين تفضيلات القسم ({section}) بنجاح\nالآن سيتم إرسال الجمل بشكلها الطبيعي")
            else:
                await u.message.reply_text(f"لا يوجد تفضيل محفوظ للقسم ({section})")
        else:
            await u.message.reply_text("القسم غير موجود\nاستخدم: ريست [اسم القسم]\nمثال: ريست جمم")
        storage.log_cmd("ريست")
        return

    active_sessions = storage.get_all_active_sessions(cid)

    command, word_count = extract_number_from_text(text)

    if command in ["جمم", "ويكي", "صج", "شك", "جش"] or text in ["جمم", "ويكي", "صج", "شك", "جش"]:
        section = command if word_count else text
        storage.log_cmd(section)

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers[section], word_count)
            if sent:
                storage.del_session(cid, section)
                storage.save_session(uid, cid, section, sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
            else:
                sent = managers[section].get()
                storage.del_session(cid, section)
                storage.save_session(uid, cid, section, sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(display)
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers[section], pref_count)
                if not sent:
                    sent = managers[section].get()
            else:
                sent = managers[section].get()
            storage.del_session(cid, section)
            storage.save_session(uid, cid, section, sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
        return

    if command == "فر" or text == "فر":
        section = "فر"
        storage.log_cmd("فر")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["فر"], word_count)
            if sent:
                storage.del_session(cid, "فر")
                storage.save_session(uid, cid, "فر", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
            else:
                sent = managers["فر"].get()
                storage.del_session(cid, "فر")
                storage.save_session(uid, cid, "فر", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(display)
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["فر"], pref_count)
                if not sent:
                    sent = managers["فر"].get()
            else:
                sent = managers["فر"].get()
            storage.del_session(cid, "فر")
            storage.save_session(uid, cid, "فر", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
        return

    if command == "E" or text in ["E", "e"]:
        section = "E"
        storage.log_cmd("E")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["E"], word_count)
            if sent:
                storage.del_session(cid, "E")
                storage.save_session(uid, cid, "E", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
            else:
                sent = managers["E"].get()
                storage.del_session(cid, "E")
                storage.save_session(uid, cid, "E", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(display)
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["E"], pref_count)
                if not sent:
                    sent = managers["E"].get()
            else:
                sent = managers["E"].get()
            storage.del_session(cid, "E")
            storage.save_session(uid, cid, "E", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
        return

    if text == "رق":
        async with chat_locks[cid]:
            message_id = f"{cid}_رق_{uid}_{current_time}"
            
            if current_time - sent_message_tracker[cid]["رق"] < 0.5:
                return
            
            storage.log_cmd("رق")
            sent = generate_random_sentence(uid, NUMBER_WORDS, 7, 20, "رق")
            storage.del_session(cid, "رق")
            storage.save_session(uid, cid, "رق", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
            sent_message_tracker[cid]["رق"] = current_time
        return

    if text == "حر":
        async with chat_locks[cid]:
            message_id = f"{cid}_حر_{uid}_{current_time}"
            
            if current_time - sent_message_tracker[cid]["حر"] < 0.5:
                return
            
            storage.log_cmd("حر")
            sent = generate_random_sentence(uid, LETTER_WORDS, 7, 20, "حر")
            storage.del_session(cid, "حر")
            storage.save_session(uid, cid, "حر", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
            sent_message_tracker[cid]["حر"] = current_time
        return

    if command == "كرر" or text == "كرر":
        async with chat_locks[cid]:
            message_id = f"{cid}_كرر_{uid}_{current_time}"
            
            if current_time - sent_message_tracker[cid]["كرر"] < 0.5:
                return
            
            storage.log_cmd("كرر")

            if word_count and 1 <= word_count <= 20:
                patterns = gen_pattern(uid, word_count)
                result_lines = []
                storage.del_session(cid, "كرر")
                for i, pattern in enumerate(patterns, 1):
                    result_lines.append(f"{i}. {pattern}")
                    if i == 1:
                        storage.save_session(uid, cid, "كرر", pattern, time.time(), sent=True)

                await u.message.reply_text('\n'.join(result_lines))
            else:
                pattern = gen_pattern(uid, 1)[0]
                storage.del_session(cid, "كرر")
                storage.save_session(uid, cid, "كرر", pattern, time.time(), sent=True)
                await u.message.reply_text(pattern)
            
            sent_message_tracker[cid]["كرر"] = current_time
        return

    if command == "شرط" or text == "شرط":
        section = "شرط"
        storage.log_cmd("شرط")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["شرط"], word_count)
            if not sent:
                sent = managers["شرط"].get()
            cond = random.choice(CONDITIONS)
            full = f"{sent}||{cond}"
            storage.del_session(cid, "شرط")
            storage.save_session(uid, cid, "شرط", full, time.time(), sent=True)
            await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
            await asyncio.sleep(0.5)
            await u.message.reply_text(cond)
            await asyncio.sleep(2)
            await c.bot.send_message(chat_id=cid, text=format_display(sent))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["شرط"], pref_count)
                if not sent:
                    sent = managers["شرط"].get()
            else:
                sent = managers["شرط"].get()
            cond = random.choice(CONDITIONS)
            full = f"{sent}||{cond}"
            storage.del_session(cid, "شرط")
            storage.save_session(uid, cid, "شرط", full, time.time(), sent=True)
            await u.message.reply_text(cond)
            await asyncio.sleep(2)
            await c.bot.send_message(chat_id=cid, text=format_display(sent))
        return

    if command == "فكك" or text == "فكك":
        section = "فكك"
        storage.log_cmd("فكك")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["فكك"], word_count)
            if not sent:
                sent = managers["فكك"].get()
            storage.del_session(cid, "فكك_تفكيك")
            storage.save_session(uid, cid, "فكك_تفكيك", sent, time.time(), sent=True)
            await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
            await asyncio.sleep(0.5)
            await u.message.reply_text(format_display(sent))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["فكك"], pref_count)
                if not sent:
                    sent = managers["فكك"].get()
            else:
                sent = managers["فكك"].get()
            storage.del_session(cid, "فكك_تفكيك")
            storage.save_session(uid, cid, "فكك_تفكيك", sent, time.time(), sent=True)
            await u.message.reply_text(format_display(sent))
        return

    if command == "دبل" or text == "دبل":
        section = "دبل"
        storage.log_cmd("دبل")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["دبل"], word_count)
            if not sent:
                sent = managers["دبل"].get()
            storage.del_session(cid, "دبل")
            storage.save_session(uid, cid, "دبل", sent, time.time(), sent=True)
            await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
            await asyncio.sleep(0.5)
            await u.message.reply_text(format_display(sent))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["دبل"], pref_count)
                if not sent:
                    sent = managers["دبل"].get()
            else:
                sent = managers["دبل"].get()
            storage.del_session(cid, "دبل")
            storage.save_session(uid, cid, "دبل", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
        return

    if command == "تر" or text == "تر":
        section = "تر"
        storage.log_cmd("تر")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["تر"], word_count)
            if not sent:
                sent = managers["تر"].get()
            storage.del_session(cid, "تر")
            storage.save_session(uid, cid, "تر", sent, time.time(), sent=True)
            await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
            await asyncio.sleep(0.5)
            await u.message.reply_text(format_display(sent))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["تر"], pref_count)
                if not sent:
                    sent = managers["تر"].get()
            else:
                sent = managers["تر"].get()
            storage.del_session(cid, "تر")
            storage.save_session(uid, cid, "تر", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
        return

    if command == "عكس" or text == "عكس":
        section = "عكس"
        storage.log_cmd("عكس")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["عكس"], word_count)
            if not sent:
                sent = managers["عكس"].get()
            storage.del_session(cid, "عكس")
            storage.save_session(uid, cid, "عكس", sent, time.time(), sent=True)
            await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
            await asyncio.sleep(0.5)
            await u.message.reply_text(format_display(sent))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["عكس"], pref_count)
                if not sent:
                    sent = managers["عكس"].get()
            else:
                sent = managers["عكس"].get()
            storage.del_session(cid, "عكس")
            storage.save_session(uid, cid, "عكس", sent, time.time(), sent=True)
            display = format_display(sent)
            await u.message.reply_text(display)
        return

    for session in active_sessions:
        typ = session.get("type")
        orig = session.get("text")
        start_time = session.get("time")
        elapsed = time.time() - start_time

        if elapsed > 60:
            continue

        matched = False

        if typ in ["جمم", "ويكي", "صج", "شك", "جش"]:
            if match_text(orig, text, "arabic"):
                matched = True
        elif typ == "فر":
            if match_text(orig, text, "persian"):
                matched = True
        elif typ == "E":
            if match_text(orig, text, "english"):
                matched = True
        elif typ in ["رق", "حر"]:
            if match_text(orig, text, "arabic"):
                matched = True
        elif typ == "كرر":
            valid, err = validate_repeat(orig, text)
            if valid:
                matched = True
        elif typ == "شرط":
            orig_s, cond = orig.split('||')
            valid, exp = validate_condition(cond, orig_s, text)
            if valid:
                matched = True
        elif typ == "فكك_تفكيك":
            if is_correct_disassembly(orig, text):
                matched = True
        elif typ == "دبل":
            valid, err = validate_double(orig, text)
            if valid:
                matched = True
        elif typ == "تر":
            valid, err = validate_triple(orig, text)
            if valid:
                matched = True
        elif typ == "عكس":
            valid, err = validate_reverse(orig, text)
            if valid:
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
                        f"ممتاز سرعتك!\n"
                        f"({wpm:.2f} WPM) - - - ({elapsed:.2f} ثانية)\n"
                        f"الفوز رقم: {wins}/{target}"
                        f"{round_stats}"
                    )
                    storage.end_round(cid)
                else:
                    await u.message.reply_text(
                        f"صحيح {mention}!\n"
                        f"ممتاز سرعتك!\n"
                        f"({wpm:.2f} WPM) - - - ({elapsed:.2f} ثانية)\n"
                        f"التقدم: {wins}/{target}"
                        f"{round_stats}"
                    )
            else:
                await u.message.reply_text(
                    f"ممتاز سرعتك!\n"
                    f"({wpm:.2f} WPM) - - - ({elapsed:.2f} ثانية)"
                )

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
            storage.save(force=True)

async def periodic_auto_cleanup():
    while True:
        await asyncio.sleep(30)
        removed = storage.cleanup_inactive_auto_modes()
        if removed:
            print(f"Removed {len(removed)} inactive auto modes")

async def handle_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query = u.callback_query
    await query.answer()

    if query.data == "show_commands":
        await show_bot_commands(u, c, is_callback=True)
        return
    
    if query.data == "show_sections":
        await show_bot_sections(u, c, is_callback=True)
        return

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

async def post_init(application: Application):
    asyncio.create_task(periodic_save())
    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(periodic_round_cleanup())
    asyncio.create_task(periodic_auto_cleanup())
    print("[BACKGROUND] Started background tasks: periodic_save, periodic_cleanup, periodic_round_cleanup, periodic_auto_cleanup")

def main():
    if not BOT_TOKEN:
        print("Error: BOT_TOKEN environment variable not set!")
        return

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

    print("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
