import random
import time
import re
import requests
import json
import os
import asyncio
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = {5562144078}

processing_locks = {}
processed_messages = set()
last_wiki_request = {}
URLS = {
    "جمم": "https://raw.githubusercontent.com/AL3ATEL/TXT-bot-telegram-/refs/heads/main/sentences.txt",
    "شرط": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-2/refs/heads/main/conditions.txt",
    "فكك": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-3/refs/heads/main/FKK.txt",
    "مكت": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-4/refs/heads/main/arabic_sentences.json",
    "شكت": "https://raw.githubusercontent.com/BoulahiaAhmed/Arabic-Quotes-Dataset/main/Arabic_Quotes.csv",
    "اكت": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-5/refs/heads/main/3amh.txt"
}
REPEAT_WORDS = ["صمت", "صوف", "سين", "عين", "جيم", "كتب", "خبر", "حلم", "جمل", "تعب", "حسد", "نار", "برد", "علي", "عمر", "قطر", "درب", "خطر", "علم", "صوت"]
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

MARATHON_SECTIONS = {
    "1": "جمم",
    "2": "ويكي",
    "3": "شرط",
    "4": "فكك",
    "5": "مكت",
    "6": "شكت",
    "7": "اكت",
    "8": "كرر"
}

MARATHON_INACTIVITY_TIMEOUT = 180

class Storage:
    def __init__(self):
        self.file = "bot_data.json"
        self.data = self.load()

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
                "marathon_state": {},
                "marathon_stats": {}
            }

    def save(self):
        try:
            with open(self.file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving data: {e}")

    def add_user(self, uid, usr, name):
        self.data["users"][str(uid)] = {
            "username": usr, 
            "first_name": name, 
            "created_at": datetime.now().isoformat()
        }
        self.save()

    def add_chat(self, cid, title):
        self.data["chats"][str(cid)] = {
            "title": title, 
            "created_at": datetime.now().isoformat()
        }
        self.save()

    def is_banned(self, uid):
        return str(uid) in self.data["banned"]

    def ban_user(self, uid):
        if str(uid) not in self.data["banned"]:
            self.data["banned"].append(str(uid))
        self.data["sessions"].pop(f"{uid}_*", None)
        self.save()

    def unban_user(self, uid):
        if str(uid) in self.data["banned"]:
            self.data["banned"].remove(str(uid))
        self.save()

    def update_score(self, uid, typ, wpm):
        key = f"{uid}_{typ}"
        self.data["scores"][key] = max(self.data["scores"].get(key, 0), wpm)
        self.save()

    def get_score(self, uid, typ):
        return self.data["scores"].get(f"{uid}_{typ}", 0)

    def add_pattern(self, uid, key):
        if str(uid) not in self.data["patterns"]:
            self.data["patterns"][str(uid)] = []
        if key not in self.data["patterns"][str(uid)]:
            self.data["patterns"][str(uid)].append(key)
            self.save()

    def is_pattern_used(self, uid, key):
        return key in self.data["patterns"].get(str(uid), [])

    def clear_patterns(self, uid):
        self.data["patterns"][str(uid)] = []
        self.save()

    def save_session(self, uid, cid, typ, txt, tm):
        key = f"{cid}_{typ}"
        self.data["sessions"][key] = {
            "type": typ, 
            "text": txt, 
            "time": tm,
            "starter_uid": uid
        }
        self.save()

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
            self.save()

        return active_sessions

    def del_session(self, cid, typ):
        self.data["sessions"].pop(f"{cid}_{typ}", None)
        self.save()

    def cancel_user_session_in_type(self, uid, cid, typ):
        key = f"{cid}_{typ}"
        session = self.data["sessions"].get(key)
        if session and session.get("starter_uid") == uid:
            self.data["sessions"].pop(key, None)
            self.save()
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
        self.save()

    def get_awards(self, uid):
        return self.data["weekly_awards"].get(str(uid), [])

    def log_cmd(self, cmd):
        dt = datetime.now().strftime("%Y-%m-%d")
        if dt not in self.data["stats"]:
            self.data["stats"][dt] = {}
        if cmd not in self.data["stats"][dt]:
            self.data["stats"][dt][cmd] = 0
        self.data["stats"][dt][cmd] += 1
        self.save()

    def set_broadcast_mode(self, uid, status):
        self.data["broadcast_mode"][str(uid)] = status
        self.save()

    def get_broadcast_mode(self, uid):
        return self.data["broadcast_mode"].get(str(uid), False)

    def start_round(self, cid, target):
        self.data["rounds"][str(cid)] = {
            "target": target, 
            "wins": {}, 
            "started_at": datetime.now().isoformat()
        }
        self.save()

    def get_round(self, cid):
        return self.data["rounds"].get(str(cid))

    def end_round(self, cid):
        self.data["rounds"].pop(str(cid), None)
        self.save()

    def add_win(self, cid, uid):
        if str(cid) not in self.data["rounds"]:
            return False

        if str(uid) not in self.data["rounds"][str(cid)]["wins"]:
            self.data["rounds"][str(cid)]["wins"][str(uid)] = 0

        self.data["rounds"][str(cid)]["wins"][str(uid)] += 1
        self.save()
        return self.data["rounds"][str(cid)]["wins"][str(uid)]

    def set_round_mode(self, cid, status):
        self.data["round_mode"][str(cid)] = status
        self.save()

    def get_round_mode(self, cid):
        return self.data["round_mode"].get(str(cid), False)

    def start_marathon_selection(self, uid, cid):
        key = f"{cid}"
        self.data["marathon_state"][key] = {
            "state": "waiting_participants",
            "creator_uid": uid,
            "participants": {},
            "sections": [],
            "started_at": datetime.now().isoformat(),
            "last_activity": time.time()
        }
        self.save()

    def add_marathon_participant(self, uid, cid, name, username):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            self.data["marathon_state"][key]["participants"][str(uid)] = {
                "name": name,
                "username": username,
                "joined_at": datetime.now().isoformat(),
                "last_activity": time.time()
            }
            self.data["marathon_state"][key]["last_activity"] = time.time()
            self.save()
            return True
        return False

    def update_participant_activity(self, uid, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            if str(uid) in self.data["marathon_state"][key]["participants"]:
                self.data["marathon_state"][key]["participants"][str(uid)]["last_activity"] = time.time()
                self.data["marathon_state"][key]["last_activity"] = time.time()
                self.save()

    def remove_marathon_participant(self, uid, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            if str(uid) in self.data["marathon_state"][key]["participants"]:
                del self.data["marathon_state"][key]["participants"][str(uid)]
                self.save()
                return True
        return False

    def is_marathon_creator(self, uid, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            return self.data["marathon_state"][key].get("creator_uid") == uid
        return False

    def add_marathon_section(self, cid, section):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            if section not in self.data["marathon_state"][key]["sections"]:
                self.data["marathon_state"][key]["sections"].append(section)
                self.save()
                return True
        return False

    def start_marathon_running(self, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            self.data["marathon_state"][key]["state"] = "running"
            self.data["marathon_state"][key]["current_sentence"] = None
            self.data["marathon_state"][key]["sentence_start_time"] = None
            self.data["marathon_state"][key]["sentence_type"] = None
            self.data["marathon_state"][key]["last_activity"] = time.time()

            if key not in self.data["marathon_stats"]:
                self.data["marathon_stats"][key] = {}

            for participant_uid in self.data["marathon_state"][key]["participants"].keys():
                if participant_uid not in self.data["marathon_stats"][key]:
                    self.data["marathon_stats"][key][participant_uid] = {
                        "total_sentences": 0,
                        "speeds": [],
                        "start_time": time.time(),
                        "fastest_speed": 0,
                        "fastest_sentence": ""
                    }

            self.save()
            return True
        return False

    def get_marathon_state(self, cid):
        key = f"{cid}"
        return self.data["marathon_state"].get(key)

    def check_marathon_inactivity(self, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            state = self.data["marathon_state"][key]

            if state.get("state") == "waiting_participants":
                return False

            participants = state.get("participants", {})
            if not participants:
                return True

            last_activity = state.get("last_activity", 0)
            if time.time() - last_activity > MARATHON_INACTIVITY_TIMEOUT:
                return True

        return False

    def set_marathon_sentence(self, cid, sentence, sentence_type):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            self.data["marathon_state"][key]["current_sentence"] = sentence
            self.data["marathon_state"][key]["sentence_start_time"] = time.time()
            self.data["marathon_state"][key]["sentence_type"] = sentence_type
            self.data["marathon_state"][key]["answered_by"] = []
            self.save()

    def mark_participant_answered(self, uid, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            if "answered_by" not in self.data["marathon_state"][key]:
                self.data["marathon_state"][key]["answered_by"] = []
            if str(uid) not in self.data["marathon_state"][key]["answered_by"]:
                self.data["marathon_state"][key]["answered_by"].append(str(uid))
                self.save()
                return True
        return False

    def all_participants_answered(self, cid):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            state = self.data["marathon_state"][key]
            participants = set(state.get("participants", {}).keys())
            answered = set(state.get("answered_by", []))
            return participants == answered and len(participants) >= 1
        return False

    def add_marathon_speed(self, uid, cid, wpm, sentence):
        key = f"{cid}"
        if key not in self.data["marathon_stats"]:
            self.data["marathon_stats"][key] = {}

        if str(uid) not in self.data["marathon_stats"][key]:
            self.data["marathon_stats"][key][str(uid)] = {
                "total_sentences": 0,
                "speeds": [],
                "start_time": time.time(),
                "fastest_speed": 0,
                "fastest_sentence": ""
            }

        self.data["marathon_stats"][key][str(uid)]["speeds"].append(wpm)
        self.data["marathon_stats"][key][str(uid)]["total_sentences"] += 1

        if wpm > self.data["marathon_stats"][key][str(uid)]["fastest_speed"]:
            self.data["marathon_stats"][key][str(uid)]["fastest_speed"] = wpm
            self.data["marathon_stats"][key][str(uid)]["fastest_sentence"] = sentence

        self.save()

    def get_marathon_stats(self, cid):
        key = f"{cid}"
        return self.data["marathon_stats"].get(key)

    def end_marathon(self, cid):
        key = f"{cid}"
        self.data["marathon_state"].pop(key, None)
        stats = self.data["marathon_stats"].pop(key, None)
        self.save()
        return stats

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
            self.save()

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

class WikiManager:
    def __init__(self, api_url, namespace=0):
        self.api_url = api_url
        self.namespace = namespace
        self.used = set()
        self.last_fetch = 0
        self.headers = {'User-Agent': 'NKL-TypingBot/1.0'}
        self.max_used_size = 500

    def fetch(self):
        if time.time() - self.last_fetch < 2:
            time.sleep(2 - (time.time() - self.last_fetch))

        if len(self.used) >= self.max_used_size:
            self.used.clear()

        try:
            r = requests.get(
                self.api_url, 
                params={
                    'action': 'query', 
                    'list': 'random', 
                    'rnnamespace': self.namespace, 
                    'rnlimit': 20, 
                    'format': 'json'
                }, 
                headers=self.headers, 
                timeout=10
            ).json()

            for page in r.get('query', {}).get('random', []):
                c = requests.get(
                    self.api_url, 
                    params={
                        'action': 'query', 
                        'pageids': page['id'], 
                        'prop': 'extracts', 
                        'exchars': 1200, 
                        'explaintext': True, 
                        'format': 'json'
                    }, 
                    headers=self.headers, 
                    timeout=10
                ).json()

                extract_text = c.get('query', {}).get('pages', {}).get(str(page['id']), {}).get('extract', '')
                for s in re.split(r'[.!?؟]\s+', extract_text):
                    s = clean_wiki(s.strip())
                    if 8 <= len(s.split()) <= 21 and s not in self.used:
                        self.used.add(s)
                        self.last_fetch = time.time()
                        return s
        except Exception as e:
            print(f"Error fetching from Wikipedia: {e}")

        self.last_fetch = time.time()
        return "جرب مرة أخرى"

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

def clean_wiki(txt):
    txt = re.sub(r'\([^)]*\)', '', txt)
    txt = re.sub(r'\[[^\]]*\]', '', txt)
    txt = re.sub(r'[^\u0600-\u06FF\s≈]', '', txt)
    txt = re.sub(r'[،,:;؛\-–—\.\!؟\?\(\)\[\]\{\}""''«»…]', ' ', txt)

    def rep_num(m):
        n = m.group()
        return NUM_WORDS.get(n, ' '.join(NUM_WORDS.get(d, d) for d in n) if len(n) > 1 else n)

    txt = re.sub(r'\d+', rep_num, txt)
    return re.sub(r'\s+', ' ', txt).strip()

def normalize(txt):
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt)
    return re.sub(r'\s+', ' ', ''.join(CHAR_MAP.get(c, c) for c in txt)).strip()

def format_display(s):
    return ' ≈ '.join(s.split())

def match_text(orig, usr):
    return normalize(orig) == normalize(usr)

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
        return f"{words[1]} {words[0]} {' '.join(words[2:])}"

    elif cond == "بدل آخر كلمتين" and len(words) >= 2: 
        return f"{' '.join(words[:-2])} {words[-1]} {words[-2]}"

    elif cond == "بدل ثاني كلمة والكلمة الأخيرة" and len(words) >= 2:
        if len(words) == 2:
            return f"{words[1]} {words[0]}"
        else:
            return f"{words[0]} {words[-1]} {' '.join(words[2:-1])} {words[1]}"

    return sent

def validate_condition(cond, orig, usr):
    exp = apply_condition(cond, orig)
    if normalize(exp) == normalize(usr):
        return True, exp
    return False, exp

def gen_pattern(uid):
    repeat_count = random.randint(2, 5)
    words = []

    for _ in range(repeat_count):
        word = random.choice(REPEAT_WORDS)
        repeats = random.randint(2, 4)
        words.append(' '.join([word] * repeats))

    pattern = ' '.join(words)
    base_key = normalize(pattern)

    attempt = 0
    while storage.is_pattern_used(uid, base_key) and attempt < 20:
        words = []
        for _ in range(repeat_count):
            word = random.choice(REPEAT_WORDS)
            repeats = random.randint(2, 4)
            words.append(' '.join([word] * repeats))
        pattern = ' '.join(words)
        base_key = normalize(pattern)
        attempt += 1

    storage.add_pattern(uid, base_key)
    return pattern

def validate_repeat(pattern, user_text):
    pattern_norm = normalize(pattern)
    user_norm = normalize(user_text)

    if pattern_norm != user_norm:
        return False, "النمط غير متطابق"

    return True, None

def arabic_to_num(text):
    arabic_nums = {
        'واحد': 1, 'اثنان': 2, 'ثلاثة': 3, 'أربعة': 4, 'خمسة': 5,
        'ستة': 6, 'سبعة': 7, 'ثمانية': 8, 'تسعة': 9, 'عشرة': 10,
        'احد عشر': 11, 'اثنا عشر': 12, 'ثلاثة عشر': 13, 'أربعة عشر': 14, 'خمسة عشر': 15,
        'ستة عشر': 16, 'سبعة عشر': 17, 'ثمانية عشر': 18, 'تسعة عشر': 19, 'عشرون': 20,
        'ثلاثون': 30, 'أربعون': 40, 'خمسون': 50, 'ستون': 60, 'سبعون': 70, 'ثمانون': 80, 'تسعون': 90, 'مائة': 100
    }

    text = text.strip()

    if text.isdigit():
        return int(text)

    return arabic_nums.get(text)

def is_admin(uid):
    return uid in ADMIN_IDS

managers = {
    "جمم": RemoteManager(URLS["جمم"]),
    "شرط": RemoteManager(URLS["شرط"]),
    "فكك": RemoteManager(URLS["فكك"], disasm=True),
    "مكت": RemoteManager(URLS["مكت"]),
    "شكت": CSVQuotesManager(URLS["شكت"]),
    "اكت": RemoteManager(URLS["اكت"]),
    "ويكي": WikiManager("https://ar.wikipedia.org/w/api.php")
}

async def broadcast_message(context, msg_text):
    success_count = 0
    fail_count = 0

    for chat_id in storage.data["chats"].keys():
        try:
            await context.bot.send_message(chat_id=int(chat_id), text=msg_text)
            success_count += 1
            await asyncio.sleep(0.1)
        except Exception:
            fail_count += 1

    return success_count, fail_count

async def send_marathon_sentence(update: Update, context: ContextTypes.DEFAULT_TYPE, cid):
    marathon_state = storage.get_marathon_state(cid)
    if not marathon_state or marathon_state["state"] != "running":
        return

    sections = marathon_state["sections"]
    if not sections:
        await update.message.reply_text("لا توجد أقسام محددة!")
        storage.end_marathon(cid)
        return

    section = random.choice(sections)
    sentence = None
    sentence_type = section

    if section == "جمم":
        sentence = managers["جمم"].get()
    elif section == "ويكي":
        sentence = managers["ويكي"].fetch()
    elif section == "مكت":
        sentence = managers["مكت"].get()
    elif section == "اكت":
        sentence = managers["اكت"].get()
    elif section == "شكت":
        sentence = managers["شكت"].get()
    elif section == "كرر":
        creator_uid = marathon_state["creator_uid"]
        sentence = gen_pattern(creator_uid)
        sentence_type = "كرر"
    elif section == "شرط":
        s = managers["شرط"].get()
        cond = random.choice(CONDITIONS)
        sentence = s
        storage.set_marathon_sentence(cid, f"{s}||{cond}", sentence_type)
        await update.message.reply_text(f"القسم: شرط\n\n{cond}\n\n{format_display(s)}")
        return
    elif section == "فكك":
        s = managers["فكك"].get()
        sentence = s
        storage.set_marathon_sentence(cid, sentence, sentence_type)
        await update.message.reply_text(f"القسم: فكك\n\nفكك الجملة التالية:\n\n{format_display(s)}")
        return

    if sentence:
        storage.set_marathon_sentence(cid, sentence, sentence_type)
        section_name_display = {
            "جمم": "جمم", "ويكي": "ويكي", "مكت": "مكت", 
            "اكت": "اكت", "شكت": "شكت", "كرر": "كرر"
        }
        await update.message.reply_text(f"القسم: {section_name_display.get(section, section)}\n\n{format_display(sentence)}")

def get_marathon_statistics_message(cid):
    marathon_state = storage.get_marathon_state(cid)
    if not marathon_state:
        return None

    creator_uid = marathon_state.get("creator_uid")
    creator_data = storage.data["users"].get(str(creator_uid), {})
    creator_name = creator_data.get("first_name", "غير معروف")
    creator_username = creator_data.get("username")

    creator_display = f"@{creator_username}" if creator_username else creator_name

    participants = marathon_state.get("participants", {})
    participant_count = len(participants)

    sections = marathon_state.get("sections", [])
    sections_display = ", ".join(sections) if sections else "لم يتم اختيار أقسام بعد"

    state = marathon_state.get("state", "waiting_participants")
    state_display = "في انتظار المشاركين" if state == "waiting_participants" else "قيد التشغيل"

    msg = "⚠️ يوجد ماراثون نشط في هذه الدردشة!\n\n"
    msg += "📊 إحصائيات الماراثون الحالي:\n"
    msg += f"━━━━━━━━━━━━━━━━\n"
    msg += f"👑 المشرف: {creator_display}\n"
    msg += f"👥 عدد المشاركين: {participant_count}\n"
    msg += f"📚 الأقسام المختارة: {sections_display}\n"
    msg += f"📍 الحالة: {state_display}\n"

    if state == "running":
        stats = storage.get_marathon_stats(cid)
        if stats:
            msg += f"\n🏆 النتائج الحالية:\n"
            sorted_stats = sorted(
                stats.items(), 
                key=lambda x: sum(x[1].get("speeds", [])) / len(x[1].get("speeds", [1])) if x[1].get("speeds") else 0,
                reverse=True
            )

            for i, (uid, user_stats) in enumerate(sorted_stats[:5], 1):
                user_data = storage.data["users"].get(uid, {})
                user_name = user_data.get("first_name", "مستخدم")
                user_username = user_data.get("username")
                user_display = f"@{user_username}" if user_username else user_name

                total = user_stats.get("total_sentences", 0)
                speeds = user_stats.get("speeds", [])
                avg_speed = sum(speeds) / len(speeds) if speeds else 0

                msg += f"{i}. {user_display}: {total} جملة - متوسط {avg_speed:.2f} WPM\n"

    msg += f"\n━━━━━━━━━━━━━━━━\n"
    msg += "💡 لا يمكن فتح ماراثون جديد حتى ينتهي الماراثون الحالي\n"
    msg += "يمكن للمشرف أو الأدمن إنهاء الماراثون بكتابة 'قف' أو 'الغاء ماراثون'"

    return msg

async def check_and_close_inactive_marathons(context: ContextTypes.DEFAULT_TYPE):
    for cid in list(storage.data["marathon_state"].keys()):
        if storage.check_marathon_inactivity(int(cid)):
            stats = storage.end_marathon(int(cid))

            try:
                msg = "⏰ تم إغلاق الماراثون تلقائياً بسبب عدم النشاط لمدة 3 دقائق\n\n"

                if stats:
                    msg += "📊 الإحصائيات النهائية:\n━━━━━━━━━━━━━━━━\n"
                    sorted_stats = sorted(
                        stats.items(), 
                        key=lambda x: sum(x[1].get("speeds", [])) / len(x[1].get("speeds", [1])) if x[1].get("speeds") else 0,
                        reverse=True
                    )

                    for i, (uid, user_stats) in enumerate(sorted_stats, 1):
                        user_data = storage.data["users"].get(uid, {})
                        user_name = user_data.get("first_name", "مستخدم")
                        user_username = user_data.get("username")
                        user_display = f"@{user_username}" if user_username else user_name

                        total = user_stats.get("total_sentences", 0)
                        speeds = user_stats.get("speeds", [])
                        avg_speed = sum(speeds) / len(speeds) if speeds else 0
                        fastest = user_stats.get("fastest_speed", 0)

                        msg += f"{i}. {user_display}:\n"
                        msg += f"   جمل مكتوبة: {total}\n"
                        msg += f"   متوسط السرعة: {avg_speed:.2f} WPM\n"
                        msg += f"   أسرع سرعة: {fastest:.2f} WPM\n\n"
                else:
                    msg += "لم يشارك أحد في الماراثون"

                await context.bot.send_message(chat_id=int(cid), text=msg)
            except Exception as e:
                print(f"Error sending marathon closure message: {e}")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "اررحب في بوت  иĸℓ !\n\n"
        "الأوامر المتاحة:\n\n"
        " أقسام البوت :\n"
        "• جمم - جمل عربية\n"
        "• ويكي - جمل من ويكيبيديا\n"
        "• مكت - مولد كلمات عربية\n"
        "• اكت - جمل عامية\n"
        "• شكت - اقتباسات\n"
        "• كرر - تكرار الكلمات\n"
        "• شرط - جمل بالشروط\n"
        "• فكك - فك الجمل\n\n"
        " المسابقات:\n"
        "• فتح جولة - فتح جولة تنافسية\n"
        "• إنهاء الجولة - إنهاء الجولة وعرض النتائج\n"
        "• جولة - عرض معلومات الجولة الحالية\n"
        "• ماراثون - ماراثون الكتابة المتواصلة\n\n"

        
        " الإحصائيات:\n"
        "• الصدارة - عرض لوحة الصدارة\n"
        "• جوائزي - عرض جوائزك\n"
        "ابدأ بكتابة اختصار القسم اللي تبيه!"
    )
    await update.message.reply_text(msg)

async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    types = ['جمم', 'ويكي', 'مكت', 'اكت', 'شكت', 'كرر', 'شرط', 'فكك']
    msg = "🏆 لوحة الصدارة\n━━━━━━━━━━━━━━━━\n\n"

    for typ in types:
        board = storage.get_leaderboard(typ)
        if board:
            msg += f"📌 {typ}:\n"
            for i, (uid, username, first_name, wpm) in enumerate(board, 1):
                display = f"@{username}" if username else first_name
                msg += f"{i}. {display}: {wpm:.2f} WPM\n"
            msg += "\n"

    if msg == "🏆 لوحة الصدارة\n━━━━━━━━━━━━━━━━\n\n":
        msg += "لا توجد نتائج بعد!"

    await update.message.reply_text(msg)

async def cmd_awards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    awards = storage.get_awards(uid)

    if not awards:
        await update.message.reply_text("لم تحصل على جوائز بعد!")
        return

    msg = "🏅 جوائزك:\n━━━━━━━━━━━━━━━━\n\n"
    for award in awards:
        msg += f"🎖 {award['name']}\n"
        msg += f"   النوع: {award['type']}\n"
        msg += f"   السرعة: {award['wpm']:.2f} WPM\n"
        msg += f"   التاريخ: {award['date'][:10]}\n\n"

    await update.message.reply_text(msg)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("هذا الأمر للمشرفين فقط")
        return

    total_users = len(storage.data["users"])
    total_chats = len(storage.data["chats"])

    msg = f"📊 إحصائيات البوت:\n━━━━━━━━━━━━━━━━\n\n"
    msg += f"👥 عدد المستخدمين: {total_users}\n"
    msg += f"💬 عدد المجموعات: {total_chats}\n\n"

    today = datetime.now().strftime("%Y-%m-%d")
    if today in storage.data["stats"]:
        msg += "📈 استخدام اليوم:\n"
        for cmd, count in storage.data["stats"][today].items():
            msg += f"   {cmd}: {count}\n"

    await update.message.reply_text(msg)

async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    uid = update.effective_user.id
    name = update.effective_user.first_name or "مستخدم"
    usr = update.effective_user.username

    storage.add_user(uid, usr, name)

    if storage.is_banned(uid):
        return

    cid = update.effective_chat.id
    if update.effective_chat.type in ['group', 'supergroup']:
        chat_title = update.effective_chat.title
        storage.add_chat(cid, chat_title)

    u = update
    c = context

    marathon_state = storage.get_marathon_state(cid)

    if text in ["قف", "إيقاف"]:
        if marathon_state:
            if storage.is_marathon_creator(uid, cid) or is_admin(uid):
                stats = storage.end_marathon(cid)

                if stats:
                    msg = "تم إنهاء الماراثون!\n\n📊 الإحصائيات النهائية:\n━━━━━━━━━━━━━━━━\n"
                    sorted_stats = sorted(
                        stats.items(), 
                        key=lambda x: sum(x[1].get("speeds", [])) / len(x[1].get("speeds", [1])) if x[1].get("speeds") else 0,
                        reverse=True
                    )

                    for i, (participant_uid, user_stats) in enumerate(sorted_stats, 1):
                        user_data = storage.data["users"].get(participant_uid, {})
                        user_name = user_data.get("first_name", "مستخدم")
                        user_username = user_data.get("username")
                        user_display = f"@{user_username}" if user_username else user_name

                        total = user_stats.get("total_sentences", 0)
                        speeds = user_stats.get("speeds", [])
                        avg_speed = sum(speeds) / len(speeds) if speeds else 0
                        fastest = user_stats.get("fastest_speed", 0)

                        msg += f"{i}. {user_display}:\n"
                        msg += f"   جمل مكتوبة: {total}\n"
                        msg += f"   متوسط السرعة: {avg_speed:.2f} WPM\n"
                        msg += f"   أسرع سرعة: {fastest:.2f} WPM\n\n"

                    await u.message.reply_text(msg)
                else:
                    await u.message.reply_text("تم إنهاء الماراثون!")
            else:
                await u.message.reply_text("فقط منشئ الماراثون أو الأدمن يمكنه إيقاف الماراثون")
        return

    if text in ["الغاء ماراثون", "إلغاء ماراثون"]:
        if is_admin(uid):
            if marathon_state:
                stats = storage.end_marathon(cid)
                msg = "✅ تم إلغاء الماراثون بواسطة الأدمن"

                if stats:
                    msg += "\n\n📊 الإحصائيات:\n━━━━━━━━━━━━━━━━\n"
                    sorted_stats = sorted(
                        stats.items(), 
                        key=lambda x: sum(x[1].get("speeds", [])) / len(x[1].get("speeds", [1])) if x[1].get("speeds") else 0,
                        reverse=True
                    )

                    for i, (participant_uid, user_stats) in enumerate(sorted_stats, 1):
                        user_data = storage.data["users"].get(participant_uid, {})
                        user_name = user_data.get("first_name", "مستخدم")
                        user_username = user_data.get("username")
                        user_display = f"@{user_username}" if user_username else user_name

                        total = user_stats.get("total_sentences", 0)
                        speeds = user_stats.get("speeds", [])
                        avg_speed = sum(speeds) / len(speeds) if speeds else 0

                        msg += f"{i}. {user_display}: {total} جملة - {avg_speed:.2f} WPM\n"

                await u.message.reply_text(msg)
            else:
                await u.message.reply_text("لا يوجد ماراثون نشط حالياً")
        else:
            await u.message.reply_text("هذا الأمر متاح للأدمنز فقط")
        return

    if text == "تغيير" and marathon_state and marathon_state["state"] == "running":
        await send_marathon_sentence(u, c, cid)
        return

    if text.startswith("ازالة ") and marathon_state:
        if storage.is_marathon_creator(uid, cid) or is_admin(uid):
            target_username = text.replace("ازالة ", "").replace("@", "").strip()
            removed = False
            for participant_uid, participant_data in list(marathon_state["participants"].items()):
                if participant_data.get("username") == target_username or participant_data.get("name") == target_username:
                    storage.remove_marathon_participant(int(participant_uid), cid)
                    await u.message.reply_text(f"تم إزالة {participant_data.get('name')} من الماراثون")
                    removed = True
                    break
            if not removed:
                await u.message.reply_text("لم يتم العثور على المشارك")
        else:
            await u.message.reply_text("فقط منشئ الماراثون أو الأدمن يمكنه إزالة المشاركين")
        return

    if marathon_state and marathon_state["state"] == "waiting_participants":
        if text == "10":
            if storage.add_marathon_participant(uid, cid, name, usr):
                marathon_state = storage.get_marathon_state(cid)
                participants_count = len(marathon_state["participants"])
                await u.message.reply_text(
                    f"تم تسجيل {name} في الماراثون\n"
                    f"عدد المشاركين: {participants_count}"
                )
            else:
                await u.message.reply_text("أنت مشارك بالفعل!")
            return

        elif text in MARATHON_SECTIONS:
            if storage.is_marathon_creator(uid, cid):
                section_name = MARATHON_SECTIONS[text]
                if storage.add_marathon_section(cid, section_name):
                    marathon_state = storage.get_marathon_state(cid)
                    sections_list = ", ".join(marathon_state["sections"])
                    await u.message.reply_text(
                        f"تم إضافة قسم {section_name}\n\n"
                        f"الأقسام المختارة: {sections_list}\n\n"
                        f"هل من مزيد؟ اذا انتهيت اكتب 'بدء الماراثون'"
                    )
                else:
                    await u.message.reply_text("هذا القسم مضاف بالفعل!")
            else:
                await u.message.reply_text("فقط منشئ الماراثون يمكنه اختيار الأقسام")
            return

        elif text in ["بدء الماراثون", "بدء ماراثون"]:
            if storage.is_marathon_creator(uid, cid):
                sections = marathon_state["sections"]
                participants = marathon_state["participants"]

                if not sections:
                    await u.message.reply_text("يجب اختيار قسم واحد على الأقل!")
                    return

                if not participants:
                    await u.message.reply_text("لا يوجد مشاركين بعد!")
                    return

                await u.message.reply_text("3")
                await asyncio.sleep(1)
                await u.message.reply_text("2")
                await asyncio.sleep(1)
                await u.message.reply_text("1")
                await asyncio.sleep(1)

                storage.start_marathon_running(cid)
                await send_marathon_sentence(u, c, cid)
            else:
                await u.message.reply_text("فقط منشئ الماراثون يمكنه بدء الماراثون")
            return

    if marathon_state and marathon_state["state"] == "running":
        current_sentence = marathon_state.get("current_sentence")
        sentence_start_time = marathon_state.get("sentence_start_time")
        sentence_type = marathon_state.get("sentence_type")

        if str(uid) not in marathon_state["participants"]:
            return

        matched = False
        if current_sentence and sentence_start_time:
            if sentence_type == "كرر":
                valid, err = validate_repeat(current_sentence, text)
                if valid:
                    matched = True
            elif sentence_type == "شرط":
                orig_s, cond = current_sentence.split('||')
                valid, exp = validate_condition(cond, orig_s, text)
                if valid:
                    matched = True
            elif sentence_type == "فكك":
                if is_correct_disassembly(current_sentence, text):
                    matched = True
            else:
                if match_text(current_sentence, text):
                    matched = True

        if matched:
            if str(uid) in marathon_state.get("answered_by", []):
                return

            elapsed = time.time() - sentence_start_time
            wpm = (len(text.split()) / elapsed) * 60

            storage.add_marathon_speed(uid, cid, wpm, current_sentence)
            storage.mark_participant_answered(uid, cid)
            storage.update_participant_activity(uid, cid)

            mention = f"@{usr}" if usr else name
            await u.message.reply_text(f"ممتاز {mention}! سرعتك: {wpm:.2f} WPM")

            await send_marathon_sentence(u, c, cid)
            return

    if storage.get_broadcast_mode(uid):
        if text in ["إلغاء", "الغاء"]:
            storage.set_broadcast_mode(uid, False)
            await u.message.reply_text("تم إلغاء وضع الإذاعة")
            return

        success, failed = await broadcast_message(c, text)
        storage.set_broadcast_mode(uid, False)
        await u.message.reply_text(
            f"تم إرسال الإذاعة بنجاح\n"
            f"نجح: {success} مستخدم/مجموعة\n"
            f"فشل: {failed} مستخدم/مجموعة"
        )
        return

    if is_admin(uid) and text in ['اذاعة', 'إذاعة', 'اذاعه', 'إذاعه']:
        storage.set_broadcast_mode(uid, True)
        await u.message.reply_text(
            "وضع الإذاعة مفعل\n\n"
            "أرسل الرسالة التي تريد إذاعتها إلى جميع المستخدمين والمجموعات\n"
            "أو اكتب 'إلغاء' للإلغاء"
        )
        return

    if storage.get_round_mode(cid):
        target_num = arabic_to_num(text)
        if target_num and target_num > 0 and target_num <= 100:
            storage.start_round(cid, target_num)
            storage.set_round_mode(cid, False)
            await u.message.reply_text(f"تم فتح جولة جديدة\nالهدف: {target_num} فوز\nابدأوا اللعب الآن!")
            return
        else:
            await u.message.reply_text("الرجاء إدخال رقم صحيح من 1 إلى 100")
            return

    if text in ['فتح جولة', 'فتح جوله']:
        storage.set_round_mode(cid, True)
        await u.message.reply_text("كم عدد الانتصارات المطلوبة للفوز في الجولة؟\nأدخل الرقم (مثال: 5 أو خمسة)")
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

    if text in ['جولة', 'الجولة', 'الجوله']:
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

    if text in ['ماراثون', 'مارثون']:
        storage.log_cmd('ماراثون')

        existing_marathon = storage.get_marathon_state(cid)
        if existing_marathon:
            stats_msg = get_marathon_statistics_message(cid)
            if stats_msg:
                await u.message.reply_text(stats_msg)
            return

        storage.start_marathon_selection(uid, cid)

        msg = "ماراثون الكتابة\n\n"
        msg += "📌 اللي بيشارك في الماراثون يرسل رقم 10\n\n"
        msg += "⚠️ الماراثون اذا كان مكون من شخص واحد، اي رسالة منه البوت يعتبرها إجابة على الجملة\n\n"
        msg += "🔄 اذا كان الماراثون مكون من أكثر من شخص، أول شخص يجيب بشكل صحيح راح تتجدد الجملة (تنافسي)\n\n"
        msg += "👑 صلاحيات منشئ الماراثون:\n"
        msg += "   - يمكنك ازالة اي شخص لم يعد يكتب في الماراثون\n"
        msg += "   - استخدم: ازالة @اسم_المستخدم\n\n"
        msg += "🔒 صلاحيات الأدمن:\n"
        msg += "   - يمكن للأدمن إلغاء أي ماراثون بكتابة 'الغاء ماراثون'\n"
        msg += "   - يمكن للأدمن إزالة أي مشارك من الماراثون\n\n"
        msg += "⏰ ملاحظة: سيتم إغلاق الماراثون تلقائياً بعد 3 دقائق من عدم النشاط\n\n"
        msg += "━━━━━━━━━━━━━━━━\n\n"
        msg += "الشرح:\n"
        msg += "ماراثون الكتابة هو تحدي مستمر حيث تكتب جمل متتالية من الأقسام التي تختارها.\n\n"
        msg += "كيف يعمل:\n"
        msg += "1- اختر رقم قسم أو أكثر (أنت فقط كمنشئ يمكنك اختيار الأقسام)\n"
        msg += "2- اكتب 'بدء الماراثون' لبدء الماراثون\n"
        msg += "3- سيتم العد التنازلي 3، 2، 1\n"
        msg += "4- اكتب الجمل بسرعة وبشكل صحيح\n"
        msg += "5- سيتم إرسال جملة جديدة تلقائياً\n"
        msg += "6- اكتب 'تغيير' للحصول على جملة أخرى\n"
        msg += "7- اكتب 'قف' لإنهاء الماراثون ورؤية إحصائياتك\n\n"
        msg += "اختر الأقسام:\n"
        msg += "1 - جمم (جمل عربية)\n"
        msg += "2 - ويكي (جمل من ويكيبيديا)\n"
        msg += "3 - شرط (جمل بالشروط)\n"
        msg += "4 - فكك (فك وتركيب)\n"
        msg += "5 - مكت (مولد الكلمات العربية)\n"
        msg += "6 - شكت (اقتباسات)\n"
        msg += "7 - اكت (جمل عامية)\n"
        msg += "8 - كرر (تكرار الكلمات)\n\n"
        msg += "اكتب رقم القسم الذي تريده:"

        await u.message.reply_text(msg)
        return

    if text in ['جمم', 'ويكي', 'مكت', 'اكت', 'شكت', 'كرر', 'شرط', 'فكك', 'الصدارة', 'جوائزي', 'عرض', 'مقالات', 'احصاء']:
        storage.log_cmd(text)

    if text == 'جمم':
        storage.cancel_user_session_in_type(uid, cid, 'جمم')
        t = managers["جمم"].get()
        storage.save_session(uid, cid, 'جمم', t, time.time())
        await u.message.reply_text(format_display(t))
    elif text == 'ويكي':
        lock_key = f"{uid}_{cid}_wiki"

        if lock_key not in processing_locks:
            processing_locks[lock_key] = asyncio.Lock()

        if processing_locks[lock_key].locked():
            return

        async with processing_locks[lock_key]:
            now = time.time()
            last_time = last_wiki_request.get(lock_key, 0)

            if now - last_time < 3:
                await asyncio.sleep(3 - (now - last_time))

            storage.cancel_user_session_in_type(uid, cid, 'ويكي')

            t = managers["ويكي"].fetch()

            storage.save_session(uid, cid, 'ويكي', t, time.time())

            last_wiki_request[lock_key] = time.time()

            await u.message.reply_text(format_display(t))
    elif text == 'مكت':
        storage.cancel_user_session_in_type(uid, cid, 'مكت')
        t = managers["مكت"].get()
        storage.save_session(uid, cid, 'مكت', t, time.time())
        await u.message.reply_text(format_display(t))
    elif text == 'اكت':
        storage.cancel_user_session_in_type(uid, cid, 'اكت')
        t = managers["اكت"].get()
        storage.save_session(uid, cid, 'اكت', t, time.time())
        await u.message.reply_text(format_display(t))
    elif text == 'شكت':
        storage.cancel_user_session_in_type(uid, cid, 'شكت')
        t = managers["شكت"].get()
        storage.save_session(uid, cid, 'شكت', t, time.time())
        await u.message.reply_text(format_display(t))
    elif text == 'كرر':
        storage.cancel_user_session_in_type(uid, cid, 'كرر')
        p = gen_pattern(uid)
        storage.save_session(uid, cid, 'كرر', p, time.time())
        await u.message.reply_text(p)
    elif text == 'شرط':
        storage.cancel_user_session_in_type(uid, cid, 'شرط')
        s = managers["شرط"].get()
        cond = random.choice(CONDITIONS)
        storage.save_session(uid, cid, 'شرط', f"{s}||{cond}", time.time())
        await u.message.reply_text(f"{cond}\n\n{format_display(s)}")
    elif text == 'فكك':
        storage.cancel_user_session_in_type(uid, cid, 'فكك_تفكيك')
        s = managers["فكك"].get()
        storage.save_session(uid, cid, 'فكك_تفكيك', s, time.time())
        msg = f"فكك الجملة التالية (افصل بين حروف كل كلمة):\n\n{format_display(s)}"
        await u.message.reply_text(msg)
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

            if typ in ['جمم', 'ويكي', 'مكت', 'اكت', 'شكت'] and match_text(orig, text):
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
                wpm = (len(text.split()) / elapsed) * 60
                score_typ = 'فكك' if typ == 'فكك_تفكيك' else typ
                storage.update_score(uid, score_typ, wpm)

                round_data = storage.get_round(cid)
                if round_data:
                    wins = storage.add_win(cid, uid)
                    target = round_data['target']
                    mention = f"@{usr}" if usr else name
                    if wins >= target:
                        storage.end_round(cid)
                        await u.message.reply_text(
                            f"مبروك {mention}!\n\n"
                            f"فزت في الجولة بعد {wins} فوز\n"
                            f"سرعتك: {wpm:.2f} WPM"
                        )
                    else:
                        await u.message.reply_text(
                            f"ممتاز! سرعتك {wpm:.2f} WPM\n\n"
                            f"فوزك رقم {wins} من {target}"
                        )
                else:
                    await u.message.reply_text(f"ممتاز! سرعتك {wpm:.2f} WPM")
                storage.del_session(cid, typ)
                break

async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    storage.cleanup()
    await check_and_close_inactive_marathons(context)

def main():
    if not BOT_TOKEN:
        print("خطأ: لم يتم العثور على BOT_TOKEN في متغيرات البيئة!")
        print("الرجاء إضافة متغير البيئة BOT_TOKEN من خلال Secrets في Replit")
        return

    print("جاري تشغيل البوت...")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

    app.job_queue.run_repeating(periodic_cleanup, interval=60, first=10)

    print("البوت يعمل بنجاح!")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
