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

BOT_TOKEN = "" 
ADMIN_IDS = {5562144078}

processing_locks = {}
processed_messages = set()
processed_message_ids = {}
last_wiki_request = {}
wiki_sentence_hashes = {}

URLS = {
    "جمم": "https://raw.githubusercontent.com/AL3ATEL/TXT-bot-telegram-/refs/heads/main/sentences.txt",
    "شرط": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-2/refs/heads/main/conditions.txt",
    "فكك": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-3/refs/heads/main/FKK.txt",
    "صج": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-4/refs/heads/main/arabic_sentences.json",
    "جش": "https://raw.githubusercontent.com/BoulahiaAhmed/Arabic-Quotes-Dataset/main/Arabic_Quotes.csv",
    "شك": "https://raw.githubusercontent.com/AL3ATEL/txt-telegram-5/refs/heads/main/3amh.txt"
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
    "5": "صج",
    "6": "جش",
    "7": "شك",
    "8": "كرر"
}

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
                "marathon_stats": {},
                "wiki_history": {}
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
        uid_str = str(uid)
        if uid_str in self.data["banned"]:
            self.data["banned"].remove(uid_str)
            self.save()
            return True
        return False

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

    def save_session(self, uid, cid, typ, txt, tm, thread_id=None):
        key = f"{cid}_{typ}"
        self.data["sessions"][key] = {
            "type": typ, 
            "text": txt, 
            "time": tm,
            "starter_uid": uid,
            "thread_id": thread_id
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

    def start_marathon_selection(self, uid, cid, thread_id=None):
        key = f"{cid}"
        self.data["marathon_state"][key] = {
            "state": "waiting_participants",
            "creator_uid": uid,
            "participants": {},
            "sections": [],
            "started_at": datetime.now().isoformat(),
            "thread_id": thread_id
        }
        self.save()

    def add_marathon_participant(self, uid, cid, name, username):
        key = f"{cid}"
        if key in self.data["marathon_state"]:
            self.data["marathon_state"][key]["participants"][str(uid)] = {
                "name": name,
                "username": username,
                "joined_at": datetime.now().isoformat()
            }
            self.save()
            return True
        return False

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

            if key not in self.data["marathon_stats"]:
                self.data["marathon_stats"][key] = {}

            for participant_uid in self.data["marathon_state"][key]["participants"].keys():
                if participant_uid not in self.data["marathon_stats"][key]:
                    self.data["marathon_stats"][key][participant_uid] = {
                        "total_sentences": 0,
                        "speeds": [],
                        "start_time": time.time(),
                        "fastest_speed": 0,
                        "fastest_sentence": "",
                        "wins": 0
                    }

            self.save()
            return True
        return False

    def get_marathon_state(self, cid):
        key = f"{cid}"
        return self.data["marathon_state"].get(key)

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
                "fastest_sentence": "",
                "wins": 0
            }

        self.data["marathon_stats"][key][str(uid)]["speeds"].append(wpm)
        self.data["marathon_stats"][key][str(uid)]["total_sentences"] += 1
        self.data["marathon_stats"][key][str(uid)]["wins"] += 1

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

    def add_wiki_sentence(self, user_cid_key, sentence_hash):
        if "wiki_history" not in self.data:
            self.data["wiki_history"] = {}

        if user_cid_key not in self.data["wiki_history"]:
            self.data["wiki_history"][user_cid_key] = []

        self.data["wiki_history"][user_cid_key].append({
            "hash": sentence_hash,
            "timestamp": time.time()
        })

        if len(self.data["wiki_history"][user_cid_key]) > 100:
            self.data["wiki_history"][user_cid_key] = self.data["wiki_history"][user_cid_key][-100:]

        self.save()

    def is_wiki_sentence_used(self, user_cid_key, sentence_hash):
        if "wiki_history" not in self.data:
            return False

        history = self.data["wiki_history"].get(user_cid_key, [])

        current_time = time.time()
        recent_hashes = [
            item["hash"] for item in history 
            if current_time - item.get("timestamp", 0) < 3600
        ]

        return sentence_hash in recent_hashes

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

    def fetch(self, user_cid_key=None):
        if time.time() - self.last_fetch < 2:
            time.sleep(2 - (time.time() - self.last_fetch))

        if len(self.used) >= self.max_used_size:
            self.used.clear()

        max_attempts = 50
        attempt = 0

        while attempt < max_attempts:
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
                        if 8 <= len(s.split()) <= 21:
                            sentence_hash = hash(normalize(s))

                            if s not in self.used:
                                if user_cid_key:
                                    if not storage.is_wiki_sentence_used(user_cid_key, sentence_hash):
                                        self.used.add(s)
                                        storage.add_wiki_sentence(user_cid_key, sentence_hash)
                                        self.last_fetch = time.time()
                                        return s
                                else:
                                    self.used.add(s)
                                    self.last_fetch = time.time()
                                    return s
            except Exception as e:
                print(f"Error fetching from Wikipedia: {e}")

            attempt += 1
            time.sleep(0.5)

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
    txt = re.sub(r'[^\u0600-\u06FF\s]', '', txt)
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
    return ' ، '.join(s.split())

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

        key = "|".join(key_parts)
        if not storage.is_pattern_used(uid, key):
            storage.add_pattern(uid, key)
            return " ".join(pattern)

    storage.clear_patterns(uid)
    return gen_pattern(uid)

managers = {
    "جمم": RemoteManager(URLS["جمم"], 5, 25),
    "ويكي": WikiManager("https://ar.wikipedia.org/w/api.php", 0),
    "شرط": RemoteManager(URLS["شرط"], 5, 20),
    "فكك": RemoteManager(URLS["فكك"], 3, 10),
    "صج": RemoteManager(URLS["صج"], 5, 25),
    "شك": RemoteManager(URLS["شك"], 5, 20),
    "جش": CSVQuotesManager(URLS["جش"], 3, 30)
}

def is_admin(uid):
    return uid in ADMIN_IDS

async def send_marathon_sentence(u: Update, context: ContextTypes.DEFAULT_TYPE, cid):
    marathon_state = storage.get_marathon_state(cid)

    if not marathon_state or marathon_state["state"] != "running":
        return

    sections = marathon_state["sections"]
    if not sections:
        return

    section = random.choice(sections)
    sentence = None
    sentence_type = section
    thread_id = marathon_state.get("thread_id")

    if section == "ويكي":
        user_cid_key = f"{list(marathon_state['participants'].keys())[0]}_{cid}"
        sentence = managers["ويكي"].fetch(user_cid_key)
    elif section == "كرر":
        sentence = gen_pattern(list(marathon_state["participants"].keys())[0])
    elif section == "شرط":
        sent = managers["شرط"].get()
        cond = random.choice(CONDITIONS)

        await context.bot.send_message(
            chat_id=cid, 
            text=cond,
            message_thread_id=thread_id
        )
        await asyncio.sleep(2)

        sentence = sent
        storage.set_marathon_sentence(cid, f"{sent}||{cond}", sentence_type)
        await context.bot.send_message(
            chat_id=cid, 
            text=format_display(sent),
            message_thread_id=thread_id
        )
        return
    else:
        sentence = managers.get(section, managers["جمم"]).get()

    storage.set_marathon_sentence(cid, sentence, sentence_type)

    if section == "كرر":
        await context.bot.send_message(
            chat_id=cid,
            text=sentence,
            message_thread_id=thread_id
        )
    else:
        await context.bot.send_message(
            chat_id=cid,
            text=format_display(sentence),
            message_thread_id=thread_id
        )

async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.message.from_user.id
    usr = u.message.from_user.username
    name = u.message.from_user.first_name

    if storage.is_banned(uid):
        await u.message.reply_text("تم حظرك")
        return

    storage.add_user(uid, usr, name)
    await u.message.reply_text(
        "ارررحب في иĸℓ بوت للكتابة\n\n"
        "الأوامر المتاحة:\n\n"
        "- جمم: جمل عربية\n"
        "- ويكي: جمل من ويكيبيديا\n"
        "- صج: مولد الكلمات العربية\n"
        "- شك: (دمج أكثر من بوت)\n"
        "- جش: اقتباسات عربية\n"
        "- كرر: تكرار الكلمات\n"
        "- شرط: جمل بالشروط\n"
        "- فكك: فك وتركيب الجمل\n\n"
        "أوامر المسابقات:\n"
        "- ماراثون: ماراثون الكتابة المتواصلة\n"
        "- فتح جولة\n\n"
        "أوامر الاحصائيات:\n"
        "- جوائزي: عرض جوائزك\n"
        "- الصدارة: عرض المتصدرين\n\n"
        "- عرض/مقالات: عرض التعليمات"
    )

async def cmd_leaderboard(u: Update, c: ContextTypes.DEFAULT_TYPE):
    storage.cleanup()
    msg = "قائمة أسرع المستخدمين\n\n"

    game_types = [
        ('جمم', 'الجمل العربية'),
        ('ويكي', 'ويكيبيديا'),
        ('صج', 'مولد الكلمات العربية'),
        ('شك', 'الجمل العامية'),
        ('جش', 'الاقتباسات'),
        ('كرر', 'التكرار'),
        ('شرط', 'الشروط'),
        ('فكك', 'الفك والتركيب')
    ]

    for typ, name in game_types:
        lb = storage.get_leaderboard(typ)
        if lb:
            msg += f"{name}:\n"
            for i, (uid, usr, fname, wpm) in enumerate(lb, 1):
                storage.add_award(uid, f"المركز {i} في {name}", wpm, typ)
                mention = f"@{usr}" if usr else f"{fname} (لا يوجد يوزر)"
                msg += f"{i}. {mention}: {wpm:.1f} WPM\n"
            msg += "\n"

    if msg == "قائمة أسرع المستخدمين\n\n":
        await u.message.reply_text("لا توجد نتائج بعد!")
    else:
        await u.message.reply_text(msg)

async def cmd_awards(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.message.from_user.id
    awards = storage.get_awards(uid)

    if not awards:
        await u.message.reply_text(f"{u.message.from_user.first_name} ليس لديك جوائز أسبوعية حتى الآن")
        return

    msg = f"جوائز {u.message.from_user.first_name} الأسبوعية:\n\n"
    for a in awards:
        msg += f"- {a['name']}\nالسرعة: {a['wpm']:.1f} WPM\n\n"

    await u.message.reply_text(msg)

async def cmd_stats(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.message.from_user.id not in ADMIN_IDS:
        await u.message.reply_text("هذا الأمر للمشرفين فقط!")
        return

    stats = {}
    for i in range(7):
        dt = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        if dt in storage.data["stats"]:
            stats[dt] = storage.data["stats"][dt]

    if not stats:
        await u.message.reply_text("لا توجد إحصائيات!")
        return

    msg = "إحصائيات استخدام الأوامر:\n\n"
    for dt in sorted(stats.keys(), reverse=True):
        if dt == datetime.now().strftime("%Y-%m-%d"):
            dt_name = "اليوم"
        elif dt == (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"):
            dt_name = "أمس"
        else:
            dt_name = dt

        msg += f"{dt_name}:\n"
        for cmd, cnt in stats[dt].items():
            msg += f"  {cmd}: {cnt}\n"
        msg += f"  المجموع: {sum(stats[dt].values())}\n\n"

    await u.message.reply_text(msg)

async def broadcast_message(context: ContextTypes.DEFAULT_TYPE, message: str):
    users = storage.data["users"]
    chats = storage.data.get("chats", {})
    success = 0
    failed = 0

    for user_id in users.keys():
        try:
            await context.bot.send_message(chat_id=int(user_id), text=message)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            print(f"Failed to send to user {user_id}: {e}")
            failed += 1

    for chat_id in chats.keys():
        try:
            await context.bot.send_message(chat_id=int(chat_id), text=message)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            print(f"Failed to send to chat {chat_id}: {e}")
            failed += 1

    return success, failed

def arabic_to_num(text):
    ar_nums = {
        'صفر': 0, 'واحد': 1, 'اثنان': 2, 'اثنين': 2, 'ثلاثة': 3, 
        'أربعة': 4, 'خمسة': 5, 'ستة': 6, 'سبعة': 7, 'ثمانية': 8, 
        'تسعة': 9, 'عشرة': 10, 'عشر': 10
    }

    text_lower = text.strip().lower()
    if text_lower in ar_nums:
        return ar_nums[text_lower]

    try:
        return int(text)
    except:
        return None

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.message.from_user.id
    cid = u.message.chat.id
    name = u.message.from_user.first_name
    text = u.message.text.strip()
    usr = u.message.from_user.username
    msg_id = u.message.message_id
    thread_id = u.message.message_thread_id if u.message.is_topic_message else None

    msg_key = f"{cid}_{msg_id}"
    if msg_key in processed_messages:
        return
    processed_messages.add(msg_key)

    if len(processed_messages) > 1000:
        processed_messages.clear()

    if cid < 0:
        chat_title = u.message.chat.title if u.message.chat.title else "مجموعة"
        storage.add_chat(cid, chat_title)
    else:
        storage.add_user(uid, usr, name)

    if u.message.reply_to_message and is_admin(uid) and text == 'حظر':
        target_user = u.message.reply_to_message.from_user
        storage.ban_user(target_user.id)
        await u.message.reply_text(f"تم حظر {target_user.first_name} (ID: {target_user.id})")
        return

    if u.message.reply_to_message and is_admin(uid) and text in ['فك حظر', 'فك الحظر']:
        target_user = u.message.reply_to_message.from_user
        if storage.unban_user(target_user.id):
            await u.message.reply_text(f"تم فك حظر {target_user.first_name} (ID: {target_user.id})")
        else:
            await u.message.reply_text(f"{target_user.first_name} غير محظور")
        return

    if is_admin(uid) and text.startswith('حظر '):
        try:
            target_id = int(text.split()[1])
            storage.ban_user(target_id)
            await u.message.reply_text(f"تم حظر المستخدم (ID: {target_id})")
            return
        except (ValueError, IndexError):
            await u.message.reply_text("استخدم: حظر [user_id]")
            return

    if is_admin(uid) and text.startswith('فك حظر '):
        try:
            parts = text.split()
            if len(parts) != 3:
                await u.message.reply_text("استخدم: فك حظر [user_id]")
                return
            target_id = int(parts[2])
            if storage.unban_user(target_id):
                await u.message.reply_text(f"تم فك حظر المستخدم (ID: {target_id})")
            else:
                await u.message.reply_text(f"المستخدم (ID: {target_id}) غير محظور")
            return
        except ValueError:
            await u.message.reply_text("استخدم: فك حظر [user_id]")
            return

    if storage.is_banned(uid):
        return

    marathon_state = storage.get_marathon_state(cid)

    if text in ['النشرة', 'نشرة'] and marathon_state and marathon_state["state"] == "running":
        stats_data = storage.get_marathon_stats(cid)

        if stats_data:
            msg = "📊 نشرة الماراثون\n\n"

            stats_list = []
            for participant_uid, stats in stats_data.items():
                user_data = storage.data["users"].get(str(participant_uid), {})
                user_name = user_data.get("first_name", "مستخدم")
                user_username = user_data.get("username")
                mention = f"@{user_username}" if user_username else user_name

                stats_list.append({
                    "mention": mention,
                    "wins": stats.get("wins", 0),
                    "fastest": stats.get("fastest_speed", 0)
                })

            stats_list.sort(key=lambda x: x["wins"], reverse=True)

            msg += "عدد الفوزات:\n"
            for item in stats_list:
                msg += f"  {item['mention']}: {item['wins']} فوز\n"

            msg += "\n"

            stats_list.sort(key=lambda x: x["fastest"], reverse=True)

            msg += "أعلى سرعة:\n"
            for item in stats_list:
                msg += f"  {item['mention']}: {item['fastest']:.2f} WPM\n"

            await u.message.reply_text(msg)
        else:
            await u.message.reply_text("لا توجد إحصائيات بعد")
        return

    if text == "قف" and marathon_state and marathon_state["state"] == "running":
        stats_data = storage.end_marathon(cid)

        if stats_data:
            msg = "انتهى الماراثون\n\n"
            msg += "الإحصائيات:\n\n"

            for participant_uid, stats in stats_data.items():
                if stats["total_sentences"] > 0:
                    total_time = time.time() - stats["start_time"]
                    avg_speed = sum(stats["speeds"]) / len(stats["speeds"])

                    user_data = storage.data["users"].get(str(participant_uid), {})
                    user_name = user_data.get("first_name", "مستخدم")
                    user_username = user_data.get("username")
                    mention = f"@{user_username}" if user_username else user_name

                    msg += f"🏆 {mention}:\n"
                    msg += f"  - عدد الجمل: {stats['total_sentences']}\n"
                    msg += f"  - متوسط السرعة: {avg_speed:.2f} WPM\n"
                    msg += f"  - أسرع سرعة: {stats['fastest_speed']:.2f} WPM\n\n"

            await u.message.reply_text(msg)
        else:
            await u.message.reply_text("تم إيقاف الماراثون")
        return

    if text == "تغيير" and marathon_state and marathon_state["state"] == "running":
        await send_marathon_sentence(u, c, cid)
        return

    if text.startswith("ازالة ") and marathon_state:
        if storage.is_marathon_creator(uid, cid):
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
            await u.message.reply_text("فقط منشئ الماراثون يمكنه إزالة المشاركين")
        return

    if marathon_state and marathon_state["state"] == "waiting_participants":
        if text == "10":
            if storage.add_marathon_participant(uid, cid, name, usr):
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
                    sections_list = ", ".join(marathon_state["sections"])
                    await u.message.reply_text(
                        f"تم إضافة قسم {section_name}\n\n"
                        f"الأقسام المختارة: {sections_list}\n\n"
                        f"هل من مزيد؟ اذا انتهيت اكتب 'بدء الماراثون'"
                    )
                else:
                    await u.message.reply_text("هذا القسم مضاف بالفعل!")
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

                thread_id_to_use = marathon_state.get("thread_id")

                await c.bot.send_message(
                    chat_id=cid,
                    text="3",
                    message_thread_id=thread_id_to_use
                )
                await asyncio.sleep(1)
                await c.bot.send_message(
                    chat_id=cid,
                    text="2",
                    message_thread_id=thread_id_to_use
                )
                await asyncio.sleep(1)
                await c.bot.send_message(
                    chat_id=cid,
                    text="1",
                    message_thread_id=thread_id_to_use
                )
                await asyncio.sleep(1)

                storage.start_marathon_running(cid)
                await send_marathon_sentence(u, c, cid)
            return

    if marathon_state and marathon_state["state"] == "running":
        current_sentence = marathon_state.get("current_sentence")
        sentence_start_time = marathon_state.get("sentence_start_time")
        sentence_type = marathon_state.get("sentence_type")

        if str(uid) not in marathon_state["participants"]:
            return

        num_participants = len(marathon_state["participants"])

        matched = False
        if current_sentence and sentence_start_time:
            if sentence_type == "كرر":
                valid, err = validate_repeat(current_sentence, text)
                if valid:
                    matched = True
                elif num_participants == 1:
                    matched = False
            elif sentence_type == "شرط":
                orig_s, cond = current_sentence.split('||')
                valid, exp = validate_condition(cond, orig_s, text)
                if valid:
                    matched = True
                elif num_participants == 1:
                    matched = False
            else:
                if match_text(current_sentence, text):
                    matched = True
                elif num_participants == 1:
                    matched = False

        if matched:
            if str(uid) in marathon_state.get("answered_by", []):
                return

            elapsed = time.time() - sentence_start_time
            wpm = (len(text.split()) / elapsed) * 60

            storage.add_marathon_speed(uid, cid, wpm, current_sentence)
            storage.mark_participant_answered(uid, cid)

            mention = f"@{usr}" if usr else name
            await u.message.reply_text(f"ممتاز {mention}! سرعتك: {wpm:.2f} WPM")

            await send_marathon_sentence(u, c, cid)
            return
        elif num_participants == 1 and current_sentence and sentence_start_time:
            thread_id_to_use = marathon_state.get("thread_id")
            await c.bot.send_message(
                chat_id=cid,
                text="الجواب خاطئ",
                message_thread_id=thread_id_to_use
            )
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
        storage.start_marathon_selection(uid, cid, thread_id)

        msg = "ماراثون الكتابة\n\n"
        msg += "📌 اللي بيشارك في الماراثون يرسل رقم 10\n\n"
        msg += "⚠️ الماراثون اذا كان مكون من شخص واحد، اي رسالة منه البوت يعتبرها إجابة على الجملة\n\n"
        msg += "🔄 اذا كان الماراثون مكون من أكثر من شخص، أول شخص يجيب بشكل صحيح راح تتجدد الجملة (تنافسي)\n\n"
        msg += "👑 صلاحيات منشئ الماراثون:\n"
        msg += "   - يمكنك ازالة اي شخص لم يعد يكتب في الماراثون\n"
        msg += "   - استخدم: ازالة @اسم_المستخدم\n\n"
        msg += "📊 أثناء الماراثون:\n"
        msg += "   - اكتب 'النشرة' لعرض إحصائيات الماراثون الحالية\n"
        msg += "   - سيظهر عدد الفوزات وأعلى سرعة لكل مشارك\n\n"
        msg += "━━━━━━━━━━━━━━━━\n\n"
        msg += "الشرح:\n"
        msg += "ماراثون الكتابة هو تحدي مستمر حيث تكتب جمل متتالية من الأقسام التي تختارها.\n\n"
        msg += "كيف يعمل:\n"
        msg += "1- اختر رقم قسم أو أكثر\n"
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
        msg += "5 - صج (مولد الكلمات العربية)\n"
        msg += "6 - جش (اقتباسات)\n"
        msg += "7 - شك (جمل عامية)\n"
        msg += "8 - كرر (تكرار الكلمات)\n\n"
        msg += "اكتب رقم القسم الذي تريده:"

        await u.message.reply_text(msg)
        return

    if text in ['جمم', 'ويكي', 'صج', 'شك', 'جش', 'كرر', 'شرط', 'فكك', 'الصدارة', 'جوائزي', 'عرض', 'مقالات', 'احصاء']:
        storage.log_cmd(text)

    if text == 'جمم':
        storage.cancel_user_session_in_type(uid, cid, 'جمم')
        t = managers["جمم"].get()
        storage.save_session(uid, cid, 'جمم', t, time.time(), thread_id)
        await u.message.reply_text(format_display(t))
    elif text == 'ويكي':
        wiki_user_lock = f"{uid}_{cid}_wiki"

        if wiki_user_lock not in processing_locks:
            processing_locks[wiki_user_lock] = asyncio.Lock()

        if processing_locks[wiki_user_lock].locked():
            return

        async with processing_locks[wiki_user_lock]:
            now = time.time()
            last_time = last_wiki_request.get(wiki_user_lock, 0)

            if now - last_time < 2:
                await asyncio.sleep(2 - (now - last_time))

            storage.cancel_user_session_in_type(uid, cid, 'ويكي')

            user_cid_key = f"{uid}_{cid}"
            t = managers["ويكي"].fetch(user_cid_key)

            if not t or t == "جرب مرة أخرى":
                await u.message.reply_text(t if t else "جرب مرة أخرى")
                last_wiki_request[wiki_user_lock] = time.time()
                return

            storage.save_session(uid, cid, 'ويكي', t, time.time(), thread_id)

            last_wiki_request[wiki_user_lock] = time.time()

            await u.message.reply_text(format_display(t))
    elif text == 'صج':
        storage.cancel_user_session_in_type(uid, cid, 'صج')
        t = managers["صج"].get()
        storage.save_session(uid, cid, 'صج', t, time.time(), thread_id)
        await u.message.reply_text(format_display(t))
    elif text == 'شك':
        storage.cancel_user_session_in_type(uid, cid, 'شك')
        t = managers["شك"].get()
        storage.save_session(uid, cid, 'شك', t, time.time(), thread_id)
        await u.message.reply_text(format_display(t))
    elif text == 'جش':
        storage.cancel_user_session_in_type(uid, cid, 'جش')
        t = managers["جش"].get()
        storage.save_session(uid, cid, 'جش', t, time.time(), thread_id)
        await u.message.reply_text(format_display(t))
    elif text == 'كرر':
        storage.cancel_user_session_in_type(uid, cid, 'كرر')
        p = gen_pattern(uid)
        storage.save_session(uid, cid, 'كرر', p, time.time(), thread_id)
        await u.message.reply_text(p)
    elif text == 'شرط':
        storage.cancel_user_session_in_type(uid, cid, 'شرط')
        s = managers["شرط"].get()
        cond = random.choice(CONDITIONS)
        storage.save_session(uid, cid, 'شرط', f"{s}||{cond}", time.time(), thread_id)

        await u.message.reply_text(cond)
        await asyncio.sleep(2)
        await u.message.reply_text(format_display(s))
    elif text == 'فكك':
        storage.cancel_user_session_in_type(uid, cid, 'فكك_تفكيك')
        s = managers["فكك"].get()
        storage.save_session(uid, cid, 'فكك_تفكيك', s, time.time(), thread_id)
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
                wpm = (len(text.split()) / elapsed) * 60
                score_typ = 'فكك' if typ == 'فكك_تفكيك' else typ
                storage.update_score(uid, score_typ, wpm)

                round_data = storage.get_round(cid)
                if round_data:
                    wins = storage.add_win(cid, uid)
                    target = round_data['target']
                    wins_list = round_data.get('wins', {})
                    mention = f"@{usr}" if usr else name

                    round_stats = "\n\n📊 إحصائيات الجولة:\n"
                    sorted_wins = sorted(wins_list.items(), key=lambda x: x[1], reverse=True)
                    for i, (user_id, user_wins) in enumerate(sorted_wins, 1):
                        user_data = storage.data["users"].get(str(user_id), {})
                        user_name = user_data.get("first_name", "مستخدم")
                        user_username = user_data.get("username")
                        user_mention = f"@{user_username}" if user_username else user_name
                        round_stats += f"{i}. {user_mention}: {user_wins}/{target}\n"

                    if wins >= target:
                        storage.end_round(cid)
                        await u.message.reply_text(
                            f"مبروك {mention}!\n\n"
                            f"فزت في الجولة بعد {wins} فوز\n"
                            f"سرعتك: {wpm:.2f} WPM{round_stats}"
                        )
                    else:
                        await u.message.reply_text(
                            f"ممتاز! سرعتك {wpm:.2f} WPM\n\n"
                            f"فوزك رقم {wins} من {target}{round_stats}"
                        )
                else:
                    await u.message.reply_text(f"ممتاز! سرعتك {wpm:.2f} WPM")
                storage.del_session(cid, typ)
                break

def main():
    if not BOT_TOKEN:
        print("خطأ: لم يتم العثور على BOT_TOKEN في متغيرات البيئة!")
        print("الرجاء إضافة متغير البيئة BOT_TOKEN من خلال Secrets في Replit")
        return

    print("جاري تشغيل البوت...")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

    print("البوت يعمل بنجاح!")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
