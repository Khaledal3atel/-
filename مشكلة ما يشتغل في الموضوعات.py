import random
import time
import re
import json
import os
import asyncio
import requests
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, JobQueue
from telegram.error import BadRequest
from collections import defaultdict
from cachetools import TTLCache, LRUCache
from functools import lru_cache
import aiohttp

BOT_TOKEN = "8219268566:AAFXjWgfsVS4hIVInXn7tXn1xZbnIsbxtic"
OWNER_ID = 5562144078

message_cache = {}
last_save_time = time.time()
save_interval = 60.0
dirty_count = 0
max_dirty_count = 200

file_cache = TTLCache(maxsize=10, ttl=3600)
admin_cache = LRUCache(maxsize=50)
banned_cache = LRUCache(maxsize=100)

chat_locks = defaultdict(asyncio.Lock)
last_command_time = defaultdict(lambda: defaultdict(float))
sent_message_tracker = defaultdict(lambda: defaultdict(float))
last_bot_send_time = defaultdict(float)
bot_send_lock = asyncio.Lock()

http_session = None

speed_bot_states = {}
speed_bot_lock = asyncio.Lock()
speed_bot_tasks = {}

processed_updates = set()
last_processed_update_cleanup = time.time()
MAX_PROCESSED_UPDATES = 1000

last_message_second = defaultdict(int)
messages_in_current_second = defaultdict(int)
sent_messages_per_second = defaultdict(list)
message_send_lock = asyncio.Lock()

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
    "E": "https://raw.githubusercontent.com/Khaledal3atel/-/refs/heads/main/English.txt",
    "قص": "https://drive.google.com/file/d/1wiW-O09wfLy6LbpAbCb8YseFIQ26W4Zn/view?usp=drivesdk"
}

REPEAT_WORDS = ["صمت", "لحم", "سين", "عين", "جيم", "لون", "خبر", "حلم", "جمل", "تعب", "عرب", "خمش", "برد", "نسر", "حصن", "حل", "دور", "خسر", "غرب", "صوت", "سون", "كل", "حلو", "عطر", "همس", "لهب", "بحر", "فرق", "ود", "وشم", "رمل"]

NUMBER_WORDS = ["واحد", "اثنين", "ثلاثه", "اربعه", "خمسه", "سته", "سبعه", "ثمانيه", "تسعه", "عشرة", "عشرين", "خمسين", "ميه", "الف", "مليون", "ون", "تو", "ثري", "فور", "فايف", "سكس", "سفن", "ايت", "ناين", "تن", "تولف", "ايلفن", "تونتي"]

LETTER_WORDS = ["الف", "باء", "جيم", "دال", "شين", "ضاد", "قاف", "كاف", "لام", "ميم", "نون", "واو", "اكس", "واي", "ام", "جي", "كيو", "باي", "رو", "بيتا", "فاي"]

JAB_WORDS = ["حي", "اسرع", "هيا", "عز", "رجال", "اكتب", "سريع", "ولد", "لا", "روح", "خصم", "جلد", "اقوى", "دوس", "مقال", "ادعس", "فوز", "تكلج", "شيل", "مقال", "كتابة", "رتب", "اول", "خش", "في", "حق", "طير", "ولع", "كاتب", "اشخط", "تصير", "ركب فشخ", "العب", "حد", "روق", "وحش", "ركض", "نهر", "ما", "يجري", "برق", "حيك", "راس", "رعد", "ريح", "قدها", "واصل", "ركز", "خرج", "استمر", "سر", "نسوخي", "مايك", "براد", "من", "شد", "ليه", "مر", "طباع", "طار", "وفر", "لي", "عيني", "شطب", "اصمل", "درّس", "بطل", "صح", "عيون"]

NASS_DRIVE_URLS = [
    "https://drive.google.com/file/d/1AwJ5s6aAeGnH5JQ6LjvGMOunlKlhDZYg/view?usp=drivesdk",
    "https://drive.google.com/file/d/1oVR2c9bUrziGg3M-QsYduV-j_zGFK8v5/view?usp=drivesdk",
    "https://drive.google.com/file/d/1_2IhMuw3ejAXq6cgTYtowSa6wCGTbgJw/view?usp=drivesdk",
    "https://drive.google.com/file/d/1Jy1LYGW7PLDSAChy4euUTl2J7boR9_bb/view?usp=drivesdk",
    "https://drive.google.com/file/d/1X5xl8kvTwjNmHO6BY1tlGyuflrV2-YYL/view?usp=drivesdk",
    "https://drive.google.com/file/d/1ozXsI-FdaMdzO4yToMYXKe_NFOBG-OTu/view?usp=drivesdk",
    "https://drive.google.com/file/d/1GCv06T6vGGKNRfMCq7tLkjR35sMvPUg3/view?usp=drivesdk",
    "https://drive.google.com/file/d/1awe5WHPiuLH_Pubi1usPSMbW0bAqWJCO/view?usp=drivesdk",
    "https://drive.google.com/file/d/192GKrNseDLyrUiSYm0slGfQevjM6u2nh/view?usp=drivesdk",
    "https://drive.google.com/file/d/1HykcSAX8PdkmgHXWLVoVFit1ZoGVSp-j/view?usp=drivesdk",
    "https://drive.google.com/file/d/1LlTBYUrCsrO50g4y7S-cnsgLx_rAxuDG/view?usp=drivesdk",
    "https://drive.google.com/file/d/1fibh-s7qosrUAtrQln6UQ9UDzre7wgzI/view?usp=drivesdk",
    "https://drive.google.com/file/d/1MnqUqtVZAYDO16TBin-h9OOL0S4ZFQTH/view?usp=drivesdk",
    "https://drive.google.com/file/d/1L53NoUAwIL6Ki84O_l6KK_Bmg4MOj1iv/view?usp=drivesdk",
    "https://drive.google.com/file/d/1VYrQY0Na8wzmRZdgWYl3mA_y7vfYhPFr/view?usp=drivesdk",
    "https://drive.google.com/file/d/1GA6Yi13CokJzx40aViGuNqvFe21HsAhQ/view?usp=drivesdk",
    "https://drive.google.com/file/d/1rd9l8Rzp1MxKaV55gvsDbypDfRcMeVAp/view?usp=drivesdk",
    "https://drive.google.com/file/d/1vGPWDzJLbBNDciozwiRCRzu89POiR80-/view?usp=drivesdk",
    "https://drive.google.com/file/d/1zpfQb2JnWRERoZBD6Nof7o83T_O9MVm_/view?usp=drivesdk",
    "https://drive.google.com/file/d/1p62GBB9bJ7tWrLiAx3Dyhemc9SgonHDQ/view?usp=drivesdk",
    "https://drive.google.com/file/d/1Medd6sRGxg2XjDnexq9vwxscpZi-U8Dq/view?usp=drivesdk",
    "https://drive.google.com/file/d/1-TujLpPljWM5ttz3F4Den6CkC27gVL9S/view?usp=drivesdk",
    "https://drive.google.com/file/d/10YiLlQvoB8GdgEmyXsyjS-dhau8RLGCN/view?usp=drivesdk",
    "https://drive.google.com/file/d/1naAUbU5Ji0sENuJOA5JWMk4QHbsxGQpr/view?usp=drivesdk",
    "https://drive.google.com/file/d/1RY1ghRYgPVwiH9lEsjM38XNzd0wzHk_C/view?usp=drivesdk",
    "https://drive.google.com/file/d/1uW6Je9Lqt3WV1d9qsJgQLev0fILOvjsj/view?usp=drivesdk",
    "https://drive.google.com/file/d/1g7R_ESOIFXZbsE_nTTE_ZesDOfuF9AMW/view?usp=drivesdk",
    "https://drive.google.com/file/d/1mXIJWP-NBWI0WXA-Uv1G9IFtxdAuT9_O/view?usp=drivesdk",
    "https://drive.google.com/file/d/1UfgAnJ8kg8e9nqddZObYhp-0yNd7J1oy/view?usp=drivesdk",
    "https://drive.google.com/file/d/16gLkV3GfIDkovm0Ty47AoXwqdq4G7qZM/view?usp=drivesdk",
    "https://drive.google.com/file/d/1FhzyGzMHtbYFGJ00ayJL6vGFybZx5EXk/view?usp=drivesdk",
    "https://drive.google.com/file/d/1PHzu7eERF43hG92LHmkSUMX6zo4u1_f7/view?usp=drivesdk",
    "https://drive.google.com/file/d/1zfymM9mB-EbrgcJPKkxMkrQSKGJUUMWh/view?usp=drivesdk",
    "https://drive.google.com/file/d/1_L6ThPRrotnEgHV-UtVJerlQRUYtI7sy/view?usp=drivesdk",
    "https://drive.google.com/file/d/17T2BrwV8cVCDSbgRfypFOolOMm9-Vo3L/view?usp=drivesdk",
    "https://drive.google.com/file/d/1zpmxWY5wE4ppYd-zJ_z_PLWNaMsptQtL/view?usp=drivesdk",
    "https://drive.google.com/file/d/18aU2SJoeBw_-JZY6NtM7Ue2mky0T5uPe/view?usp=drivesdk",
    "https://drive.google.com/file/d/11wJD6WkKw_Gc4KGPTsoMgzFOjdcYIfRw/view?usp=drivesdk",
    "https://drive.google.com/file/d/1ARWqrKLPu8mGNJvqLx1bbvCPQ9a4F4pk/view?usp=drivesdk",
    "https://drive.google.com/file/d/1uSi3EoCgVXiwHxAvcOKyObFtPQxYcKlb/view?usp=drivesdk",
    "https://drive.google.com/file/d/1xjk112E_bBk52x56RINmBjx58uKsFJmx/view?usp=drivesdk",
    "https://drive.google.com/file/d/1ATNu42EPouBumxNRaMWUxlem1IUlUmbz/view?usp=drivesdk",
    "https://drive.google.com/file/d/1-Au6SwhMTHUUBk3uC9l6bli_Fp-WEwZM/view?usp=drivesdk",
    "https://drive.google.com/file/d/1hHZCAeLamkTv0jaZ7QBewJU1B9K9Jbr6/view?usp=drivesdk",
    "https://drive.google.com/file/d/1ON9R9m3Qh4u8yrOus-f296P-I4Nu2fGe/view?usp=drivesdk",
    "https://drive.google.com/file/d/1F8fL0BUSIXWpSsoH_7ESWGQv26pxNepq/view?usp=drivesdk",
    "https://drive.google.com/file/d/17pWPIiPZ_vTISjmw2B-KNFhMgklu_mwP/view?usp=drivesdk",
    "https://drive.google.com/file/d/10H5sHixnkZO5fI0d480C4hW6SF6503Pp/view?usp=drivesdk",
    "https://drive.google.com/file/d/1qhRdVhBqZV0eHj0rYX9dvpIIRnlkjAh5/view?usp=drivesdk",
    "https://drive.google.com/file/d/1idOMgj4EH1iJwxPPHMS-nM-VAKMmhkWP/view?usp=drivesdk",
    "https://drive.google.com/file/d/1ekcxe8pw0xWAiFVqHNf8y4MefjyL0lpC/view?usp=drivesdk",
    "https://drive.google.com/file/d/1kQdGclgAJWOZ2KHM5d_xl85yOAS6azI7/view?usp=drivesdk",
    "https://drive.google.com/file/d/1VXptSX4PNRC9o9mZZ6wsewmVaY3Nzg5S/view?usp=drivesdk",
    "https://drive.google.com/file/d/1aIYilqapzAO-8BUNX48jeX_ugafJiqGV/view?usp=drivesdk",
    "https://drive.google.com/file/d/10dtSEapz-N-jela0PA9U1oCXr35abaiz/view?usp=drivesdk",
    "https://drive.google.com/file/d/1G3ffM94zldw6Uy50gsvMheR8b2wtFEyf/view?usp=drivesdk",
    "https://drive.google.com/file/d/1dBCxMIBaDYKb_g-9UYwxxXaD4awnSur1/view?usp=drivesdk",
    "https://drive.google.com/file/d/1xs2sxsilxttcI8FFGYflTRu__fFEqQJf/view?usp=drivesdk",
    "https://drive.google.com/file/d/1SoVQP6a9FaHXPVk31lnUDI2z6lO-3oA5/view?usp=drivesdk",
    "https://drive.google.com/file/d/1Usvvt5HQyocYqhlEXlIKs2jh7dlI9bhd/view?usp=drivesdk",
    "https://drive.google.com/file/d/19AWenU_8s7F3ImYUxVUFLbf0C5aF069p/view?usp=drivesdk"
]

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
NUM_WORDS = {'0': 'صفر', '1': 'واحد', '2': 'اثنان', '3': 'ثلاثة', '4': 'أربعة', '5': 'خمسة', '6': 'ستة', '7': 'سبعة', '8': 'ثمانية', '9': 'تسعة', '10': 'عشرة', '20': 'عشرون', '30': 'ثلاثون', '40': 'أربعون', '50': 'خمسون', '60': 'ستون', '70': 'سبعون', '80': 'ثمانون', '90': 'تسعون', '100': 'مائة', '1000': 'ألف'}

async def init_http_session():
    global http_session
    if http_session is None:
        timeout = aiohttp.ClientTimeout(total=15)
        connector = aiohttp.TCPConnector(limit=5, limit_per_host=3)
        http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return http_session

async def close_http_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None

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

def contains_11_to_19(num):
    """فحص إذا كان الرقم يحتوي على 11-19 داخله"""
    num_str = str(num)
    for i in range(len(num_str) - 1):
        two_digit = int(num_str[i:i+2])
        if 11 <= two_digit <= 19:
            return True
    return False

def number_to_arabic_words(num):
    ones = ["صفر", "واحد", "اثنين", "ثلاثه", "اربعه", "خمسه", "سته", "سبعه", "ثمانيه", "تسعه"]
    tens = ["", "", "عشرين", "ثلاثين", "اربعين", "خمسين", "ستين", "سبعين", "ثمانين", "تسعين"]
    teens = ["عشرة", "احدعشر", "اثنعشر", "ثلاثعشر", "اربععشر", "خمسعشر", "ستعشر", "سبععشر", "ثمانعشر", "تسععشر"]

    if contains_11_to_19(num):
        return ""
    if num == 0:
        return "صفر"
    if num < 10:
        return ones[num]
    elif num < 20:
        return teens[num - 10]
    elif num < 100:
        tens_digit = num // 10
        ones_digit = num % 10
        if ones_digit == 0:
            return tens[tens_digit]
        return ones[ones_digit] + " و" + tens[tens_digit]
    else:
        hundreds_digit = num // 100
        remainder = num % 100
        result = "ميه" if hundreds_digit == 1 else ones[hundreds_digit] + " ميه"
        if remainder == 0:
            return result
        return result + " و" + number_to_arabic_words(remainder)

def generate_hard_numbers_sentence(count=None):
    if count is None:
        count = random.randint(5, 10)
    else:
        count = max(1, min(int(count), 40))
    numbers = []
    for _ in range(count):
        while True:
            num = random.randint(0, 1000)
            if not contains_11_to_19(num):
                numbers.append(num)
                break
    return numbers, " , ".join(str(n) for n in numbers)

def generate_easy_numbers_sentence(count=None):
    if count is None:
        count = random.randint(5, 10)
    else:
        count = max(1, min(int(count), 40))
    numbers = []
    for _ in range(count):
        while True:
            num = random.randint(0, 100)
            if not contains_11_to_19(num):
                numbers.append(num)
                break
    return numbers, " , ".join(str(n) for n in numbers)

def convert_numbers_to_arabic_words(numbers_str):
    numbers = [int(n.strip()) for n in numbers_str.split(',') if n.strip().isdigit()]
    words = []
    for num in numbers:
        words.append(number_to_arabic_words(num))
    return " ".join(words)

def normalize_number_text(text):
    normalized = text
    normalized = re.sub(r'\s*و\s*', ' و ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    replacements = {
        r'\b(ميتين|ميه\s*و\s*اثنين|مائتين|مائتان)\b': 'اثنين ميه',
        r'\b(ثلاث\s*ميه|ثلاثه\s*ميه|ثلاثميه)\b': 'ثلاثه ميه',
        r'\b(اربع\s*ميه|اربعه\s*ميه|اربعميه)\b': 'اربعه ميه',
        r'\b(خمس\s*ميه|خمسه\s*ميه|خمسميه)\b': 'خمسه ميه',
        r'\b(ست\s*ميه|سته\s*ميه|ستميه)\b': 'سته ميه',
        r'\b(سبع\s*ميه|سبعه\s*ميه|سبعميه)\b': 'سبعه ميه',
        r'\b(ثمان\s*ميه|ثمانيه\s*ميه|ثمانه\s*ميه|ثمانميه)\b': 'ثمانيه ميه',
        r'\b(تسع\s*ميه|تسعه\s*ميه|تسعميه)\b': 'تسعه ميه',
        r'\b(واحد|احد)\b': 'واحد', r'\b(اثنين|اثنان)\b': 'اثنين',
        r'\b(ثلاثه|ثلاثة|ثلاث)\b': 'ثلاثه', r'\b(اربعه|اربعة|اربع)\b': 'اربعه',
        r'\b(خمسه|خمسة|خمس)\b': 'خمسه', r'\b(سته|ستة|ست)\b': 'سته',
        r'\b(سبعه|سبعة|سبع)\b': 'سبعه', r'\b(ثمانيه|ثمانية|ثمان)\b': 'ثمانيه',
        r'\b(تسعه|تسعة|تسع)\b': 'تسعه', r'\b(عشرة|عشره|عشر)\b': 'عشرة',
        r'\b(احدعشر|احد\s*عشر|احدعش|احد\s*عش|احدى\s*عشر|احدى\s*عش|احدعشرة)\b': 'احدعشر',
        r'\b(اثنعشر|اثن\s*عشر|اثنعش|اثن\s*عش|اثنا\s*عشر|اثنا\s*عش|اثنعشرة)\b': 'اثنعشر',
        r'\b(ثلاثعشر|ثلاث\s*عشر|ثلاثعش|ثلاث\s*عش|ثل\s*عشر|ثل\s*عش|ثلاطعش|ثل\s*طعش|ثلاثعشرة)\b': 'ثلاثعشر',
        r'\b(اربععشر|اربع\s*عشر|اربععش|اربع\s*عش|اربعطعش|اربع\s*طعش|اربععشرة)\b': 'اربععشر',
        r'\b(خمسعشر|خمس\s*عشر|خمسعش|خمس\s*عش|خمسطعش|خمس\s*طعش|خمسعشرة)\b': 'خمسعشر',
        r'\b(ستعشر|ست\s*عشر|ستعش|ست\s*عش|سطعش|ست\s*طعش|ستعشرة)\b': 'ستعشر',
        r'\b(سبععشر|سبع\s*عشر|سبععش|سبع\s*عش|سبعطعش|سبع\s*طعش|سبععشرة)\b': 'سبععشر',
        r'\b(ثمانعشر|ثمان\s*عشر|ثمانعش|ثمان\s*عش|ثمنطعش|ثمن\s*طعش|ثمانعشرة)\b': 'ثمانعشر',
        r'\b(تسععشر|تسع\s*عشر|تسععش|تسع\s*عش|تسعطعش|تسع\s*طعش|تسععشرة)\b': 'تسععشر',
        r'\b(عشرين|عشرون|عشري)\b': 'عشرين',
        r'\b(ثلاثين|ثلاثون|ثلاثي)\b': 'ثلاثين',
        r'\b(اربعين|اربعون|اربعي)\b': 'اربعين',
        r'\b(خمسين|خمسون|خمسي)\b': 'خمسين',
        r'\b(ستين|ستون|ستي)\b': 'ستين',
        r'\b(سبعين|سبعون|سبعي)\b': 'سبعين',
        r'\b(ثمانين|ثمانون|ثماني)\b': 'ثمانين',
        r'\b(تسعين|تسعون|تسعي)\b': 'تسعين',
        r'\b(مئة|ميه|مائة|مية)\b': 'ميه',
        r'\b(الف|الفة)\b': 'الف'
    }

    for pattern, replacement in replacements.items():
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

async def can_bot_send(cid):
    async with message_send_lock:
        current_second = int(time.time())

        if last_message_second[cid] == current_second:
            return False

        last_message_second[cid] = current_second
        messages_in_current_second[cid] = 1

        return True

async def track_and_verify_message(context, cid, message_id):
    async with message_send_lock:
        current_second = int(time.time())

        sent_messages_per_second[cid] = [
            (sec, msg_id) for sec, msg_id in sent_messages_per_second[cid]
            if current_second - sec < 3
        ]

        sent_messages_per_second[cid].append((current_second, message_id))

        messages_in_this_second = [
            msg_id for sec, msg_id in sent_messages_per_second[cid]
            if sec == current_second
        ]

        if len(messages_in_this_second) >= 2:
            for msg_id in messages_in_this_second:
                try:
                    await context.bot.delete_message(chat_id=cid, message_id=msg_id)
                except Exception as e:
                    print(f"Error deleting duplicate message {msg_id}: {e}")

            sent_messages_per_second[cid] = [
                (sec, msg_id) for sec, msg_id in sent_messages_per_second[cid]
                if sec != current_second
            ]

            return False

        return True

async def safe_reply(update, context, text, sleep_if_blocked=False):
    cid = update.effective_chat.id

    if not await can_bot_send(cid):
        if sleep_if_blocked:
            await asyncio.sleep(1.0)
            if not await can_bot_send(cid):
                return None
        else:
            return None

    try:
        sent_message = await update.message.reply_text(text)

        if sent_message:
            keep_message = await track_and_verify_message(context, cid, sent_message.message_id)
            if not keep_message:
                return None

        return sent_message
    except Exception as e:
        print(f"Error sending safe reply: {e}")
        return None

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

def convert_repeat_pattern_to_words(pattern):
    def replace_pattern(match):
        word = match.group(1)
        count = int(match.group(2))
        return ' '.join([word] * count)

    converted = re.sub(r'(\S+)\((\d+)\)', replace_pattern, pattern)
    return converted

def convert_to_double(sentence):
    words = sentence.split()
    result_words = []
    for word in words:
        result_words.append(word)
        result_words.append(word)
    return ' '.join(result_words)

def convert_to_triple(sentence):
    words = sentence.split()
    result_words = []
    for word in words:
        result_words.append(word)
        result_words.append(word)
        result_words.append(word)
    return ' '.join(result_words)

def calculate_typing_speed(base_wpm, sentence_type=None):
    base_fluctuation = random.uniform(-0.05, -0.15) if random.random() < 0.5 else random.uniform(0.05, 0.15)

    multiplier = 1.0
    if sentence_type in ["كرر", "دبل", "تر"]:
        multiplier = random.uniform(1.20, 1.30)

    final_wpm = base_wpm * (1 + base_fluctuation) * multiplier
    final_wpm += 20

    return max(50, min(5000, final_wpm))

def build_speed_output(sentence):
    words = sentence.split()
    return " ~ ".join(words)

async def speed_bot_type_sentence(context, cid, sentence, wpm, sentence_type, start_time):
    try:
        speed_text = build_speed_output(sentence)
        words = sentence.split()
        total_chars = sum(len(w) for w in words)

        chars_per_second = (wpm * 5) / 60.0
        total_time_needed = total_chars / chars_per_second

        chunks = []
        current_chunk = ""
        current_word_idx = 0

        for word in words:
            if current_chunk:
                current_chunk += " ~ " + word
            else:
                current_chunk = word
            current_word_idx += 1

            if current_word_idx % 2 == 0 or current_word_idx == len(words):
                chunks.append(current_chunk)

        if not chunks:
            chunks = [speed_text]

        chunk_delay = total_time_needed / len(chunks)

        message = None
        for i, chunk in enumerate(chunks):
            if i > 0:
                jitter = random.uniform(0.8, 1.2)
                await asyncio.sleep(chunk_delay * jitter)

            try:
                if message is None:
                    if not await can_bot_send(cid):
                        await asyncio.sleep(1.0)
                        if not await can_bot_send(cid):
                            return 0

                    message = await context.bot.send_message(chat_id=cid, text=chunk)

                    if message:
                        keep_message = await track_and_verify_message(context, cid, message.message_id)
                        if not keep_message:
                            return 0
                else:
                    await message.edit_text(chunk)
            except Exception as e:
                print(f"Error in speed bot typing: {e}")
                break

        elapsed_time = time.time() - start_time
        actual_wpm = (len(words) / elapsed_time) * 60 + 20

        final_text = f"كفو يا\n\nسرعتك: {actual_wpm:.2f} كلمة/دقيقة\nالوقت : {elapsed_time:.2f} ثانية"
        try:
            if message:
                await message.edit_text(final_text)
        except:
            pass

        return actual_wpm

    except asyncio.CancelledError:
        print(f"Speed bot task cancelled for chat {cid}")
        raise
    except Exception as e:
        print(f"Error in speed_bot_type_sentence: {e}")
        return 0

async def trigger_speed_bot_if_enabled(context, cid, sentence, sentence_type):
    try:
        speed_config = storage.get_speed_bot_config(cid)
        if not speed_config["enabled"]:
            task = speed_bot_tasks.pop(str(cid), None)
            if task and not task.done():
                task.cancel()
            return

        task_key = str(cid)
        old_task = speed_bot_tasks.get(task_key)
        if old_task and not old_task.done():
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"Error cancelling old speed task: {e}")

        speed_bot_tasks.pop(task_key, None)

        converted_sentence = sentence
        if sentence_type == "كرر":
            converted_sentence = convert_repeat_pattern_to_words(sentence)
        elif sentence_type == "دبل":
            converted_sentence = convert_to_double(sentence)
        elif sentence_type == "تر":
            converted_sentence = convert_to_triple(sentence)

        base_wpm = speed_config["base_wpm"]
        wpm = calculate_typing_speed(base_wpm, sentence_type)
        start_time = time.time()

        task = asyncio.create_task(
            speed_bot_type_sentence(context, cid, converted_sentence, wpm, sentence_type, start_time)
        )
        speed_bot_tasks[task_key] = task
        task.add_done_callback(
            lambda t, key=task_key: speed_bot_tasks.pop(key, None) if speed_bot_tasks.get(key) is t else None
        )

    except Exception as e:
        print(f"Error triggering speed bot: {e}")

class Storage:
    def __init__(self):
        self.file = "bot_data.json"
        self.data = self.load()
        self.dirty = False
        self._dirty_count = 0

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
                "auto_mode": {},
                "speed_bot": {},
                "levels": {}
            }

    def save(self, force=False):
        global last_save_time
        current_time = time.time()

        if not self.dirty and not force:
            return

        if not force:
            if self._dirty_count < max_dirty_count and (current_time - last_save_time) < save_interval:
                return

        try:
            with open(self.file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            self.dirty = False
            self._dirty_count = 0
            last_save_time = current_time
            print(f"[STORAGE] Data saved successfully at {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Error saving data: {e}")

    def mark_dirty(self):
        self.dirty = True
        self._dirty_count += 1

        if self._dirty_count >= max_dirty_count:
            self.save(force=True)

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
            return True
        return False

    def save_number_type(self, uid, number_type):
        uid_str = str(uid)
        if "number_types" not in self.data:
            self.data["number_types"] = {}
        self.data["number_types"][uid_str] = number_type
        self.mark_dirty()

    def get_number_type(self, uid):
        uid_str = str(uid)
        if "number_types" not in self.data:
            return None
        return self.data["number_types"].get(uid_str)

    def is_banned(self, uid):
        uid_str = str(uid)

        if uid_str in banned_cache:
            return banned_cache[uid_str]

        is_in_list = uid_str in self.data["banned"]
        banned_cache[uid_str] = is_in_list
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

        banned_cache[uid_str] = True
        self.mark_dirty()
        self.save(force=True)

    def unban_user(self, uid):
        uid_str = str(uid)
        print(f"[UNBAN] Attempting to unban user {uid}. Current banned list: {self.data['banned']}")

        if uid_str in self.data["banned"]:
            self.data["banned"].remove(uid_str)
            banned_cache[uid_str] = False
            self.mark_dirty()
            self.save(force=True)
            print(f"[UNBAN] User {uid} has been unbanned. Updated banned list: {self.data['banned']}")
            return True
        else:
            print(f"[UNBAN] User {uid} was not in banned list")
            return False

    def is_admin(self, uid):
        cache_key = f"admin_{uid}"
        if cache_key in admin_cache:
            return admin_cache[cache_key]

        result = str(uid) in self.data["admins"]
        admin_cache[cache_key] = result
        return result

    def is_owner(self, uid):
        cache_key = f"owner_{uid}"
        if cache_key in admin_cache:
            return admin_cache[cache_key]

        result = str(uid) in self.data["owners"]
        admin_cache[cache_key] = result
        return result

    def is_main_owner(self, uid):
        return uid == OWNER_ID

    def add_admin(self, uid):
        uid_str = str(uid)
        if uid_str not in self.data["admins"]:
            self.data["admins"].append(uid_str)
            admin_cache.clear()
            self.mark_dirty()
            self.save(force=True)

    def add_owner(self, uid):
        uid_str = str(uid)
        if uid_str not in self.data["owners"]:
            self.data["owners"].append(uid_str)
            admin_cache.clear()
            self.mark_dirty()
            self.save(force=True)

    def remove_admin(self, uid):
        uid_str = str(uid)
        if uid_str in self.data["admins"]:
            self.data["admins"].remove(uid_str)
            admin_cache.clear()
            self.mark_dirty()
            self.save(force=True)

    def remove_owner(self, uid):
        uid_str = str(uid)
        if uid_str in self.data["owners"]:
            self.data["owners"].remove(uid_str)
            admin_cache.clear()
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

    def save_session(self, uid, cid, typ, txt, tm, sent=False, random_mode=True):
        key = f"{cid}_{typ}"
        self.data["sessions"][key] = {
            "type": typ,
            "text": txt,
            "time": tm,
            "starter_uid": uid,
            "sent": sent,
            "random_mode": random_mode
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
        return scores[:5]

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

    def get_level_info(self, uid):
        uid_str = str(uid)
        if "levels" not in self.data:
            self.data["levels"] = {}
        if uid_str not in self.data["levels"]:
            self.data["levels"][uid_str] = {"level": 1, "progress": 0}
        return self.data["levels"][uid_str]

    def get_level_requirement(self, level):
        return 5 * level

    def add_correct_sentence(self, uid):
        uid_str = str(uid)
        if "levels" not in self.data:
            self.data["levels"] = {}
        if uid_str not in self.data["levels"]:
            self.data["levels"][uid_str] = {"level": 1, "progress": 0}

        level_data = self.data["levels"][uid_str]
        current_level = level_data["level"]
        current_progress = level_data["progress"]
        requirement = self.get_level_requirement(current_level)

        current_progress += 1

        if current_progress >= requirement:
            level_data["level"] += 1
            level_data["progress"] = 0
        else:
            level_data["progress"] = current_progress

        self.mark_dirty()
        return level_data

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

    def get_speed_bot_config(self, cid):
        if "speed_bot" not in self.data:
            self.data["speed_bot"] = {}
        return self.data["speed_bot"].get(str(cid), {
            "enabled": False,
            "base_wpm": 160
        })

    def set_speed_bot_enabled(self, cid, enabled):
        if "speed_bot" not in self.data:
            self.data["speed_bot"] = {}
        key = str(cid)
        if key not in self.data["speed_bot"]:
            self.data["speed_bot"][key] = {"enabled": enabled, "base_wpm": 160}
        else:
            self.data["speed_bot"][key]["enabled"] = enabled
        self.mark_dirty()
        self.save(force=True)

    def set_speed_bot_wpm(self, cid, wpm):
        if "speed_bot" not in self.data:
            self.data["speed_bot"] = {}
        key = str(cid)
        if key not in self.data["speed_bot"]:
            self.data["speed_bot"][key] = {"enabled": False, "base_wpm": wpm}
        else:
            self.data["speed_bot"][key]["base_wpm"] = wpm
        self.mark_dirty()
        self.save(force=True)

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

class StoriesManager:
    def __init__(self, gdrive_url, min_words=5, max_words=50):
        self.gdrive_url = gdrive_url
        self.min_words = min_words
        self.max_words = max_words
        self.stories = []
        self.used_indices = set()
        self.data_file = "stories_data.json"
        self.last_update = 0
        self.update_interval = 5 * 24 * 60 * 60
        self.target_count = 5000
        self.total_chunks = 0
        self._load_cached_data()

    def _convert_gdrive_url_to_direct(self, url):
        try:
            file_id = url.split('/d/')[1].split('/')[0]
            return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
        except:
            return url

    def _load_cached_data(self):
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.stories = data.get('stories', [])
                    self.used_indices = set(data.get('used_indices', []))
                    self.last_update = data.get('last_update', 0)
                    self.total_chunks = data.get('total_chunks', 0)
                    print(f"[STORIES] Loaded {len(self.stories)} stories from cache")
        except Exception as e:
            print(f"[STORIES] Error loading cached data: {e}")
            self.stories = []
            self.used_indices = set()

    def _save_cached_data(self):
        try:
            data = {
                'stories': self.stories,
                'used_indices': list(self.used_indices),
                'last_update': self.last_update,
                'total_chunks': self.total_chunks
            }
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"[STORIES] Saved {len(self.stories)} stories to cache")
        except Exception as e:
            print(f"[STORIES] Error saving cached data: {e}")

    def _download_and_parse_chunks(self):
        try:
            print("[STORIES] Downloading stories from Google Drive...")
            direct_url = self._convert_gdrive_url_to_direct(self.gdrive_url)

            response = requests.get(direct_url, timeout=30)
            if response.status_code != 200:
                print(f"[STORIES] Failed to download: HTTP {response.status_code}")
                return []

            text = response.text
            chunks = re.split(r'\n\s*\n', text)

            valid_chunks = []
            for chunk in chunks:
                chunk = chunk.strip()
                if chunk:
                    cleaned_chunk = clean(chunk)
                    word_count = len(clean_text_for_word_count(cleaned_chunk).split())
                    if self.min_words <= word_count <= self.max_words:
                        valid_chunks.append(cleaned_chunk)

            print(f"[STORIES] Found {len(valid_chunks)} valid chunks from file")
            return valid_chunks

        except Exception as e:
            print(f"[STORIES] Error downloading/parsing chunks: {e}")
            return []

    def _select_random_chunks(self, all_chunks, count):
        available_indices = set(range(len(all_chunks))) - self.used_indices

        if len(available_indices) < count:
            print(f"[STORIES] Resetting used indices (only {len(available_indices)} remaining)")
            self.used_indices = set()
            available_indices = set(range(len(all_chunks)))

        selected_indices = random.sample(list(available_indices), min(count, len(available_indices)))
        self.used_indices.update(selected_indices)

        selected_chunks = [all_chunks[i] for i in selected_indices]
        return selected_chunks

    def load(self):
        current_time = time.time()

        if self.stories and (current_time - self.last_update) < self.update_interval:
            return

        print("[STORIES] Starting stories update...")

        all_chunks = self._download_and_parse_chunks()

        if not all_chunks:
            print("[STORIES] No chunks downloaded, keeping existing stories")
            return

        self.total_chunks = len(all_chunks)
        self.stories = self._select_random_chunks(all_chunks, self.target_count)
        self.last_update = current_time

        self._save_cached_data()

        print(f"[STORIES] Update complete: {len(self.stories)} stories loaded")

    def get(self):
        if not self.stories or time.time() - self.last_update > self.update_interval:
            self.load()
        return random.choice(self.stories) if self.stories else "لا توجد قصص حالياً"

    def get_multiple(self, count=2):
        if not self.stories or time.time() - self.last_update > self.update_interval:
            self.load()
        if self.stories:
            return random.sample(self.stories, min(count, len(self.stories)))
        return []

class NassContentManager:
    def __init__(self, gdrive_urls, min_words=5, max_words=50):
        self.gdrive_urls = gdrive_urls
        self.min_words = min_words
        self.max_words = max_words
        self.sentences = []
        self.data_file = "nass_data.json"
        self.last_update = 0
        self.update_interval = 3 * 24 * 60 * 60
        self.lines_per_url = 500
        self._load_cached_data()

    def _convert_gdrive_url_to_direct(self, url):
        try:
            file_id = url.split('/d/')[1].split('/')[0]
            return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
        except:
            return url

    def _load_cached_data(self):
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.sentences = data.get('sentences', [])
                    self.last_update = data.get('last_update', 0)
                    print(f"[NASS] Loaded {len(self.sentences)} sentences from cache")
        except Exception as e:
            print(f"[NASS] Error loading cached data: {e}")
            self.sentences = []

    def _save_cached_data(self):
        try:
            data = {
                'sentences': self.sentences,
                'last_update': self.last_update
            }
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"[NASS] Saved {len(self.sentences)} sentences to cache")
        except Exception as e:
            print(f"[NASS] Error saving cached data: {e}")

    def _download_from_url(self, url, url_index, total_urls):
        try:
            print(f"[NASS] [{url_index+1}/{total_urls}] Downloading from {url[:50]}...")
            direct_url = self._convert_gdrive_url_to_direct(url)
            response = requests.get(direct_url, timeout=20)

            if response.status_code != 200:
                print(f"[NASS] [{url_index+1}/{total_urls}] Failed: HTTP {response.status_code}")
                return []

            lines = response.text.split('\n')
            valid_lines = []

            for line in lines:
                line = line.strip()
                if line:
                    cleaned_line = clean_nass_text(line)
                    if not cleaned_line:
                        continue
                    word_count = len(clean_text_for_word_count(cleaned_line).split())
                    if self.min_words <= word_count <= self.max_words:
                        valid_lines.append(cleaned_line)
                        if len(valid_lines) >= self.lines_per_url:
                            break

            if len(valid_lines) > self.lines_per_url:
                selected_lines = random.sample(valid_lines, self.lines_per_url)
            else:
                selected_lines = valid_lines

            print(f"[NASS] [{url_index+1}/{total_urls}] Got {len(selected_lines)} lines")
            return selected_lines

        except Exception as e:
            print(f"[NASS] [{url_index+1}/{total_urls}] Error: {e}")
            return []

    def load(self):
        current_time = time.time()

        if self.sentences and (current_time - self.last_update) < self.update_interval:
            print(f"[NASS] Using cached data ({len(self.sentences)} sentences)")
            return

        print(f"[NASS] Starting نص content update from {len(self.gdrive_urls)} sources...")
        print(f"[NASS] This may take a few minutes, please wait...")

        all_sentences = []
        successful_downloads = 0
        total_urls = len(self.gdrive_urls)

        for index, url in enumerate(self.gdrive_urls):
            sentences_from_url = self._download_from_url(url, index, total_urls)
            if sentences_from_url:
                all_sentences.extend(sentences_from_url)
                successful_downloads += 1
                if successful_downloads % 10 == 0:
                    print(f"[NASS] Progress: {successful_downloads}/{total_urls} sources completed, {len(all_sentences)} total sentences")
            time.sleep(0.2)

        if all_sentences:
            self.sentences = all_sentences
            self.last_update = current_time
            self._save_cached_data()
            print(f"[NASS]  Update complete: {len(self.sentences)} total sentences from {successful_downloads}/{total_urls} sources")
        else:
            print("[NASS]  No sentences downloaded, keeping existing cache")

    def get(self):
        return random.choice(self.sentences) if self.sentences else "لا توجد جمل حالياً"

    def get_multiple(self, count=2):
        if self.sentences:
            return random.sample(self.sentences, min(count, len(self.sentences)))
        return []

    def needs_update(self):
        if not self.sentences:
            return True
        return (time.time() - self.last_update) > self.update_interval

class WajabManager:
    def __init__(self, word_list, min_length=7, max_length=20):
        self.word_list = word_list
        self.min_length = min_length
        self.max_length = max_length

    def get(self):
        sentence_length = random.randint(self.min_length, self.max_length)
        selected_words = random.sample(self.word_list, min(sentence_length, len(self.word_list)))
        return ' '.join(selected_words)

    def get_multiple(self, count=2):
        return [self.get() for _ in range(count)]

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

def clean_nass_text(txt):
    txt = txt.replace('\u0640', '')
    txt = re.sub(r'[\u064B-\u065F\u0670]', '', txt.replace(' ≈ ', ' ').replace('≈', ' '))
    txt = re.sub(r'\([^)]*[a-zA-Z]+[^)]*\)', '', txt)
    txt = re.sub(r'\[[^\]]*\]', '', txt)
    txt = re.sub(r'\([^)]*\)', '', txt)
    txt = ' '.join([w for w in txt.split() if not re.search(r'[a-zA-Z]', w)])
    txt = re.sub(r'[0-9٠-٩]', '', txt)
    txt = re.sub(r'[،,:;؛\-–—\.\!؟\?\(\)\[\]\{\}""''«»…@#$%^&*+=<>~/\\|_]', ' ', txt)
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
    # أولاً: حذف الرموز من داخل الكلمات (بدون إضافة مسافات)
    cleaned_text = re.sub(r'[≈=\-~_|/\\@#$%^&*+=<>~`]+', '', text)
    # ثانياً: تحويل الفواصل بين الكلمات إلى مسافات
    cleaned_text = re.sub(r'[،,:;؛\.\!؟\?]+', ' ', cleaned_text)
    # ثالثاً: توحيد المسافات
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    words = cleaned_text.split()
    return len(words)

def match_numbers(orig, usr):
    """تطابق أرقام مع تجاهل جميع الرموز بين الكلمات وتحويل الأرقام العادية لكلمات عربية"""
    # تنظيف الرموز
    orig_cleaned = re.sub(r'[≈=\-~_|/\\،,:;؛\.\!؟\?\(\)\[\]\{\}""''«»…@#$%^&*+=<>~/|`]+', ' ', orig)
    usr_cleaned = re.sub(r'[≈=\-~_|/\\،,:;؛\.\!؟\?\(\)\[\]\{\}""''«»…@#$%^&*+=<>~/|`]+', ' ', usr)

    # إذا كان original يحتوي على أرقام عادية، حول الاثنين إلى كلمات عربية
    if re.search(r'\d', orig_cleaned):
        try:
            # تحويل المسافات بين الأرقام إلى فواصل للتعامل مع دالة convert_numbers_to_arabic_words
            orig_for_conversion = orig_cleaned.strip()
            orig_for_conversion = re.sub(r'\s+', ',', orig_for_conversion)

            # تحويل الأرقام العادية من orig إلى كلمات عربية
            expected_words = normalize(normalize_number_text(convert_numbers_to_arabic_words(orig_for_conversion)))
            usr_normalized = normalize(normalize_number_text(usr_cleaned))
            if expected_words == usr_normalized:
                return True

            expected_word_set = set(expected_words.split())
            usr_word_set = set(usr_normalized.split())
            if expected_word_set and expected_word_set == usr_word_set:
                return True
        except:
            pass

    # إذا لم تحتوي على أرقام أو فشل التحويل، قارن النصين المنظفة مباشرة
    orig_normalized = normalize(orig_cleaned)
    usr_normalized = normalize(usr_cleaned)

    if orig_normalized == usr_normalized:
        return True

    orig_word_set = set(orig_normalized.split())
    usr_word_set = set(usr_normalized.split())
    if orig_word_set and orig_word_set == usr_word_set:
        return True

    return False

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

    orig_words = set(orig_normalized.split())
    usr_words = set(usr_with_spaces_normalized.split())
    if orig_words and orig_words == usr_words:
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

def gen_pattern(uid, count=1, exclude_words=None):
    """توليد عدد معين من الكلمات المكررة - كل كلمة لها رقم تكرار خاص بها"""
    if exclude_words is None:
        exclude_words = []

    pattern = []
    used_words_in_pattern = []
    available_words = [w for w in REPEAT_WORDS if w not in exclude_words]

    if not available_words:
        available_words = REPEAT_WORDS

    for _ in range(count):
        for attempt in range(100):
            word = random.choice(available_words)
            w_clean = word.replace('\u0640', '')
            c = random.randint(2, 4)
            word_with_count = f"{w_clean}({c})"

            # التحقق من عدم تكرار نفس الكلمة في نفس الرسالة (حتى مع أرقام مختلفة)
            if w_clean not in used_words_in_pattern and word_with_count not in pattern:
                pattern.append(word_with_count)
                used_words_in_pattern.append(w_clean)
                break
        else:
            # إذا فشل البحث عن كلمة جديدة، أضفها على أي حال
            if w_clean not in used_words_in_pattern:
                pattern.append(word_with_count)
                used_words_in_pattern.append(w_clean)

    return pattern


def gen_pattern_with_word_count(uid, total_words):
    if total_words < 4 or total_words > 50:
        return None

    for attempt in range(100):
        num_words = random.randint(2, min(6, total_words))
        words = random.sample(REPEAT_WORDS, num_words)

        remaining = total_words
        counts = []
        for i in range(num_words):
            if i == num_words - 1:
                counts.append(remaining)
            else:
                min_count = 1
                max_count = min(remaining - (num_words - i - 1), 10)
                if max_count < min_count:
                    break
                count = random.randint(min_count, max_count)
                counts.append(count)
                remaining -= count

        if sum(counts) != total_words:
            continue

        pattern = []
        key_parts = []
        for w, c in zip(words, counts):
            w_clean = w.replace('\u0640', '')
            pattern.append(f"{w_clean}({c})")
            key_parts.append(f"{w_clean}_{c}")

        key = '_'.join(key_parts)
        if not storage.is_pattern_used(uid, key):
            storage.add_pattern(uid, key)
            return ' '.join(pattern)

    storage.clear_patterns(uid)
    return gen_pattern_with_word_count(uid, total_words)

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
    "E": RemoteManager(URLS["E"], min_words=3, max_words=30, lang="english"),
    "قص": StoriesManager(URLS["قص"], min_words=5, max_words=50),
    "نص": NassContentManager(NASS_DRIVE_URLS, min_words=5, max_words=50),
    "جب": WajabManager(JAB_WORDS, min_length=7, max_length=20)
}

async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id

    if storage.is_banned(uid):
        await u.message.reply_text("انت محظور تواصل مع @XXVV_99")
        return

    await show_bot_sections(u, c)

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
        "- (نص) - 25 مليون جملة عشوائية من مصادر مختلفة\n"
        "- (جمم) - جمل عادية\n"
        "- (ويكي) - جمل ويكيبيديا\n"
        "- (صج) - كلمات عشوائية صعبة\n"
        "- (جب) - كلمات عشوائية سهلة\n"
        "- (شك) - جمل عامية\n"
        "- (جش) - اقتباسات\n"
        "- (قص) - قصص وأجزاء أدبية\n"
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
        "🔈 https://t.me/dzatttt\n"
        "كل ما يخص البوت راح تلقاه هنا بأذن الله\n\n"
        "أوامر البوت:\n\n"
        "- (الصدارة) - المتصدرين\n" 
        "- (جوائزي) - عرض جوائزك\n"
        "- (تقدمي) - عرض مستواك والتقدم نحو المستوى التالي\n\n"
        "- (فتح جولة) - فتح جولة جديدة\n"
        "- (جولة) - عرض إحصائيات الجولة\n"
        "- (انهاء جولة) - إنهاء الجولة الحالية\n\n"
        "- (تلقائي) - وضع الجمل التلقائية المتتالية\n\n"
        "- (تغيير رق) - عشان تغيير نظام رق\n\n"
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

    types = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]
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

async def distribute_daily_awards(bot):
    types = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]
    award_names = ["الذهبية", "الفضية", "البرونزية"]

    for typ in types:
        lb = storage.get_leaderboard(typ)
        for rank, (uid_str, username, first_name, wpm) in enumerate(lb[:3], 1):
            if rank <= 3:
                award_name = award_names[rank - 1]
                storage.add_award(int(uid_str), award_name, wpm, typ)

async def daily_leaderboard_job(context: ContextTypes.DEFAULT_TYPE):
    """وظيفة يومية لإرسال الصدارة وتوزيع الجوائز في الساعة 12 ليل"""
    try:
        types = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]
        award_names = ["الذهبية", "الفضية", "البرونزية"]
        sections = []

        await distribute_daily_awards(context.bot)

        for typ in types:
            lb = storage.get_leaderboard(typ)
            if lb:
                s = f"{typ}\n"
                for i, (uid_str, username, first_name, wpm) in enumerate(lb, 1):
                    display = f"@{username}" if username else first_name
                    award = ""
                    if i <= 3:
                        award = f" ({award_names[i-1]})"
                    s += f"{i}. {display}: {wpm:.2f} WPM{award}\n"
                sections.append(s)

        if sections:
            msg = "صدارة اليوم:\n\n" + "\n".join(sections)
            await context.bot.send_message(chat_id=OWNER_ID, text=msg)
    except Exception as e:
        print(f"[ERROR] Daily leaderboard job error: {e}")

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
    types = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر"]

    total_usage = {}
    for date, commands in storage.data["stats"].items():
        for cmd, count in commands.items():
            if cmd in types:
                total_usage[cmd] = total_usage.get(cmd, 0) + count

    for typ in types:
        usage = total_usage.get(typ, 0)
        stats_details += f"• {typ}: {usage} مرة\n"

    msg = (
        f"إحصائيات البوت:\n\n"
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

    msg = "قائمة الإشراف:\n\n"

    msg += "المالك الأساسي:\n"
    owner_data = storage.data["users"].get(str(OWNER_ID), {})
    owner_username = owner_data.get("username")
    if owner_username:
        msg += f"• @{owner_username}\n"
    else:
        msg += f"• {owner_data.get('first_name', 'المالك الأساسي')}\n"

    owners = storage.get_all_owners()
    if owners:
        msg += "\nالملاك:\n"
        for owner_id in owners:
            user_data = storage.data["users"].get(str(owner_id), {})
            username = user_data.get("username")
            if username:
                msg += f"• @{username}\n"
            else:
                msg += f"• {user_data.get('first_name', 'مالك')}\n"

    admins = storage.get_all_admins()
    if admins:
        msg += "\nالادمنز:\n"
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

    if selected_section in ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "فر", "E"]:
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
        pref_count = storage.get_preference(auto_data["uid"], "كرر")
        if pref_count is None or pref_count < 2:
            pref_count = random.randint(2, 4)
        patterns = gen_pattern(auto_data["uid"], pref_count)
        pattern = " ".join(patterns)
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
    elif selected_section == "جب":
        sent = managers["جب"].get()
        storage.save_session(auto_data["uid"], cid, f"تلقائي_{selected_section}", sent, time.time(), sent=True)
        display = format_display(sent)
        await c.bot.send_message(chat_id=cid, text=display, message_thread_id=message_thread_id)

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    global processed_updates, last_processed_update_cleanup

    if not u.message or not u.message.text:
        return

    update_id = u.update_id

    if update_id in processed_updates:
        return

    processed_updates.add(update_id)

    current_time = time.time()
    if len(processed_updates) >= MAX_PROCESSED_UPDATES or (current_time - last_processed_update_cleanup > 300):
        processed_updates.clear()
        last_processed_update_cleanup = current_time

    uid = u.effective_user.id
    cid = u.effective_chat.id
    text = u.message.text.strip()
    usr = u.effective_user.username
    name = u.effective_user.first_name
    
    # تحديد نوع الدردشة
    is_forum = u.effective_chat.is_forum
    message_thread_id = u.message.message_thread_id if is_forum else None
    in_general_chat = is_forum and message_thread_id is None

    storage.add_user(uid, usr, name)

    if u.effective_chat.type in ['group', 'supergroup']:
        chat_title = u.effective_chat.title
        storage.add_chat(cid, chat_title)
    elif is_forum and not in_general_chat:
        chat_title = u.effective_chat.title
        storage.add_chat(cid, chat_title)

    if u.message.reply_to_message and (storage.is_main_owner(uid) or storage.is_owner(uid) or storage.is_admin(uid)):
        replied_user = u.message.reply_to_message.from_user
        replied_uid = replied_user.id
        replied_username = replied_user.username
        replied_name = replied_user.first_name

        auto_reply_commands = ["باند", "تقييد", "كتم", "الغاء تقييد", "الغاء باند", "فك باند", "طرد", "الغاء كتم"]

        if text == "باند":
            if storage.is_main_owner(uid) or storage.is_owner(uid) or storage.is_admin(uid):
                if not storage.is_main_owner(replied_uid):
                    storage.ban_user(replied_uid)
                    await u.message.reply_to_message.reply_text("تم ودن طار يا الامير من البوت")
                    return
        elif text in ["الغاء باند", "فك باند"]:
            if storage.is_main_owner(uid) or storage.is_owner(uid) or storage.is_admin(uid):
                storage.unban_user(replied_uid)
                await u.message.reply_to_message.reply_text("عتقناه لوجه الله ، الله يستر عليه")
                return
        elif text in auto_reply_commands:
            await u.message.reply_to_message.reply_text(text)
            return

        if text == "رفع ادمن" and storage.is_main_owner(uid):
            storage.add_admin(replied_uid)
            mention = f"@{replied_username}" if replied_username else replied_name
            await u.message.reply_text(f"تم رفع {mention} إلى رتبة ادمن")
            return

        if text == "رفع مالك":
            storage.add_owner(replied_uid)
            mention = f"@{replied_username}" if replied_username else replied_name
            await u.message.reply_text(f"تم رفع {mention} إلى رتبة مالك")
            return

    is_broadcast_mode = storage.get_broadcast_mode(uid)

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

    if text == "سبيد":
        task_key = str(cid)
        existing_task = speed_bot_tasks.get(task_key)
        if existing_task:
            if not existing_task.done():
                existing_task.cancel()
                try:
                    await existing_task
                except asyncio.CancelledError:
                    pass
            speed_bot_tasks.pop(task_key, None)
        storage.set_speed_bot_enabled(cid, True)
        await u.message.reply_text("سبيد الحين بيكسر راسك تجهز للطحن")
        return

    if text == "سبيد وقف":
        task_key = str(cid)
        task = speed_bot_tasks.pop(task_key, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        storage.set_speed_bot_enabled(cid, False)
        await u.message.reply_text("تم إيقاف سبيد بوت")
        return

    if text.startswith("سبيد "):
        if storage.is_main_owner(uid):
            try:
                parts = text.split()
                if len(parts) == 2:
                    wpm = int(parts[1])
                    if 50 <= wpm <= 5000:
                        storage.set_speed_bot_wpm(cid, wpm)
                        await u.message.reply_text(f"تم تعيين سرعة سبيد إلى {wpm} كلمة/دقيقة")
                    else:
                        await u.message.reply_text("السرعة يجب أن تكون بين 5000 و 50")
                else:
                    await u.message.reply_text("الاستخدام: سبيد [رقم بين 50-5000]")
            except ValueError:
                await u.message.reply_text("الرجاء إدخال رقم صحيح")
        else:
            await u.message.reply_text("هذا الأمر للمالك الأساسي فقط")
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
        if text == "قف":
            storage.end_auto_mode(cid)
            await u.message.reply_text("تم إيقاف نظام تلقائي")
            storage.log_cmd("قف")
            return

        if text == "ة" and auto_mode["active"]:
            storage.update_auto_activity(cid)
            await send_auto_sentence(c, cid, auto_mode)
            return

        if text == "ق":
            storage.end_auto_mode(cid)
            storage.start_auto_mode(cid, uid, message_thread_id)
            await u.message.reply_text("اختار أقسام جديدة ترا القديمة انحذفت اذا تبيها حطها من جديد\nاكتب الأقسام اللي تبيها وحين تنتهي اكتب انهاء")
            storage.log_cmd("ق")
            return

        if auto_mode["collecting"]:
            valid_sections = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "كرر", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر", "جب"]

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

                    updated_auto_mode = storage.get_auto_mode(cid)
                    await send_auto_sentence(c, cid, updated_auto_mode)
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

                if section_type in ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب"]:
                    if match_text(orig, text, "arabic"):
                        matched = True
                elif section_type == "فر":
                    if match_text(orig, text, "persian"):
                        matched = True
                elif section_type == "E":
                    if match_text(orig, text, "english"):
                        matched = True
                elif section_type == "رق":
                    if match_numbers(orig, text):
                        matched = True
                elif section_type in ["حر", "جب"]:
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
                    wpm = (word_count / max(elapsed, 0.01)) * 60 + 10
                    storage.update_score(uid, section_type, wpm)
                    storage.add_correct_sentence(uid)

                    storage.del_session(cid, typ)
                    storage.update_auto_activity(cid)

                    username = u.effective_user.username or u.effective_user.first_name or "مستخدم"

                    await u.message.reply_text(f"كفو يا {username}\n\nسرعتك: {wpm:.1f} كلمة/دقيقة\nالوقت : {elapsed:.2f} ثانية")
                    await asyncio.sleep(0.5)
                    await send_auto_sentence(c, cid, auto_mode)
                    return

    current_time = time.time()
    if current_time - last_command_time[cid][text] < 1:
        return
    last_command_time[cid][text] = current_time

    if text == "تلقائي":
        storage.start_auto_mode(cid, uid, message_thread_id)
        await u.message.reply_text("اختر انواع الأقسام اللتي تريدها حين الإنتهاء اكتب انهاء\n\nعشان تغير الجملة اكتب [ة] وعشان تغير أقسام اكتب [ق] وعشان توقف البوت اكتب قف وبيوقف")
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

    if text == "تقدمي":
        level_info = storage.get_level_info(uid)
        level = level_info["level"]
        progress = level_info["progress"]
        requirement = storage.get_level_requirement(level)
        percentage = (progress / requirement) * 100

        progress_bar_length = 5
        filled = int((percentage / 100) * progress_bar_length)
        bar = "" * filled + "" * (progress_bar_length - filled)

        next_level_cost = storage.get_level_requirement(level + 1)

        await u.message.reply_text(
            f"مستواك الحالي\n\n"
            f"المستوى: {level}\n"
            f"التقدم: {progress}/{requirement} جملة\n\n"
            f"شريط التقدم:\n{bar}\n"
            f"النسبة: {percentage:.0f}%\n\n"
            f"إلى المستوى التالي: {next_level_cost} جملة"
        )
        storage.log_cmd("تقدمي")
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

    if text in ["عرض", "مقالات", "بوت"]:
        await show_bot_sections(u, c)
        storage.log_cmd(text)
        return

    if text == "ريست":
        all_sections = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر", "جب", "كرر", "تلقائي"]
        for section in all_sections:
            storage.clear_preference(uid, section)
        storage.clear_patterns(uid)
        storage.save_preference(uid, "كرر_recent_words", [])
        await u.message.reply_text("تم إعادة تعيين تفضيلات جميع الأقسام بنجاح\nالآن سيتم إرسال جميع الجمل بشكلها الطبيعي العشوائي")
        storage.log_cmd("ريست")
        return

    if text.startswith("ريست "):
        section = text[5:].strip()
        if section in ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "شرط", "فكك", "دبل", "تر", "عكس", "فر", "E", "رق", "حر", "جب", "كرر"]:
            if section == "رق":
                storage.save_preference(uid, "رق_عدد", None)
                await u.message.reply_text(f"تم إعادة تعيين تفضيلات القسم ({section}) بنجاح\nالآن سيتم إرسال الأرقام بشكلها الطبيعي العشوائي")
            elif section == "كرر":
                storage.clear_preference(uid, section)
                storage.save_preference(uid, "كرر_recent_words", [])
                storage.clear_patterns(uid)
                await u.message.reply_text(f"تم إعادة تعيين تفضيلات القسم ({section}) بنجاح\nالآن سيتم إرسال الجمل بشكلها الطبيعي")
            else:
                if storage.clear_preference(uid, section):
                    storage.clear_patterns(uid)
                    await u.message.reply_text(f"تم إعادة تعيين تفضيلات القسم ({section}) بنجاح\nالآن سيتم إرسال الجمل بشكلها الطبيعي")
                else:
                    await u.message.reply_text(f"لا يوجد تفضيل محفوظ للقسم ({section})")
        else:
            await u.message.reply_text("القسم غير موجود\nاستخدم: ريست [اسم القسم]\nمثال: ريست جمم")
        storage.log_cmd("ريست")
        return

    active_sessions = storage.get_all_active_sessions(cid)

    command, word_count = extract_number_from_text(text)

    game_commands = ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "فر", "E", "e", "رق", "حر", "جب", "كرر", "شرط", "فكك", "دبل", "تر", "عكس"]
    is_game_command = (command in game_commands or text in game_commands)

    if is_game_command:
        if not await can_bot_send(cid):
            return

    if command in ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب"] or text in ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب"]:
        section = command if word_count else text
        storage.log_cmd(section)

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers[section], word_count)
            if sent:
                storage.del_session(cid, section)
                storage.save_session(uid, cid, section, sent, time.time(), sent=True, random_mode=False)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, section))
            else:
                sent = managers[section].get()
                storage.del_session(cid, section)
                storage.save_session(uid, cid, section, sent, time.time(), sent=True, random_mode=True)
                display = format_display(sent)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, section))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers[section], pref_count)
                if not sent:
                    sent = managers[section].get()
            else:
                sent = managers[section].get()
            storage.del_session(cid, section)
            storage.save_session(uid, cid, section, sent, time.time(), sent=True, random_mode=False)
            display = format_display(sent)
            await u.message.reply_text(display)
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, section))
        return

    if command == "فر" or text == "فر":
        section = "فر"
        storage.log_cmd("فر")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["فر"], word_count)
            if sent:
                storage.del_session(cid, "فر")
                storage.save_session(uid, cid, "فر", sent, time.time(), sent=True, random_mode=False)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "فر"))
            else:
                sent = managers["فر"].get()
                storage.del_session(cid, "فر")
                storage.save_session(uid, cid, "فر", sent, time.time(), sent=True, random_mode=False)
                display = format_display(sent)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "فر"))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["فر"], pref_count)
                if not sent:
                    sent = managers["فر"].get()
            else:
                sent = managers["فر"].get()
            storage.del_session(cid, "فر")
            storage.save_session(uid, cid, "فر", sent, time.time(), sent=True, random_mode=False)
            display = format_display(sent)
            await u.message.reply_text(display)
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "فر"))
        return

    if command == "E" or text in ["E", "e"]:
        section = "E"
        storage.log_cmd("E")

        if word_count and 1 <= word_count <= 60:
            storage.save_preference(uid, section, word_count)
            sent = get_text_with_word_count(managers["E"], word_count)
            if sent:
                storage.del_session(cid, "E")
                storage.save_session(uid, cid, "E", sent, time.time(), sent=True, random_mode=False)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "E"))
            else:
                sent = managers["E"].get()
                storage.del_session(cid, "E")
                storage.save_session(uid, cid, "E", sent, time.time(), sent=True, random_mode=False)
                display = format_display(sent)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "E"))
        else:
            pref_count = storage.get_preference(uid, section)
            if pref_count and 1 <= pref_count <= 60:
                sent = get_text_with_word_count(managers["E"], pref_count)
                if not sent:
                    sent = managers["E"].get()
            else:
                sent = managers["E"].get()
            storage.del_session(cid, "E")
            storage.save_session(uid, cid, "E", sent, time.time(), sent=True, random_mode=False)
            display = format_display(sent)
            await u.message.reply_text(display)
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "E"))
        return

    if text == "تغيير رق":
        async with chat_locks[cid]:
            storage.log_cmd("تغيير رق")
            keyboard = [
                [InlineKeyboardButton("لفظ", callback_data="رق_لفظ")],
                [InlineKeyboardButton("ارقام صعبة", callback_data="رق_صعبة")],
                [InlineKeyboardButton("ارقام سهلة", callback_data="رق_سهلة")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await u.message.reply_text("إذا تبي تغير نهج الأرقام اختر:\n\n", reply_markup=reply_markup)
        return

    if text == "ريست رق":
        async with chat_locks[cid]:
            storage.log_cmd("ريست رق")
            storage.save_preference(uid, "رق_عدد", None)
            await u.message.reply_text("تم إعادة تعيين عدد الأرقام للوضع العشوائي")
        return

    if text.startswith("رق "):
        async with chat_locks[cid]:
            try:
                num_str = text.replace("رق ", "").strip()
                num = int(num_str)
                if 1 <= num <= 40:
                    storage.log_cmd(f"رق {num}")
                    storage.save_preference(uid, "رق_عدد", num)

                    await u.message.reply_text(f"تم حولنا رق لـ {num} {'رقم' if num == 1 else 'أرقام'}")
                    await asyncio.sleep(0.5)

                    saved_type = storage.get_number_type(uid)

                    if saved_type == "لفظ":
                        sent = generate_random_sentence(uid, NUMBER_WORDS, num, num, "رق")
                        storage.del_session(cid, "رق")
                        storage.save_session(uid, cid, "رق", sent, time.time(), sent=True)
                        display = format_display(sent)
                        await u.message.reply_text(display)
                        asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "رق"))
                    elif saved_type == "صعبة":
                        numbers, numbers_str = generate_hard_numbers_sentence(num)
                        storage.del_session(cid, "رق_صعبة")
                        storage.save_session(uid, cid, "رق_صعبة", numbers_str, time.time(), sent=True)
                        await u.message.reply_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
                    elif saved_type == "سهلة":
                        numbers, numbers_str = generate_easy_numbers_sentence(num)
                        storage.del_session(cid, "رق_سهلة")
                        storage.save_session(uid, cid, "رق_سهلة", numbers_str, time.time(), sent=True)
                        await u.message.reply_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
                    else:
                        numbers, numbers_str = generate_hard_numbers_sentence(num)
                        storage.del_session(cid, "رق_صعبة")
                        storage.save_session(uid, cid, "رق_صعبة", numbers_str, time.time(), sent=True)
                        await u.message.reply_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
                else:
                    await u.message.reply_text("الرجاء إدخال رقم بين 1 و 40")
            except ValueError:
                pass
        return

    if command == "رق" or text == "رق":
        async with chat_locks[cid]:
            if current_time - sent_message_tracker[cid]["رق"] < 0.5:
                return
            storage.log_cmd("رق")
            saved_type = storage.get_number_type(uid)
            pref_count = storage.get_preference(uid, "رق_عدد")

            if saved_type == "لفظ":
                count = pref_count or random.randint(7, 20)
                sent = generate_random_sentence(uid, NUMBER_WORDS, count, count, "رق")
                storage.del_session(cid, "رق")
                storage.save_session(uid, cid, "رق", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "رق"))
            elif saved_type == "صعبة":
                count = pref_count or random.randint(5, 10)
                numbers, numbers_str = generate_hard_numbers_sentence(count)
                storage.del_session(cid, "رق_صعبة")
                storage.save_session(uid, cid, "رق_صعبة", numbers_str, time.time(), sent=True)
                await u.message.reply_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
            elif saved_type == "سهلة":
                count = pref_count or random.randint(5, 10)
                numbers, numbers_str = generate_easy_numbers_sentence(count)
                storage.del_session(cid, "رق_سهلة")
                storage.save_session(uid, cid, "رق_سهلة", numbers_str, time.time(), sent=True)
                await u.message.reply_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
            else:
                keyboard = [
                    [InlineKeyboardButton("لفظ", callback_data="رق_لفظ")],
                    [InlineKeyboardButton("ارقام صعبة", callback_data="رق_صعبة")],
                    [InlineKeyboardButton("ارقام سهلة", callback_data="رق_سهلة")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await u.message.reply_text("اختر نوع الأرقام:", reply_markup=reply_markup)
            sent_message_tracker[cid]["رق"] = current_time
        return

    if command == "حر" or text == "حر":
        async with chat_locks[cid]:
            message_id = f"{cid}_حر_{uid}_{current_time}"

            if current_time - sent_message_tracker[cid]["حر"] < 0.5:
                return

            storage.log_cmd("حر")

            if word_count and 1 <= word_count <= 60:
                storage.save_preference(uid, "حر", word_count)
                sent = generate_random_sentence(uid, LETTER_WORDS, word_count, word_count, "حر")
                storage.del_session(cid, "حر")
                storage.save_session(uid, cid, "حر", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "حر"))
            else:
                pref_count = storage.get_preference(uid, "حر")
                if pref_count and 1 <= pref_count <= 60:
                    sent = generate_random_sentence(uid, LETTER_WORDS, pref_count, pref_count, "حر")
                else:
                    sent = generate_random_sentence(uid, LETTER_WORDS, 7, 20, "حر")
                storage.del_session(cid, "حر")
                storage.save_session(uid, cid, "حر", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "حر"))

            sent_message_tracker[cid]["حر"] = current_time
        return

    if command == "جب" or text == "جب":
        async with chat_locks[cid]:
            message_id = f"{cid}_جب_{uid}_{current_time}"

            if current_time - sent_message_tracker[cid]["جب"] < 0.5:
                return

            storage.log_cmd("جب")

            if word_count and 1 <= word_count <= 60:
                storage.save_preference(uid, "جب", word_count)
                sent = generate_random_sentence(uid, JAB_WORDS, word_count, word_count, "جب")
                storage.del_session(cid, "جب")
                storage.save_session(uid, cid, "جب", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(f"تم الحين الكلمات {word_count} كلمة في الجملة")
                await asyncio.sleep(0.5)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "جب"))
            else:
                pref_count = storage.get_preference(uid, "جب")
                if pref_count and 1 <= pref_count <= 60:
                    sent = generate_random_sentence(uid, JAB_WORDS, pref_count, pref_count, "جب")
                else:
                    sent = generate_random_sentence(uid, JAB_WORDS, 7, 20, "جب")
                storage.del_session(cid, "جب")
                storage.save_session(uid, cid, "جب", sent, time.time(), sent=True)
                display = format_display(sent)
                await u.message.reply_text(display)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "جب"))

            sent_message_tracker[cid]["جب"] = current_time
        return

    if command == "كرر" or text == "كرر":
        async with chat_locks[cid]:
            if current_time - sent_message_tracker[cid]["كرر"] < 0.5:
                return

            storage.log_cmd("كرر")

            # الحصول على الكلمات المستخدمة في آخر 4 رسائل
            recent_words = storage.get_preference(uid, "كرر_recent_words") or []

            if word_count and 1 <= word_count <= 60:
                storage.save_preference(uid, "كرر", word_count)
                patterns = gen_pattern(uid, word_count, exclude_words=recent_words)
                pattern = " ".join(patterns)
                # حفظ الكلمات المستخدمة
                words_used = [p.split('(')[0] for p in patterns]
                new_recent = (recent_words + words_used)[-4:]
                storage.save_preference(uid, "كرر_recent_words", new_recent)
                storage.del_session(cid, "كرر")
                storage.save_session(uid, cid, "كرر", pattern, time.time(), sent=True)
                await u.message.reply_text(f"تم تحديد كرر بـ {word_count} {'كلمة' if word_count == 1 else 'كلمات'}")
                await asyncio.sleep(0.5)
                await u.message.reply_text(pattern)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, pattern, "كرر"))
            else:
                pref_count = storage.get_preference(uid, "كرر")
                if pref_count and 1 <= pref_count <= 60:
                    patterns = gen_pattern(uid, pref_count, exclude_words=recent_words)
                    pattern = " ".join(patterns)
                else:
                    random_count = random.randint(3, 5)
                    patterns = gen_pattern(uid, random_count, exclude_words=recent_words)
                    pattern = " ".join(patterns)
                # حفظ الكلمات المستخدمة
                words_used = [p.split('(')[0] for p in patterns]
                new_recent = (recent_words + words_used)[-4:]
                storage.save_preference(uid, "كرر_recent_words", new_recent)
                storage.del_session(cid, "كرر")
                storage.save_session(uid, cid, "كرر", pattern, time.time(), sent=True)
                await u.message.reply_text(pattern)
                asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, pattern, "كرر"))

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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "شرط"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "شرط"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "فكك"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "فكك"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "دبل"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "دبل"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "تر"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "تر"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "عكس"))
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
            asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "عكس"))
        return

    best_match = None
    best_elapsed = None

    for session in active_sessions:
        typ = session.get("type")
        orig = session.get("text")
        start_time = session.get("time")
        elapsed = time.time() - start_time

        if elapsed > 120:
            continue

        matched = False

        try:
            if typ in ["جمم", "ويكي", "صج", "شك", "جش", "قص", "نص", "جب"]:
                if match_text(orig, text, "arabic"):
                    matched = True
            elif typ == "فر":
                if match_text(orig, text, "persian"):
                    matched = True
            elif typ == "E":
                if match_text(orig, text, "english"):
                    matched = True
            elif typ == "رق":
                if match_numbers(orig, text):
                    matched = True
            elif typ == "حر":
                if match_text(orig, text, "arabic"):
                    matched = True
            elif typ in ["رق_صعبة", "رق_سهلة"]:
                if match_numbers(orig, text):
                    matched = True
            elif typ == "كرر":
                valid, err = validate_repeat(orig, text)
                if valid:
                    matched = True
            elif typ == "شرط":
                if '||' in orig:
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
        except Exception as e:
            print(f"Error matching session: {e}")
            continue

        if matched:
            if best_match is None or elapsed < best_elapsed:
                best_match = session
                best_elapsed = elapsed

    if best_match:
        typ = best_match.get("type")
        orig = best_match.get("text")
        elapsed = best_elapsed
        random_mode = best_match.get("random_mode", True)

        word_count = count_words_for_wpm(text)
        elapsed = max(elapsed, 0.01)
        wpm = (word_count / elapsed) * 60
        score_typ = 'فكك' if typ == 'فكك_تفكيك' else (typ.split('_')[0] if '_' in typ else typ)
        storage.update_score(uid, score_typ, wpm)
        if random_mode:
            storage.add_correct_sentence(uid)

        mention = f"@{usr}" if usr else name

        round_data = storage.get_round(cid)
        if round_data:
            storage.update_round_activity(cid)
            wins = storage.add_win(cid, uid)
            target = round_data['target']
            wins_list = round_data.get('wins', {})

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
                    f"كفو يا {mention}\n\nسرعتك: {wpm:.2f} كلمة/دقيقة\nالوقت : {elapsed:.2f} ثانية\n"
                    f"مبروك - أنت الفائز في الجولة!\n"
                    f"الفوز رقم: {wins}/{target}"
                    f"{round_stats}"
                )
                storage.end_round(cid)
            else:
                await u.message.reply_text(
                    f"كفو يا {mention}\n\nسرعتك: {wpm:.2f} كلمة/دقيقة\nالوقت : {elapsed:.2f} ثانية\n"
                    f"التقدم: {wins}/{target}"
                    f"{round_stats}"
                )
        else:
            await u.message.reply_text(
                f"كفو يا {mention}\n\nسرعتك: {wpm:.2f} كلمة/دقيقة\nالوقت : {elapsed:.2f} ثانية"
            )

        storage.del_session(cid, typ)

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

async def periodic_stories_update():
    while True:
        await asyncio.sleep(24 * 60 * 60)
        try:
            if "قص" in managers:
                current_time = time.time()
                stories_manager = managers["قص"]
                if (current_time - stories_manager.last_update) >= stories_manager.update_interval:
                    print("[STORIES] Starting scheduled update...")
                    stories_manager.load()
                    print("[STORIES] Scheduled update completed")
        except Exception as e:
            print(f"[STORIES] Error in periodic update: {e}")

async def periodic_nass_update():
    if "نص" not in managers:
        return

    nass_manager = managers["نص"]

    print("[NASS] Starting initial content load...")
    try:
        await asyncio.to_thread(nass_manager.load)
        print("[NASS] Initial load completed")
    except Exception as e:
        print(f"[NASS] Error in initial load: {e}")

    while True:
        await asyncio.sleep(3600)
        try:
            if nass_manager.needs_update():
                print("[NASS] Starting scheduled update...")
                await asyncio.to_thread(nass_manager.load)
                print("[NASS] Scheduled update completed")
        except Exception as e:
            print(f"[NASS] Error in periodic update: {e}")

async def handle_callback(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query = u.callback_query
    uid = u.effective_user.id
    cid = u.effective_chat.id
    await query.answer()

    if query.data == "show_commands":
        await show_bot_commands(u, c, is_callback=True)
        return

    if query.data == "show_sections":
        await show_bot_sections(u, c, is_callback=True)
        return

    if query.data == "رق_لفظ":
        storage.save_number_type(uid, "لفظ")
        pref_count = storage.get_preference(uid, "رق") or random.randint(7, 20)
        sent = generate_random_sentence(uid, NUMBER_WORDS, pref_count, pref_count, "رق")
        storage.del_session(cid, "رق")
        storage.save_session(uid, cid, "رق", sent, time.time(), sent=True)
        display = format_display(sent)
        await query.edit_message_text(display)
        asyncio.create_task(trigger_speed_bot_if_enabled(c, cid, sent, "رق"))
        return

    if query.data == "رق_صعبة":
        storage.save_number_type(uid, "صعبة")
        numbers, numbers_str = generate_hard_numbers_sentence()
        storage.del_session(cid, "رق_صعبة")
        storage.save_session(uid, cid, "رق_صعبة", numbers_str, time.time(), sent=True)
        await query.edit_message_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
        return

    if query.data == "رق_سهلة":
        storage.save_number_type(uid, "سهلة")
        numbers, numbers_str = generate_easy_numbers_sentence()
        storage.del_session(cid, "رق_سهلة")
        storage.save_session(uid, cid, "رق_سهلة", numbers_str, time.time(), sent=True)
        await query.edit_message_text(f"اكتب الأرقام لفظاً:\n\n{numbers_str}")
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
    await init_http_session()
    asyncio.create_task(periodic_save())
    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(periodic_round_cleanup())
    asyncio.create_task(periodic_auto_cleanup())
    asyncio.create_task(periodic_stories_update())
    asyncio.create_task(periodic_nass_update())

    job_queue = application.job_queue
    job_queue.run_daily(daily_leaderboard_job, time=datetime.now().replace(hour=0, minute=0, second=0).time(), name="daily_leaderboard")

    print("[BACKGROUND] Started background tasks: periodic_save, periodic_cleanup, periodic_round_cleanup, periodic_auto_cleanup, periodic_stories_update, periodic_nass_update")
    print("[BACKGROUND] Scheduled daily leaderboard job at 00:00 (midnight)")
    print("[HTTP] Initialized shared HTTP session")

async def shutdown(application: Application):
    print("[SHUTDOWN] Saving final data...")
    storage.save(force=True)
    print("[HTTP] Closing HTTP session...")
    await close_http_session()
    print("[SHUTDOWN] Cleanup complete")

def main():
    if not BOT_TOKEN:
        print("Error: BOT_TOKEN environment variable not set!")
        return

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(8).post_init(post_init).post_shutdown(shutdown).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

    print("Bot starting...")
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        print("[SHUTDOWN] Keyboard interrupt received")
    except Exception as e:
        print(f"[ERROR] Bot error: {e}")
        import time
        time.sleep(2)

if __name__ == '__main__':
    main()
