import pandas as pd
from gnews import GNews
from dateutil import parser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sumy.parsers.plaintext import PlaintextParser
from selenium.webdriver.common.by import By
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from nltk.tokenize import sent_tokenize
import undetected_chromedriver as uc
import re, json, time, trafilatura, nltk, csv, os, logging
from datetime import datetime, timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from googlenewsdecoder import gnewsdecoder
from collections import deque
from pathlib import Path

nltk.download('punkt')
nltk.download('punkt_tab')

BASE_DIR = Path(__file__).parent  # или parent.parent в зависимости от структуры
LOG_DIR = BASE_DIR / 'logs'

os.makedirs(LOG_DIR, exist_ok=True)

date_logger = logging.getLogger('date_logger')
date_logger_handler = logging.FileHandler(LOG_DIR / 'date_compare.log', mode='w', encoding='utf-8')
date_logger.setLevel(logging.INFO)
date_logger.addHandler(date_logger_handler)

url_logger = logging.getLogger('url_logger')
url_logger_handler = logging.FileHandler(LOG_DIR / 'url.log', mode='w', encoding='utf-8')
url_logger.setLevel(logging.INFO)
url_logger.addHandler(url_logger_handler)

missmatched_dates_logger = logging.getLogger('missmatched_dates_logger')
missmatched_dates_logger_handler = logging.FileHandler(LOG_DIR / 'missmatched_dates.log', mode='w', encoding='utf-8')
missmatched_dates_logger.setLevel(logging.INFO)
missmatched_dates_logger.addHandler(missmatched_dates_logger_handler)

# --- КОНСТАНТЫ И ПАТТЕРНЫ ---
excluded_domains = [
    'banki.ru/services/responses',
    'smart-lab.ru/blog',
    'www1.ru',
    'neperm.ru',
    'cheboksary.ru',
    'yamal1.ru',
    'sberbank.ru', # надо пофиксить
    'arbuztoday.ru',
    'blog.domclick.ru'
]

meta_selectors = [
    "meta[property='article:published_time']",
    "meta[itemprop='datePublished']",
    "meta[itemprop='dateModified']",
    "meta[name='publish-date']",
    "meta[property='og:published_time']", 
    "meta[name='pubdate']",
    "meta[name='originalPublicationDate']",
    "link[rel='canonical']"
]

js_scripts = [
    'application/ld+json'
]

possible_time_classes = [
    "js-ago", "date", "news-item-header--date", "b-post-time", "post-time",
    "article__info-date", "timestamp", "entry-date", "pWvg",
    "c-post__date", "page-styles__date", "news-detail-date", "tag-date", 
    "SHTMLCode", "article-details__date", "b-article__date", "article__date", 
    "time", "full_news_date", "article-header__author-writing-date",
    "article-date", "el-time", "date material__date", "date3", "date_item",
    "article-date-desktop", "article-meta__date", "faq_date","post-info__date",
    "desc"
]

possible_selectors = [
    "span[title='Дата публикации']", "div[title='Дата публикации']",
    "span[data-id='date']", "div[data-test='text']", "div[id='info-text-photo-date']",
    "div.fn-rubric-link > div", ".text-grey.text-sm.span1",
    ".tg-label-standard-regular-4b7-9-0-0.KVFz2",
    "time", "div.text-nowrap.d-flex.flex-wrap.gap-3 > div", 
    "div[class='MatterTop_date__mPSNt flex gap-[8px] mb-[16px] font-medium']",
    "div[class='tg-label-standard-regular-4b7-9-0-1 KVFz2']", "div[data-test='article-created-at']",
    "[data-qa='Datetime']", "[itemprop='datePublished']", "div[data-e2e-id='data-dynamic']",
    "div[class='col-auto fw-bold']"
]

RU_MONTH_VALUES = {
    'января': 'January', 'февраля': 'February', 'марта': 'March',
    'апреля': 'April', 'мая': 'May', 'июня': 'June',
    'июля': 'July', 'августа': 'August', 'сентября': 'September',
    'октября': 'October', 'ноября': 'November', 'декабря': 'December',
    # сокращения с точками
    'янв.': 'January', 'фев.': 'February', 'март.': 'March',
    # сокращения без точек
    'янв': 'January', 'фев': 'February', 'март': 'March',
}

months_map = {
    'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
    'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
    # сокразения на инглише
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    # сокращения (с точкой)
    'янв.': 1, 'фев.': 2, 'мар.': 3, 'апр.': 4, 'май': 5, 'июн': 6,
    'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
    # полные месяца (родительный падеж)
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    # полные названия (именительный падеж)
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

custom_patterns = { # формат списка [позиция числа, позиция месяца, позиция года, часы, минуты]
    # "2025-12-01 23:13:00+07:00" - из json
    (r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):\d{2}[+-]\d{2}:\d{2}', True): [0, 1, 2, 3, 4],

    # 2025-12-01 13:08:28
    (r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})', True): [2, 1, 0, 3, 4],

    # 12-12-2026
    (r'(\d{4})-(\d{2})-(\d{2})$', False): [0, 1, 2],

    # Паттерн: "03 декабря 2025, 11:35" или "3 дек 2025 11:35"
    (r'(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})[,\s]+(\d{1,2}):(\d{1,2})', True): [0, 1, 2, 3, 4],
    
    # Паттерн: "03 декабря 2025" или "3 дек 2025"
    (r'(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})', False): [0, 1, 2, None, None],
    
    # "Дата публикации: 02 дек 2025"
    (r'дата публикации:\s*(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})', False): [1, 2, 3, None, None],

    # 04.12.2025 в 07:56
    (r'(\d{2})\.(\d{2})\.(\d{4})\s+в\s+(\d{1,2}):(\d{2})', True): [0, 1, 2, 3, 4],

    # 04.12.2025 07:56
    (r'(\d{2})\.(\d{2})\.(\d{4})\s*(?:в)?\s*(\d{1,2}):(\d{2})', True): [0, 1, 2, 3, 4],

    # 5 декабря 2025 в 11:36
    (r'(\d{1,2})\s+([а-яa-z]+)\s+в\s+(\d{4})', True): [0, 1, 2, 3, 4],

    # 17:36, 14 декабря 2025 или 17:36 14 декабря 2025 
    (r'(\d{1,2}):(\d{2})[,\s]+(\d{1,2})\s+([а-яёa-z]+)\s+(\d{4})', True): [2, 3, 4, 0, 1],

    # 1. 05.07.2022 г. (с точкой в конце и "г.")
    (r'(\d{2})\.(\d{2})\.(\d{4})\s*г\.', False): [0, 1, 2, None, None],
    
    # 2. 30.12.25 12:53 (двузначный год)
    (r'(\d{2})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})', True): [0, 1, 2, 3, 4],
    
    # 3. пт, 02/27/2026 - 17:27 (с днем недели, слешами и дефисом)
    (r'[а-я]{2},\s*(\d{2})/(\d{2})/(\d{4})\s*-\s*(\d{2}):(\d{2})', True): [0, 1, 2, 3, 4],

    # 09.12.2025 | 18:47
    (r'(\d{2})\.(\d{2})\.(\d{4})\s*|\s*(\d{2}):(\d{2})', True): [0, 1, 2, 3, 4],

    # 4 декабря 2025 года, 11:04
    (r'(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})\s+года?\s*[,]?\s*(\d{1,2}):(\d{2})', True): [0, 1, 2, 3, 4]
}

patterns = [
    r'^\d{4}—\d{4}$', # Интервалы типа 2024—2025
]

failed_urls_days = {}
processing_url = {}
attempted_match_logs = []

def is_date_suitable(parsed_date, target_date, date_source, raw_date, has_time):

    # Твоя проверка (разница дней <= 1, месяц и год совпадают)
    is_day_match = (abs(parsed_date.date().day - target_date.date().day) <= 1 and 
                    parsed_date.month == target_date.month and 
                    parsed_date.year == target_date.year)
        
    if is_day_match and has_time:
        missmatched_dates_logger.info(f"  [PERFECT MATCH] Src: {date_source} | Raw: '{raw_date}' | Parsed: {parsed_date}")
        return "perfect"
    elif is_day_match:
        missmatched_dates_logger.info(f"  [PARTIAL MATCH (No Time)] Src: {date_source} | Raw: '{raw_date}' | Parsed: {parsed_date}")
        return "partial"
    else:
        missmatched_dates_logger.info(f"  [NO MATCH] Src: {date_source} | Raw: '{raw_date}' | Parsed: {parsed_date} | Days diff: {abs(parsed_date.date().day - target_date.date().day)} | Target days: {target_date.day}")
        return "none"
        
def translate_month(date_str):
    """Заменяет русские месяцы на английские для корректного парсинга."""
    if not date_str: return date_str
    date_str = date_str.lower()
    for ru, en in RU_MONTH_VALUES.items():
        if ru in date_str:
            date_str = date_str.replace(ru, en)
            break
    return date_str

def robust_parse(date_str, default_date_obj=None):
    """Парсит строку в datetime. Если находит только время — склеивает с default_date_obj."""
    if not date_str:
        return None

    date_str = date_str.lower().strip().replace('t', ' ').replace('z', '')

    # 1. ОБРАБОТКА "ТОЛЬКО ВРЕМЯ" (Например: "18:30" или "18:30:00")
    time_match = re.search(r'^(\d{1,2}):(\d{2})', date_str)
    if time_match and len(date_str) <= 8:
        # ПРАВКА: Теперь мы уверены, что работаем с объектом datetime
        if isinstance(default_date_obj, datetime):
            hour, minute = int(time_match.group(1)), int(time_match.group(2))
            return default_date_obj.replace(hour=hour, minute=minute, second=0, microsecond=0), True
        return None, False # Если объекта даты нет, время бесполезно

    for (pattern, has_time), indices in custom_patterns.items():
            match = re.search(pattern, date_str)
            if match:
                groups = match.groups()
                try:
                    # Извлекаем данные по индексам из схемы
                    d_idx, m_idx, y_idx, h_idx, min_idx = indices
                    
                    day = int(groups[d_idx]) if d_idx is not None else (default_date_obj.day if default_date_obj else 1)
                    year = int(groups[y_idx]) if y_idx is not None else (default_date_obj.year if default_date_obj else 2025)
                    
                    # Логика месяца (название или число)
                    month_raw = groups[m_idx] if m_idx is not None else None
                    if not month_raw:
                        month = default_date_obj.month if default_date_obj else 1
                    elif month_raw.isdigit():
                        month = int(month_raw)
                    else:
                        month = None
                        for k, v in months_map.items():
                            if k in month_raw:
                                month = v
                                break
                        if not month: continue # Месяц не распознан
                    
                    hour = int(groups[h_idx]) if h_idx is not None else 8
                    minute = int(groups[min_idx]) if min_idx is not None else 0
                    
                    return datetime(year, month, day, hour, minute), has_time
                except Exception:
                    continue

    # 3. ФОЛЛБЕК (БИБЛИОТЕКА)
    try:
        translated = translate_month(date_str)
        # Убираем fuzzy=False, так как в мета-тегах часто бывает лишний текст
        dt = parser.parse(translated, dayfirst=False, yearfirst=True, fuzzy=True)
        return dt, (':' in date_str)
    except:
        return None, False

def is_bad_pattern(text, url):
    """Проверяет, соответствует ли текст нежелательным паттернам."""
    if not text:
        date_logger.info(f'НЕТ ТЕКСТА В URL {url}')
        return True
    text = text.strip()
    for pattern in patterns:
        if re.match(pattern, text):
            date_logger.info(f'ПЛОХОЙ ПАТТЕРН В URL {url}')
            return True
    return False

def is_valid_date_string(date_str):
    """
    Универсальная проверка: существует ли строка, 
    достаточно ли она длинная и не содержит ли мусора.
    """
    if not date_str:
        return False
    
    date_str = date_str.strip()
    
    # Отсекаем "12:30", "5 мин" и пустые строки
    if len(date_str) <= 5:
        return False
        
    # Проверяем на плохие паттерны
    for pattern in patterns:
        if re.match(pattern, date_str):
            return False
            
    return True

def find_key_recursive(obj, key_to_find):
    """Рекурсивный поиск ключа в словаре или списке."""
    if isinstance(obj, dict):
        if key_to_find in obj:
            return obj[key_to_find]
        for v in obj.values():
            result = find_key_recursive(v, key_to_find)
            if result: return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_key_recursive(item, key_to_find)
            if result: return result
    return None

def extract_page_date(driver, url, gnews_date_str):
    missmatched_dates_logger.info(f"\nURL: {url}\nGNews Target: {gnews_date_str}\nFound attempts:")
    try:
        gnews_dt_obj = parser.parse(gnews_date_str)
    except:
        gnews_dt_obj = None

    fallback_date = {"date": None, "has_time": False}

    def process_element(raw_value, source):
        nonlocal fallback_date
        if not raw_value or len(raw_value.strip()) < 4: return None

        # Получаем дату и флаг наличия времени
        parsed_dt, has_time = robust_parse(raw_value, gnews_date_str)
        
        if parsed_dt:
            # Твоя оригинальная проверка match_status
            match_status = is_date_suitable(parsed_dt, gnews_dt_obj, source, raw_value, has_time)
            
            if match_status == "perfect":
                return parsed_dt # СРАЗУ ВЫХОДИМ! Нашли идеальное совпадение
            
            elif match_status == "partial":
                # ПРАВКА: Логика обновления словаря
                # Обновляем если: еще ничего нет ИЛИ если новая дата с временем, а старая была без
                if not fallback_date["date"] or (has_time and not fallback_date["has_time"]):
                    fallback_date["date"] = parsed_dt
                    fallback_date["has_time"] = has_time
                    missmatched_dates_logger.info(f"  [FALLBACK UPDATED] {source}: {parsed_dt} (has_time: {has_time})")

    # scripts
    for item in js_scripts:
        try:
            WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, f'script[type="{item}"]')))
            scripts = driver.find_elements(By.CSS_SELECTOR, f'script[type="{item}"]')

            for script in scripts:
                try:         
                    json_text = script.get_attribute("textContent")
                    data = json.loads(json_text)
                    res = find_key_recursive(data, "datePublished") or find_key_recursive(data, "dateCreated")
                    if res := process_element(res, "json-ld:datePublished"): 
                        return res
                            
                except:
                    continue
        except:
            url_logger.info(f'webdriverWait/find_element ERROR | url {url} | GnewsDate: {gnews_date_str}')
            pass

    # Meta
    for s in meta_selectors:
            try:
                val = driver.find_element(By.CSS_SELECTOR, s).get_attribute("content")
                if res := process_element(val, f"meta:{s}"):
                    return res
            except: continue

    # Селекторы и классы
    all_selectors = possible_time_classes + possible_selectors
    for item in all_selectors:
        if not any(c in item for c in ['.', '[', '#']):
            search_variants = [item, f".{item}"] # и 'time', и '.time'
        else:
            search_variants = [item]
        for sel in search_variants:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in elements:
                    if res := process_element(el.get_attribute("datetime"), f"attr:datetime in {sel}"): return res
                    if res := process_element(el.get_attribute("content"), f"attr:content in {sel}"): return res
                    if res := process_element(el.text.strip(), f"text in {sel}"): return res
            except: continue

    if fallback_date:
        missmatched_dates_logger.info("  --> Returned PARTIAL match (no time found).")
    else:
        missmatched_dates_logger.info(f"  [NOT FOUND] No dates found for URL in any source.")
    return fallback_date

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    chrome_options.add_argument("--ignore-ssl-errors=yes")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    chrome_options.page_load_strategy = 'normal'
    prefs = {
        "profile.managed_default_content_settings.images": 2, # 2 = блокировать
        "profile.default_content_settings.ads": 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(10)
    return driver

def init_stealth_driver():
    options = uc.ChromeOptions()

    options.add_argument("--disable-blink-features=AutomationControlled")

    options.add_argument("--window-position=3200,0") 
    options.add_argument("--window-size=1280,600")

    driver = uc.Chrome(options=options, version_main=145)
    return driver

def get_summary(text, max_sentences=4):
    if not text or len(text) < 100: return "Текст слишком короткий"
    clean_text = " ".join(text.replace("\n", " ").split())
    try:
        parser_sum = PlaintextParser.from_string(clean_text, Tokenizer("russian"))
        summarizer = LexRankSummarizer()
        sumy_result = summarizer(parser_sum.document, max_sentences)
        raw_summary = " ".join([str(s) for s in sumy_result])
        real_sentences = sent_tokenize(raw_summary, language="russian")
        return ' '.join(real_sentences[:max_sentences]) if real_sentences else raw_summary
    except Exception as e:
        return f"Ошибка обработки: {e}"

def fetch_with_selenium(keyword, start_date, end_date):
    driver = init_stealth_driver()
    all_news = []
    failed_dates = [] # Сюда попадут только URL с полным нулем
    google_news = GNews(language='ru', country='RU', max_results=100, exclude_websites=excluded_domains)

    try:
        google_news.start_date = (start_date.year, start_date.month, start_date.day)
        google_news.end_date = (end_date.year, end_date.month, end_date.day)
        results = google_news.get_news(keyword)
        
        for item in results:
            url = item['url']
            try:                  
                decoded_data = gnewsdecoder(url)
                decoded_url = decoded_data.get('decoded_url', url) if isinstance(decoded_data, dict) else str(decoded_data)
            
            except:
                decoded_url = url

            if any(domain in decoded_url for domain in excluded_domains):
                continue

            try:
                driver.get(decoded_url)
                # time.sleep(3)
                html = driver.page_source
                
                if any(x in html for x in ["Национального УЦ Минцифры", "403 Error"]):
                    continue

                text = trafilatura.extract(html, include_comments=False)

                if not text:
                    url_logger.warning(f"BLOCKED | Текст не извлечен из html для {url} | GnewsDate: {item.get('published date')}")
                    continue

                elif len(text) < 300:
                    url_logger.warning(f"SHORT TEXT | Слишком короткий текст на {url} | GnewsDate: {item.get('published date')}")
                    continue
                
                
                # ВЫЗОВ ФУНКЦИИ (теперь без лишних параметров внутри)
                page_date = extract_page_date(driver, url, item.get('published date'))

                if page_date:
                    # Дата найдена (неважно, совпала или нет)
                    final_date = page_date
                    date_logger.info(f"OK | Дата: {final_date} | URL: {url}")
                else:
                    # ВООБЩЕ ничего не нашли по всем спискам
                    failed_dates.append(url)
                    final_date = item.get('published date')
                    url_logger.warning(f"EMPTY | Элементы даты не найдены на {url}")
                
                if text:
                    all_news.append({
                        'date': item.get('published date'),
                        'scraped_date': final_date,
                        'title': item.get('title'),
                        'url': driver.current_url,
                        'summary': get_summary(text)
                    })
            except TimeoutException:
                failed_dates.append(url)
                url_logger.warning(f"TimeoutException | Страница не загрузилась за 10 сек {url} | GnewsDate: {item.get('published date')}")
            except WebDriverException as e:
                failed_dates.append(url)
                if "Timed out receiving message from renderer" in str(e):
                    url_logger.warning(f"WEBDRIVER TIMEOUT | Ошибка выполнения запроса на {url} | GnewsDate: {item.get('published date')}")
                else:
                    url_logger.warning(f"WEBDRIVER UNKNOWN ERROR | Неизвестная ошибка выполнения запроса на {url} | GnewsDate: {item.get('published date')}")
            except Exception as e:
                failed_dates.append(url)
                url_logger.warning(f"UNKNOW NERROR | Неизвестная ошибка выполнения запроса на {url} | GnewsDate: {item.get('published date')}")
    finally:
        driver.quit()
    return pd.DataFrame(all_news), pd.DataFrame(failed_dates)

def main():
    KEYWORD = "сбербанк"
    start_date = datetime(2025, 12, 1)
    end_date = datetime(2026, 2, 23)
    WINDOW = 3
    
    current_date = start_date
    while current_date <= end_date:
        next_date = current_date + timedelta(days=WINDOW)

        try:
            df, failed = fetch_with_selenium(KEYWORD, current_date, next_date)
            
            if not df.empty:
                file_name = f'{KEYWORD}_{WINDOW}day_news.csv'
                df.to_csv(file_name, mode='a', index=False, header=not os.path.exists(file_name), encoding='utf-8-sig')
            
            if not failed.empty:
                for f_url in failed.values:
                    url_key = f_url[0]
                    # Получаем данные или пустой кортеж, если данных нет
                    debug_data = failed_urls_days.get(url_key)
                    
                    if debug_data:
                        extracted_d, default_d, raw_s = debug_data
                        date_logger.info(
                            f"Failed match for: {url_key}\n"
                            f"  -> Default (GNews): {default_d}\n"
                            f"  -> Extracted (Parsed): {extracted_d}\n"
                            f"  -> Raw string from site: '{raw_s}'"
                    )
        except Exception as e:
            url_logger.error(f"Ошибка выполнения: {e}")
        finally:
            current_date = next_date

if __name__ == "__main__":
    main()