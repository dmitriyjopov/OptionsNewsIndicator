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
from selenium.common.exceptions import NoSuchElementException
from nltk.tokenize import sent_tokenize
import re, json, time, trafilatura, nltk, csv, os, logging
from datetime import datetime, timedelta
from googlenewsdecoder import gnewsdecoder

# --- ИНИЦИАЛИЗАЦИЯ ---
nltk.download('punkt')
nltk.download('punkt_tab')

if not os.path.exists('logs'):
    os.makedirs('logs')

date_logger = logging.getLogger('date_logger')
date_logger_handler = logging.FileHandler('logs/date_compare.log', mode='w', encoding='utf-8')
date_logger.setLevel(logging.INFO)
date_logger.addHandler(date_logger_handler)

url_logger = logging.getLogger('url_logger')
url_logger_handler = logging.FileHandler('logs/url.log', mode='w', encoding='utf-8')
url_logger.setLevel(logging.INFO)
url_logger.addHandler(url_logger_handler)

missmatched_dates_logger = logging.getLogger('missmatched_dates_logger')
missmatched_dates_logger_handler = logging.FileHandler('logs/missmatched_dates.log', mode='w', encoding='utf-8')
missmatched_dates_logger.setLevel(logging.INFO)
missmatched_dates_logger.addHandler(missmatched_dates_logger_handler)

# --- КОНСТАНТЫ И ПАТТЕРНЫ ---
excluded_domains = [
    'banki.ru/services/responses',
    'smart-lab.ru',
    'www1.ru',
    'neperm.ru',
    'cheboksary.ru',
    'yamal1.ru'
]

meta_selectors = [
    "meta[property='article:published_time']",
    "meta[itemprop='datePublished']",
    "meta[itemprop='dateModified']",
    "meta[name='publish-date']",
    "meta[property='og:published_time']",      # Очень частый тег (Open Graph)
    "meta[name='pubdate']",                   # Встречается на старых сайтах
    "meta[name='originalPublicationDate']",   # Встречается у агрегаторов
    "link[rel='canonical']"
]

js_scripts = [

]

possible_time_classes = [
    "js-ago", "date", "news-item-header--date", "b-post-time",
    "article__info-date", "timestamp", "entry-date", "pWvg"
    "c-post__date", "page-styles__date", "news-detail-date", "tag-date", 
    "SHTMLCode", "article-details__date", "b-article__date",
    "time", "full_news_date", "article-header__author-writing-date",
    "article-date", "el-time", "date material__date", "date3", "date_item"
]

possible_selectors = [
    "span[title='Дата публикации']", "div[title='Дата публикации']",
    "span[data-id='date']", "div[data-test='text']", "div[id='info-text-photo-date']",
    "div.fn-rubric-link > div", ".text-grey.text-sm.span1",
    ".tg-label-standard-regular-4b7-9-0-0.KVFz2",
    "time", "div.text-nowrap.d-flex.flex-wrap.gap-3 > div", 
    "div[class='MatterTop_date__mPSNt flex gap-[8px] mb-[16px] font-medium']",
    "div[class='tg-label-standard-regular-4b7-9-0-1 KVFz2']", "div[data-test='article-created-at']",
    "[data-qa='Datetime']", "[itemprop='datePublished']", "div[data-e2e-id='data-dynamic']"
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

patterns = [
    r'^\d{4}—\d{4}$', # Интервалы типа 2024—2025
]

failed_urls_days = {}

def translate_month(date_str):
    """Заменяет русские месяцы на английские для корректного парсинга."""
    if not date_str: return date_str
    date_str = date_str.lower()
    for ru, en in RU_MONTH_VALUES.items():
        if ru in date_str:
            date_str = date_str.replace(ru, en)
            break
    return date_str

def robust_parse(date_str):
    if not date_str:
        return None
    
    # 1. Предварительная очистка
    date_str = date_str.lower().strip()
    date_str = date_str.replace('t', ' ').replace('z', '')

    # 2. ISO формат (самый приоритетный)
    try:
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return parser.isoparse(date_str)
    except: pass

    # 3. ФУНДАМЕНТ ДЛЯ ПАТТЕРНОВ
    # Месяцы для поиска внутри паттернов
    months_map = {
        'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
        'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    # Список регулярок. Сюда ты можешь легко добавлять новые.
    # Структура: (регулярка, содержит_ли_время)
    custom_patterns = [
        # Паттерн: "03 декабря 2025, 11:35" или "3 дек 2025 11:35"
        (r'(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})[,\s]+(\d{1,2}):(\d{1,2})', True),
        
        # Паттерн: "03 декабря 2025" или "3 дек 2025"
        (r'(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})', False),
        
        # "Дата публикации: 02 дек 2025"
        (r'Дата публикации:\s*(\d{1,2})\s+([а-яa-z]+)\s+(\d{4})', False),

        # 04.12.2025 в 07:56
        (r'(\d{2})\.(\d{2})\.(\d{4})\s+в\s+(\d{2}):(\d{2})', True),

        # 04.12.2025 07:56
        (r'(\d{2})\.(\d{2})\.(\d{4})\s*(?:в)?\s*(\d{2}):(\d{2})', True),

        # 5 декабря 2025 в 11:36
        (r'(\d{1,2})\s+([а-яa-z]+)\s+в\s+(\d{4})', True)

        # 17:36, 14 декабря 2025 или 17:36 14 декабря 2025 
        (r'(\d{1,2}:\d{2})[,]?\s+(\d{1,2})\s+([а-яА-Яa-zA-Z]+)\s+(\d{4})', True)
    ]

    for pattern, has_time in custom_patterns:
        match = re.search(pattern, date_str)
        if match:
            groups = match.groups()
            day = int(groups[0])
            month_name = groups[1]
            year = int(groups[2])
            
            # Определяем номер месяца
            m_val = None
            for m_name, m_num in months_map.items():
                if m_name in month_name:
                    m_val = m_num
                    break
            
            if m_val:
                try:
                    if has_time:
                        hour = int(groups[3])
                        minute = int(groups[4])
                        return datetime(year, m_val, day, hour, minute)
                    else:
                        return datetime(year, m_val, day)
                except ValueError: # На случай если дата некорректная (32 декабря)
                    continue

    # 4. Последний шанс (стандартный парсер)
    try:
        # Используем твою функцию перевода для библиотечного парсера
        translated = translate_month(date_str)
        # Ограничиваем fuzzy, чтобы он не подтягивал сегодняшнее число (26-е)
        return parser.parse(translated, dayfirst=True, yearfirst=False, fuzzy=False)
    except:
        # Если совсем ничего не помогло, пробуем fuzzy напоследок
        try:
            return parser.parse(translated, dayfirst=True, fuzzy=True, default=datetime(2000, 1, 1))
        except:
            return None

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

def days_match(date_str, default_date_str):
    if not date_str or not default_date_str:
        return False
    try:
        clean_date_str = translate_month(date_str)
        
        # 1. dayfirst=True — ГЛАВНОЕ ИСПРАВЛЕНИЕ для формата ДД.ММ
        # 2. yearfirst=False — чтобы исключить трактовку 12.02.2025 как ГГ.ММ.ДД
        d1 = parser.parse(
            clean_date_str, 
            fuzzy=True, 
            dayfirst=True, 
            yearfirst=False, 
        )
        
        d2 = parser.parse(default_date_str)
        
        # Расширенное логирование, чтобы видеть входную строку
        missmatched_dates_logger.info(
            f'INPUT: "{clean_date_str}" | PARSED D1: {d1} | TARGET D2: {d2} | '
            f'RESULT: {d1.day == d2.day and d1.year == d2.year}'
        )
        
        return abs(d1.day - d2.day) <=1 and d1.year == d2.year
    except Exception as e:
        # Логируем даже ошибку парсинга
        missmatched_dates_logger.error(f'Error parsing "{date_str}": {e}')
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

def extract_page_date(driver, url, gnews_date_str):
    """
    Ищет все возможные даты. 
    Если находит дату, совпадающую с GNews — возвращает её.
    Если находит только другие даты — возвращает первую попавшуюся и пишет в missmatched лог.
    Если не находит вообще ничего — возвращает None.
    """
    found_dates = [] 

    def process_element(raw_value, source):
        if not raw_value or len(raw_value) <= 5: return
        parsed = robust_parse(raw_value)
        if parsed:
            found_dates.append((parsed, source, raw_value))

    # scripts
    for item in js_scripts:
        scripts = driver.find_elements(By.CSS_SELECTOR, item)
        for script in scripts:
            try:
                # Получаем текст внутри скрипта и превращаем в словарь Python
                json_text = script.get_attribute("textContent")
                data = json.loads(json_text)
                
                # JSON-LD может быть списком словарей или одним словарем
                if isinstance(data, list):
                    items = data
                else:
                    items = [data]
                    
                for item in items:
                    # Ищем ключ datePublished (или dateCreated / publishedTime)
                    d_pub = item.get("datePublished") or item.get("dateCreated")
                    if d_pub:
                        process_element(d_pub, "json-ld:datePublished")
            except:
                continue

    # Meta
    for s in meta_selectors:
        try:
            val = driver.find_element(By.CSS_SELECTOR, s).get_attribute("content")
            process_element(val, f"meta:{s}")
        except: continue

    # Селекторы и классы
    all_selectors = possible_time_classes + possible_selectors
    for item in all_selectors:
        sel = item if any(c in item for c in ['.', '[', '#']) else f".{item}"
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in elements:
                process_element(el.get_attribute("datetime"), f"attr:datetime in {sel}")
                process_element(el.get_attribute("content"), f"attr:content in {sel}")
                process_element(el.text.strip(), f"text in {sel}")
        except: continue

    # --- 2. АНАЛИЗ РЕЗУЛЬТАТОВ ---
    if not found_dates:
        # Вообще ничего не нашли — возвращаем None, URL уйдет в failed_dates
        return None

    target_date = None
    try:
        target_date = parser.parse(gnews_date_str)
    except: pass

    best_date = None
    log_lines = [f"URL: {url}", f"GNews Target: {gnews_date_str}", "Found attempts:"]
    match_found = False

    for dt, src, raw in found_dates:
        # Проверка по твоему условию: день и год (добавил месяц для точности)
        is_match = False
        if target_date:
            is_match = (abs(dt.day - target_date.day) <= 1 and 
                        dt.month == target_date.month and 
                        dt.year == target_date.year and
                        dt.time is not None)
        
        status = "[MATCH]" if is_match else "[NO MATCH]"
        log_lines.append(f"  {status} Src: {src} | Raw: '{raw}' | Parsed: {dt.strftime('%Y-%m-%d')}")
        
        if is_match and not match_found:
            best_date = dt
            match_found = True
        elif not best_date:
            # Если совпадения еще нет, запоминаем первую попавшуюся как запасную
            best_date = dt

    # Если мы нашли даты, но ни одна не совпала — пишем весь блок в лог
    if not match_found:
        missmatched_dates_logger.info("\n".join(log_lines) + "\n" + "-"*60)
    
    return best_date

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    chrome_options.add_argument("--ignore-ssl-errors=yes")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
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
    driver = init_driver()
    all_news = []
    failed_dates = [] # Сюда попадут только URL с полным нулем
    google_news = GNews(language='ru', country='RU', max_results=100, exclude_websites=excluded_domains)

    try:
        google_news.start_date = (start_date.year, start_date.month, start_date.day)
        google_news.end_date = (end_date.year, end_date.month, end_date.day)
        results = google_news.get_news(keyword)
        
        for item in results:
            url = item['url']

            decoded_url = gnewsdecoder(url)
            if any(domain in decoded_url for domain in excluded_domains):
                continue

            try:
                driver.get(url)
                time.sleep(2)
                html = driver.page_source
                
                if any(x in html for x in ["Национального УЦ Минцифры", "403 Error"]):
                    continue

                text = trafilatura.extract(html, include_comments=False)
                
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
            except Exception as e:
                print(f"❌ Ошибка на {url}: {e}")
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
                
        current_date = next_date

if __name__ == "__main__":
    main()