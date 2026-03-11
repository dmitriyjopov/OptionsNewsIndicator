from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os, logging, re, json, time
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from googlenewsdecoder import gnewsdecoder
from dateutil import parser
import trafilatura

# --- ЛОГИРОВАНИЕ ---
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

#  --- СКРИНШОТЫ ---
SCREENSHOT_DIR = BASE_DIR / 'debug_screenshots'
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

debug_logger = logging.getLogger('single_url_debug')
debug_handler = logging.FileHandler(LOG_DIR / 'single_url_debug.log', mode='w', encoding='utf-8')
debug_logger.setLevel(logging.INFO)
debug_logger.addHandler(debug_handler)

# --- ТВОЯ СХЕМА И МЕСЯЦЫ ---
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

DATE_SCHEMA = { # формат списка [позиция числа, позиция месяца, позиция года, часы, минуты]
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


def robust_parse_debug(date_str):
    if not date_str: return None, "EMPTY_STRING"
    clean_str = date_str.strip().lower()
    
    for (pattern, has_time), indices in DATE_SCHEMA.items():
        match = re.search(pattern, clean_str)
        if match:
            return "MATCHED_SCHEMA", pattern
            
    try:
        parser.parse(clean_str, fuzzy=True)
        return "MATCHED_DATEUTIL", "fuzzy_parser"
    except:
        return "FAILED", "no_pattern_match"

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

def get_verbose_date_info(driver, url, gnewsdate):
    debug_logger.info(f"\n{'#'*30} START ANALYSIS: {url} {'#'*30}")
    
    # 1. Список селекторов для проверки (из твоего кода)
    all_selectors = [
        "time", ".date", ".time", "meta[property*='date']", "meta[name*='date']",
        "span[title*='Дата']", "div[title*='Дата']", "span[data-id='date']",
        "[itemprop='datePublished']", ".article__info-date", ".js-ago"
    ]

    for selector in all_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if not elements:
                continue
                
            debug_logger.info(f"\n[SELECTOR: {selector}] - found {len(elements)} elements | date: {gnewsdate}")
            
            for i, el in enumerate(elements):
                # Собираем все возможные данные из элемента
                txt = el.text.strip()
                dt_attr = el.get_attribute("datetime")
                cont_attr = el.get_attribute("content")
                outer_html = el.get_attribute('outerHTML')[:100] # для понимания структуры

                sources = [("TEXT", txt), ("ATTR_DATETIME", dt_attr), ("ATTR_CONTENT", cont_attr)]
                
                for src_name, val in sources:
                    if val:
                        status, pattern_info = robust_parse_debug(val)
                        log_msg = f"   -> {src_name}: '{val}' | STATUS: {status} | INFO: {pattern_info} | DATE {gnewsdate}"
                        debug_logger.info(log_msg)
                    else:
                        debug_logger.info(f"   -> {src_name}: [EMPTY]")
                
                # Если всё пусто, логгируем HTML элемента, чтобы понять, что это вообще такое
                if not any([txt, dt_attr, cont_attr]):
                    debug_logger.info(f"   -> WARNING: Element is empty. HTML: {outer_html}")

        except Exception as e:
            debug_logger.error(f"   -> ERROR processing {selector}: {e}")

    # 2. JSON-LD отдельно
    debug_logger.info("\n[CHECKING JSON-LD]")
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'script[type="application/ld+json"]'))
        )
        debug_logger.info("Элемент найден после ожидания")

        page_source = driver.page_source # проверяем есть ли вообще элемент в исходом коде страницы
        text = trafilatura.extract(page_source, include_comments=False)
        debug_logger.info(f"Длина текста: {len(text)}")
        if not text or len(text) < 350:
            debug_logger.info(f"Пустой или очень маленный текст (скорее всего ошибка)")
        if 'application/ld+json' in page_source:
            debug_logger.info("Подстрока 'application/ld+json' присутствует в page_source")
        else:
            debug_logger.info("Подстрока 'application/ld+json' ОТСУТСТВУЕТ в page_source")

        scripts = driver.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')
        debug_logger.info(f'Поиск по CSS_SELECTOR:')
        if scripts:
            for s in scripts:
                content = s.get_attribute("textContent")
                try:
                    data = json.loads(content)
                    # Ищем дату в любом месте JSON (и в корне, и в @graph)
                    res = find_key_recursive(data, "datePublished") or find_key_recursive(data, "dateCreated")
                    
                    if res:
                        debug_logger.info(f"!!! НАШЛИ ДАТУ: {res}")
                        # Здесь можно сразу вернуть значение, если это не просто дебаг-функция
                        # return res 
                    else:
                        debug_logger.info("Дата в этом блоке JSON-LD не найдена")
                        
                except json.JSONDecodeError:
                    debug_logger.error("Ошибка: Реально невалидный JSON")
                except Exception as e:
                    debug_logger.error(f"Ошибка логики при разборе JSON: {e}")
        else:
            debug_logger.info(f"   -> No JSON-LD found")

    
    except: # пробуем по xpath
        debug_logger.info("Элемент не появился после 3 секунд, пробуем поиск по XPath")
        debug_logger.info(f'Поиск по XPath:')
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
        if scripts:
            for s in scripts:
                content = s.get_attribute("textContent")
                try:
                    data = json.loads(content)
                    debug_logger.info(f"   -> Raw JSON-LD: {str(data)}")
                except:
                    debug_logger.info(f"   -> Invalid JSON in script tag")
        else:
            debug_logger.info(f"   -> No JSON-LD found")
            
            website_name = url.split('/')[2]
            driver.save_screenshot(f'{BASE_DIR / "debug_screenshots"}/{website_name}.png')
            debug_logger.info("Скриншот сохранён")

    debug_logger.info(f"{'#'*30} END ANALYSIS {'#'*30}\n")

def init_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def main():
    url_file = LOG_DIR / 'url.log'
    if not url_file.exists():
        print("url.log не найден")
        return

    with open(url_file, 'r', encoding='utf-8') as f:
        entries = [(line.split(" | ")[1].split()[-1].strip(), 
            line.split(" | ")[-1].split("GnewsDate: ", 1)[-1].strip()) 
            for line in f if line.strip() and line.split(" | ")[0] == 'EMPTY'] # элементы - (url, date)
        
    # entries = [('https://news.google.com/rss/articles/CBMiV0FVX3lxTFBfRXZCbC10NkpYZmJEREZUS3BvbmFrTVdBWTBsWnVURXNoTU9RYUxUNWFULWpCQlE4MUJSMTZIOVJVaVFxNWVqdWItZHJ4dTVfb1M5SHV0dw?oc=5&hl=en-US&gl=US&ceid=US:en', 'Sun, 07 Dec 2025 08:00:00 GMT')]
    print(len(entries))

    driver = init_driver()
    for url, gnewsdate in entries:
        print(url, gnewsdate)
        decoded_data = gnewsdecoder(url)
        decoded_url = decoded_data['decoded_url']
        print(f"Анализируем: {decoded_url}")
        try:
            driver.get(decoded_url)
            # time.sleep(3) # ждем прогрузки
            
            get_verbose_date_info(driver, decoded_url, gnewsdate)
        except Exception as e:
            print(f"Ошибка на {decoded_url}: {e}")
    
    driver.quit()
    print("Отладка завершена. Смотри logs/date_extraction_debug.log")

if __name__ == "__main__":
    main()