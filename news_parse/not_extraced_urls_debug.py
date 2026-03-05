import os, logging, re, json, time
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from dateutil import parser

# --- ЛОГИРОВАНИЕ ---
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

debug_logger = logging.getLogger('verbose_debug')
debug_handler = logging.FileHandler(LOG_DIR / 'date_extraction_debug.log', mode='w', encoding='utf-8')
debug_logger.setLevel(logging.INFO)
debug_logger.addHandler(debug_handler)

# --- ТВОЯ СХЕМА И МЕСЯЦЫ ---
months_map = {'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6, 'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12}

DATE_SCHEMA = {
    (r'(\d{4})-(\d{2})-(\d{2})', False): [2, 1, 0, None, None],
    (r'(\d{1,2})\.(\d{2})\.(\d{4})\s*(?:в|,)?\s*(\d{1,2}):(\d{2})', True): [0, 1, 2, 3, 4],
    (r'(?:дата публикации|опубликовано|опубликована):\s*(\d{1,2})\s+([а-яёa-z\.]+)\s+(\d{4})', False): [0, 1, 2, None, None],
    (r'(\d{1,2})\s+([а-яёa-z]+)\s+(\d{4})[,\s]+(\d{1,2}):(\d{2})', True): [0, 1, 2, 3, 4],
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

def get_verbose_date_info(driver, url):
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
                
            debug_logger.info(f"\n[SELECTOR: {selector}] - found {len(elements)} elements")
            
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
                        log_msg = f"   -> {src_name}: '{val}' | STATUS: {status} | INFO: {pattern_info}"
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
    scripts = driver.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')
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
        urls = [line.split()[-1] for line in f]

    driver = init_driver()
    for url in urls:
        print(f"Анализируем: {url}")
        try:
            driver.get(url)
            time.sleep(3) # ждем прогрузки
            get_verbose_date_info(driver, url)
        except Exception as e:
            print(f"Ошибка на {url}: {e}")
    
    driver.quit()
    print("Отладка завершена. Смотри logs/date_extraction_debug.log")

if __name__ == "__main__":
    main()