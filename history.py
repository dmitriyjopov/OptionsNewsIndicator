import pandas as pd
from ddgs import DDGS
import trafilatura
from tqdm import tqdm
import time
import random

def get_news_ddg(keyword, max_results=20):
    """
    Получает новости напрямую через DuckDuckGo.
    Это надежнее, так как ссылки прямые.
    """
    results = []
    # ddgs.news — специальный метод для поиска новостей
    with DDGS() as ddgs:
        # timelimit: 'd' (день), 'w' (неделя), 'm' (месяц)
        # Для истории используем поиск без жесткого лимита времени
        ddgs_gen = ddgs.news(keyword, region="ru-ru", safesearch="off", timelimit="m", max_results=max_results)
        for r in ddgs_gen:
            results.append(r)
    return results

def fast_parse(url):
    """Качественное извлечение текста статьи"""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            # Извлекаем основной текст
            text = trafilatura.extract(downloaded, include_comments=False)
            return text[:1000].replace('\n', ' ') if text else "Текст не найден"
    except:
        return "Ошибка доступа к сайту"
    return "Пусто"

# --- ОСНОВНОЙ ЦИКЛ ---
KEYWORD = "сбербанк"
print(f"🔎 Ищем новости по теме: {KEYWORD}")

# 1. Получаем список новостей
raw_news = get_news_ddg(KEYWORD, max_results=30)
print(f"Найдено ссылок: {len(raw_news)}")

# 2. Обрабатываем каждую новость
final_data = []
for item in tqdm(raw_news):
    url = item['url']
    # Парсим текст напрямую по ссылке
    content = fast_parse(url)
    
    final_data.append({
        'date': item['date'],
        'title': item['title'],
        'source': item['source'],
        'url': url,
        'content': content
    })
    # Небольшая пауза, чтобы сайты СМИ нас не забанили
    time.sleep(random.uniform(0.5, 1.5))

# 3. Сохраняем
df = pd.DataFrame(final_data)
df.to_csv(f'news_ddg_{KEYWORD}.csv', index=False, encoding='utf-8-sig')

print("\n--- ПРОВЕРКА (Первые 3 новости) ---")
print(df[['source', 'content']].head(3))