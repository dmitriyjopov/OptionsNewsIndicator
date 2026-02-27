import requests
from fake_useragent import UserAgent
from itertools import cycle
import time
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RotatingProxyFetcher:
    """
    Класс для выполнения HTTP-запросов с ротацией прокси и случайным User-Agent.
    """
    def __init__(self, proxy_list=None, max_retries=3, timeout=10, use_ua_random=True):
        """
        :param proxy_list: список прокси в формате 'http://user:pass@ip:port' или 'socks5://ip:port'
        :param max_retries: максимальное количество попыток для одного URL
        :param timeout: таймаут запроса в секундах
        :param use_ua_random: если True, генерировать случайный User-Agent (может тормозить при первом запуске)
        """
        self.proxy_pool = cycle(proxy_list) if proxy_list else None
        self.max_retries = max_retries
        self.timeout = timeout
        self.ua = UserAgent() if use_ua_random else None
        self.session = requests.Session()

    def _get_headers(self):
        """Формирует заголовки с случайным User-Agent."""
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        if self.ua:
            headers['User-Agent'] = self.ua.random
        return headers

    def _get_proxy(self):
        """Возвращает следующий прокси из пула или None."""
        if self.proxy_pool:
            return {'http': next(self.proxy_pool), 'https': next(self.proxy_pool)}
        return None

    def fetch(self, url, retry_count=0):
        """
        Выполняет GET-запрос к URL с обработкой ошибок и повторными попытками.
        :param url: целевой URL
        :param retry_count: текущий номер попытки (для внутреннего использования)
        :return: объект Response или None в случае неудачи
        """
        try:
            headers = self._get_headers()
            proxies = self._get_proxy()

            logger.info(f"Запрос {url} (попытка {retry_count+1}/{self.max_retries})")
            if proxies:
                logger.debug(f"Используем прокси: {proxies}")

            response = self.session.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=self.timeout,
                allow_redirects=True
            )

            if response.status_code == 200:
                logger.info(f"Успешно получен ответ от {url}, размер: {len(response.content)} байт")
                return response
            else:
                logger.warning(f"Статус код {response.status_code} для {url}")
                # Если статус не 200, пробуем ещё раз (кроме 404 - бессмысленно)
                if response.status_code != 404 and retry_count < self.max_retries - 1:
                    time.sleep(1.5)  # небольшая пауза перед повтором
                    return self.fetch(url, retry_count + 1)
                return None

        except requests.exceptions.ProxyError as e:
            logger.error(f"Ошибка прокси: {e}. Пробуем следующий прокси.")
            if retry_count < self.max_retries - 1:
                return self.fetch(url, retry_count + 1)
            return None

        except requests.exceptions.Timeout:
            logger.error(f"Таймаут при запросе {url}")
            if retry_count < self.max_retries - 1:
                time.sleep(2)
                return self.fetch(url, retry_count + 1)
            return None

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Ошибка соединения: {e}")
            if retry_count < self.max_retries - 1:
                time.sleep(2)
                return self.fetch(url, retry_count + 1)
            return None

        except Exception as e:
            logger.exception(f"Неизвестная ошибка при запросе {url}: {e}")
            return None


# Пример использования
if __name__ == "__main__":
    # Список прокси (примеры, замените на свои)
    proxies = [
        # "http://user:pass@123.45.67.89:8080",
        "socks5://159.223.53.194:1080",
        # можно добавить больше
    ]

    # Создаём экземпляр загрузчика
    fetcher = RotatingProxyFetcher(proxy_list=None, max_retries=3, timeout=15)

    # Тестируем
    url = "https://bcs-express.ru/tehanaliz/sber/04.12.25"  # сервис возвращает IP, с которого пришёл запрос
    response = fetcher.fetch(url)

    if response:
        print("Ответ:", response.text)
    else:
        print("Не удалось получить страницу.")