import requests
from bs4 import BeautifulSoup
import logging
import re
import time

logging.basicConfig(level=logging.INFO, filename='kufar_parser.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def parse_kufar(city="minsk", min_price=100, max_price=300):
    url = f"https://www.kufar.by/l/r~{city}/kvartiry/snyat?cur=USD&prc=r%3A{min_price}%2C{max_price}&sort=lst.d"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }

    try:
        time.sleep(3)  # Задержка для предотвращения блокировок
        response = requests.get(url, headers=headers, timeout=15)
        logging.info(f"HTTP статус для {url}: {response.status_code}")
        logging.debug(f"Первые 500 символов ответа: {response.text[:500]}")
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Проверка на пустую страницу
        if "ничего не найдено" in response.text.lower():
            logging.warning(f"На странице {url} объявлений не найдено")
            return []

        # Обновленные селекторы
        listings = soup.find_all('a', class_=re.compile(r'listing-item'))
        if not listings:
            logging.error(f"Не найдены элементы с классом listing-item для {url}")
            return []

        parsed_data = []
        logging.info(f"Найдено {len(listings)} объявлений на странице {url}")

        for listing in listings:
            try:
                # Цена
                price_elem = listing.find('span', class_=re.compile(r'price'))
                price_text = price_elem.text.strip() if price_elem else ""
                price_match = re.search(r'\d+', price_text.replace(' ', ''))
                price = int(price_match.group()) if price_match else None

                # Параметры
                params_elem = listing.find('p', class_=re.compile(r'params'))
                rooms, area, floor_info = None, None, None
                if params_elem:
                    params_text = params_elem.text
                    rooms_match = re.search(r'(\d+)\s*комн\.', params_text)
                    area_match = re.search(r'(\d+)\s*м²', params_text)
                    floor_match = re.search(r'этаж\s*(\d+)\s*из\s*(\d+)', params_text)
                    rooms = int(rooms_match.group(1)) if rooms_match else None
                    area = int(area_match.group(1)) if area_match else None
                    floor_info = floor_match.group(0) if floor_match else None

                # Описание
                description_elem = listing.find('p', class_=re.compile(r'description'))
                description = description_elem.text.strip() if description_elem else None

                # Адрес
                address_elem = listing.find('span', class_=re.compile(r'address'))
                address = address_elem.text.strip() if address_elem else None

                # Изображение
                image_elem = listing.find('img')
                image = image_elem['src'] if image_elem and 'src' in image_elem.attrs else None

                parsed_data.append({
                    'price': price,
                    'rooms': rooms,
                    'area': area,
                    'floor': floor_info,
                    'description': description,
                    'address': address,
                    'image': image
                })
                logging.debug(f"Спарсено объявление: price={price}, rooms={rooms}, area={area}")

            except Exception as e:
                logging.error(f"Ошибка при парсинге объявления: {str(e)}")

        return parsed_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при загрузке страницы {url}: {str(e)}")
        return []
