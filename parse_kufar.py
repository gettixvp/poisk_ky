import requests
from bs4 import BeautifulSoup
import logging
import re

logging.basicConfig(level=logging.INFO, filename='kufar_parser.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def parse_kufar(city="minsk", min_price=100, max_price=300):
    url = f"https://re.kufar.by/l/{city}/snyat/kvartiru-dolgosrochno/bez-posrednikov?cur=USD&prc=r%3A{min_price}%2C{max_price}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        listings = soup.find_all('div', class_='styles_wrapper__Q06m9')
        parsed_data = []
        logging.info(f"Найдено {len(listings)} объявлений на странице {url}")

        for listing in listings:
            try:
                price_elem = listing.find('div', class_='styles_price__usd__HpXMa')
                price = int(re.search(r'\d+', price_elem.text).group()) if price_elem else None

                params_elem = listing.find('div', class_='styles_parameters__7zKlL')
                rooms, area, floor_info = None, None, None
                if params_elem:
                    params_text = params_elem.text
                    rooms_match = re.search(r'(\d+)\s*комн\.', params_text)
                    area_match = re.search(r'(\d+)\s*м²', params_text)
                    floor_match = re.search(r'этаж\s*(\d+)\s*из\s*(\d+)', params_text)
                    rooms = int(rooms_match.group(1)) if rooms_match else None
                    area = int(area_match.group(1)) if area_match else None
                    floor_info = floor_match.group(0) if floor_match else None

                description_elem = listing.find('div', class_='styles_body__5BrnC styles_body__r33c8')
                description = description_elem.text.strip() if description_elem else None

                address_elem = listing.find('div', class_='styles_address__l6Qe_')
                address = address_elem.text.strip() if address_elem else None

                image_elem = listing.find('img', class_='styles_segments___oyEJ')
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

            except Exception as e:
                logging.error(f"Ошибка при парсинге объявления: {e}")

        return parsed_data

    except Exception as e:
        logging.error(f"Ошибка при загрузке страницы {url}: {e}")
        return []