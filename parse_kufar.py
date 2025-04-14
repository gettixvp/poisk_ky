import requests
from bs4 import BeautifulSoup
import logging
import re
import time
import random

logging.basicConfig(
    level=logging.INFO,
    filename='kufar_parser.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_kufar(city="minsk", min_price=100, max_price=300):
    url = f"https://www.kufar.by/l/r~{city}/kvartiry/snyat?cur=USD&prc=r%3A{min_price}%2C{max_price}&sort=lst.d"
    headers = {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"
        ]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }

    try:
        # Random delay to mimic human behavior
        time.sleep(random.uniform(10, 15))
        response = requests.get(url, headers=headers, timeout=15)
        logger.info(f"Fetching {url}: HTTP {response.status_code}")
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Check for empty results
        if "ничего не найдено" in response.text.lower() or soup.find(string=re.compile("ничего не найдено", re.I)):
            logger.warning(f"No listings found at {url}")
            return []

        # Updated selectors based on Kufar's structure (as of 2025)
        listings = soup.find_all('a', class_=re.compile(r'ListingsCard.*'))
        if not listings:
            logger.error(f"No listing elements found for {url}")
            return []

        parsed_data = []
        logger.info(f"Found {len(listings)} listings at {url}")

        for listing in listings:
            try:
                # Price
                price_elem = listing.find('div', class_=re.compile(r'price.*'))
                price_text = price_elem.text.strip() if price_elem else ""
                price_match = re.search(r'\d+', price_text.replace(' ', '').replace('$', ''))
                price = int(price_match.group()) if price_match else None

                # Parameters
                params_elem = listing.find('div', class_=re.compile(r'params.*'))
                rooms, area, floor_info = None, None, None
                if params_elem:
                    params_text = params_elem.text
                    rooms_match = re.search(r'(\d+)\s*комн\.|студия', params_text, re.I)
                    area_match = re.search(r'(\d+)\s*м²', params_text)
                    floor_match = re.search(r'этаж\s*(\d+)\s*из\s*(\d+)', params_text)
                    rooms = int(rooms_match.group(1)) if rooms_match and rooms_match.group(1) else "studio" if rooms_match else None
                    area = int(area_match.group(1)) if area_match else None
                    floor_info = floor_match.group(0) if floor_match else None

                # Description
                description_elem = listing.find('div', class_=re.compile(r'description.*'))
                description = description_elem.text.strip() if description_elem else None

                # Address
                address_elem = listing.find('div', class_=re.compile(r'address.*'))
                address = address_elem.text.strip() if address_elem else None

                # Image
                image_elem = listing.find('img', class_=re.compile(r'image.*'))
                image = image_elem['src'] if image_elem and 'src' in image_elem.attrs else "https://via.placeholder.com/150"

                parsed_data.append({
                    'price': price,
                    'rooms': rooms,
                    'area': area,
                    'floor': floor_info,
                    'description': description,
                    'address': address,
                    'image': image
                })
                logger.debug(f"Parsed listing: price={price}, rooms={rooms}, area={area}, address={address}")

            except Exception as e:
                logger.error(f"Error parsing listing: {str(e)}")
                continue

        return parsed_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in parse_kufar: {str(e)}")
        return []
