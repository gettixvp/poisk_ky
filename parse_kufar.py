import requests
from bs4 import BeautifulSoup
import logging
import re
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    filename='kufar_parser.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_kufar(city="minsk", min_price=100, max_price=300):
    base_url = "https://re.kufar.by/l/{city}/snyat/kvartiru-dolgosrochno/bez-posrednikov"
    url = base_url.format(city=city.lower()) + f"?cur=USD&prc=r%3A{min_price}%2C{max_price}&size=30"
    
    headers = {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"
        ]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://re.kufar.by/",
        "DNT": "1"
    }

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        time.sleep(random.uniform(1, 3))
        logger.info(f"Attempting to fetch URL: {url}")
        response = session.get(url, headers=headers, timeout=10)
        logger.info(f"Fetching {url}: HTTP {response.status_code}")
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        if "ничего не найдено" in response.text.lower():
            logger.warning(f"No listings found at {url}")
            return []

        # Попробуем разные селекторы
        listings = soup.find_all('div', class_=re.compile(r'styles_wrapper__\w+'))
        if not listings:
            listings = soup.find_all('article')
            logger.info(f"Tried alternative selector: found {len(listings)} articles")
        
        if not listings:
            logger.error(f"No listing elements found for {url}")
            with open("kufar_error.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.info(".saved raw HTML to kufar_error.html")
            # Логируем структуру страницы
            logger.debug(f"Page structure: {soup.find('body').prettify()[:1000]}")
            return []

        parsed_data = []
        logger.info(f"Found {len(listings)} listings at {url}")

        for listing in listings:
            try:
                # Listing URL
                link_elem = listing.find('a', href=True)
                listing_url = link_elem['href'] if link_elem else None
                if listing_url and not listing_url.startswith('http'):
                    listing_url = f"https://re.kufar.by{listing_url}"

                # Price
                price_elem = listing.find('div', class_=re.compile(r'styles_price__usd__\w+'))
                price_text = price_elem.text.strip() if price_elem else ""
                price_match = re.search(r'\d+', price_text.replace('$', '').replace(' ', ''))
                price = int(price_match.group()) if price_match else None

                # Parameters
                params_elem = listing.find('div', class_=re.compile(r'styles_parameters__\w+'))
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
                desc_elem = listing.find('div', class_=re.compile(r'styles_body__\w+'))
                description = desc_elem.text.strip() if desc_elem else None

                # Address
                address_elem = listing.find('div', class_=re.compile(r'styles_address__\w+'))
                address = address_elem.text.strip() if address_elem else None

                # Image
                image_elem = listing.find('img')
                image = image_elem['src'] if image_elem and 'src' in image_elem.attrs else "https://via.placeholder.com/150"

                parsed_data.append({
                    'price': price,
                    'rooms': rooms,
                    'area': area,
                    'floor': floor_info,
                    'description': description,
                    'address': address,
                    'image': image,
                    'listing_url': listing_url
                })
                logger.debug(f"Parsed listing: price={price}, rooms={rooms}, area={area}, address={address}, url={listing_url}")

            except Exception as e:
                logger.error(f"Error parsing listing: {str(e)}")
                continue

        return parsed_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {str(e)}")
        if 'response' in locals():
            with open("kufar_error.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.info("Saved raw HTML to kufar_error.html")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in parse_kufar at {url}: {str(e)}")
        return []
