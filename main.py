import os
import logging
import asyncio
import uuid
import json
import hmac
import hashlib
import urllib.parse
from datetime import datetime
from typing import List, Optional, Dict, Any
import asyncpg
import aiofiles
from fastapi import (
    FastAPI,
    HTTPException,
    Form,
    UploadFile,
    File,
    Request,
    Depends,
    Response,
    status
)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import RetryAfter
import cloudinary
import cloudinary.uploader
from fastapi.security import APIKeyHeader
import requests
from bs4 import BeautifulSoup
import re
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
DATABASE_URL = "postgresql://neondb_owner:npg_MJr6nebWzp3C@ep-fragrant-math-a2ladk0z-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
TELEGRAM_TOKEN = "7846698102:AAFR2bhmjAkPiV-PjtnFIu_oRnzxYPP1xVo"
RENDER_URL = "https://jake-3.onrender.com"
UPLOAD_DIR = "Uploads"

# Cloudinary configuration
CLOUDINARY_CLOUD_NAME = "dhj2zl3ks"
CLOUDINARY_API_KEY = "216733364197713"
CLOUDINARY_API_SECRET = "r2FhpGSCbUttF5wrUfj7VFqN-_c"

cloudinary.config(
    cloud_name=CLOUDINARY_CLOUD_NAME,
    api_key=CLOUDINARY_API_KEY,
    api_secret=CLOUDINARY_API_SECRET,
    secure=True
)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# --- Pydantic Models ---
class SearchRequest(BaseModel):
    telegram_id: int
    city: str
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    rooms: Optional[str] = None
    captcha_code: Optional[str] = None

class UserProfileRequest(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None

class ToggleFavoriteRequest(BaseModel):
    telegram_id: int
    ad_id: str
    state: bool

class DeleteListingRequest(BaseModel):
    telegram_id: int
    ad_id: str

class StatusResponse(BaseModel):
    status: str
    message: Optional[str] = None

class UserProfileResponse(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None

class ListingResponse(BaseModel):
    id: str
    telegram_id: Optional[int] = None
    username: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[int] = None
    rooms: Optional[str] = None
    area: Optional[int] = None
    city: Optional[str] = None
    address: Optional[str] = None
    image: Optional[str] = None
    images: List[str] = []
    listing_url: Optional[str] = Field(None, alias="link")
    created_at: Optional[datetime] = None
    date: Optional[str] = None
    time_posted: Optional[str] = None
    source: Optional[str] = None
    is_favorite: bool = False
    views: Optional[int] = 0
    floor: Optional[str] = None

class AdsListResponse(BaseModel):
    ads: List[ListingResponse]

class UserListingsResponse(BaseModel):
    ads: List[ListingResponse]
    profile: Optional[UserProfileResponse] = None
    favorites: int = 0
    messages: int = 0

class FavoritesResponse(BaseModel):
    status: str
    favorites: List[str]

class ToggleFavoriteResponse(BaseModel):
    status: str
    newState: bool
    totalFavorites: Optional[int] = None

class LogsResponse(BaseModel):
    logs: str

# --- FastAPI Application Setup ---
app = FastAPI(title="Квартирный Помощник API")

# --- Database Pool ---
db_pool: Optional[asyncpg.Pool] = None

async def get_db_connection():
    if db_pool is None:
        logger.error("Database pool is not initialized.")
        raise HTTPException(status_code=500, detail="Database connection not available.")
    async with db_pool.acquire() as connection:
        yield connection

# --- Telegram Bot Application State ---
class AppState:
    bot_app: Optional[Application] = None

app.state = AppState()

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response

# --- Static Files ---
if not os.path.exists("static"):
    os.makedirs("static")
    logger.info("Created 'static' directory.")
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)
    logger.info(f"Created '{UPLOAD_DIR}' directory.")

app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount(f"/{UPLOAD_DIR}", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# --- Authentication ---
telegram_auth = APIKeyHeader(name="X-Telegram-Init-Data", auto_error=False)

async def verify_telegram_user(init_data: str = Depends(telegram_auth)) -> int:
    if not init_data:
        raise HTTPException(status_code=401, detail="Telegram initData missing.")
    try:
        parsed_data = urllib.parse.parse_qs(init_data)
        data_check_string = '\n'.join(f"{k}={v[0]}" for k, v in sorted(parsed_data.items()) if k != 'hash')
        secret_key = hmac.new(
            b"WebAppData",
            TELEGRAM_TOKEN.encode(),
            hashlib.sha256
        ).digest()
        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256
        ).hexdigest()
        if calculated_hash != parsed_data.get('hash', [''])[0]:
            raise ValueError("Invalid hash")
        user_data = json.loads(parsed_data['user'][0])
        telegram_id = user_data.get("id")
        if not telegram_id:
            raise ValueError("User ID not found in initData.")
        return telegram_id
    except Exception as e:
        logger.error(f"Invalid Telegram initData: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid Telegram initData.")

# --- Database Initialization ---
async def init_db():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        if db_pool is None:
            raise Exception("Failed to create database pool.")
        logger.info("Database connection pool created successfully.")

        async with db_pool.acquire() as conn:
            # Create tables
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS listings (
                    id UUID PRIMARY KEY,
                    telegram_id BIGINT,
                    title TEXT,
                    description TEXT,
                    price INTEGER,
                    rooms TEXT,
                    area INTEGER,
                    city TEXT,
                    address TEXT,
                    floor TEXT,
                    images TEXT[],
                    listing_url TEXT UNIQUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    time_posted TEXT,
                    source TEXT DEFAULT 'user',
                    is_active BOOLEAN DEFAULT TRUE,
                    views INTEGER DEFAULT 0,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id) ON DELETE SET NULL
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS favorites (
                    user_telegram_id BIGINT NOT NULL,
                    listing_id UUID NOT NULL,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_telegram_id, listing_id),
                    FOREIGN KEY (user_telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE,
                    FOREIGN KEY (listing_id) REFERENCES listings(id) ON DELETE CASCADE
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS history (
                    user_telegram_id BIGINT NOT NULL,
                    listing_id UUID NOT NULL,
                    viewed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_telegram_id, listing_id),
                    FOREIGN KEY (user_telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE,
                    FOREIGN KEY (listing_id) REFERENCES listings(id) ON DELETE CASCADE
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS comparisons (
                    user_telegram_id BIGINT NOT NULL,
                    listing_id UUID NOT NULL,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_telegram_id, listing_id),
                    FOREIGN KEY (user_telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE,
                    FOREIGN KEY (listing_id) REFERENCES listings(id) ON DELETE CASCADE
                );
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_listings_url ON listings (listing_url);
                CREATE INDEX IF NOT EXISTS idx_listings_source ON listings (source);
                CREATE INDEX IF NOT EXISTS idx_listings_telegram_id ON listings (telegram_id);
                CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites (user_telegram_id);
                CREATE INDEX IF NOT EXISTS idx_favorites_listing ON favorites (listing_id);
                CREATE INDEX IF NOT EXISTS idx_history_user ON history (user_telegram_id);
                CREATE INDEX IF NOT EXISTS idx_comparisons_user ON comparisons (user_telegram_id);
            ''')

            # Add floor column if it doesn't exist
            await conn.execute('''
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'listings' 
                        AND column_name = 'floor'
                    ) THEN
                        ALTER TABLE listings ADD COLUMN floor TEXT;
                    END IF;
                END $$;
            ''')

        logger.info("Database schema initialized and migrations applied.")
    except Exception as e:
        logger.exception(f"Failed to initialize database: {str(e)}")
        raise

# --- parse_kufar Implementation ---
def parse_kufar(city: str = "minsk", min_price: int = 100, max_price: int = 300, rooms: Optional[str] = None) -> List[Dict]:
    base_url = "https://re.kufar.by/l/{city}/snyat/kvartiru-dolgosrochno"
    if rooms and rooms != "studio":
        try:
            rooms = f"{int(rooms)}k"
        except ValueError:
            logger.warning(f"Invalid rooms value: {rooms}, treating as None")
            rooms = None
    rooms_part = f"/{rooms}" if rooms else ""
    url = (
        base_url.format(city=city.lower()) + 
        rooms_part + 
        "/bez-posrednikov" + 
        f"?cur=USD&prc=r%3A{min_price}%2C{max_price}&size=30"
    )
    
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
        logger.info(f"Attempting to fetch URL: {url} (rooms: {rooms})")
        response = session.get(url, headers=headers, timeout=10)
        logger.info(f"Fetching {url}: HTTP {response.status_code}")
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        if "ничего не найдено" in response.text.lower():
            logger.warning(f"No listings found at {url}")
            return []

        listings = soup.find_all('a', class_='styles_wrapper__Q06m9')
        if not listings:
            logger.error(f"No listing elements found for {url}")
            divs = soup.find_all('div')
            classes = set()
            for div in divs:
                if div.get('class'):
                    classes.update(div.get('class'))
            logger.debug(f"Found div classes: {classes}")
            with open("kufar_error.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.info("Saved raw HTML to kufar_error.html")
            return []

        parsed_data = []
        logger.info(f"Found {len(listings)} listings at {url}")

        for ad in listings:
            try:
                listing_url = ad.get('href')
                if listing_url and not listing_url.startswith('http'):
                    listing_url = f"https://re.kufar.by{listing_url}"

                price_element = ad.select_one(".styles_price__usd__HpXMa")
                price = int(re.sub(r"\D", "", price_element.text)) if price_element else None
                if not price:
                    logger.warning(f"Failed to parse price for listing: {listing_url}")

                params_element = ad.select_one(".styles_parameters__7zKlL")
                rooms_val, area, floor_info = None, None, None
                if params_element:
                    params_text = params_element.text
                    rooms_match = re.search(r"(\d+)\s*комн\.|студия", params_text, re.I)
                    area_match = re.search(r"(\d+)\s*м²", params_text)
                    floor_match = re.search(r"этаж\s*(\d+)\s*из\s*(\d+)", params_text)
                    rooms_val = int(rooms_match.group(1)) if rooms_match and rooms_match.group(1) else "studio" if rooms_match else None
                    area = int(area_match.group(1)) if area_match else None
                    floor_info = floor_match.group(0) if floor_match else None

                desc_element = ad.select_one(".styles_body__5BrnC")
                description = desc_element.text.strip() if desc_element else "Описание не указано 📝"

                address_element = ad.select_one(".styles_address__l6Qe_")
                address = address_element.text.strip() if address_element else "Адрес не указан 🏠"

                image_element = ad.select_one("img")
                image = image_element.get("src") if image_element else "https://via.placeholder.com/150"

                time_element = ad.select_one(".styles_date__ssUVP span")
                time_posted = time_element.text.strip() if time_element else "Дата не указана"

                parsed_data.append({
                    'price': price,
                    'rooms': rooms_val,
                    'area': area,
                    'floor': floor_info,
                    'description': description,
                    'address': address,
                    'image': image,
                    'listing_url': listing_url,
                    'time_posted': time_posted
                })
                logger.debug(f"Parsed listing: price={price}, rooms={rooms_val}, area={area}, address={address}, time={time_posted}, url={listing_url}")

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

# --- Telegram Bot Setup ---
async def set_webhook_with_retry(bot, webhook_url, max_attempts=5, initial_delay=1):
    attempt = 0
    delay = initial_delay
    while attempt < max_attempts:
        try:
            await bot.set_webhook(url=webhook_url, allowed_updates=Update.ALL_TYPES)
            logger.info(f"Attempt {attempt+1}: Webhook set to {webhook_url}")
            return True
        except RetryAfter as e:
            logger.warning(f"Attempt {attempt+1}: Flood control waiting for {e.retry_after} seconds...")
            await asyncio.sleep(e.retry_after)
            delay = initial_delay
        except Exception as e:
            logger.error(f"Attempt {attempt+1}: Failed to set webhook: {str(e)}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2
        attempt += 1
    logger.error(f"Failed to set webhook after {max_attempts} attempts.")
    raise HTTPException(status_code=500, detail="Failed to set Telegram webhook.")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        logger.warning("Received /start command with missing message or user.")
        return
    user = update.effective_user
    try:
        welcome_message = (
            "🏠 Добро пожаловать в бот поиска квартир!\n\n"
            "Я помогу вам найти идеальное жилье в Беларуси. "
            "Используйте наше приложение для удобного поиска по городам, ценам и количеству комнат.\n\n"
            "👇 Нажмите кнопку ниже, чтобы начать:"
        )
        web_app_url = RENDER_URL.rstrip('/') + "/"
        keyboard = [[{"text": "🚀 Открыть приложение", "web_app": {"url": web_app_url}}]]
        await update.message.reply_text(
            welcome_message,
            reply_markup={"inline_keyboard": keyboard}
        )
        logger.info(f"Sent /start response to user {user.id} ({user.username})")
        # Manually acquire a database connection for registration
        async with db_pool.acquire() as conn:
            await register_or_update_user(
                UserProfileRequest(
                    telegram_id=user.id,
                    username=user.username,
                    first_name=user.first_name
                ),
                conn
            )
    except Exception as e:
        logger.exception(f"Error in start_command for user {user.id}: {str(e)}")
        await update.message.reply_text("Произошла ошибка. Попробуйте позже.")

# --- FastAPI Lifecycle Events ---
@app.on_event("startup")
async def startup_event():
    logger.info("Application startup initiated.")
    await init_db()
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    logger.info(f"Ensured upload directory: {UPLOAD_DIR}")
    try:
        bot_app = Application.builder().token(TELEGRAM_TOKEN).build()
        bot_app.add_handler(CommandHandler("start", start_command))
        await bot_app.initialize()
        webhook_url = RENDER_URL.rstrip('/') + "/telegram_webhook"
        await set_webhook_with_retry(bot_app.bot, webhook_url)
        app.state.bot_app = bot_app
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.exception(f"Failed to initialize Telegram bot: {str(e)}")
        raise
    logger.info("Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated.")
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed.")
    if hasattr(app.state, "bot_app") and app.state.bot_app:
        if app.state.bot_app.running:
            await app.state.bot_app.stop()
        await app.state.bot_app.shutdown()
        logger.info("Telegram bot stopped.")
    logger.info("Application shutdown complete.")

# --- Helper Functions ---
async def register_or_update_user(
    profile: UserProfileRequest,
    conn: asyncpg.Connection
):
    try:
        result = await conn.execute(
            '''
            INSERT INTO users (telegram_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (telegram_id) DO UPDATE
            SET username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
            WHERE users.username IS DISTINCT FROM EXCLUDED.username
               OR users.first_name IS DISTINCT FROM EXCLUDED.first_name;
            ''',
            profile.telegram_id, profile.username, profile.first_name
        )
        action = "updated" if "UPDATE" in result else "registered"
        logger.info(f"User {profile.telegram_id} {action}.")
        return StatusResponse(status="success", message=f"User {action}")
    except Exception as e:
        logger.exception(f"Error registering/updating user {profile.telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error during user registration.")

async def upload_image_to_cloudinary(file: UploadFile) -> str:
    try:
        result = await cloudinary.uploader.upload(
            file.file,
            folder="apartments",
            resource_type="image",
            allowed_formats=["jpg", "png", "webp"]
        )
        return result["secure_url"]
    except Exception as e:
        logger.error(f"Cloudinary upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to upload image.")

async def send_telegram_notification(telegram_id: int, message: str):
    try:
        await app.state.bot_app.bot.send_message(
            chat_id=telegram_id,
            text=message
        )
        logger.info(f"Notification sent to user {telegram_id}: {message}")
    except Exception as e:
        logger.error(f"Failed to send notification to user {telegram_id}: {str(e)}")

async def parse_and_store_kufar_listings():
    try:
        listings = parse_kufar()
        async with db_pool.acquire() as conn:
            for listing in listings:
                listing_id = uuid.uuid4()
                await conn.execute('''
                    INSERT INTO listings (
                        id, title, description, price, rooms, area, floor, city, address,
                        images, listing_url, time_posted, source
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (listing_url) DO NOTHING
                ''', listing_id, listing.get("title"), listing["description"],
                    listing["price"], str(listing["rooms"]), listing.get("area"),
                    listing.get("floor"), "minsk", listing["address"],
                    [listing["image"]], listing["listing_url"],
                    listing["time_posted"], "kufar")
        logger.info(f"Stored {len(listings)} Kufar listings.")
    except Exception as e:
        logger.error(f"Error parsing/storing Kufar listings: {str(e)}")
        raise

async def is_premium_user(telegram_id: int, conn: asyncpg.Connection) -> bool:
    return False  # Placeholder

# --- API Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def serve_html():
    html_file_path = os.path.join("static", "index.html")
    try:
        async with aiofiles.open(html_file_path, mode="r", encoding="utf-8") as f:
            content = await f.read()
        return HTMLResponse(content=content)
    except FileNotFoundError:
        logger.error(f"HTML file not found at {html_file_path}")
        raise HTTPException(status_code=404, detail="Web application file not found.")
    except Exception as e:
        logger.exception(f"Error serving index.html: {str(e)}")
        raise HTTPException(status_code=500, detail="Could not load web application.")

@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    if not hasattr(app.state, "bot_app") or not app.state.bot_app:
        logger.error("Webhook called but bot_app is not initialized.")
        return JSONResponse(content={"status": "error", "message": "Bot not ready"}, status_code=503)
    try:
        update_data = await request.json()
        update = Update.de_json(update_data, app.state.bot_app.bot)
        logger.debug(f"Processing update ID: {update.update_id}")
        await app.state.bot_app.process_update(update)
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        logger.exception(f"Error processing webhook: {str(e)}")
        return JSONResponse(content={"status": "processing_error"}, status_code=200)

@app.post("/api/register_user", response_model=StatusResponse)
async def register_or_update_user_endpoint(
    profile: UserProfileRequest,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    return await register_or_update_user(profile, conn)

@app.get("/api/profile", response_model=UserProfileResponse)
async def get_profile(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        user = await conn.fetchrow('''
            SELECT telegram_id, username, first_name
            FROM users
            WHERE telegram_id = $1
        ''', telegram_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found.")
        return UserProfileResponse(
            telegram_id=user["telegram_id"],
            username=user["username"],
            first_name=user["first_name"]
        )
    except Exception as e:
        logger.error(f"Error fetching profile for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch profile.")

async def _get_listings_from_db(
    conn: asyncpg.Connection,
    telegram_id: Optional[int] = None,
    source: Optional[str] = None,
    sort: str = 'newest',
    limit: int = 10,
    get_all: bool = False,
    requester_telegram_id: Optional[int] = None
) -> List[ListingResponse]:
    query = """
        SELECT
            l.id, l.telegram_id, l.title, l.description, l.price, l.rooms, l.area,
            l.city, l.address, l.floor, l.images, l.listing_url, l.created_at, l.time_posted, l.source,
            l.views, u.username,
            EXISTS (
                SELECT 1 FROM favorites f
                WHERE f.listing_id = l.id AND f.user_telegram_id = $1
            ) as is_favorite
        FROM listings l
        LEFT JOIN users u ON l.telegram_id = u.telegram_id
        WHERE l.is_active = TRUE
    """
    params = [requester_telegram_id]
    param_count = 1

    if telegram_id is not None:
        param_count += 1
        query += f" AND l.telegram_id = ${param_count}"
        params.append(telegram_id)
    if source is not None:
        param_count += 1
        query += f" AND l.source = ${param_count}"
        params.append(source)

    if sort == 'newest':
        query += " ORDER BY l.created_at DESC"
    elif sort == 'oldest':
        query += " ORDER BY l.created_at ASC"
    elif sort == 'price_asc':
        query += " ORDER BY l.price ASC NULLS LAST, l.created_at DESC"
    elif sort == 'price_desc':
        query += " ORDER BY l.price DESC NULLS LAST, l.created_at DESC"
    else:
        query += " ORDER BY l.created_at DESC"

    if not get_all:
        param_count += 1
        query += f" LIMIT ${param_count}"
        params.append(limit)

    rows = await conn.fetch(query, *params)
    return [
        ListingResponse(
            id=str(row['id']),
            telegram_id=row['telegram_id'],
            username=row['username'],
            title=row['title'],
            description=row['description'],
            price=row['price'],
            rooms=str(row['rooms']) if row['rooms'] else None,
            area=row['area'],
            city=row['city'],
            address=row['address'],
            floor=row['floor'],
            images=row['images'] or [],
            link=row.get('listing_url'),
            created_at=row['created_at'],
            date=row['created_at'].isoformat() if row['created_at'] else None,
            time_posted=row['time_posted'],
            source=row['source'],
            is_favorite=row['is_favorite'],
            views=row['views']
        ) for row in rows
    ]

@app.get("/api/new_listings", response_model=AdsListResponse)
async def get_new_listings_api(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        ads = await _get_listings_from_db(
            conn,
            source='kufar',
            sort='newest',
            limit=10,
            requester_telegram_id=telegram_id
        )
        logger.info(f"Returned {len(ads)} new 'kufar' listings for user {telegram_id}")
        return AdsListResponse(ads=ads)
    except Exception as e:
        logger.exception(f"Error fetching new listings for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/user_listings", response_model=UserListingsResponse)
async def get_user_listings_api(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        ads = await _get_listings_from_db(
            conn,
            telegram_id=telegram_id,
            source='user',
            sort='newest',
            get_all=True,
            requester_telegram_id=telegram_id
        )
        profile_row = await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
        profile_resp = UserProfileResponse(**profile_row) if profile_row else None
        fav_count = await conn.fetchval("SELECT COUNT(*) FROM favorites WHERE user_telegram_id = $1", telegram_id)
        logger.info(f"Returned {len(ads)} own listings for user {telegram_id}")
        return UserListingsResponse(ads=ads, profile=profile_resp, favorites=fav_count or 0, messages=0)
    except Exception as e:
        logger.exception(f"Error fetching user listings for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/all_user_listings", response_model=AdsListResponse)
async def get_all_user_listings_api(
    sort: str = 'newest',
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        ads = await _get_listings_from_db(
            conn,
            source='user',
            sort=sort,
            get_all=True,
            requester_telegram_id=telegram_id
        )
        logger.info(f"Returned {len(ads)} total user listings for {telegram_id}")
        return AdsListResponse(ads=ads)
    except Exception as e:
        logger.exception(f"Error fetching all user listings: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/get_listing_details", response_model=ListingResponse)
async def get_listing_details_api(
    ad_id: str,
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        listing_uuid = uuid.UUID(ad_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid listing ID format.")
    row = await conn.fetchrow(
        """SELECT l.*, u.username,
                  EXISTS (
                      SELECT 1 FROM favorites f
                      WHERE f.listing_id = l.id AND f.user_telegram_id = $2
                  ) as is_favorite
           FROM listings l
           LEFT JOIN users u ON l.telegram_id = u.telegram_id
           WHERE l.id = $1 AND l.is_active = TRUE""",
        listing_uuid, telegram_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="Listing not found.")
    await conn.execute('''
        UPDATE listings SET views = views + 1
        WHERE id = $1
    ''', listing_uuid)
    return ListingResponse(
        id=str(row['id']),
        telegram_id=row['telegram_id'],
        username=row['username'],
        title=row['title'],
        description=row['description'],
        price=row['price'],
        rooms=str(row['rooms']) if row['rooms'] else None,
        area=row['area'],
        city=row['city'],
        address=row['address'],
        floor=row['floor'],
        images=row['images'] or [],
        link=row.get('listing_url'),
        created_at=row['created_at'],
        date=row['created_at'].isoformat() if row['created_at'] else None,
        time_posted=row['time_posted'],
        source=row['source'],
        is_favorite=row['is_favorite'],
        views=row['views']
    )

@app.post("/api/search", response_model=AdsListResponse)
async def search_api(
    request: SearchRequest,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    if request.captcha_code and request.captcha_code != "3A7B9C":
        raise HTTPException(status_code=400, detail="Invalid captcha code.")
    try:
        listings_data = parse_kufar(
            city=request.city,
            min_price=request.min_price or 100,
            max_price=request.max_price or 300,
            rooms=request.rooms
        )
        async with db_pool.acquire() as conn:
            for listing in listings_data:
                listing_id = uuid.uuid4()
                await conn.execute('''
                    INSERT INTO listings (
                        id, title, description, price, rooms, area, floor, city, address,
                        images, listing_url, time_posted, source
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (listing_url) DO NOTHING
                ''', listing_id, listing.get("title"), listing["description"],
                    listing["price"], str(listing["rooms"]), listing.get("area"),
                    listing.get("floor"), request.city, listing["address"],
                    [listing["image"]], listing["listing_url"],
                    listing["time_posted"], "kufar")
        ads = await _get_listings_from_db(
            conn,
            source='kufar',
            requester_telegram_id=request.telegram_id,
            limit=20
        )
        logger.info(f"Returned {len(ads)} Kufar listings for user {request.telegram_id}")
        return AdsListResponse(ads=ads)
    except Exception as e:
        logger.exception(f"Error during Kufar search for user {request.telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error during external search.")

@app.post("/api/add_listing", response_model=StatusResponse)
@app.post("/api/update_listing", response_model=StatusResponse)
async def manage_listing(
    request: Request,
    telegram_id: int = Form(...),
    title: str = Form(...),
    description: Optional[str] = Form(None),
    price: int = Form(...),
    rooms: str = Form(...),
    area: Optional[int] = Form(None),
    city: str = Form(...),
    address: Optional[str] = Form(None),
    latitude: Optional[float] = Form(None),
    longitude: Optional[float] = Form(None),
    ad_id: Optional[str] = Form(None),
    photos: List[UploadFile] = File(default=[]),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    is_update = request.url.path.endswith("/update_listing")
    listing_uuid: Optional[uuid.UUID] = None
    existing_images: List[str] = []

    if is_update:
        if not ad_id:
            raise HTTPException(status_code=400, detail="Missing 'ad_id' for update.")
        try:
            listing_uuid = uuid.UUID(ad_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid 'ad_id' format.")
        owner_check = await conn.fetchrow("SELECT telegram_id, images FROM listings WHERE id = $1 AND is_active = TRUE", listing_uuid)
        if not owner_check:
            raise HTTPException(status_code=404, detail="Listing not found or inactive.")
        if owner_check['telegram_id'] != telegram_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this listing.")
        existing_images = owner_check['images'] or []
        logger.info(f"Updating listing {ad_id} for user {telegram_id}. Existing images: {len(existing_images)}")
    else:
        listing_uuid = uuid.uuid4()
        logger.info(f"Adding new listing for user {telegram_id}")

    max_images = 10 if await is_premium_user(telegram_id, conn) else 5
    if len(photos) + len(existing_images) > max_images:
        raise HTTPException(status_code=400, detail=f"Maximum {max_images} images allowed.")

    uploaded_image_urls = existing_images
    for photo in photos:
        if photo.filename:
            url = await upload_image_to_cloudinary(photo)
            uploaded_image_urls.append(url)

    await conn.execute('''
        INSERT INTO listings (
            id, telegram_id, title, description, price, rooms, area, city, address,
            images, created_at, source
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (id)
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            price = EXCLUDED.price,
            rooms = EXCLUDED.rooms,
            area = EXCLUDED.area,
            city = EXCLUDED.city,
            address = EXCLUDED.address,
            images = EXCLUDED.images,
            created_at = CURRENT_TIMESTAMP
    ''', listing_uuid, telegram_id, title, description, price, rooms, area,
        city, address, uploaded_image_urls, datetime.utcnow(), "user")

    action = "updated" if is_update else "created"
    await send_telegram_notification(telegram_id, f"Your from typing import AsyncGenerator

listing '{title}' has been {action}!")
    return StatusResponse(status="success", message=f"Listing {action}")

@app.delete("/api/listings/{ad_id}", response_model=StatusResponse)
async def delete_listing(
    ad_id: str,
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        result = await conn.execute('''
            UPDATE listings
            SET is_active = FALSE
            WHERE id = $1 AND telegram_id = $2 AND is_active = TRUE
        ''', uuid.UUID(ad_id), telegram_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Listing not found or not authorized.")
        await send_telegram_notification(telegram_id, "Your listing has been deleted.")
        return StatusResponse(status="success", message="Listing deleted.")
    except Exception as e:
        logger.error(f"Error deleting listing {ad_id} for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete listing.")

@app.post("/api/favorites", response_model=ToggleFavoriteResponse)
async def toggle_favorite(
    request: ToggleFavoriteRequest,
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    if request.telegram_id != telegram_id:
        raise HTTPException(status_code=403, detail="Unauthorized telegram_id.")
    try:
        if request.state:
            await conn.execute('''
                INSERT INTO favorites (user_telegram_id, listing_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            ''', telegram_id, uuid.UUID(request.ad_id))
        else:
            await conn.execute('''
                DELETE FROM favorites
                WHERE user_telegram_id = $1 AND listing_id = $2
            ''', telegram_id, uuid.UUID(request.ad_id))
        favorites_count = await conn.fetchval('''
            SELECT COUNT(*) FROM favorites WHERE user_telegram_id = $1
        ''', telegram_id)
        return ToggleFavoriteResponse(
            status="success",
            newState=request.state,
            totalFavorites=favorites_count
        )
    except Exception as e:
        logger.error(f"Error toggling favorite for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to toggle favorite.")

@app.get("/api/favorites", response_model=AdsListResponse)
async def get_favorites(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        listings = await conn.fetch('''
            SELECT l.*, u.username, TRUE as is_favorite
            FROM listings l
            LEFT JOIN users u ON l.telegram_id = u.telegram_id
            JOIN favorites f ON l.id = f.listing_id
            WHERE f.user_telegram_id = $1 AND l.is_active = TRUE
            ORDER BY f.added_at DESC
        ''', telegram_id)
        return AdsListResponse(ads=[
            ListingResponse(
                id=str(row["id"]),
                telegram_id=row["telegram_id"],
                username=row["username"],
                title=row["title"],
                description=row["description"],
                price=row["price"],
                rooms=str(row["rooms"]) if row["rooms"] else None,
                area=row["area"],
                city=row["city"],
                address=row["address"],
                floor=row["floor"],
                images=row["images"] or [],
                link=row["listing_url"],
                created_at=row["created_at"],
                date=row["created_at"].isoformat() if row["created_at"] else None,
                time_posted=row["time_posted"],
                source=row["source"],
                is_favorite=True,
                views=row["views"]
            ) for row in listings
        ])
    except Exception as e:
        logger.error(f"Error fetching favorites for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch favorites.")

@app.get("/api/history", response_model=AdsListResponse)
async def get_history(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        listings = await conn.fetch('''
            SELECT l.*, u.username,
                   EXISTS (
                       SELECT 1 FROM favorites f
                       WHERE f.user_telegram_id = $1 AND f.listing_id = l.id
                   ) as is_favorite
            FROM listings l
            LEFT JOIN users u ON l.telegram_id = u.telegram_id
            JOIN history h ON l.id = h.listing_id
            WHERE h.user_telegram_id = $1 AND l.is_active = TRUE
            ORDER BY h.viewed_at DESC
        ''', telegram_id)
        return AdsListResponse(ads=[
            ListingResponse(
                id=str(row["id"]),
                telegram_id=row["telegram_id"],
                username=row["username"],
                title=row["title"],
                description=row["description"],
                price=row["price"],
                rooms=str(row["rooms"]) if row["rooms"] else None,
                area=row["area"],
                city=row["city"],
                address=row["address"],
                floor=row["floor"],
                images=row["images"] or [],
                link=row["listing_url"],
                created_at=row["created_at"],
                date=row["created_at"].isoformat() if row["created_at"] else None,
                time_posted=row["time_posted"],
                source=row["source"],
                is_favorite=row["is_favorite"],
                views=row["views"]
            ) for row in listings
        ])
    except Exception as e:
        logger.error(f"Error fetching history for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch history.")

@app.post("/api/history", response_model=StatusResponse)
async def add_to_history(
    ad_id: str = Form(...),
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        await conn.execute('''
            INSERT INTO history (user_telegram_id, listing_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
        ''', telegram_id, uuid.UUID(ad_id))
        return StatusResponse(status="success", message="Added to history.")
    except Exception as e:
        logger.error(f"Error adding to history for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to add to history.")

@app.post("/api/compare", response_model=StatusResponse)
async def add_to_compare(
    ad_id: str = Form(...),
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        count = await conn.fetchval('''
            SELECT COUNT(*) FROM comparisons
            WHERE user_telegram_id = $1
        ''', telegram_id)
        if count >= 3:
            raise HTTPException(status_code=400, detail="Maximum 3 listings for comparison.")
        await conn.execute('''
            INSERT INTO comparisons (user_telegram_id, listing_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
        ''', telegram_id, uuid.UUID(ad_id))
        return StatusResponse(status="success", message="Added to comparison.")
    except Exception as e:
        logger.error(f"Error adding to comparison for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to add to comparison.")

@app.delete("/api/compare", response_model=StatusResponse)
async def clear_compare(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        await conn.execute('''
            DELETE FROM comparisons
            WHERE user_telegram_id = $1
        ''', telegram_id)
        return StatusResponse(status="success", message="Comparison cleared.")
    except Exception as e:
        logger.error(f"Error clearing comparison for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to clear comparison.")

@app.get("/api/compare", response_model=AdsListResponse)
async def get_compare(
    telegram_id: int = Depends(verify_telegram_user),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    try:
        listings = await conn.fetch('''
            SELECT l.*, u.username,
                   EXISTS (
                       SELECT 1 FROM favorites f
                       WHERE f.user_telegram_id = $1 AND f.listing_id = l.id
                   ) as is_favorite
            FROM listings l
            LEFT JOIN users u ON l.telegram_id = u.telegram_id
            JOIN comparisons c ON l.id = c.listing_id
            WHERE c.user_telegram_id = $1 AND l.is_active = TRUE
            ORDER BY c.added_at DESC
        ''', telegram_id)
        return AdsListResponse(ads=[
            ListingResponse(
                id=str(row["id"]),
                telegram_id=row["telegram_id"],
                username=row["username"],
                title=row["title"],
                description=row["description"],
                price=row["price"],
                rooms=str(row["rooms"]) if row["rooms"] else None,
                area=row["area"],
                city=row["city"],
                address=row["address"],
                floor=row["floor"],
                images=row["images"] or [],
                link=row["listing_url"],
                created_at=row["created_at"],
                date=row["created_at"].isoformat() if row["created_at"] else None,
                time_posted=row["time_posted"],
                source=row["source"],
                is_favorite=row["is_favorite"],
                views=row["views"]
            ) for row in listings
        ])
    except Exception as e:
        logger.error(f"Error fetching comparison for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch comparison.")

@app.post("/api/upload", response_model=dict)
async def upload_image(
    file: UploadFile = File(...),
    telegram_id: int = Depends(verify_telegram_user)
):
    try:
        url = await upload_image_to_cloudinary(file)
        return {"url": url}
    except Exception as e:
        logger.error(f"Error uploading image for {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to upload image.")

@app.get("/api/logs", response_model=LogsResponse)
async def get_logs():
    try:
        async with aiofiles.open("app.log", mode="r", encoding="utf-8") as f:
            content = await f.read()
        return LogsResponse(logs=content)
    except Exception as e:
        logger.error(f"Error reading logs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to read logs.")

@app.on_event("startup")
async def start_kufar_parser():
    async def parse_periodically():
        while True:
            await parse_and_store_kufar_listings()
            await asyncio.sleep(3600)
    asyncio.create_task(parse_periodically())
    logger.info("Started Kufar parsing background task.")
