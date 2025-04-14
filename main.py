import os
import logging
from fastapi import FastAPI, HTTPException, Form, UploadFile, File, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import asyncpg
from parse_kufar import parse_kufar
from telegram import Update
from telegram.ext import Updater, CommandHandler, Dispatcher
from dotenv import load_dotenv
from typing import List, Optional
import uuid
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    filename='kufar_parser.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Конфигурация
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_MJr6nebWzp3C@ep-fragrant-math-a2ladk0z-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7846698102:AAFR2bhmjAkPiV-PjtnFIu_oRnzxYPP1xVo")
RENDER_URL = os.getenv("RENDER_URL", "https://jake-3.onrender.com")

# Модели данных
class SearchRequest(BaseModel):
    telegram_id: int
    city: str
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    rooms: Optional[str] = None
    captcha_code: Optional[str] = None

# Инициализация базы данных
async def init_db():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                telegram_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT
            );
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
                image TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT DEFAULT 'user'
            );
        ''')
        logger.info("Database initialized successfully")
        await conn.close()
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

@app.on_event("startup")
async def startup_event():
    await init_db()
    # Инициализация Telegram бота с webhook
    try:
        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        dispatcher = updater.dispatcher
        dispatcher.add_handler(CommandHandler("start", start_command))
        updater.start_webhook(
            listen="0.0.0.0",
            port=8443,  # Render allows this port for webhooks
            url_path="/telegram_webhook",
            webhook_url=f"{RENDER_URL}/telegram_webhook"
        )
        updater.bot.set_webhook(f"{RENDER_URL}/telegram_webhook")
        app.state.updater = updater
        logger.info(f"Telegram webhook set to {RENDER_URL}/telegram_webhook")
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    if hasattr(app.state, "updater"):
        try:
            app.state.updater.stop()
            logger.info("Telegram bot stopped")
        except Exception as e:
            logger.error(f"Error stopping Telegram bot: {str(e)}")

# Telegram /start команда
def start_command(update, context):
    try:
        welcome_message = (
            "🏠 Добро пожаловать в бот поиска квартир!\n\n"
            "Я помогу вам найти идеальное жилье в Беларуси. "
            "Используйте наше приложение для удобного поиска по городам, ценам и количеству комнат.\n\n"
            "👉 Нажмите кнопку ниже, чтобы начать:"
        )
        keyboard = [[{"text": "Открыть приложение", "web_app": {"url": RENDER_URL}}]]
        update.message.reply_text(welcome_message, reply_markup={"inline_keyboard": keyboard})
        logger.info(f"Sent /start response to user {update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error in start_command: {str(e)}")

# Webhook для Telegram
@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
        update_obj = Update.de_json(update, app.state.updater.bot)
        if update_obj:
            app.state.updater.dispatcher.process_update(update_obj)
            logger.debug(f"Processed Telegram update: {update_obj.update_id}")
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return {"status": "error"}

# Регистрация пользователя
@app.post("/api/register_user")
async def register_user(telegram_id: int):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING",
            telegram_id
        )
        await conn.close()
        logger.info(f"Registered user {telegram_id}")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error registering user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Поиск объявлений
@app.post("/api/search")
async def search(request: SearchRequest):
    try:
        if request.captcha_code and request.captcha_code != "3A7B9C":
            logger.warning(f"Invalid CAPTCHA for user {request.telegram_id}")
            raise HTTPException(status_code=400, detail="INVALID_CAPTCHA")
        
        if not request.captcha_code:
            logger.info(f"CAPTCHA required for user {request.telegram_id}")
            return {"error": "CAPTCHA_REQUIRED"}
        
        listings = parse_kufar(city=request.city, min_price=request.min_price, max_price=request.max_price)
        logger.info(f"Parsed {len(listings)} listings for city {request.city}")
        
        filtered_listings = [
            listing for listing in listings
            if not request.rooms or str(listing['rooms']) == request.rooms or 
               (request.rooms == "4+" and listing['rooms'] and listing['rooms'] >= 4) or
               (request.rooms == "studio" and "студия" in (listing['description'] or "").lower())
        ]
        
        # Заглушка для теста
        if not filtered_listings:
            filtered_listings = [{
                'price': 200,
                'rooms': 2,
                'area': 50,
                'floor': 'этаж 5 из 9',
                'description': 'Тестовая квартира для отладки',
                'address': 'ул. Тестовая 123, Минск',
                'image': 'https://via.placeholder.com/150'
            }]
            logger.warning(f"No results from Kufar for {request.city}. Using stub data.")

        # Сохранение в базу
        conn = await asyncpg.connect(DATABASE_URL)
        for listing in filtered_listings:
            listing_id = uuid.uuid4()
            title = f"Квартира в {request.city} - {listing['rooms'] or 'не указан'} комн."
            await conn.execute(
                '''
                INSERT INTO listings (id, telegram_id, title, description, price, rooms, area, city, address, image, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT DO NOTHING
                ''',
                listing_id, request.telegram_id, title, listing['description'],
                listing['price'], str(listing['rooms']), listing['area'], request.city,
                listing['address'], listing['image'], 'kufar'
            )
        await conn.close()
        
        return {"ads": filtered_listings}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in search for user {request.telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Получение популярных объявлений
@app.get("/api/ads")
async def get_popular_ads():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT * FROM listings WHERE source = 'kufar' ORDER BY created_at DESC LIMIT 5")
        await conn.close()
        
        ads = [{
            "id": str(row['id']),
            "title": row['title'],
            "description": row['description'],
            "price": row['price'],
            "rooms": row['rooms'],
            "area": row['area'],
            "city": row['city'],
            "address": row['address'],
            "image": row['image'],
            "date": row['created_at'].isoformat(),
            "source": row['source']
        } for row in rows]
        
        logger.info(f"Returned {len(ads)} popular ads")
        return {"ads": ads}
    except Exception as e:
        logger.error(f"Error fetching popular ads: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Получение новых объявлений
@app.get("/api/new_listings")
async def get_new_listings(telegram_id: int):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch(
            "SELECT * FROM listings WHERE source = 'kufar' ORDER BY created_at DESC LIMIT 10"
        )
        await conn.close()
        
        ads = [{
            "id": str(row['id']),
            "title": row['title'],
            "description": row['description'],
            "price": row['price'],
            "rooms": row['rooms'],
            "area": row['area'],
            "city": row['city'],
            "address": row['address'],
            "image": row['image'],
            "date": row['created_at'].isoformat(),
            "source": row['source']
        } for row in rows]
        
        logger.info(f"Returned {len(ads)} new listings for user {telegram_id}")
        return {"ads": ads}
    except Exception as e:
        logger.error(f"Error fetching new listings for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Получение пользовательских объявлений
@app.get("/api/user_listings")
async def get_user_listings(telegram_id: int):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch(
            "SELECT * FROM listings WHERE telegram_id = $1 AND source = 'user'",
            telegram_id
        )
        await conn.close()
        
        ads = [{
            "id": str(row['id']),
            "title": row['title'],
            "description": row['description'],
            "price": row['price'],
            "rooms": row['rooms'],
            "area": row['area'],
            "city": row['city'],
            "address": row['address'],
            "image": row['image'],
            "date": row['created_at'].isoformat(),
            "source": row['source']
        } for row in rows]
        
        logger.info(f"Returned {len(ads)} user listings for user {telegram_id}")
        return {"ads": ads, "favorites": 0, "messages": 0}
    except Exception as e:
        logger.error(f"Error fetching user listings for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Добавление объявления
@app.post("/api/add_listing")
async def add_listing(
    telegram_id: int = Form(...),
    title: str = Form(...),
    description: Optional[str] = Form(None),
    price: int = Form(...),
    rooms: str = Form(...),
    area: Optional[int] = Form(None),
    city: str = Form(...),
    address: Optional[str] = Form(None),
    photos: List[UploadFile] = File(None)
):
    try:
        image_url = None
        if photos:
            image_url = f"https://example.com/images/{uuid.uuid4()}.jpg"  # Replace with actual storage logic
        
        conn = await asyncpg.connect(DATABASE_URL)
        listing_id = uuid.uuid4()
        await conn.execute(
            '''
            INSERT INTO listings (id, telegram_id, title, description, price, rooms, area, city, address, image, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''',
            listing_id, telegram_id, title, description, price, rooms, area, city, address, image_url, 'user'
        )
        await conn.close()
        
        logger.info(f"Added listing {listing_id} for user {telegram_id}")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error adding listing for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Главная страница
@app.get("/", response_class=HTMLResponse)
async def serve_html():
    try:
        with open("static/index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except Exception as e:
        logger.error(f"Error serving index.html: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Поддержка HEAD
@app.head("/")
async def head_root():
    return Response(status_code=200)

# Favicon
@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)

# Логи для отладки
@app.get("/api/logs")
async def get_logs():
    try:
        with open("kufar_parser.log", "r") as f:
            lines = f.readlines()[-50:]
        return {"logs": "".join(lines)}
    except Exception as e:
        logger.error(f"Error reading logs: {str(e)}")
        return {"logs": f"Ошибка чтения логов: {str(e)}"}
