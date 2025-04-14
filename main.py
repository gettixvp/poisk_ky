import os
import logging
from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import asyncpg
from parse_kufar import parse_kufar
from telegram import Bot
from telegram.ext import Application, CommandHandler
from dotenv import load_dotenv
from typing import List, Optional
import uuid
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, filename='kufar_parser.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Конфигурация
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_MJr6nebWzp3C@ep-fragrant-math-a2ladk0z-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7846698102:AAFR2bhmjAkPiV-PjtnFIu_oRnzxYPP1xVo")
RENDER_URL = "https://jake-3.onrender.com"

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
    await conn.close()

@app.on_event("startup")
async def startup_event():
    await init_db()
    # Инициализация Telegram бота
    bot_app = Application.builder().token(TELEGRAM_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start_command))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)

# Telegram /start команда
async def start_command(update, context):
    welcome_message = (
        "🏠 Добро пожаловать в бот поиска квартир!\n\n"
        "Я помогу вам найти идеальное жилье в Беларуси. "
        "Используйте наше приложение для удобного поиска по городам, ценам и количеству комнат.\n\n"
        "👉 Нажмите кнопку ниже, чтобы начать:"
    )
    keyboard = [[{"text": "Открыть приложение", "web_app": {"url": RENDER_URL}}]]
    await update.message.reply_text(welcome_message, reply_markup={"inline_keyboard": keyboard})

# Регистрация пользователя
@app.post("/api/register_user")
async def register_user(telegram_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute(
        "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING",
        telegram_id
    )
    await conn.close()
    return {"status": "success"}

# Поиск объявлений
@app.post("/api/search")
async def search(request: SearchRequest):
    if request.captcha_code and request.captcha_code != "3A7B9C":
        raise HTTPException(status_code=400, detail="CAPTCHA_REQUIRED")
    
    listings = parse_kufar(city=request.city, min_price=request.min_price, max_price=request.max_price)
    filtered_listings = [
        listing for listing in listings
        if not request.rooms or str(listing['rooms']) == request.rooms or 
           (request.rooms == "4+" and listing['rooms'] and listing['rooms'] >= 4) or
           (request.rooms == "studio" and "студия" in (listing['description'] or "").lower())
    ]
    
    # Заглушка для теста, если парсинг не дал результатов
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
        logging.warning(f"Парсинг Kufar не вернул результатов для города {request.city}. Возвращена заглушка.")

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

# Получение популярных объявлений
@app.get("/api/ads")
async def get_popular_ads():
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
    
    return {"ads": ads}

# Получение новых объявлений
@app.get("/api/new_listings")
async def get_new_listings(telegram_id: int):
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
    
    return {"ads": ads}

# Получение пользовательских объявлений
@app.get("/api/user_listings")
async def get_user_listings(telegram_id: int):
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
    
    return {"ads": ads, "favorites": 0, "messages": 0}

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
    image_url = None
    if photos:
        image_url = f"https://example.com/images/{uuid.uuid4()}.jpg"  # Имитация загрузки
    
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
    
    return {"status": "success"}

# Главная страница
@app.get("/", response_class=HTMLResponse)
async def serve_html():
    with open("static/index.html", "r") as f:
        return HTMLResponse(content=f.read())

# Поддержка HEAD для здоровья
@app.head("/")
async def head_root():
    return Response(status_code=200)

# Favicon заглушка
@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)

# Логи для отладки
@app.get("/api/logs")
async def get_logs():
    try:
        with open("kufar_parser.log", "r") as f:
            # Читаем последние 50 строк
            lines = f.readlines()[-50:]
        return {"logs": "".join(lines)}
    except Exception as e:
        return {"logs": f"Ошибка чтения логов: {str(e)}"}
