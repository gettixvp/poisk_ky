import os
import logging
import asyncio
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

# Third-party libraries
import asyncpg # Database driver
import aiofiles # Async file operations
from fastapi import (
    FastAPI,
    HTTPException,
    Form,
    UploadFile,
    File,
    Request,
    Depends, # For dependency injection
    Response,
    status
)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware # Example middleware
from pydantic import BaseModel, Field
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import RetryAfter
from dotenv import load_dotenv

# Local imports (assuming parse_kufar is in the same directory or installable)
try:
    from parse_kufar import parse_kufar
except ImportError:
    # Provide a dummy function if parse_kufar is not available
    def parse_kufar(**kwargs):
        print(f"Warning: parse_kufar module not found. Using dummy implementation. Args: {kwargs}")
        # Return dummy data matching expected structure
        return [
             {'listing_url': f'https://dummy.com/{uuid.uuid4()}', 'rooms': '1', 'description': 'Dummy desc 1', 'price': 300, 'area': 40, 'address': 'Dummy Address 1', 'image': f'https://placehold.co/600x400/e0e0e0/ffffff?text=Dummy+1', 'time_posted': 'сегодня'},
             {'listing_url': f'https://dummy.com/{uuid.uuid4()}', 'rooms': '2', 'description': 'Dummy desc 2', 'price': 450, 'area': 55, 'address': 'Dummy Address 2', 'image': f'https://placehold.co/600x400/e0e0e0/ffffff?text=Dummy+2', 'time_posted': 'вчера'},
        ]
    print("Warning: 'parse_kufar' module not found. Using dummy data.")


# --- Configuration Loading ---
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
RENDER_URL = os.getenv("RENDER_URL") # Base URL for web app and webhook
UPLOAD_DIR = "uploads" # Directory to save uploaded images

# Validate essential configuration
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set.")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN environment variable not set.")
if not RENDER_URL:
    raise ValueError("RENDER_URL environment variable not set.")


# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    filename='app.log', # Changed log filename
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Add console handler as well for visibility during development/debugging
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)


# --- Pydantic Models ---
# Request Models
class SearchRequest(BaseModel):
    telegram_id: int
    city: str
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    rooms: Optional[str] = None
    captcha_code: Optional[str] = None # Added based on frontend code

class UserProfileRequest(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None

class ToggleFavoriteRequest(BaseModel):
    telegram_id: int
    ad_id: str # Assuming ad IDs are UUIDs stored as strings
    state: bool # true to add, false to remove

class DeleteListingRequest(BaseModel):
    telegram_id: int
    ad_id: str # Assuming ad IDs are UUIDs stored as strings

# Response Models
class StatusResponse(BaseModel):
    status: str
    message: Optional[str] = None

class UserProfileResponse(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None

class ListingResponse(BaseModel):
    id: str # UUID as string
    telegram_id: Optional[int] = None # Make optional as Kufar ads won't have it
    username: Optional[str] = None # Add username for user ads
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[int] = None
    rooms: Optional[str] = None
    area: Optional[int] = None
    city: Optional[str] = None
    address: Optional[str] = None
    image: Optional[str] = None
    images: List[str] = [] # For multiple images
    listing_url: Optional[str] = Field(None, alias="link") # Alias for frontend compatibility
    created_at: Optional[datetime] = None
    date: Optional[str] = None # Keep 'date' if frontend expects isoformat string
    time_posted: Optional[str] = None
    source: Optional[str] = None
    is_favorite: bool = False # Add favorite status

class AdsListResponse(BaseModel):
    ads: List[ListingResponse]

class UserListingsResponse(BaseModel):
    ads: List[ListingResponse]
    profile: Optional[UserProfileResponse] = None
    favorites: int = 0 # Example count, replace with real data if needed
    messages: int = 0 # Example count

class FavoritesResponse(BaseModel):
    status: str
    favorites: List[str] # List of favorite ad IDs

class ToggleFavoriteResponse(BaseModel):
    status: str
    newState: bool
    totalFavorites: Optional[int] = None # Optional: return updated count

class LogsResponse(BaseModel):
    logs: str


# --- FastAPI Application Setup ---
app = FastAPI(title="Квартирный Помощник API")

# --- Database Pool ---
db_pool: Optional[asyncpg.Pool] = None

async def get_db_connection():
    """Dependency to get a database connection from the pool."""
    if db_pool is None:
        logger.error("Database pool is not initialized.")
        raise HTTPException(status_code=500, detail="Database connection not available.")
    async with db_pool.acquire() as connection:
        yield connection

# --- Telegram Bot Application State ---
# Use a simple dictionary for state if complex state management isn't needed yet
class AppState:
    bot_app: Optional[Application] = None

app.state = AppState()

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins for simplicity, restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Basic Security Headers Middleware (Example)
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    # Add other headers like CSP if needed
    return response

# --- Static Files ---
# Ensure the 'static' directory exists
if not os.path.exists("static"):
    os.makedirs("static")
    logger.info("Created 'static' directory.")
# Ensure the 'uploads' directory exists
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)
    logger.info(f"Created '{UPLOAD_DIR}' directory.")

app.mount("/static", StaticFiles(directory="static"), name="static")
# Mount uploads directory to serve images (consider a more robust solution like CDN in production)
app.mount(f"/{UPLOAD_DIR}", StaticFiles(directory=UPLOAD_DIR), name="uploads")


# --- Database Initialization ---
async def init_db():
    """Initializes the database schema."""
    global db_pool
    try:
        # Create pool
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        if db_pool is None:
             raise Exception("Failed to create database pool.")
        logger.info("Database connection pool created successfully.")

        async with db_pool.acquire() as conn:
            # Ensure users table schema
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            logger.info("Checked/Created 'users' table.")

            # Ensure listings table schema
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS listings (
                    id UUID PRIMARY KEY,
                    telegram_id BIGINT, -- Can be NULL for non-user listings
                    title TEXT,
                    description TEXT,
                    price INTEGER,
                    rooms TEXT,
                    area INTEGER,
                    city TEXT,
                    address TEXT,
                    images TEXT[], -- Use TEXT array for multiple image URLs
                    listing_url TEXT UNIQUE, -- For Kufar etc. to check duplicates
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    time_posted TEXT,
                    source TEXT DEFAULT 'user', -- 'user', 'kufar', etc.
                    is_active BOOLEAN DEFAULT TRUE, -- To mark listings as deleted without removing row
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id) ON DELETE SET NULL -- Optional: link to user
                );
            ''')
            logger.info("Checked/Created 'listings' table.")

            # Ensure favorites table schema
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
            logger.info("Checked/Created 'favorites' table.")

            # Ensure necessary indexes exist
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_url ON listings (listing_url);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_source ON listings (source);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_telegram_id ON listings (telegram_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites (user_telegram_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_favorites_listing ON favorites (listing_id);")
            logger.info("Checked/Created indexes.")

        logger.info("Database schema initialization/check complete.")

    except Exception as e:
        logger.exception(f"CRITICAL: Failed to initialize database or pool: {str(e)}") # Use logger.exception to include traceback
        # Optionally re-raise to stop the application if DB is critical
        raise

# --- Telegram Bot Setup ---
async def set_webhook_with_retry(bot, webhook_url, max_attempts=5, initial_delay=1):
    """Sets the Telegram webhook with retry logic."""
    attempt = 0
    delay = initial_delay
    while attempt < max_attempts:
        try:
            await bot.set_webhook(url=webhook_url, allowed_updates=Update.ALL_TYPES)
            logger.info(f"Attempt {attempt+1}: Webhook set successfully to {webhook_url}")
            return True
        except RetryAfter as e:
            logger.warning(f"Attempt {attempt+1}: Flood control waiting for {e.retry_after} seconds...")
            await asyncio.sleep(e.retry_after)
            delay = initial_delay # Reset delay after RetryAfter
        except Exception as e:
            logger.error(f"Attempt {attempt+1}: Failed to set webhook: {str(e)}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2 # Exponential backoff for other errors
        attempt += 1

    logger.error(f"Failed to set webhook after {max_attempts} attempts.")
    raise HTTPException(status_code=500, detail="Failed to set Telegram webhook.")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
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
        # Ensure RENDER_URL ends with a slash for the web app URL if needed by the app
        web_app_url = RENDER_URL.rstrip('/') + "/"
        keyboard = [[{"text": "🚀 Открыть приложение", "web_app": {"url": web_app_url}}]]
        await update.message.reply_text(
             welcome_message,
             reply_markup={"inline_keyboard": keyboard},
             # Consider adding parse_mode='MarkdownV2' or 'HTML' if needed
        )
        logger.info(f"Sent /start response to user {user.id} ({user.username})")

        # Register or update user info in DB (run concurrently)
        asyncio.create_task(register_or_update_user(UserProfileRequest(
            telegram_id=user.id,
            username=user.username,
            first_name=user.first_name
        )))

    except Exception as e:
        logger.exception(f"Error in start_command for user {user.id}: {str(e)}")
        try:
             # Try sending a simple error message back to the user
             await update.message.reply_text("Произошла ошибка при обработке команды. Пожалуйста, попробуйте позже.")
        except Exception as send_error:
             logger.error(f"Failed to send error message to user {user.id}: {send_error}")


# --- FastAPI Lifecycle Events ---
@app.on_event("startup")
async def startup_event():
    """FastAPI startup event: Initialize DB and Telegram bot."""
    logger.info("Application startup sequence initiated.")
    await init_db()

    # Create uploads directory if it doesn't exist
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    logger.info(f"Ensured upload directory exists: {UPLOAD_DIR}")

    try:
        bot_app = Application.builder().token(TELEGRAM_TOKEN).build()
        bot_app.add_handler(CommandHandler("start", start_command))
        await bot_app.initialize()

        webhook_path = "/telegram_webhook" # Define path explicitly
        webhook_url = RENDER_URL.rstrip('/') + webhook_path
        logger.info(f"Attempting to set webhook to: {webhook_url}")

        if await set_webhook_with_retry(bot_app.bot, webhook_url):
             # Start polling in background only if webhook setup failed (for local dev)
             # This is usually NOT recommended for production with webhooks
             # await bot_app.start_polling() # Uncomment for local dev IF webhook fails
             # logger.info("Webhook setup successful, bot handler ready.")
             pass # Webhook is set, no need to poll
        else:
             logger.error("Webhook setup failed permanently. Bot might not receive updates.")
             # Decide whether to stop the app or continue without webhook updates

        app.state.bot_app = bot_app
        logger.info("Telegram bot application initialized and handlers registered.")

    except Exception as e:
        logger.exception(f"CRITICAL: Failed to initialize Telegram bot application: {str(e)}")
        # Stop the app if bot initialization fails
        raise RuntimeError("Telegram bot initialization failed") from e
    logger.info("Application startup sequence complete.")


@app.on_event("shutdown")
async def shutdown_event():
    """FastAPI shutdown event: Close DB pool and stop Telegram bot."""
    logger.info("Application shutdown sequence initiated.")
    if db_pool:
        try:
            await db_pool.close()
            logger.info("Database connection pool closed.")
        except Exception as e:
            logger.error(f"Error closing database pool: {str(e)}")

    if hasattr(app.state, "bot_app") and app.state.bot_app:
        try:
             if app.state.bot_app.running: # Check if it's running
                 await app.state.bot_app.stop()
             await app.state.bot_app.shutdown()
             logger.info("Telegram bot application stopped and shut down.")
        except Exception as e:
            logger.error(f"Error stopping/shutting down Telegram bot: {str(e)}")
    logger.info("Application shutdown sequence complete.")


# --- API Endpoints ---

# Root endpoint / Web App Host
@app.get("/", response_class=HTMLResponse)
async def serve_html():
    """Serves the main HTML file for the Web App."""
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

# Webhook endpoint for Telegram updates
@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    """Handles incoming updates from Telegram."""
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
        # Return 200 OK to Telegram even on error to prevent retries for processing issues
        return JSONResponse(content={"status": "processing_error"}, status_code=200)

# User Registration/Update
@app.post("/api/register_user", response_model=StatusResponse)
async def register_or_update_user(
    profile: UserProfileRequest,
    conn: asyncpg.Connection = Depends(get_db_connection) # Inject DB connection
):
    """Registers a new user or updates existing user's info."""
    try:
        # Use ON CONFLICT for atomic insert/update
        result = await conn.execute(
            '''
            INSERT INTO users (telegram_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (telegram_id) DO UPDATE
            SET username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                -- Optionally update an 'updated_at' timestamp here
            WHERE users.username IS DISTINCT FROM EXCLUDED.username
               OR users.first_name IS DISTINCT FROM EXCLUDED.first_name;
            ''',
            profile.telegram_id, profile.username, profile.first_name
        )
        # result format: INSERT 0 1 or UPDATE 1 etc.
        action = "updated" if "UPDATE" in result else "registered"
        logger.info(f"User {profile.telegram_id} {action}.")
        return StatusResponse(status="success", message=f"User {action}")
    except Exception as e:
        logger.exception(f"Error registering/updating user {profile.telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error during user registration.")

# Get User Profile Info (Example - not used by current frontend)
@app.get("/api/profile", response_model=UserProfileResponse)
async def get_profile(
    telegram_id: int,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Fetches user profile information."""
    row = await conn.fetchrow(
        "SELECT telegram_id, username, first_name FROM users WHERE telegram_id = $1",
        telegram_id
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return UserProfileResponse(**row)

# Get Listings (Used for Kufar, User's Own, All User Ads)
async def _get_listings_from_db(
    conn: asyncpg.Connection,
    telegram_id: Optional[int] = None, # Filter by user if provided
    source: Optional[str] = None,      # Filter by source ('user', 'kufar')
    sort: str = 'newest',              # 'newest', 'oldest', 'price_asc', 'price_desc'
    limit: int = 10,
    get_all: bool = False,             # Flag to ignore limit
    requester_telegram_id: Optional[int] = None # ID of user making the request (for favorites)
) -> List[ListingResponse]:
    """Helper function to fetch listings based on filters."""
    query = """
        SELECT
            l.id, l.telegram_id, l.title, l.description, l.price, l.rooms, l.area,
            l.city, l.address, l.images, l.listing_url, l.created_at, l.time_posted, l.source,
            u.username, -- Get username from joined users table
            EXISTS (
                SELECT 1 FROM favorites f
                WHERE f.listing_id = l.id AND f.user_telegram_id = $1
            ) as is_favorite -- Check if favorited by the requester
        FROM listings l
        LEFT JOIN users u ON l.telegram_id = u.telegram_id -- Join to get username
        WHERE l.is_active = TRUE
    """
    params = [requester_telegram_id] # Parameter for favorite check
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
        query += " ORDER BY l.price ASC NULLS LAST, l.created_at DESC" # Handle NULL prices
    elif sort == 'price_desc':
        query += " ORDER BY l.price DESC NULLS LAST, l.created_at DESC"
    else: # Default to newest
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
            username=row['username'], # Added username
            title=row['title'],
            description=row['description'],
            price=row['price'],
            rooms=row['rooms'],
            area=row['area'],
            city=row['city'],
            address=row['address'],
            images=row['images'] or [], # Ensure it's a list
            # Use listing_url for 'link' alias
            link=row.get('listing_url'), # Fetch correct column
            created_at=row['created_at'],
            date=row['created_at'].isoformat() if row['created_at'] else None, # Keep iso string for frontend
            time_posted=row['time_posted'],
            source=row['source'],
            is_favorite=row['is_favorite'] # Added favorite status
        ) for row in rows
    ]

# Get "New" listings (e.g., Kufar source)
@app.get("/api/new_listings", response_model=AdsListResponse)
async def get_new_listings_api(
    telegram_id: int, # ID of the user requesting (for favorite status)
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Fetches recent listings from external sources (like Kufar)."""
    try:
        ads = await _get_listings_from_db(
             conn,
             source='kufar', # Example: fetch only Kufar ads here
             sort='newest',
             limit=10,
             requester_telegram_id=telegram_id
        )
        logger.info(f"Returned {len(ads)} new 'kufar' listings for requesting user {telegram_id}")
        return AdsListResponse(ads=ads)
    except Exception as e:
        logger.exception(f"Error fetching new listings for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Get User's Own Listings (for "My" tab)
@app.get("/api/user_listings", response_model=UserListingsResponse)
async def get_user_listings_api(
    telegram_id: int, # ID of the owner AND requester
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Fetches listings owned by the specified user."""
    # TODO: Add fetching of actual favorite/message counts if needed
    try:
        ads = await _get_listings_from_db(
            conn,
            telegram_id=telegram_id, # Filter by owner
            source='user',
            sort='newest',
            get_all=True, # Show all user's listings
            requester_telegram_id=telegram_id # Check favorites for the owner themselves
        )
        # Fetch user profile info separately if needed for the response structure
        profile_row = await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
        profile_resp = UserProfileResponse(**profile_row) if profile_row else None

        # Get favorite count for this user
        fav_count = await conn.fetchval("SELECT COUNT(*) FROM favorites WHERE user_telegram_id = $1", telegram_id)

        logger.info(f"Returned {len(ads)} own listings for user {telegram_id}")
        return UserListingsResponse(ads=ads, profile=profile_resp, favorites=fav_count or 0, messages=0) # messages=0 is placeholder
    except Exception as e:
        logger.exception(f"Error fetching user listings for user {telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Get All User Listings (for Home tab)
@app.get("/api/all_user_listings", response_model=AdsListResponse)
async def get_all_user_listings_api(
    # Add telegram_id of the *requester* if you need to show favorite status
    request: Request, # Get requester ID from verified request data if possible, else query param
    telegram_id: Optional[int] = None, # Requester ID from query param as fallback
    sort: str = 'newest',
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Fetches all active listings created by users."""
    # Here, you'd ideally verify the requester's telegram_id securely
    # For now, we use the query parameter if provided
    requester_id = telegram_id
    try:
        ads = await _get_listings_from_db(
            conn,
            source='user',
            sort=sort,
            get_all=True, # Fetch all user listings for the home page feed
            requester_telegram_id=requester_id # Pass requester ID for favorite status
        )
        logger.info(f"Returned {len(ads)} total user listings (requested by {requester_id})")
        return AdsListResponse(ads=ads)
    except Exception as e:
        logger.exception(f"Error fetching all user listings: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Get Listing Details (for Edit form)
@app.get("/api/get_listing_details", response_model=ListingResponse)
async def get_listing_details_api(
    ad_id: str, # UUID as string
    telegram_id: int, # User requesting (for auth check)
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Fetches details for a specific listing, verifying ownership."""
    try:
        listing_uuid = uuid.UUID(ad_id) # Convert string to UUID
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid listing ID format.")

    row = await conn.fetchrow(
        """SELECT l.*, u.username FROM listings l
           LEFT JOIN users u ON l.telegram_id = u.telegram_id
           WHERE l.id = $1 AND l.is_active = TRUE""",
        listing_uuid
    )

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Listing not found.")

    # Basic ownership check (allow fetching any active ad for now, refine if needed)
    # if row['telegram_id'] != telegram_id:
    #    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You are not authorized to view these details.")

    return ListingResponse(
         id=str(row['id']),
         telegram_id=row['telegram_id'],
         username=row['username'],
         title=row['title'],
         description=row['description'],
         price=row['price'],
         rooms=row['rooms'],
         area=row['area'],
         city=row['city'],
         address=row['address'],
         images=row['images'] or [],
         link=row.get('listing_url'),
         created_at=row['created_at'],
         date=row['created_at'].isoformat() if row['created_at'] else None,
         time_posted=row['time_posted'],
         source=row['source'],
         is_favorite=False # Favorite status not needed for edit form usually
    )


# Search Endpoint (Kufar Parsing)
@app.post("/api/search", response_model=AdsListResponse)
async def search_api(
    request: SearchRequest,
    conn: asyncpg.Connection = Depends(get_db_connection) # Inject connection
):
    """Parses Kufar based on search criteria and returns results."""
    # Add Captcha check simulation if needed
    # if request.captcha_code != "EXPECTED_CODE":
    #     return JSONResponse(content={"error": "CAPTCHA_REQUIRED"}, status_code=400)

    try:
        # Consider running CPU-bound parsing in a thread pool executor
        # For now, assume parse_kufar is reasonably fast or includes async I/O
        listings_data = parse_kufar(
            city=request.city,
            min_price=request.min_price,
            max_price=request.max_price,
            rooms=request.rooms
        )
        logger.info(f"Parsed {len(listings_data)} Kufar listings for city {request.city} (User: {request.telegram_id})")

        # Format results into ListingResponse model
        # Check favorite status for the requesting user
        ads_response = []
        if listings_data:
             # Check favorite status for parsed listings (less efficient than joining)
             # Alternative: Save parsed listings first, then fetch with favorite status.
             # Current approach: Return directly without checking favorites from Kufar search.
             ads_response = [
                 ListingResponse(
                     id=f"kufar_{uuid.uuid4()}", # Generate temporary ID or use URL hash
                     title=listing.get('title', f"Квартира в {request.city}"),
                     description=listing.get('description'),
                     price=listing.get('price'),
                     rooms=str(listing.get('rooms')) if listing.get('rooms') else None,
                     area=listing.get('area'),
                     city=request.city,
                     address=listing.get('address'),
                     images=[listing['image']] if listing.get('image') else [],
                     link=listing.get('listing_url'),
                     time_posted=listing.get('time_posted'),
                     source='kufar',
                     is_favorite=False # Cannot easily check favorite status here
                 ) for listing in listings_data[:20] # Limit results
             ]

        # Optional: Save new Kufar listings to DB asynchronously
        # asyncio.create_task(save_new_kufar_listings(conn, listings_data, request.city))

        return AdsListResponse(ads=ads_response)

    except Exception as e:
        logger.exception(f"Error during Kufar search for user {request.telegram_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error during external search.")

# Add/Update Listing (Handles file uploads)
async def save_upload_file(upload_file: UploadFile, destination: str) -> None:
    """Asynchronously saves an uploaded file."""
    try:
        async with aiofiles.open(destination, 'wb') as out_file:
            while content := await upload_file.read(1024 * 1024):  # Read file in chunks 1MB
                await out_file.write(content)
    except Exception as e:
        logger.exception(f"Error saving file {upload_file.filename} to {destination}: {e}")
        # Decide if failure to save one image should fail the whole request
        # For now, log the error and continue, the image URL won't be saved/updated

@app.post("/api/add_listing", response_model=StatusResponse)
@app.post("/api/update_listing", response_model=StatusResponse) # Use same endpoint logic for update
async def manage_listing(
    request: Request, # Get request object to check URL path
    conn: asyncpg.Connection = Depends(get_db_connection),
    # Form fields
    telegram_id: int = Form(...),
    title: str = Form(...),
    description: Optional[str] = Form(None),
    price: int = Form(...),
    rooms: str = Form(...),
    area: Optional[int] = Form(None),
    city: str = Form(...),
    address: Optional[str] = Form(None),
    ad_id: Optional[str] = Form(None), # For identifying listing to update
    photos: List[UploadFile] = File(default=[]), # Accept multiple files
):
    """Adds a new listing or updates an existing one."""
    is_update = request.url.path.endswith("/update_listing")
    listing_uuid: Optional[uuid.UUID] = None
    existing_images: List[str] = []

    if is_update:
        if not ad_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing 'ad_id' for update.")
        try:
            listing_uuid = uuid.UUID(ad_id)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid 'ad_id' format.")

        # Verify ownership and get existing images
        owner_check = await conn.fetchrow("SELECT telegram_id, images FROM listings WHERE id = $1 AND is_active = TRUE", listing_uuid)
        if not owner_check:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Listing not found or inactive.")
        if owner_check['telegram_id'] != telegram_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You are not authorized to update this listing.")
        existing_images = owner_check['images'] or []
        logger.info(f"Updating listing {ad_id} for user {telegram_id}. Existing images: {len(existing_images)}")
    else:
        listing_uuid = uuid.uuid4() # Generate new ID for add
        logger.info(f"Adding new listing for user {telegram_id}")

    uploaded_image_urls: List[str] = []
    save_tasks = []

    # Process new file uploads
    if photos:
         if len(photos) > 5:
              raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Maximum 5 photos allowed.")
         for photo in photos:
            if photo.filename: # Check if a file was actually uploaded
                # Sanitize filename (basic example)
                safe_filename = f"{uuid.uuid4()}_{photo.filename.replace(' ', '_')}"
                file_path = os.path.join(UPLOAD_DIR, safe_filename)
                save_tasks.append(save_upload_file(photo, file_path))
                # Store the URL path, not the local filesystem path
                image_url = f"/{UPLOAD_DIR}/{safe_filename}"
                uploaded_image_urls.append(image_url)
            else:
                 logger.warning("Received file input with empty filename.")

    # Wait for all file saves to complete
    if save_tasks:
         logger.info(f"Saving {len(save_tasks)} uploaded photos...")
         await asyncio.gather(*save_tasks)
         logger.info("Finished saving photos.")

    # Determine final image list: If updating and new photos uploaded, replace old ones.
    # If updating and no new photos, keep old ones. If adding, use new ones.
    final_image_urls = uploaded_image_urls if (is_update and uploaded_image_urls) or not is_update else existing_images

    try:
        if is_update:
            await conn.execute(
                '''
                UPDATE listings
                SET title = $2, description = $3, price = $4, rooms = $5, area = $6,
                    city = $7, address = $8, images = $9
                    -- Optionally update 'updated_at' timestamp
                WHERE id = $1 AND telegram_id = $10 AND is_active = TRUE
                ''',
                listing_uuid, title, description, price, rooms, area, city, address, final_image_urls, telegram_id
            )
            action_msg = f"Listing {ad_id} updated"
        else:
            await conn.execute(
                '''
                INSERT INTO listings (
                    id, telegram_id, title, description, price, rooms, area, city,
                    address, images, source, is_active
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'user', TRUE)
                ''',
                listing_uuid, telegram_id, title, description, price, rooms, area, city,
                address, final_image_urls
            )
            action_msg = f"Listing {listing_uuid} added"

        logger.info(f"{action_msg} for user {telegram_id}")
        return StatusResponse(status="success", message=action_msg)

    except asyncpg.exceptions.UniqueViolationError:
         logger.warning(f"Potential duplicate listing based on unique constraint for user {telegram_id}.")
         # This shouldn't happen for 'user' source if listing_url is NULL, check constraints
         raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Duplicate content detected.")
    except Exception as e:
        logger.exception(f"Error {'updating' if is_update else 'adding'} listing for user {telegram_id}: {str(e)}")
        # Consider deleting saved files if DB operation fails after saving
        # for url_path in uploaded_image_urls:
        #     try: os.remove(os.path.join(UPLOAD_DIR, os.path.basename(url_path)))
        #     except OSError: pass
        raise HTTPException(status_code=500, detail="Database error during listing management.")


# Delete Listing
@app.post("/api/delete_listing", response_model=StatusResponse)
async def delete_listing(
    payload: DeleteListingRequest,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Marks a user's listing as inactive."""
    try:
        listing_uuid = uuid.UUID(payload.ad_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid listing ID format.")

    # Mark as inactive instead of deleting row to preserve history/favorites link if needed
    result = await conn.execute(
        """
        UPDATE listings
        SET is_active = FALSE
        WHERE id = $1 AND telegram_id = $2 AND is_active = TRUE
        """,
        listing_uuid, payload.telegram_id
    )

    if "UPDATE 1" in result:
        logger.info(f"Marked listing {payload.ad_id} as inactive for user {payload.telegram_id}")
         # Also remove from favorites table
        await conn.execute("DELETE FROM favorites WHERE listing_id = $1", listing_uuid)
        return StatusResponse(status="success", message="Listing deleted.")
    else:
        # Check if listing exists but belongs to another user or was already inactive
        exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM listings WHERE id = $1)", listing_uuid)
        if not exists:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Listing not found.")
        else:
             # Could be ownership issue or already inactive
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Could not delete listing. Check ownership or status.")

# Get Favorites
@app.get("/api/get_favorites", response_model=FavoritesResponse)
async def get_favorites(
    telegram_id: int,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Gets the list of favorite listing IDs for a user."""
    rows = await conn.fetch(
        "SELECT listing_id FROM favorites WHERE user_telegram_id = $1",
        telegram_id
    )
    favorite_ids = [str(row['listing_id']) for row in rows]
    logger.info(f"User {telegram_id} has {len(favorite_ids)} favorites.")
    return FavoritesResponse(status="success", favorites=favorite_ids)

# Toggle Favorite
@app.post("/api/toggle_favorite", response_model=ToggleFavoriteResponse)
async def toggle_favorite(
    payload: ToggleFavoriteRequest,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Adds or removes a listing from a user's favorites."""
    try:
        listing_uuid = uuid.UUID(payload.ad_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid listing ID format.")

    # Check if the listing exists and is active
    listing_exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM listings WHERE id = $1 AND is_active = TRUE)", listing_uuid)
    if not listing_exists:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot favorite: Listing not found or inactive.")

    try:
        if payload.state: # Add to favorites
            await conn.execute(
                """
                INSERT INTO favorites (user_telegram_id, listing_id)
                VALUES ($1, $2)
                ON CONFLICT (user_telegram_id, listing_id) DO NOTHING
                """,
                payload.telegram_id, listing_uuid
            )
            logger.info(f"User {payload.telegram_id} added listing {payload.ad_id} to favorites.")
            action = "added"
        else: # Remove from favorites
            await conn.execute(
                "DELETE FROM favorites WHERE user_telegram_id = $1 AND listing_id = $2",
                payload.telegram_id, listing_uuid
            )
            logger.info(f"User {payload.telegram_id} removed listing {payload.ad_id} from favorites.")
            action = "removed"

        # Get updated count
        fav_count = await conn.fetchval("SELECT COUNT(*) FROM favorites WHERE user_telegram_id = $1", payload.telegram_id)

        return ToggleFavoriteResponse(status="success", newState=payload.state, totalFavorites=fav_count or 0)

    except Exception as e:
         logger.exception(f"Error toggling favorite for user {payload.telegram_id}, listing {payload.ad_id}: {e}")
         raise HTTPException(status_code=500, detail="Database error while updating favorites.")


# Get Listings by IDs (for Favorites Tab)
@app.get("/api/get_listings_by_ids", response_model=AdsListResponse)
async def get_listings_by_ids(
    ids: str, # Comma-separated list of UUID strings
    telegram_id: int, # User requesting (to check favorite status *of these specific ads*)
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """Fetches details for a list of listing IDs."""
    try:
        listing_uuids = [uuid.UUID(id_str.strip()) for id_str in ids.split(',') if id_str.strip()]
        if not listing_uuids:
            return AdsListResponse(ads=[]) # Return empty list if no valid IDs provided
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid listing ID format in list.")

    # Use uuid[] = ANY($...) for efficient lookup
    query = """
        SELECT
            l.id, l.telegram_id, l.title, l.description, l.price, l.rooms, l.area,
            l.city, l.address, l.images, l.listing_url, l.created_at, l.time_posted, l.source,
            u.username,
            EXISTS (
                SELECT 1 FROM favorites f
                WHERE f.listing_id = l.id AND f.user_telegram_id = $1
            ) as is_favorite
        FROM listings l
        LEFT JOIN users u ON l.telegram_id = u.telegram_id
        WHERE l.id = ANY($2::UUID[]) AND l.is_active = TRUE
        ORDER BY l.created_at DESC -- Or maintain order from input IDs if needed
    """
    rows = await conn.fetch(query, telegram_id, listing_uuids)

    ads = [
        ListingResponse(
            id=str(row['id']),
            telegram_id=row['telegram_id'],
            username=row['username'],
            title=row['title'],
            description=row['description'],
            price=row['price'],
            rooms=row['rooms'],
            area=row['area'],
            city=row['city'],
            address=row['address'],
            images=row['images'] or [],
            link=row.get('listing_url'),
            created_at=row['created_at'],
            date=row['created_at'].isoformat() if row['created_at'] else None,
            time_posted=row['time_posted'],
            source=row['source'],
            is_favorite=row['is_favorite']
        ) for row in rows
    ]
    logger.info(f"Fetched {len(ads)} listings by IDs for user {telegram_id}.")
    return AdsListResponse(ads=ads)


# --- Simple Health Check & Logs ---
@app.head("/")
async def head_root():
    """Handles HEAD requests for health checks."""
    return Response(status_code=200)

@app.get("/favicon.ico", status_code=204)
async def favicon():
    """Returns No Content for favicon requests."""
    return Response(status_code=204)

@app.get("/api/health")
async def health_check():
    """Simple health check endpoint."""
    # Add DB check if needed: try acquiring a connection
    try:
         conn = await get_db_connection().__anext__() # Get one connection
         await conn.execute("SELECT 1")
         db_status = "ok"
    except Exception:
         db_status = "error"
    return {"status": "ok", "database_status": db_status}


@app.get("/api/logs", response_model=LogsResponse)
async def get_logs_api():
    """Retrieves the last 50 lines of the application log."""
    log_file = "app.log"
    try:
        async with aiofiles.open(log_file, mode="r", encoding="utf-8") as f:
            lines = await f.readlines()
        # Get last 50 lines
        log_content = "".join(lines[-50:])
        return LogsResponse(logs=log_content)
    except FileNotFoundError:
        logger.warning(f"Log file not found: {log_file}")
        return LogsResponse(logs="Log file not found.")
    except Exception as e:
        logger.exception(f"Error reading log file: {str(e)}")
        return LogsResponse(logs=f"Ошибка чтения логов: {str(e)}")


# --- Main Execution Guard ---
# This part is typically handled by the ASGI server (like uvicorn)
# Uvicorn command: uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# (Replace main with your filename if different)

# Example of how you might run with uvicorn programmatically (less common)
# if __name__ == "__main__":
#     import uvicorn
#     port = int(os.getenv("PORT", 8000)) # Use PORT env var if available
#     host = os.getenv("HOST", "0.0.0.0")
#     logger.info(f"Starting Uvicorn server on {host}:{port}")
#     uvicorn.run("main:app", host=host, port=port, reload=True) # reload=True for development
