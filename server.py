from fastapi import FastAPI, APIRouter, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timedelta
import random
import jwt
from passlib.context import CryptContext
import re
from bson import ObjectId

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Security
JWT_SECRET = os.environ.get('JWT_SECRET', 'sai_satcharitra_secret_key_2024')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRATION_HOURS = 24 * 7  # 7 days

security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Create the main app
app = FastAPI(title="Sai Satcharitra API", version="1.0.0")
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper function to convert ObjectId to string
def serialize_document(doc):
    if doc:
        # Convert _id to id for API responses
        if "_id" in doc:
            doc["id"] = str(doc["_id"])
        
        # Convert any ObjectId fields to strings
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                doc[key] = str(value)
    return doc

# Pydantic Models
class User(BaseModel):
    id: Optional[str] = None
    email: EmailStr
    name: str
    phone: Optional[str] = None
    role: str = "user"  # "user" or "admin"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
    reading_progress: Dict[str, Any] = Field(default_factory=dict)
    preferences: Dict[str, Any] = Field(default_factory=dict)

class UserCreate(BaseModel):
    email: EmailStr
    name: str
    phone: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr

class AdminLogin(BaseModel):
    email: EmailStr
    password: str

class OTPVerification(BaseModel):
    email: EmailStr
    otp: str

class OTPResponse(BaseModel):
    message: str
    expires_at: datetime
    otp_for_testing: Optional[str] = None  # For testing purposes

class AuthResponse(BaseModel):
    access_token: str
    token_type: str
    user: User

class Chapter(BaseModel):
    id: Optional[str] = None
    number: int
    title: Dict[str, str]  # Multilingual titles
    content: Dict[str, str]  # Multilingual content
    summary: Dict[str, str]  # Multilingual summaries
    audio_url: Optional[Dict[str, str]] = None  # TTS audio URLs
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class ChapterCreate(BaseModel):
    number: int
    title: Dict[str, str]
    content: Dict[str, str]
    summary: Dict[str, str]

class Bookmark(BaseModel):
    id: Optional[str] = None
    user_id: str
    chapter_number: int
    position: int  # Character position in text
    text_snippet: str
    note: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class BookmarkCreate(BaseModel):
    chapter_number: int
    position: int
    text_snippet: str
    note: Optional[str] = None

class ReadingProgress(BaseModel):
    id: Optional[str] = None
    user_id: str
    chapter_number: int
    progress_percentage: float
    last_position: int
    reading_time_minutes: int
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class ProgressUpdate(BaseModel):
    chapter_number: int
    progress_percentage: float
    last_position: int
    reading_time_minutes: int

class SpiritualQuote(BaseModel):
    id: Optional[str] = None
    content: Dict[str, str]  # Multilingual quotes
    author: str = "Sai Baba"
    date: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True

class QuoteCreate(BaseModel):
    content: Dict[str, str]
    author: str = "Sai Baba"

class ArthiVideo(BaseModel):
    id: Optional[str] = None
    title: Dict[str, str]
    description: Dict[str, str]
    video_url: str
    thumbnail_url: Optional[str] = None
    duration: Optional[int] = None  # in seconds
    category: str = "aarthi"
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)

class VideoCreate(BaseModel):
    title: Dict[str, str]
    description: Dict[str, str]
    video_url: str
    thumbnail_url: Optional[str] = None
    duration: Optional[int] = None
    category: str = "aarthi"

class MeditationSession(BaseModel):
    id: Optional[str] = None
    user_id: str
    duration_minutes: int
    mood_before: Optional[str] = None
    mood_after: Optional[str] = None
    session_type: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class SessionCreate(BaseModel):
    duration_minutes: int
    mood_before: Optional[str] = None
    mood_after: Optional[str] = None
    session_type: Optional[str] = None

# Email service for OTP
async def send_otp_email(email: str, otp: str):
    """Send OTP via email - Mock implementation for now"""
    logger.info(f"Sending OTP {otp} to {email}")
    # In production, implement actual email sending
    return True

# Authentication helpers
def generate_otp():
    return str(random.randint(100000, 999999))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return encoded_jwt

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        user = await db.users.find_one({"email": email})
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return serialize_document(user)
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_admin_user(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user

# Authentication Routes
@api_router.post("/auth/send-otp", response_model=OTPResponse)
async def send_otp(user_login: UserLogin, background_tasks: BackgroundTasks):
    """Send OTP to user's email"""
    email = user_login.email
    otp = generate_otp()
    expires_at = datetime.utcnow() + timedelta(minutes=10)
    
    # Store OTP in database
    await db.otps.delete_many({"email": email})  # Remove existing OTPs
    await db.otps.insert_one({
        "email": email,
        "otp": otp,
        "expires_at": expires_at,
        "used": False
    })
    
    # Send OTP via email
    background_tasks.add_task(send_otp_email, email, otp)
    
    # For testing purposes, include OTP in response
    return {
        "message": f"OTP sent to {email}. It will expire in 10 minutes.",
        "expires_at": expires_at,
        "otp_for_testing": otp  # This makes testing easier
    }

@api_router.post("/auth/verify-otp", response_model=AuthResponse)
async def verify_otp(otp_verification: OTPVerification):
    """Verify OTP and login/register user"""
    email = otp_verification.email
    otp = otp_verification.otp
    
    # Check OTP
    otp_doc = await db.otps.find_one({
        "email": email,
        "otp": otp,
        "used": False,
        "expires_at": {"$gt": datetime.utcnow()}
    })
    
    if not otp_doc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OTP"
        )
    
    # Mark OTP as used
    await db.otps.update_one(
        {"_id": otp_doc["_id"]},
        {"$set": {"used": True}}
    )
    
    # Find or create user
    user = await db.users.find_one({"email": email})
    if not user:
        # Create new user
        new_user = User(email=email, name=email.split("@")[0])
        user_dict = new_user.dict()
        result = await db.users.insert_one(user_dict)
        user = await db.users.find_one({"_id": result.inserted_id})
    
    user = serialize_document(user)
    
    # Create access token
    access_token = create_access_token(data={"sub": email})
    
    return AuthResponse(
        access_token=access_token,
        token_type="bearer",
        user=User(**user)
    )

@api_router.post("/auth/admin-login", response_model=AuthResponse)
async def admin_login(admin_data: AdminLogin):
    """Admin login with email and password"""
    email = admin_data.email
    password = admin_data.password
    
    # Check if user exists and is admin
    user = await db.users.find_one({"email": email, "role": "admin"})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin credentials"
        )
    
    # For demo purposes, accept any password for admin@saibaba.com
    # In production, implement proper password hashing
    if email == "admin@saibaba.com" and password == "admin123":
        user = serialize_document(user)
        access_token = create_access_token(data={"sub": email})
        
        return AuthResponse(
            access_token=access_token,
            token_type="bearer",
            user=User(**user)
        )
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid admin credentials"
    )

@api_router.get("/auth/me", response_model=User)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Get current user information"""
    return User(**current_user)

@api_router.put("/auth/profile", response_model=User)
async def update_profile(user_update: UserCreate, current_user: dict = Depends(get_current_user)):
    """Update user profile"""
    update_data = user_update.dict()
    update_data["updated_at"] = datetime.utcnow()
    
    await db.users.update_one(
        {"_id": ObjectId(current_user["_id"])},
        {"$set": update_data}
    )
    
    updated_user = await db.users.find_one({"_id": ObjectId(current_user["_id"])})
    return User(**serialize_document(updated_user))

# Chapter Routes
@api_router.get("/chapters", response_model=List[Chapter])
async def get_chapters(language: str = "english"):
    """Get all chapters"""
    chapters = await db.chapters.find().sort("number", 1).to_list(100)
    return [Chapter(**serialize_document(chapter)) for chapter in chapters]

@api_router.get("/chapters/{chapter_number}", response_model=Chapter)
async def get_chapter(chapter_number: int, language: str = "english"):
    """Get specific chapter"""
    chapter = await db.chapters.find_one({"number": chapter_number})
    if not chapter:
        raise HTTPException(status_code=404, detail="Chapter not found")
    return Chapter(**serialize_document(chapter))

@api_router.post("/chapters", response_model=Chapter)
async def create_chapter(chapter: ChapterCreate, admin_user: dict = Depends(get_admin_user)):
    """Create new chapter (Admin only)"""
    chapter_dict = chapter.dict()
    result = await db.chapters.insert_one(chapter_dict)
    created_chapter = await db.chapters.find_one({"_id": result.inserted_id})
    return Chapter(**serialize_document(created_chapter))

@api_router.put("/chapters/{chapter_number}", response_model=Chapter)
async def update_chapter(chapter_number: int, chapter: ChapterCreate, admin_user: dict = Depends(get_admin_user)):
    """Update chapter (Admin only)"""
    chapter_dict = chapter.dict()
    chapter_dict["updated_at"] = datetime.utcnow()
    
    result = await db.chapters.update_one(
        {"number": chapter_number},
        {"$set": chapter_dict}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Chapter not found")
    
    updated_chapter = await db.chapters.find_one({"number": chapter_number})
    return Chapter(**serialize_document(updated_chapter))

# Bookmark Routes
@api_router.get("/bookmarks", response_model=List[Bookmark])
async def get_bookmarks(current_user: dict = Depends(get_current_user)):
    """Get user's bookmarks"""
    bookmarks = await db.bookmarks.find({"user_id": current_user["_id"]}).sort("created_at", -1).to_list(100)
    return [Bookmark(**serialize_document(bookmark)) for bookmark in bookmarks]

@api_router.post("/bookmarks", response_model=Bookmark)
async def create_bookmark(bookmark: BookmarkCreate, current_user: dict = Depends(get_current_user)):
    """Create new bookmark"""
    bookmark_dict = bookmark.dict()
    bookmark_dict["user_id"] = current_user["_id"]
    result = await db.bookmarks.insert_one(bookmark_dict)
    created_bookmark = await db.bookmarks.find_one({"_id": result.inserted_id})
    return Bookmark(**serialize_document(created_bookmark))

@api_router.delete("/bookmarks/{bookmark_id}")
async def delete_bookmark(bookmark_id: str, current_user: dict = Depends(get_current_user)):
    """Delete bookmark"""
    if not ObjectId.is_valid(bookmark_id):
        raise HTTPException(status_code=400, detail="Invalid bookmark ID")
        
    result = await db.bookmarks.delete_one({
        "_id": ObjectId(bookmark_id),
        "user_id": current_user["_id"]
    })
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Bookmark not found")
    return {"message": "Bookmark deleted"}

# Reading Progress Routes
@api_router.get("/progress", response_model=List[ReadingProgress])
async def get_reading_progress(current_user: dict = Depends(get_current_user)):
    """Get user's reading progress"""
    progress = await db.reading_progress.find({"user_id": current_user["_id"]}).sort("updated_at", -1).to_list(100)
    return [ReadingProgress(**serialize_document(p)) for p in progress]

@api_router.put("/progress", response_model=ReadingProgress)
async def update_reading_progress(progress: ProgressUpdate, current_user: dict = Depends(get_current_user)):
    """Update reading progress for a chapter"""
    progress_dict = progress.dict()
    progress_dict["user_id"] = current_user["_id"]
    progress_dict["updated_at"] = datetime.utcnow()
    
    # Upsert progress
    await db.reading_progress.update_one(
        {"user_id": current_user["_id"], "chapter_number": progress.chapter_number},
        {"$set": progress_dict},
        upsert=True
    )
    
    updated_progress = await db.reading_progress.find_one({
        "user_id": current_user["_id"],
        "chapter_number": progress.chapter_number
    })
    return ReadingProgress(**serialize_document(updated_progress))

# Daily Quotes Routes
@api_router.get("/quotes/daily", response_model=SpiritualQuote)
async def get_daily_quote(language: str = "english"):
    """Get quote of the day"""
    # Simple algorithm: use day of year to get consistent daily quote
    day_of_year = datetime.now().timetuple().tm_yday
    quotes = await db.spiritual_quotes.find({"is_active": True}).to_list(1000)
    
    if not quotes:
        # Return default quote if none exist
        return SpiritualQuote(
            content={
                "english": "Allah Malik - God is the Master of all",
                "hindi": "अल्लाह मालिक - ईश्वर सभी के स्वामी हैं",
                "telugu": "అల్లా మాలిక్ - దేవుడు అందరికీ యజమాని",
                "marathi": "अल्लाह मालिक - देव सर्वांचा स्वामी आहे"
            },
            author="Sai Baba"
        )
    
    # Get quote based on day of year
    quote_index = day_of_year % len(quotes)
    quote = quotes[quote_index]
    return SpiritualQuote(**serialize_document(quote))

@api_router.get("/quotes/random", response_model=SpiritualQuote)
async def get_random_quote():
    """Get random quote"""
    quotes = await db.spiritual_quotes.find({"is_active": True}).to_list(1000)
    if not quotes:
        return await get_daily_quote()
    
    import random
    quote = random.choice(quotes)
    return SpiritualQuote(**serialize_document(quote))

@api_router.post("/quotes", response_model=SpiritualQuote)
async def create_quote(quote: QuoteCreate, admin_user: dict = Depends(get_admin_user)):
    """Create new quote (Admin only)"""
    quote_dict = quote.dict()
    result = await db.spiritual_quotes.insert_one(quote_dict)
    created_quote = await db.spiritual_quotes.find_one({"_id": result.inserted_id})
    return SpiritualQuote(**serialize_document(created_quote))

# Aarthi Video Routes
@api_router.get("/arthi", response_model=List[ArthiVideo])
async def get_arthi_videos():
    """Get all aarthi videos"""
    videos = await db.arthi_videos.find({"is_active": True}).sort("created_at", -1).to_list(100)
    return [ArthiVideo(**serialize_document(video)) for video in videos]

@api_router.post("/arthi", response_model=ArthiVideo)
async def create_arthi_video(video: VideoCreate, admin_user: dict = Depends(get_admin_user)):
    """Create new aarthi video (Admin only)"""
    video_dict = video.dict()
    result = await db.arthi_videos.insert_one(video_dict)
    created_video = await db.arthi_videos.find_one({"_id": result.inserted_id})
    return ArthiVideo(**serialize_document(created_video))

# Meditation Routes
@api_router.get("/meditation/videos", response_model=List[ArthiVideo])
async def get_meditation_videos():
    """Get meditation videos"""
    videos = await db.arthi_videos.find({"category": "meditation", "is_active": True}).sort("created_at", -1).to_list(100)
    return [ArthiVideo(**serialize_document(video)) for video in videos]

@api_router.post("/meditation/sessions", response_model=MeditationSession)
async def create_meditation_session(session: SessionCreate, current_user: dict = Depends(get_current_user)):
    """Log meditation session"""
    session_dict = session.dict()
    session_dict["user_id"] = current_user["_id"]
    result = await db.meditation_sessions.insert_one(session_dict)
    created_session = await db.meditation_sessions.find_one({"_id": result.inserted_id})
    return MeditationSession(**serialize_document(created_session))

@api_router.get("/meditation/sessions", response_model=List[MeditationSession])
async def get_meditation_sessions(current_user: dict = Depends(get_current_user)):
    """Get user's meditation sessions"""
    sessions = await db.meditation_sessions.find({"user_id": current_user["_id"]}).sort("created_at", -1).to_list(100)
    return [MeditationSession(**serialize_document(session)) for session in sessions]

# Search Route
@api_router.get("/search")
async def search_chapters(q: str, language: str = "english"):
    """Search chapters by content"""
    if len(q) < 3:
        raise HTTPException(status_code=400, detail="Search query must be at least 3 characters")
    
    # Text search in content and title
    search_query = {
        "$or": [
            {f"title.{language}": {"$regex": q, "$options": "i"}},
            {f"content.{language}": {"$regex": q, "$options": "i"}},
            {f"summary.{language}": {"$regex": q, "$options": "i"}}
        ]
    }
    
    chapters = await db.chapters.find(search_query).sort("number", 1).to_list(20)
    return [Chapter(**serialize_document(chapter)) for chapter in chapters]

# Admin Routes
@api_router.get("/admin/users", response_model=List[User])
async def get_all_users(admin_user: dict = Depends(get_admin_user)):
    """Get all users (Admin only)"""
    users = await db.users.find().sort("created_at", -1).to_list(1000)
    return [User(**serialize_document(user)) for user in users]

@api_router.get("/admin/analytics/overview")
async def get_analytics_overview(admin_user: dict = Depends(get_admin_user)):
    """Get analytics overview (Admin only)"""
    total_users = await db.users.count_documents({})
    total_chapters = await db.chapters.count_documents({})
    total_bookmarks = await db.bookmarks.count_documents({})
    total_sessions = await db.meditation_sessions.count_documents({})
    
    # Active users (logged in within last 30 days)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    active_users = await db.users.count_documents({
        "created_at": {"$gte": thirty_days_ago}
    })
    
    return {
        "total_users": total_users,
        "active_users": active_users,
        "total_chapters": total_chapters,
        "total_bookmarks": total_bookmarks,
        "total_meditation_sessions": total_sessions,
        "analytics_date": datetime.utcnow()
    }

# Health check
@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow()}

# Initialize sample data
@api_router.post("/init-data")
async def initialize_sample_data(force: bool = False):
    """Initialize sample chapters and admin user"""
    # Check if data already exists
    existing_chapters = await db.chapters.count_documents({})
    
    # Always ensure admin user exists
    admin_user_exists = await db.users.find_one({"email": "admin@saibaba.com", "role": "admin"})
    if not admin_user_exists:
        # Create or update admin user
        await db.users.update_one(
            {"email": "admin@saibaba.com"},
            {"$set": {
                "email": "admin@saibaba.com",
                "name": "Admin",
                "role": "admin",
                "created_at": datetime.utcnow(),
                "is_active": True,
                "reading_progress": {},
                "preferences": {}
            }},
            upsert=True
        )
    
    if existing_chapters > 0 and not force:
        return {"message": "Data already initialized, admin user ensured"}
    
    # Clear existing data if force is True
    if force:
        await db.chapters.delete_many({})
        await db.spiritual_quotes.delete_many({})
    
    # Load real chapter data
    import subprocess
    import sys
    
    # Run conversion script to get chapter data
    result = subprocess.run([sys.executable, '/app/backend/convert_chapters.py'], 
                          capture_output=True, text=True, cwd='/app/backend')
    
    if result.returncode != 0:
        print(f"Conversion script failed: {result.stderr}")
        return {"message": "Failed to convert chapter data"}
    
    # Import and use the conversion functions
    sys.path.append('/app/backend')
    from convert_chapters import parse_copy_data, convert_to_mongodb_format, generate_sample_quotes
    
    # Parse and convert the real chapter data
    parsed_data = parse_copy_data('/app/backend/backup_chapters.sql')
    real_chapters = convert_to_mongodb_format(parsed_data)
    
    # Insert real chapters
    await db.chapters.insert_many(real_chapters)
    
    # Insert spiritual quotes
    real_quotes = generate_sample_quotes()
    await db.spiritual_quotes.insert_many(real_quotes)
    
    return {"message": f"Initialized {len(real_chapters)} chapters, {len(real_quotes)} quotes, and admin user"}

# Include router in main app
app.include_router(api_router)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)