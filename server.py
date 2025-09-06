from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import json
from datetime import datetime, timedelta
import jwt
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Sai Satcharitra API", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()
JWT_SECRET = os.getenv("JWT_SECRET", "fallback-secret-key")

# In-memory storage
users_db = {}
chapters_db = []
quotes_db = []
bookmarks_db = []
progress_db = []

# Sample data
SAMPLE_CHAPTERS = [
    {
        "id": i,
        "title": f"Chapter {i}",
        "english": {"title": f"Chapter {i}", "content": f"This is the content of chapter {i} in English. " * 50, "summary": f"Summary of chapter {i}"},
        "hindi": {"title": f"अध्याय {i}", "content": f"यह अध्याय {i} की हिंदी में सामग्री है। " * 50, "summary": f"अध्याय {i} का सारांश"},
        "telugu": {"title": f"అధ్యాయము {i}", "content": f"ఇది తెలుగులో అధ్యాయం {i} యొక్క కంటెంట్. " * 50, "summary": f"అధ్యాయం {i} సారాంశం"},
        "marathi": {"title": f"प्रकरण {i}", "content": f"हा प्रकरण {i} चा मराठी मधील मजकूर आहे. " * 50, "summary": f"प्रकरण {i} चा सारांश"}
    }
    for i in range(1, 49)
]

SAMPLE_QUOTES = [
    {
        "id": i,
        "english": f"Spiritual quote {i} in English",
        "hindi": f"आध्यात्मिक उद्धरण {i}",
        "telugu": f"ఆధ్యాత్మిక కోట్ {i}",
        "marathi": f"आध्यात्मिक उद्धरण {i}",
        "date": datetime.now().isoformat()
    }
    for i in range(1, 21)
]

# Initialize data
@app.on_event("startup")
def initialize_data():
    global chapters_db, quotes_db
    chapters_db = SAMPLE_CHAPTERS.copy()
    quotes_db = SAMPLE_QUOTES.copy()

# Models
class UserCreate(BaseModel):
    email: str

class OTPVerify(BaseModel):
    email: str
    otp: str

class AdminLogin(BaseModel):
    email: str
    password: str

# Routes
@app.get("/")
def read_root():
    return {"message": "Sai Satcharitra API - Temporary In-Memory Version", "status": "running"}

@app.get("/api/init-data")
def init_data():
    initialize_data()
    return {"message": "Data initialized successfully", "chapters": len(chapters_db), "quotes": len(quotes_db)}

@app.get("/api/chapters")
def get_chapters(language: str = "english"):
    if not chapters_db:
        initialize_data()
    
    formatted_chapters = []
    for chapter in chapters_db:
        lang_data = chapter.get(language, chapter["english"])
        formatted_chapters.append({
            "id": chapter["id"],
            "title": lang_data["title"],
            "content": lang_data["content"],
            "summary": lang_data["summary"],
            "read_time": "5 min"
        })
    
    return formatted_chapters

@app.get("/api/chapters/{chapter_id}")
def get_chapter(chapter_id: int, language: str = "english"):
    chapter = next((c for c in chapters_db if c["id"] == chapter_id), None)
    if not chapter:
        raise HTTPException(status_code=404, detail="Chapter not found")
    
    lang_data = chapter.get(language, chapter["english"])
    return {
        "id": chapter["id"],
        "title": lang_data["title"],
        "content": lang_data["content"],
        "summary": lang_data["summary"],
        "read_time": "5 min"
    }

@app.get("/api/quotes/daily")
def get_daily_quote(language: str = "english"):
    if not quotes_db:
        initialize_data()
    
    # Get today's quote (first quote for demo)
    quote = quotes_db[0]
    return {
        "id": quote["id"],
        "text": quote.get(language, quote["english"]),
        "date": quote["date"]
    }

@app.get("/api/quotes/random")
def get_random_quote(language: str = "english"):
    if not quotes_db:
        initialize_data()
    
    import random
    quote = random.choice(quotes_db)
    return {
        "id": quote["id"],
        "text": quote.get(language, quote["english"]),
        "date": quote["date"]
    }

# Simple auth endpoints (for demo)
@app.post("/api/auth/send-otp")
def send_otp(user: UserCreate):
    # Demo: just return success
    return {"message": "OTP sent successfully"}

@app.post("/api/auth/verify-otp")
def verify_otp(verification: OTPVerify):
    # Demo: accept any OTP
    token = jwt.encode({"email": verification.email, "exp": datetime.utcnow() + timedelta(days=30)}, JWT_SECRET, algorithm="HS256")
    users_db[verification.email] = {"email": verification.email, "role": "user"}
    return {"access_token": token, "token_type": "bearer"}

@app.post("/api/auth/admin-login")
def admin_login(admin: AdminLogin):
    if admin.email == "admin@saibaba.com" and admin.password == "admin123":
        token = jwt.encode({"email": admin.email, "role": "admin", "exp": datetime.utcnow() + timedelta(days=7)}, JWT_SECRET, algorithm="HS256")
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/api/auth/me")
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=["HS256"])
        return {"email": payload["email"], "role": payload.get("role", "user")}
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

# Bookmark endpoints (demo)
@app.post("/api/bookmarks")
def create_bookmark(bookmark: dict):
    bookmark["id"] = len(bookmarks_db) + 1
    bookmarks_db.append(bookmark)
    return {"message": "Bookmark created", "id": bookmark["id"]}

@app.get("/api/bookmarks")
def get_bookmarks():
    return bookmarks_db

@app.delete("/api/bookmarks/{bookmark_id}")
def delete_bookmark(bookmark_id: int):
    global bookmarks_db
    bookmarks_db = [b for b in bookmarks_db if b.get("id") != bookmark_id]
    return {"message": "Bookmark deleted"}

# Progress endpoints (demo)
@app.put("/api/progress")
def update_progress(progress: dict):
    # Find existing or create new
    existing = next((p for p in progress_db if p.get("chapter_id") == progress.get("chapter_id")), None)
    if existing:
        existing.update(progress)
    else:
        progress_db.append(progress)
    return {"message": "Progress updated"}

@app.get("/api/progress")
def get_progress():
    return progress_db

# Search endpoint
@app.get("/api/search")
def search_chapters(q: str, language: str = "english"):
    if not chapters_db:
        initialize_data()
    
    results = []
    for chapter in chapters_db:
        lang_data = chapter.get(language, chapter["english"])
        if q.lower() in lang_data["title"].lower() or q.lower() in lang_data["content"].lower():
            results.append({
                "id": chapter["id"],
                "title": lang_data["title"],
                "summary": lang_data["summary"],
                "relevance": 0.9
            })
    
    return results[:10]  # Return top 10 results

# Admin endpoints (demo)
@app.get("/api/admin/analytics/overview")
def get_analytics():
    return {
        "total_users": len(users_db),
        "total_chapters": len(chapters_db),
        "total_bookmarks": len(bookmarks_db),
        "active_sessions": 5
    }

@app.get("/api/admin/users")
def get_all_users():
    return [{"email": email, "role": data.get("role", "user")} for email, data in users_db.items()]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
