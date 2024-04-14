from motor.motor_asyncio import AsyncIOMotorClient
from src.config import DATABASE_NAME, DATABASE_COLLECTION, DATABASE_URL


client = AsyncIOMotorClient(DATABASE_URL)
db = client[DATABASE_NAME]
collection = db[DATABASE_COLLECTION]