from pymongo import MongoClient
from src.config import DATABASE_NAME, DATABASE_COLLECTION, DATABASE_URL


client = MongoClient(DATABASE_URL)
db = client[DATABASE_NAME]
collection = db[DATABASE_COLLECTION]
