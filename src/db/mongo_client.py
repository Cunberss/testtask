from pymongo import MongoClient
from src.config import DATABASE_NAME, DATABASE_COLLECTION


client = MongoClient()
db = client[DATABASE_NAME]
collection = db[DATABASE_COLLECTION]