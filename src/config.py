from dotenv import load_dotenv
import os

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_COLLECTION = os.getenv('DATABASE_COLLECTION')