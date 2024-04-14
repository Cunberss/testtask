import os.path
from datetime import datetime

from aiogram import Router, F
from aiogram.types import Message
from src.validation import validate_input
from src.bot import bot
from src.db.functions import aggregate_salary_data

router = Router(name='messages-router')


@router.message()
async def main_handler(message: Message):
    data = validate_input(message.text)
    if data:
        answer = aggregate_salary_data(data['dt_from'], data['dt_upto'], data['group_type'])
        await bot.send_message(message.from_user.id, answer)
    else:
        await bot.send_message(message.from_user.id, 'Невалидный запрос')