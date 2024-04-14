from aiogram import Router
from aiogram.types import Message
from src.validation import validate_input
from src.bot import bot
from src.db.functions import aggregate_salary_data

router = Router(name='messages-router')


@router.message()
async def main_handler(message: Message):
    data = validate_input(message.text)
    if data:
        answer = await aggregate_salary_data(data['dt_from'], data['dt_upto'], data['group_type'])
        await bot.send_message(message.from_user.id, answer)
    else:
        await bot.send_message(message.from_user.id, 'Невалидный запрос. Пример запроса: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')