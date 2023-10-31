import asyncio
import logging
import sys
import os
import json
from datetime import datetime
from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold
from script import get_aggreg_data


TOKEN = os.getenv('TG_TOKEN')
dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    user_name = message.from_user.first_name
    await message.answer(f"Hello, {user_name}!")


@dp.message()
async def aggregate(message: Message):
    try:
        received_json = json.loads(message.text)
        dt_from = received_json['dt_from']
        dt_upto = received_json['dt_upto']
        group_type = received_json['group_type']
        
        answer = await get_aggreg_data(dt_from, dt_upto, group_type)
        await message.answer(json.dumps(answer))
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        print(e)
        await message.answer('Допустимо отправлять только следующего вида запрос:\n '
                             '{"dt_from": "2022-09-01T00:00:00", '
                             '"dt_upto": "2022-12-31T23:59:00", '
                             '"group_type": "month"}')



async def main() -> None:
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
