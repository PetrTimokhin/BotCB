import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramForbiddenError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import cbrapi


TOKEN = os.getenv("TOKEN")  # Telegram токен
# DATABASE_URL = os.getenv("DATABASE_URL")  # Supabase URL

bot = Bot(TOKEN)
dp = Dispatcher()

# "база данных"
db_lst = list()
# данные металлов
metals_data = ""


# ОБНОВЛЕНИЕ ДАННЫХ
async def update_metals():
    global metals_data

    df = cbrapi.get_metals_prices(first_date='2026-03-06', last_date='2026-03-06')
    latest = df.tail(1)
    quote_date = latest.index[0]  # это datetime.date или datetime
    dt = quote_date.strftime("%d.%m.%Y")
    metals_dict = latest.to_dict(orient='records')[0]
    print_metals_dict = ("\n".join(f"{k}: {v}" for k, v in metals_dict.items()))
    data = dt, print_metals_dict
    metals_data = f"Дата котировок: {data[0]} \n\nКурсы металлов:\n{data[1]}"


# данные для первой рассылки
asyncio.run(update_metals())


# START
@dp.message(CommandStart())
async def start_cmd(message: Message):
    global metals_data
    user_id = message.from_user.id
    db_lst.append(str(user_id))
    await message.answer("Вы подписаны на рассылку курсов металлов ⛏")
    await bot.send_message(str(user_id), metals_data)


# РАССЫЛКА
async def send_metals():
    global db_lst

    for user_id in list(db_lst):
        try:
            await bot.send_message(user_id, metals_data)

        except TelegramForbiddenError:
            # пользователь заблокировал бота
            db_lst.remove(user_id)
            print(f"user {user_id} удалён из базы")


# SCHEDULER
async def main():
    scheduler = AsyncIOScheduler()
    # 08:59 обновление
    scheduler.add_job(update_metals, "cron", hour=8, minute=00)
    # 09:00 рассылка
    scheduler.add_job(send_metals, "cron", hour=9, minute=00)
    scheduler.start()

    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        print('Starting bot...')
        asyncio.run(main())
    except KeyboardInterrupt as ex:
        # print(ex)
        print("Bot was stopped!")
