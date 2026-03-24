import os
from datetime import datetime, timedelta
import asyncio
from aiogram import Bot, Dispatcher
from zoneinfo import ZoneInfo
from aiogram.exceptions import TelegramForbiddenError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import cbrapi
from aiogram.filters import CommandStart
from aiogram.types import Message

# токен тест бота
TOKEN = os.getenv("TOKEN")  # Telegram токен
# DATABASE_URL = os.getenv("DATABASE_URL")  # Supabase URL

bot = Bot(TOKEN)
dp = Dispatcher()

# "база данных"
db_set = set()
# котировки по металлам
metals_rates = ''


# ОБНОВЛЕНИЕ ДАННЫХ
async def update_metals() -> None:
    global metals_rates
    # Диапазон дат
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=10)

    # Получаем данные с cbrapi
    try:
        df = cbrapi.get_metals_prices(first_date=str(start_date),
                                      last_date=str(end_date))
        if df.empty:
            metals_rates = "Нет данных по металлам ❌"
            return
    except Exception as e:
        metals_rates = f"Ошибка получения данных: {e}"
        return

    metals_rates = df.to_string(float_format="%.2f")


# РАССЫЛКА
async def send_metals():
    for user_id in list(db_set):
        try:
            # await bot.send_message(user_id, metals_rates)
            await bot.send_message(user_id, f"<pre>{metals_rates}</pre>",
                                   parse_mode="HTML")

        except TelegramForbiddenError:
            # пользователь заблокировал бота
            db_set.remove(user_id)
            print(f"user {user_id} удалён из базы")
        except Exception as e:
            print(f"Ошибка отправки {user_id}: {e}")


# обработка команды старт
@dp.message(CommandStart())
async def cmd_start(message: Message):
    db_set.add(message.from_user.id)
    await message.answer("Вы подписались на ежедневную рассылку ✅")
    await message.answer(metals_rates)
    await message.answer(f"<pre>{metals_rates}</pre>", parse_mode="HTML")


# SCHEDULER
async def main():
    # данные для первой рассылки
    await update_metals()

    scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Moscow"))
    # 09:00 обновление
    scheduler.add_job(update_metals, trigger="cron", hour=9, minute=0)
    # 10:00 рассылка
    scheduler.add_job(send_metals, "cron", hour=10, minute=00)
    scheduler.start()

    await dp.start_polling(bot,
                           allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        print('Starting bot...')
        asyncio.run(main())
    except KeyboardInterrupt as ex:
        # print(ex)
        print("Bot was stopped!")
