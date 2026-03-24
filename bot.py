import os
from datetime import datetime, timedelta
import asyncio
from aiogram import Bot, Dispatcher
from zoneinfo import ZoneInfo
from aiogram.exceptions import TelegramForbiddenError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import cbrapi


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

    # Получаем данные
    df = cbrapi.get_metals_prices(first_date=str(start_date),
                                  last_date=str(end_date))

    columns = ['GOLD', 'SILVER', 'PLATINUM', 'PALLADIUM']
    df = df[columns]

    # Заголовок таблицы
    header = f"{'DATE':<10} {'GOLD':>0} {'SILVER':>10} {'PLATINUM':>9} {'PALLADIUM':>7}"
    separator = "-" * len(header)

    lines = [header, separator]

    # Формируем строки таблицы
    for date, row in df.iterrows():
        line = (
            f"{date.strftime('%d-%m-%Y'):<10} "
            f"{format(row['GOLD'], '.2f'):>8} "
            f"{format(row['SILVER'], '.2f'):>6} "
            f"{format(row['PLATINUM'], '.2f'):>8} "
            f"{format(row['PALLADIUM'], '.2f'):>8}"
        )
        lines.append(line)

    # Итоговый текст
    metals_rates = "\n".join(lines)


# данные для первой рассылки
asyncio.run(update_metals())


# РАССЫЛКА
async def send_metals():
    for user_id in list(db_set):
        try:
            await bot.send_message(user_id, metals_rates)

        except TelegramForbiddenError:
            # пользователь заблокировал бота
            db_set.remove(user_id)
            print(f"user {user_id} удалён из базы")


# SCHEDULER
async def main():
    scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Moscow"))
    # 09:00 обновление
    scheduler.add_job(update_metals, "cron", hour=9, minute=00)
    # 10:00 рассылка
    scheduler.add_job(send_metals, "cron", hour=10, minute=00)
    scheduler.start()

    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        print('Starting bot...')
        asyncio.run(main())
    except KeyboardInterrupt as ex:
        # print(ex)
        print("Bot was stopped!")
