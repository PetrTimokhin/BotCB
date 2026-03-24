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
usd_rate = ''
cny_rate = ''
key_rate = ''
eur_rate = ''

# ОБНОВЛЕНИЕ ДАННЫХ
async def update_metals() -> None:
    global metals_rates
    global usd_rate
    global cny_rate
    global key_rate
    global eur_rate
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

    # переводим фрейм сразу в строку
    # metals_rates = df.to_string(float_format="%.2f")

    # Заголовок и строки как раньше, только добавляем <pre> для Telegram
    columns = ['GOLD', 'SILVER', 'PLATINUM', 'PALLADIUM']
    df = df[columns]

    # ширины колонок
    date_w = 7
    gold_w = 5
    silver_w = 5
    platinum_w = 5
    palladium_w = 5

    # Заголовок
    header = (
        f"{'Дата':<{date_w}}"
        f"{'Au':>{gold_w}}"
        f"{'Ag':>{silver_w}}"
        f"{'Pt':>{platinum_w}}"
        f"{'Pd':>{palladium_w}}"
    )
    separator = "*" * len(header)
    lines = [header, separator]

    # Строки
    for date, row in df.iterrows():
        line = (
            f"{date.strftime('%d.%m'):<{date_w}}"
            f"{row['GOLD']:>{gold_w}.0f}"
            f"{row['SILVER']:>{silver_w}.0f}"
            f"{row['PLATINUM']:>{platinum_w}.0f}"
            f"{row['PALLADIUM']:>{palladium_w}.0f}"
        )
        lines.append(line)
    # Итоговый текст с моноширинным блоком
    metals_rates = "<pre>" + "\n".join(lines) + "</pre>"

    # получение курсов валют и ключевой ставки
    df = cbrapi.get_key_rate(first_date=str(start_date), period='D')
    key_rate += 'Ключевая ставка ЦБ\n'
    for d, value in df.items():
        d = d.strftime("%d.%m")
        key_rate += f"{d}: {value}%\n"

    usd = cbrapi.get_time_series("USD", first_date=str(start_date),
                                 last_date=str(end_date), period='D')
    usd_rate += 'Курс USD\n'
    for d, value in usd.items():
        d = d.strftime("%d.%m")
        usd_rate += f"{d}: {round(value, 2)}\n"

    eur = cbrapi.get_time_series("EUR", first_date=str(start_date),
                                 last_date=str(end_date), period='D')
    eur_rate += 'Курс EUR\n'
    for d, value in eur.items():
        d = d.strftime("%d.%m")
        eur_rate += f"{d}: {round(value, 2)}\n"

    cny = cbrapi.get_time_series("CNY", first_date=str(start_date),
                                 last_date=str(end_date), period='D')
    cny_rate += 'Курс CNY\n'
    for d, value in cny.items():
        d = d.strftime("%d.%m")
        cny_rate += f"{d}: {round(value, 2)}\n"


# РАССЫЛКА
async def send_metals():
    for user_id in list(db_set):
        try:
            await bot.send_message(user_id, metals_rates, parse_mode="HTML")
            await bot.send_message(user_id, usd_rate)
            await bot.send_message(user_id, eur_rate)
            await bot.send_message(user_id, cny_rate)
            await bot.send_message(user_id, key_rate)

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
    await message.answer(metals_rates, parse_mode="HTML")
    await message.answer(usd_rate)
    await message.answer(eur_rate)
    await message.answer(cny_rate)
    await message.answer(key_rate)


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
