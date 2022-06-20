from aiogram import Bot, Dispatcher, executor, types
import asyncio
import pyodbc
import pandas as pd
import pathlib

TOKEN = '5493895594:AAHa3N_cAkS_A3QEi252m0d4m05y3P03TuY'
DIR_PHOTO = pathlib.Path('//z2/base/ftp/foto')
SERVER = '192.168.20.5'
DATABASE = 'IZH_SQL_2018'
USERNAME = 'sa'
PASSWORD = ''
# максимальное кол-во сообщений
MAX_MESSAGE = 50


bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# создаем клавиатуру
keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
buttons = ['Колготки и белье', 'Общий прайс']
for button in buttons:
    keyboard.add(button)

# вытягиваем запросом инфу
def exec_query(where_str):
    qry = f'select top {MAX_MESSAGE} * from dbo.BOT (nolock) where SGI_CODE {where_str}'
    with pyodbc.connect('DRIVER={SQL Server};SERVER=' + SERVER +
                        ';DATABASE=' + DATABASE +
                        ';UID=' + USERNAME +
                        ';PWD=' + PASSWORD) as conn:
        return pd.read_sql(qry, conn)

@dp.message_handler()
async def cmd_start(message: types.Message):
    if message.text == buttons[0]:
        arr = exec_query("in ('7', '72', '73', '8')")
    elif message.text == buttons[1]:
        arr = exec_query("not in ('7', '72', '73', '8')")
    else:
        await message.answer('Какие новинки вам показать?', reply_markup=keyboard)
        return 0

    # для красоты имитируем отправку фото
    await types.ChatActions.upload_photo(1)

    for _, row in arr.iterrows():
        photo01 = pathlib.Path(DIR_PHOTO, row['PHOTO01'])
        photo02 = pathlib.Path(DIR_PHOTO, row['PHOTO02'])

        # создаем сообщение с двумя фото
        media = types.MediaGroup()
        media_txt = f"{row['MODEL_DESCR'].strip()}" \
                    f"\n*{row['PRICE']:.2f}* \u20bd" \
                    f"\n{row['INGRID'].strip()}" \
                    f"\n{row['COMMENT'].strip()}"

        # если фото существет, то прикрепляем его к сообщению
        if photo01.suffix == '.jpg' and photo01.exists():
            media.attach_photo(photo=types.InputFile(photo01), caption=media_txt, parse_mode='Markdown')
            if photo02.suffix == '.jpg' and photo02.exists():
                media.attach_photo(photo=types.InputFile(photo02))

        # если есть хотя бы одно фото, то выводим сообщение с фото
        if media.media:
            await bot.send_media_group(chat_id=message.chat.id, media=media)
        else:
            await bot.send_message(chat_id=message.chat.id, text=media_txt, parse_mode='Markdown')


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)