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


bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
buttons = ['Колготки и белье', 'Общий прайс']
for button in buttons:
    keyboard.add(button)


def exec_query(sgi_str):
    qry = f'select * from dbo.BOT (nolock) where SGI_CODE {sgi_str}'
    with pyodbc.connect('DRIVER={SQL Server};SERVER=' + SERVER +
                        ';DATABASE=' + DATABASE +
                        ';UID=' + USERNAME +
                        ';PWD=' + PASSWORD) as conn:
        return pd.read_sql(qry, conn)

@dp.message_handler()
async def cmd_start(message: types.Message):
    if message.text == buttons[0]:
        arr = exec_query("in ('7')")
    elif message.text == buttons[1]:
        arr = exec_query("in ('1')")
    else:
        await message.answer('Какие новинки вам показать?', reply_markup=keyboard)
        return 0

    # await asyncio.sleep(1)
    await types.ChatActions.upload_photo()

    for _, row in arr.iterrows():
        photo01 = pathlib.Path(DIR_PHOTO, row['PHOTO01'])
        photo02 = pathlib.Path(DIR_PHOTO, row['PHOTO02'])

        media = types.MediaGroup()
        media_txt = f"{row['MODEL_DESCR'].strip()}, *{row['PRICE']:.2f}* \u20bd"
        if photo01.exists():
            media.attach_photo(types.InputFile(photo01), caption=media_txt, parse_mode='Markdown')
            if photo02.exists():
                media.attach_photo(types.InputFile(photo02))

        if media.media:
            await bot.send_media_group(chat_id=message.chat.id, media=media)
        else:
            await message.answer('Новинок нет', reply_markup=keyboard)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)