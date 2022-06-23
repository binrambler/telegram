from aiogram import Bot, Dispatcher, executor, types
import asyncio
import pyodbc
import pandas as pd
import pathlib

# id бота
# amber_izh_bot
TOKEN = '5493895594:AAHa3N_cAkS_A3QEi252m0d4m05y3P03TuY'
# test_amberbot
# TOKEN = '5366236312:AAHAjXEjYVIlOVyrkvKBT2awpAaM-Y8xkaQ'
DIR_PHOTO = pathlib.Path('//z2/base/ftp/foto')
SERVER = '192.168.20.5'
DATABASE = 'IZH_SQL_2018'
USERNAME = 'sa'
PASSWORD = ''
# коды СГИ колготок и белья
SCI_CODE_KOLG = ('7', '72', '73', '8')
# максимальное кол-во сообщений
MAX_MESSAGE = 50


bot = Bot(token=TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)


# создаем клавиатуру
# главное меню
menu_main = types.ReplyKeyboardMarkup(resize_keyboard=True)
butt_main = ['Новинки']
for butt in butt_main:
    menu_main.add(butt)


# меню новинок
menu_news = types.ReplyKeyboardMarkup(resize_keyboard=True)
butt_news = ['Колготки и белье', 'Общий прайс', '<< В главное меню']
for butt in butt_news:
    menu_news.add(butt)


# вытягиваем запросом инфу
async def exec_query(qry, mode='select'):
    with pyodbc.connect('DRIVER={SQL Server};SERVER=' + SERVER +
                        ';DATABASE=' + DATABASE +
                        ';UID=' + USERNAME +
                        ';PWD=' + PASSWORD) as conn:
        if mode == 'select':
            return pd.read_sql(qry, conn)
        else:
            conn.execute(qry)
            return True


# команда старт и кнопка назад из новинок в главное меню
@dp.message_handler(lambda message: message.text in ['/start', butt_news[-1]])
async def show_menu_main(message: types.Message):
    # добавляем ID подписчика в базу данных
    if message.text == '/start':
        # сначала проверяем, что его там нет
        if len(await exec_query(f'select * from dbo.BOT_USERS where ID = {message.from_user.id}')) == 0:
            # добавляем подписчика в базу
            qry = f"insert into dbo.BOT_USERS values (" \
                  f"{message.from_user.id}, " \
                  f"'{message.from_user.full_name}', " \
                  f"'{message.date.strftime('%Y%m%d')}')"
            await exec_query(qry, mode='')
    await message.answer('Что вам показать?', reply_markup=menu_main)


# показываем меню новинок
@dp.message_handler(lambda message: message.text == butt_main[0])
async def show_menu_news(message: types.Message):
    await message.answer('Выберите новинки:', reply_markup=menu_news)


# выбираем что-то из меню новинок
@dp.message_handler(lambda message: message.text != butt_news[-1])
async def select_menu_news(message: types.Message):
    if message.text == butt_news[0]:
        where_str = f"in {SCI_CODE_KOLG}"
    elif message.text == butt_news[1]:
        where_str = f"not in {SCI_CODE_KOLG}"
    else:
        await show_menu_main(message)
        return True
    # строим и выполняем запрос
    qry = f'select top {MAX_MESSAGE} * from dbo.BOT_NEWS (nolock) where SGI_CODE {where_str} order by DATE'
    arr = await exec_query(qry)

    if len(arr) == 0:
        await bot.send_message(chat_id=message.chat.id, text='Новинок нет')
        return True

    # для красоты имитируем отправку фото
    await asyncio.sleep(1)
    await types.ChatActions.upload_photo(2)

    for _, row in arr.iterrows():
        # телеграм позволяет отправлять 30 сообщений в сек,
        # поэтому введем принудительную паузу
        await asyncio.sleep(0.1)
        # создаем сообщение с двумя фото
        media = types.MediaGroup()
        media_txt = f"{row['MODEL_DESCR'].strip()}" \
                    f"\n<b>{row['PRICE']:.2f}</b>\u20bd" \
                    f"\n{row['INGRID'].strip()}" \
                    f"\n{row['COMMENT'].strip()}"

        photo01 = pathlib.Path(DIR_PHOTO, row['PHOTO01'].strip())
        photo02 = pathlib.Path(DIR_PHOTO, row['PHOTO02'].strip())

        # если файл фото существет, то прикрепляем его к сообщению
        if photo01.suffix == '.jpg' and photo01.exists():
            media.attach_photo(photo=types.InputFile(photo01), caption=media_txt)
            if photo02.suffix == '.jpg' and photo02.exists():
                media.attach_photo(photo=types.InputFile(photo02))

        # если есть хотя бы одно фото, то выводим сообщение с фото
        if media.media:
            await bot.send_media_group(chat_id=message.chat.id, media=media)
        # если фото нет, то просто текст
        else:
            await bot.send_message(chat_id=message.chat.id, text=media_txt)


# проверяем есть ли новые публикации по полю IS_NEW
async def check_news(where_str):
    qry = f"select * from dbo.BOT_NEWS (nolock) where IS_NEW is null or IS_NEW = 0 and SGI_CODE {where_str}"
    return len(await exec_query(qry)) > 0


# отправляем известие подписчикам бота
async def send_info(str_send):
    qry = 'select * from dbo.BOT_USERS (nolock)'
    users = await exec_query(qry)
    for _, row in users.iterrows():
        await asyncio.sleep(0.1)
        await bot.send_message(chat_id=row['ID'], text=str_send)


# помечаем, что известие отправлено
async def mark_news(where_str):
    qry = f"update dbo.BOT_NEWS set IS_NEW = 1 where SGI_CODE {where_str}"
    await exec_query(qry, mode='')
    return True


# периодически проверяем новые публикации и отправляем известия об этом подписчикам бота
async def scheduled(wait_for):
    while True:
        # проверяем есть ли новые публикации колготок и белья
        if await check_news(f"in {SCI_CODE_KOLG}"):
            # отправляем сообщение подписчикам
            await send_info('Посмотрите новинки колготок и белья')
            # пометим, что известие отправлено
            await mark_news(f"in {SCI_CODE_KOLG}")

        if await check_news(f"not in {SCI_CODE_KOLG}"):
            await send_info('Посмотрите новинки общего прайса')
            # пометим, что известие отправлено
            await mark_news(f"not in {SCI_CODE_KOLG}")
        await asyncio.sleep(wait_for)


if __name__ == "__main__":
    # периодический запуск функции scheduled
    loop = asyncio.get_event_loop()
    loop.create_task(scheduled(1))
    #...периодический
    executor.start_polling(dp, skip_updates=True)