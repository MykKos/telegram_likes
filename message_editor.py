"""
This script gets new likes from queue and edit original messages
"""
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from model.redis_queue import RQueue
import time
import re
import os


def loop(func):
    """
    Decorator running infinity loop for choosen function
    """
    def wrapper(*a, **b):
        while True:
            func(*a, **b)
    return wrapper


def main():
    """
    The main function
    Initialising variables and starting the loop
    """
    rqueue = RQueue()
    bot = telegram.Bot(token=os.environ.get('TG_BOT'))
    check_on_updates(rqueue, bot)


@loop
def check_on_updates(rqueue: RQueue, bot: telegram.Bot):
    """
    Checks for new update in redis,
    if found, than edit posts.
    Timeout set because of telegram API limitations
    (No way to edit more than 1 message in channel in less than 2 seconds)
    """
    updates = rqueue.get_keys("updates:*")
    if len(updates) != 0:
        for update_key in updates:
            update = rqueue.get_key(update_key)
            edit_updated(update, update_key.decode('utf-8'), bot)
            rqueue.delete_key(update_key)
            time.sleep(2)
    time.sleep(0.1)


def edit_message_keyboard(
      bot: telegram.Bot, message_id: str,
      chat_id: str, likes: str,
      dislikes: str, url: str):
    """
    Function changing the keyboard on choosen message with
    updated values
    """
    keyboard = InlineKeyboardMarkup([
        [
         InlineKeyboardButton(text=f'{likes} ❤️', callback_data="L"),
         InlineKeyboardButton(text=f'{dislikes} 💔', callback_data="D")
         ], [
         InlineKeyboardButton(text='Читать', url=f"{url}")
         ]])
    while True:
        try:
            bot.edit_message_reply_markup(chat_id=chat_id,
                                          message_id=message_id,
                                          reply_markup=keyboard)
            break
        except telegram.error.BadRequest:
            break
        except telegram.error.TimedOut:
            time.sleep(1)
        except telegram.error.RetryAfter as e:
            print(e.message)
            tm = re.search("(?<=Retry in )[0-9]+", e.message)
            if tm is not None:
                tm = int(tm.group(0)) + 5
                time.sleep(tm)


def edit_updated(update: str, update_key: str, bot: telegram.Bot):
    """
    Proccess the data from key-value and update the message
    """
    chat_id, message_id = update_key.split(':')[1:]
    url, likes, dislikes = update.decode('utf-8').split('^')
    edit_message_keyboard(bot, message_id, chat_id, likes, dislikes, url)


if __name__ == '__main__':
    main()
