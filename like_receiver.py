"""
This script starts the bot, which receive updates from telegram and
sends them to queue
"""
from telegram.ext import Updater, CallbackQueryHandler
from model.redis_queue import RQueue
import os

REACTS = {
    'L': ['like', '❤️'],
    'D': ['dislike', '💔'],
    # 'H': ['hot', '🔥'],
    # 'I': ['ice', '❄️'],
    # 'M': ['mount', '⛰'],
    # 'E': ['explode', '🌋']
}


def reacting(update, context):
    """
    Handler callback
    Send to queue users likes
    Save user information
    """
    callback_query = update['callback_query']
    message = callback_query.message
    from_user = callback_query.from_user
    data = callback_query['data']
    reaction = REACTS[data[0]]
    # save_user(from_user)
    save_reaction(reaction, message, from_user)
    answer(update['callback_query'].answer, reaction)


def answer(answer_func, reaction):
    """
    Show answer depends on reaction
    """
    if reaction[0] != 'like' and reaction[0] != 'dislike':
        answer_func(text="Мы отключили эту реакцию")
        return
    answer_text = 'понравился❤️' if reaction[0] == 'like'\
                  else 'не понравился💔'
    answer_func(text=f"Вам {answer_text} этот пост")


def save_reaction(reaction, message, from_user):
    """
    Send user reaction to queue
    """
    channel = message['chat']['id']
    message = message['message_id']
    reply_markup = message['reply_markup']
    url = reply_markup['inline_keyboard'][-1][0]['url']
    uid = from_user['id']
    key = [channel, message, uid]
    value = [reaction[0], url]
    rqueue = RQueue()
    rqueue.set_key(key, value)


def save_user(from_user):
    """
    IMPLEMENT:
    Save user information
    """
    user = {
        'id': from_user['id'],
        'name': from_user['first_name'],
        'surname': from_user['last_name'],
        'username': from_user['username']
    }
    return user


def main():
    """
    Main function, initialising bot and handler
    """
    updater = Updater(os.environ.get('TG_BOT'))
    react_handler = CallbackQueryHandler(reacting)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(react_handler)
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
