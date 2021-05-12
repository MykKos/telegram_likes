"""
This script connects to queue and save new likes to database
"""
from model.redis_queue import RQueue
from model.clickhouse_saver import ClickHouseSaver
import time
import os


def loop(func):
    def wrapper(*a, **b):
        while True:
            func(*a, **b)
            time.sleep(0.1)
    return wrapper


def main(channel):
    """
    Inititalising queue and database controller,
    starting the loop
    """
    rqueue = RQueue()
    clsaver = ClickHouseSaver()
    get_reactions(channel, rqueue, clsaver)


@loop
def get_reactions(channel, rqueue, clsaver):
    """
    Get reactions from queue
    If found - save them
    """
    new_reacts = rqueue.get_keys(f"{channel}:*")
    if len(new_reacts) != 0:
        save_reactions(new_reacts, rqueue, clsaver)


def save_reactions(new_reacts: list, rqueue: RQueue,
                   clsaver: ClickHouseSaver):
    """
    Save recieved reactions
    """
    for reaction in new_reacts:
        rc = rqueue.get_key(reaction)
        channel, message, user = reaction.decode('utf-8').split(':')
        react, url = rc.decode('utf-8').split('^')
        like = (
            int(user),
            channel,
            react,
            int(message),
            time.time(),
            time.time(),
            time.time(),
            url,
        )
        clsaver.add_likes(like)
        rqueue.delete_key(reaction)


if __name__ == '__main__':
    main(os.environ.get('LIKES_CHANNEL'))
