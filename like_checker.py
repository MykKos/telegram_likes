"""
This script checks for new likes in database and add them to queue
for change the original message
"""
from model.clickhouse_saver import ClickHouseSaver
from model.redis_queue import RQueue
import time


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
    clsaver = ClickHouseSaver()
    rqueue = RQueue()
    get_new_likes(clsaver, rqueue)


@loop
def get_new_likes(clsaver, rqueue):
    """
    Function tries to get new likes from clickhouse
    database and if found some send them to redis queue
    """
    tm = rqueue.get_key('last_updated')
    if tm is None:
        tm = 0
    reactions = clsaver.get_likes(float(tm))
    if len(reactions) != 0:
        publish_new_likes(clsaver, rqueue, reactions)
    time.sleep(1)


def publish_new_likes(clsaver, rqueue, reactions):
    """
    Function finding and processing the data to put
    right values to key-value
    """
    last_updated = reactions[0][-1]
    rqueue.set_key(['last_updated'], [str(last_updated)])
    for r in reactions:
        key_arr = ['updates', r[0], str(r[1])]
        val_arr = [r[2], str(r[3]), str(r[4])]
        rqueue.set_key(key_arr, val_arr)


if __name__ == '__main__':
    main()
