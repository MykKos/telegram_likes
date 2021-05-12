"""
Small class working with redis
"""
import redis


class RQueue:

    def __init__(self):
        """
        Inititalising connection to Redis
        """
        self._redis = redis.Redis()

    def get_key(self, key: str) -> str:
        """
        Get key from queue
        """
        if key is None:
            return ''
        value = self._redis.get(key)
        return value

    def delete_key(self, key: str):
        """
        Delete key from queue
        """
        if key is None:
            return
        self._redis.delete(key)

    def set_key(self, key: list, value: list):
        """
        Set key to queue
        Constructing key from parts, part1:part2:part3...
        Constructing value from pats, vpart1^vpart2^vpart3...
        '^' was choosen as separator, because the data is too various
        and other special symbols could be included. ^ is rare, so
        it seems to be a better choice
        """
        if key is None or value is None:
            return
        string_key = ':'.join(key)
        string_value = '^'.join(value)
        self._redis.set(string_key, string_value)

    def get_keys(self, prefix: str) -> list:
        """
        Get all keys matching prefix from queue
        Empty string for all keys
        """
        if prefix is None:
            return []
        keys = self._redis.keys(prefix)
        return keys
