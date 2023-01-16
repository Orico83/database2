"""
Author: Ori Cohen.
Date: 16/01/2023.
Base database class.
"""


class Db:
    def __init__(self):
        """
        Database constructor.
        """
        self.database = {}

    def set_value(self, key, val):
        """
        Updates the value of the key if key is in the database, Else, adds the key and the value to the database
        :param key: key
        :param val: value to set
        :return: True if succeeded
        """
        self.database[key] = val
        return True

    def get_value(self, key):
        """
        Return the value of key if it's in database, else None
        :param key: key
        :return: The value of the key. If the key isn't in the database, returns None
        """
        try:
            return self.database[key]
        except KeyError:
            return None

    def delete_value(self, key):
        """
        Deletes the value of key in the dict and returns it if it's in database. Else, None
        :param key: key
        :return: Deleted value if key exists. Else, None
        """
        try:
            val = self.database[key]
            self.database[key] = None
            return val
        except KeyError:
            return None


if __name__ == '__main__':
    db = Db()
    assert db.set_value('a', 1)
    assert db.get_value('a') == 1
    assert db.delete_value('a') == 1
    assert db.get_value('a') is None
    assert db.delete_value(5) is None
