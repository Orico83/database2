"""
Author: Ori Cohen
Date: 16/01/2023
File database class with writing, reading and deleting capabilities to database.bin using win32file.
Inherits from DB.
"""

from db import Db
from pickle import dumps, loads
import logging
from win32file import ReadFile, WriteFile, CloseHandle, CreateFileW, GENERIC_READ, FILE_SHARE_READ, OPEN_ALWAYS, \
    GENERIC_WRITE, CREATE_ALWAYS

FILE = "database.bin"
FORMAT = '%(asctime)s.%(msecs)03d - %(message)s'
DATEFMT = '%H:%M:%S'


class FileDB(Db):
    """
    File Database class
    """
    def __init__(self):
        """
        File database constructor
        """
        super().__init__()

    def load(self):
        """
        Loads the database from the file.
        """
        file = CreateFileW(FILE, GENERIC_READ, FILE_SHARE_READ, None, OPEN_ALWAYS, 0, None)
        logging.debug("File Database: Opened file %s for reading" % FILE)
        try:
            data = ReadFile(file, 100000000)
            assert data[0] == 0
            self.database = loads(data[1])
        except EOFError:
            self.database = {}
        finally:
            CloseHandle(file)
            logging.debug("File Database: Loaded database from file " + FILE)

    def dump(self):
        """
        Updates the database to the file.
        """
        logging.debug("File Database: Opened file %s for writing" % FILE)
        file = CreateFileW(FILE, GENERIC_WRITE, 0, None, CREATE_ALWAYS, 0, None)
        try:
            WriteFile(file, dumps(self.database))
            logging.debug("File Database: Dumped database to file " + FILE)
        finally:
            CloseHandle(file)

    def set_value(self, key, val):
        """
        Updates the value of the key to the file if key is in the database.
        Else, adds the key and the value to the file database
        :param key: key
        :param val: value to set
        :return: True if succeeded. Else, False
        """
        try:
            self.load()
            res = super().set_value(key, val)
            self.dump()
            return res
        except OSError as err:
            logging.error("File Database: Got OSError %s while opening file %s, returning False for failure"
                          % (err, FILE))
            return False

    def get_value(self, key):
        """
        Return the value of key if it's in database, else None
        :param key: key
        :return: The value of the key. If the key isn't in the database, returns None
        """
        self.load()
        return super().get_value(key)

    def delete_value(self, key):
        """
        Deletes the value of key in the file dict and returns it if it's in database. Else, None
        :param key: key
        :return: Deleted value if key exists. Else, None
        """
        self.load()
        res = super().delete_value(key)
        self.dump()
        return res


if __name__ == '__main__':
    file_db = FileDB()
    assert file_db.set_value('a', 1)
    assert file_db.get_value('a') == 1
    assert file_db.delete_value('a') == 1
    assert file_db.get_value('a') is None
    assert file_db.delete_value(5) is None
    logging.basicConfig(filename="FileDB.log", filemode="a", level=logging.DEBUG, format=FORMAT, datefmt=DATEFMT)
