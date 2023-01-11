
from db import Db
from pickle import dumps, loads
import logging
import win32file

FILE = "database.bin"
FORMAT = '%(asctime)s.%(msecs)03d - %(message)s'
DATEFMT = '%H:%M:%S'


class FileDB(Db):
    """
    File Database class
    """
    def __init__(self):
        """
        Constructor for file database
        """
        super().__init__()

    def load(self):
        """
        Read database from the save file
        """
        file = win32file.CreateFileW(FILE, win32file.GENERIC_READ, win32file.FILE_SHARE_READ
                                     , None, win32file.OPEN_ALWAYS, 0, None)
        logging.debug("File Database: Opened file %s for reading" % FILE)
        try:
            data = win32file.ReadFile(file, 100000000)
            assert data[0] == 0
            self.database = loads(data[1])
        except EOFError:
            self.database = {}
        finally:
            win32file.CloseHandle(file)
            logging.debug("File Database: Loaded database from file " + FILE)

    def dump(self):
        """
        Write database to file
        """
        logging.debug("File Database: Opened file %s for writing" % FILE)
        file = win32file.CreateFileW(FILE, win32file.GENERIC_WRITE, 0, None, win32file.CREATE_ALWAYS, 0, None)
        try:
            win32file.WriteFile(file, dumps(self.database))
            logging.debug("File Database: Dumped database to file " + FILE)
        finally:
            win32file.CloseHandle(file)

    def set_value(self, key, val):
        """
        if key is a key in the database set the value to val, else add key: val as a pair to the db, and update the
        file accordingly
        :param key: key to check
        :param val: value to set
        :return: Succeeded (True/False)
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
        Return the value of key if it is a key in dict, else None
        :param key: key to check
        :return: self.database[key] if key is a key in the dict, else None
        """
        self.load()
        return super().get_value(key)

    def delete_value(self, key):
        """
        Deletes the value of key in the dict and returns it, if nonexistent raise KeyError
        :param key: key to check
        :return: deleted value if successful
        """
        self.load()
        res = super().delete_value(key)
        self.dump()
        return res


if __name__ == '__main__':
    logging.basicConfig(filename="FileDB.log", filemode="a", level=logging.DEBUG, format=FORMAT, datefmt=DATEFMT)