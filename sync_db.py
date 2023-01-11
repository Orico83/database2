

from file_db import FileDB
import logging
from win32event import CreateMutex, CreateSemaphore, WaitForSingleObject, ReleaseMutex, ReleaseSemaphore, INFINITE

FORMAT = '%(asctime)s.%(msecs)03d - %(message)s'
DATEFMT = '%H:%M:%S'
READNAME = "read"
WRITENAME = "write"


class SyncDB:
    """
    Synchronized database class
    """
    def __init__(self, db: FileDB):
        """
        Constructor for synchronized database
        """
        self.database = db
        self.read = CreateSemaphore(None, 10, 10, READNAME)
        self.write = CreateMutex(None, False, WRITENAME)

    def read_get(self):
        """
        Acquire reading permissions
        """
        WaitForSingleObject(self.read, INFINITE)
        logging.debug("Sync Database: acquired reading permissions")

    def read_release(self):
        """
        Release reading permissions
        """
        ReleaseSemaphore(self.read, 1)
        logging.debug("Sync Database: released reading permissions")

    def write_get(self):
        """
        Acquire writing permissions
        """
        WaitForSingleObject(self.write, INFINITE)
        for i in range(10):
            WaitForSingleObject(self.read, INFINITE)
        logging.debug("Sync Database: acquired writing permissions")

    def write_release(self):
        """
        Release writing permissions
        """
        ReleaseSemaphore(self.read, 10)
        ReleaseMutex(self.write)
        logging.debug("Sync Database: released writing permissions")

    def set_value(self, key, val):
        """
        set value to 'val' at key 'key'
        :param key: key
        :param val: value
        :return: Succeeded (True/False)
        """
        self.write_get()
        res = self.database.set_value(key, val)
        self.write_release()
        return res

    def get_value(self, key):
        """
        Return the value of key if it is a key in dict, else None
        :param key: key to check
        :return: value of if key is a key in the dict, else None
        """
        self.read_get()
        res = self.database.get_value(key)
        self.read_release()
        return res

    def delete_value(self, key):
        """
        Deletes the value of key in the dict and returns it, if nonexistent raise KeyError
        :param key: key to check
        :return: deleted value if successful
        """
        self.write_get()
        self.database.delete_value(key)
        self.write_release()


if __name__ == '__main__':
    logging.basicConfig(filename="SyncDB.log", filemode="a", level=logging.DEBUG, format=FORMAT, datefmt=DATEFMT)