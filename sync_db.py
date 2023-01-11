

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

    def read_acquire(self):
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

    def write_acquire(self):
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

    def get_value(self, key):
        """
        Acquire reading permission, get the key's value and then release writing permission.
        :param key: key
        :return: The key's value
        """
        self.read_acquire()
        res = self.database.get_value(key)
        self.read_release()
        return res

    def set_value(self, key, val):
        """
        Acquire writing permission, update the key's value and then release writing permission.
        :param key: key
        :param val: value to set
        :return: True if succeeded. Else, False.
        """
        self.write_acquire()
        res = self.database.set_value(key, val)
        self.write_release()
        return res

    def delete_value(self, key):
        """
        Acquire writing permission, delete the key's value and then release writing permission.
        :param key: key
        :return: The deleted value
        """
        self.write_acquire()
        self.database.delete_value(key)
        self.write_release()


if __name__ == '__main__':
    logging.basicConfig(filename="SyncDB.log", filemode="a", level=logging.DEBUG, format=FORMAT, datefmt=DATEFMT)
