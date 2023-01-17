"""
Author: Ori Cohen
Date: 18/01/2023
Implements the synchronization between threads/processes and reading and writing permissions using win32event.
"""

from file_db import FileDB
import logging
from win32event import CreateSemaphore, CreateMutex, WaitForSingleObject, ReleaseSemaphore, ReleaseMutex, INFINITE

FORMAT = '%(asctime)s.%(msecs)03d - %(message)s'
DATEFMT = '%H:%M:%S'
READ = "read"
WRITE = "write"


class SyncDB:
    """
    Synchronized database class
    """
    def __init__(self, db: FileDB):
        """
        Constructor for synchronized database
        """
        self.database = db
        self.read = CreateSemaphore(None, 10, 10, READ)
        self.write = CreateMutex(None, False, WRITE)

    def read_acquire(self):
        """
        Acquire reading permissions
        """
        WaitForSingleObject(self.read, INFINITE)
        logging.info("Sync Database: acquired reading permissions")

    def read_release(self):
        """
        Release reading permissions
        """
        logging.info("Sync Database: released reading permissions")
        ReleaseSemaphore(self.read, 1)

    def write_acquire(self):
        """
        Acquire writing permissions
        """
        WaitForSingleObject(self.write, INFINITE)
        for i in range(10):
            WaitForSingleObject(self.read, INFINITE)
        logging.info("Sync Database: acquired writing permissions")

    def write_release(self):
        """
        Release writing permissions
        """
        logging.info("Sync Database: released writing permissions")
        ReleaseSemaphore(self.read, 10)
        ReleaseMutex(self.write)

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
    sync_db = FileDB()
    assert sync_db.set_value('a', 1)
    assert sync_db.get_value('a') == 1
    assert sync_db.delete_value('a') == 1
    assert sync_db.get_value('a') is None
    assert sync_db.delete_value('b') is None
