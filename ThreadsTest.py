"""
Author: Ori Cohen
Date: 18/01/2023
Tests the database in threads mode using win32event and win32process.
"""
from sync_db import SyncDB
from file_db import FileDB
from win32process import beginthreadex
from win32event import INFINITE, WaitForSingleObject as Join
import logging

FORMAT = '%(asctime)s.%(msecs)03d - %(message)s'
DATEFMT = '%H:%M:%S'
ENTER = "--------------------------------------------------"
STACK_SIZE = 1000


def test_write(db):
    """
    Tests writing values to database
    :param db: database
    :return: None
    """
    logging.info("Write test started")
    for i in range(100):
        assert db.set_value(i, f"t{str(i)}")
    logging.info("Write test successful")


def test_read(db):
    """
    Tests reading values from database
    :param db: database
    :return: None
    """
    logging.info("Read test started")
    for i in range(100):
        assert db.get_value(i) == f"t{str(i)}"
    logging.info("Read test successful")


def test_delete(db):
    """
    Tests deleting values from database
    :param db: database
    :return: None
    """
    logging.info("Delete test started")
    for i in range(10):
        db.delete_value(i)
    for i in range(10):
        assert db.get_value(i) is None
    for i in range(10, 100):
        assert db.get_value(i) == f"t{str(i)}"
    logging.info("delete test successful")


def main():
    logging.info("Starting tests for Multithreading")
    db = SyncDB(FileDB())
    logging.info(ENTER)
    logging.info("Testing simple writing permissions")
    write = beginthreadex(None, STACK_SIZE, test_write, (db,), 0)[0]
    assert Join(write, INFINITE) == 0
    logging.info("Test successful")
    logging.info(ENTER)
    logging.info("Testing simple reading permissions")
    read = beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
    assert Join(read, INFINITE) == 0
    logging.info("Test successful")
    logging.info(ENTER)
    logging.info("Testing multiple reading permissions")
    threads = []
    for i in range(5):
        read = beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
        threads.append(read)
    for t in threads:
        assert Join(t, INFINITE) == 0
    logging.info("Multiple reading permissions test successful")
    logging.info(ENTER)
    logging.info("Testing reading blocks writing")
    readers = []
    writers = []
    for i in range(50):
        read = beginthreadex(None, STACK_SIZE, test_read, (db,), 0)[0]
        readers.append(read)
    for i in range(5):
        write = beginthreadex(None, STACK_SIZE, test_write, (db, ), 0)[0]
        writers.append(write)
    for reader in readers:
        assert Join(reader, INFINITE) == 0
    for writer in writers:
        assert Join(writer, INFINITE) == 0
    logging.info("Reading blocks writing test successful")
    logging.info(ENTER)
    logging.info("Testing writing blocks reading")
    readers = []
    writers = []
    for i in range(10):
        write = beginthreadex(None, STACK_SIZE, test_write, (db,), 0)[0]
        writers.append(write)
    for i in range(50):
        read = beginthreadex(None, STACK_SIZE, test_read, (db,), 0)[0]
        readers.append(read)
    for writer in writers:
        assert Join(writer, INFINITE) == 0
    for reader in readers:
        assert Join(reader, INFINITE) == 0
    logging.info("Writing blocks reading test successful")
    logging.info(ENTER)
    deletes = []
    logging.info("Testing simple deleting permissions")
    delete = beginthreadex(None, STACK_SIZE, test_delete, (db,), 0)[0]
    assert Join(delete, INFINITE) == 0
    logging.info("Test successful")
    logging.info(ENTER)
    logging.info("Testing multiple deletes")
    for i in range(5):
        delete = beginthreadex(None, STACK_SIZE, test_delete, (db,), 0)[0]
        deletes.append(delete)
    for delete in deletes:
        assert Join(delete, INFINITE) == 0
    logging.info("Test successful")
    logging.info("All tests successful")


if __name__ == '__main__':
    logging.basicConfig(filename="ThreadTest.log", filemode="a", level=logging.DEBUG, format=FORMAT, datefmt=DATEFMT)
    main()
