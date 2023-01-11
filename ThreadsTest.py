from sync_db import SyncDB
from file_db import FileDB
import win32process
from win32event import WaitForSingleObject as Join, INFINITE
import logging

FORMAT = '%(asctime)s.%(msecs)03d - %(message)s'
DATEFMT = '%H:%M:%S'
STACK_SIZE = 1000


def test_write(db):
    logging.debug("started write test")
    for i in range(100):
        assert db.set_value(i, "test" + str(i))


def test_read(db):
    logging.debug("started read test")
    for i in range(100):
        assert "test" + str(i) == db.get_value(i)


def main():
    logging.debug("Starting tests for Multithreading")
    db = SyncDB(FileDB())
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing simple write perms")
    p1 = win32process.beginthreadex(None, STACK_SIZE, test_write, (db, ), 0)[0]
    assert Join(p1, INFINITE) == 0
    logging.info("test successful")
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing simple read perms")
    p1 = win32process.beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
    assert Join(p1, INFINITE) == 0
    logging.info("test successful")
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing read blocks writing")
    p1 = win32process.beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
    p2 = win32process.beginthreadex(None, STACK_SIZE, test_write, (db, ), 0)[0]
    assert Join(p1, INFINITE) == 0
    assert Join(p2, INFINITE) == 0
    logging.info("test successful")
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing write blocks reading")
    p1 = win32process.beginthreadex(None, STACK_SIZE, test_write, (db, ), 0)[0]
    p2 = win32process.beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
    assert Join(p1, INFINITE) == 0
    assert Join(p2, INFINITE) == 0
    logging.info("test successful")
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing multi reading perms possible")
    threads = []
    for i in range(5):
        thread = win32process.beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
        threads.append(thread)
    for i in threads:
        assert Join(i, INFINITE) == 0
    logging.info("test successful")
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing load")
    threads = []
    for i in range(15):
        thread = win32process.beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
        threads.append(thread)
    for i in range(5):
        p1 = win32process.beginthreadex(None, STACK_SIZE, test_write, (db, ), 0)[0]
        threads.append(p1)
    for i in threads:
        assert Join(i, INFINITE) == 0
    logging.info("test successful")
    logging.debug("\n--------------------------------------------------------\n")
    logging.info("testing values stay correct")
    p1 = win32process.beginthreadex(None, STACK_SIZE, test_read, (db, ), 0)[0]
    assert Join(p1, INFINITE) == 0
    logging.info("test successful")


if __name__ == '__main__':
    logging.basicConfig(filename="ThreadTest.log", filemode="a", level=logging.DEBUG, format=FORMAT, datefmt=DATEFMT)
    main()
