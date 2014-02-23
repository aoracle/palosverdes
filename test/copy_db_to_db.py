#!/usr/bin/python
import psycopg2
import os
import threading

fromdb = psycopg2.connect("host='localhost' dbname='palos' user='andy' password=''")
todb = psycopg2.connect("host='localhost' dbname='stocks' user='andy' password=''")

r_fd, w_fd = os.pipe()

def copy_from():
    cur = fromdb.cursor()
    cur.copy_from(os.fdopen(r_fd), 'raw_option')
    cur.close()
    todb.commit()

to_thread = threading.Thread(target=copy_from)
to_thread.start()

cur = todb.cursor()
write_f = os.fdopen(w_fd, 'w')
cur.copy_to(write_f, 'raw_option_test')
write_f.close()   # or deadlock...

to_thread.join()