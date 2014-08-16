from __future__ import division
import black
import psycopg2
import pprint
from multiprocessing import Pool as ThreadPool

def chunks(l, n):
  """ Yield successive n-sized chunks from l.
  """
  for i in xrange(0, len(l), n):
      yield l[i:i+n]

def createAndStoreimplvol(cursor, database_name):
  """Creates impl_vol and stores them in to a file.

  Loads the file in to temporary table.
  Args:
    cursor: postgres connection cursor to database.
    database_name: Name of the database.
  """
  get_distinct_eod = """SELECT distinct eod FROM options_vol_50  where eod > '2014-08-08' order by eod """ 
  cursor.execute(get_distinct_eod)
  get_distinct_eod = cursor.fetchall()
  print get_distinct_eod
  for eod in get_distinct_eod:
    print eod[0]
    get_imp_vol_sql = """SELECT o.id,o.strike_symbol,o.expiry_date - o.eod expiration_time,s.close stock_price,o.strike strike_price,o.close option_price,o.option_type FROM options_vol_50 o, stocks s where o.strike_symbol = s.symbol and o.eod = s.eod and to_char(o.eod,'YYYY-MM-DD') = '%s'""" % (eod[0])
    print get_imp_vol_sql
    cursor.execute(get_imp_vol_sql)
    with open('/tmp/im_vol.data', 'w+') as file_handle:
      records = cursor.fetchmany(size=1000)
      #print records
      while records:
        write_str = ''
        for row in records:
          id = str(row[0])
          #id1 = str(row[1])
          #print 'Prinitng ids', id,id1
          write_str += '%s,%s\n' % (id,black.impliedBlack(row[6],float(row[3]), float(row[4]),float(row[2]/365), 0.003, float(row[5]))[0])
        file_handle.write(write_str)
        records = cursor.fetchmany(size=1000)
    drop_temp_table_sql = """DROP TABLE if exists impl_table"""
    cursor.execute(drop_temp_table_sql)
    create_temp_table_sql = """CREATE  TABLE impl_table(
                            id int  NOT NULL,
                            impl_vol numeric DEFAULT NULL)"""
    cursor.execute(create_temp_table_sql)
    load_data_sql = """COPY impl_table FROM '/tmp/im_vol.data' DELIMITER ',' CSV""" 
    cursor.execute(load_data_sql)
    update_impl_sql = """UPDATE options_vol_50 AS tml SET implied_vol=round(mtmp.impl_vol,2) FROM
       impl_table mtmp where  tml.id=mtmp.id
      """
    cursor.execute(update_impl_sql)
    conn.commit()
  #cleanup()


def cleanup():
  """cleanup the file created."""
  with open('/tmp/url.data', 'w+') as file_handle:
    file_handle.write('')
  return


def update_impl(records):
  for record in records:
    impl_dict = {}
    print "   ", record[1],record[2],record[3],record[4],record[5],record[6]
    if record[6] == 'C':
      cursor.execute("SELECT * FROM options o where id = %s limit 100" % (record[0]))
      records = cursor.fetchall()
      pprint.pprint(records)
      o_price = black.impliedBlack(record[6],float(record[3]), float(record[4]),float(record[2]/365), 0.003, float(record[5]))      

      #result = cursor.execute("update options_vol_50 set implied_vol = %s where id = %s" % (c_price[0],int(record[0])))
      conn.commit()
    elif record[6] == 'P':
      o_price = black.impliedBlack(record[6],float(record[3]), float(record[4]),float(record[2]/365), 0.003, float(record[5]))
      #result = cursor.execute("update options_vol_50 set implied_vol = %s where id = %s" % (p_price[0],int(record[0])))
    #impl_dict[int(record[0]) = o_price[0]
    #result = cursor.execute("insert into opt_impl_vol_temp(id,impl_val)  set implied_vol = %s where id = %s" % (c_price[0],int(record[0])))

      conn.commit()


if __name__ == '__main__':

  conn_string = "host='localhost' dbname='finance' user='postgres' password=''"
  print "Connecting to database\n ->%s" % (conn_string)
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
 
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
 
  # execute our Query
  ##cursor.execute("SELECT o.id,o.strike_symbol,o.expiry_date - o.eod expiration_time,s.close stock_price,o.strike strike_price,o.close option_price,o.option_type FROM options_vol_50 o, stocks s where o.strike_symbol = s.symbol and o.eod = s.eod")
 
  # retrieve the records from the database
  ##records = cursor.fetchall()
  #print records
  #P = list(chunks(records,5))
  #print 'Printing P : ', P
  #pool = ThreadPool(1)
  #results = pool.map(update_impl,P)
  #pool.close()
  #pool.join()
  #update_impl(records)
  createAndStoreimplvol(cursor,'finance')
  # print out the records using pretty print
  # note that the NAMES of the columns are not shown, instead just indexes.
  # for most people this isn't very useful so we'll show you how to return
  # columns as a dictionary (hash) in the next example.



