#!/usr/bin/python
import psycopg2
import sys
import pprint

input_file = '/stocks/raw_data/OPRA_20140121_test.csv' 
table_name = 'raw_option_test'
def main():
	conn_string = "host='localhost' dbname='palos' user='andy' password=''"
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (conn_string)
	try:
		# get a connection, if a connect cannot be made an exception will be raised here
		conn = psycopg2.connect(conn_string)
		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
 	
		# execute our Query
		str = "select * from raw_option limit 100;COPY %s FROM  '%s' DELIMITER ',' CSV;" % (table_name,input_file)
		out = cursor.execute("COPY %s FROM  '%s' DELIMITER ',' CSV;" % (table_name,input_file))
		print out
		print str
		# retrieve the records from the database
		#records = cursor.fetchall()
	except psycopg2.DatabaseError,e:
		print('Error %s' % (e))
		sys.exit(1)
	finally:
		if conn:
			conn.close()
 
	# print out the records using pretty print
	# note that the NAMES of the columns are not shown, instead just indexes.
	# for most people this isn't very useful so we'll show you how to return
	# columns as a dictionary (hash) in the next example.
	#pprint.pprint(records)
 
if __name__ == "__main__":
	main()