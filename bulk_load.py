#!/usr/bin/python
import psycopg2
from datetime import datetime
#mesowest.out : File containing original input data
#test.csv : Comma separated data

input_file = open("/stocks/raw_data/OPRA_20140121_test.csv", 'r')
output_file = open("/stocks/raw_data/OPRA_20140121_test.out", 'w')

#start time
tstart = datetime.now()
print tstart
#
##skip first 4 lines from mesowest.out
#for i in [1,2,3,4]:
#    input_file.readline()
#
#read entire file
lines = input_file.readlines()

#remove spaces from columns in each row
for line in lines:
   a = line.strip().split()     #trim trailing space characters and split columns on one or more spaces. Returns list of data
   output_file.write(','.join(a)+'\n')   # Write all columns to output_file separeted by comma

input_file.close()
output_file.close()

#connect to postgres server and copy data from text.csv to weatherdata table
try:
    conn = psycopg2.connect("host='localhost' dbname='palos' user='andy' password=''")
    curs = conn.cursor()
    io =  open(output_file, 'r')
    curs.copy_from(io, 'raw_option_test', ',')
    tend = datetime.now()
    print tend    
    print "Time for copy entire data: " + str(tend - tstart)
    io.close()

except Exception , e:
    print "Cursor failed to copy from file"
    conn.rollback()
    print 'ERROR:', e[0]
    curs.close

else:
    conn.commit()
    curs.close()

finally:
    if conn:
        conn.close()