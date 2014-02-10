#!/usr/bin/python
import psycopg2
import sys
import datetime
import os
import subprocess
from optparse import OptionParser

#SQL = """SELECT d.Date, SUM(d.CostUsd) FROM Stats d WHERE d.Date = '%s' GROUP BY d.Date"""
##COPY RAW_OPTION FROM '/stocks/raw_data/OPRA_20140121_test.csv' DELIMITER ',' CSV;
#
def zip():
   for i in os.listdir('/home/ec2-user/Stocks/'):
    if i.endswith(".zip"): 
       fh = open('/home/ec2-user/Stocks/%s'%(i) , 'rb')
       z = zipfile.ZipFile(fh)
       for name in z.namelist():
          #outpath = "/home/ec2-user/Stocks/load"
          exchange = i.split('_', 1)[0].replace('.', '').upper()
          outpath = "/home/ec2-user/Stocks/load_%s" %(exchange)
          if os.path.exists(outpath)==False:
             os.mkdir(outpath)
             os.chmod(outpath, 0755)
          z.extract(name, outpath)
       fh.close()
        #print i
        #continue
    #else:
    #    continue	

amex_table = 'raw_stocks_amex'
nyse_table = 'raw_stocks_nyse'
nasdaq_table = 'raw_stocks_nasdaq'
opra_table = 'raw_option'
filepath = '/stocks/raw_data/'


def copy_cmd(options, dateobj):
	exchange = ['OPRA','AMEX','NYSE','NASDAQ']
	if options.filename == 'NULL':
		for i in os.listdir('/stocks/raw_data/'):
			if i.endswith(".csv"):
				filename = filepath + i
				for j in exchange:
					if i.startswith(j): == 'OPRA':
						subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (opra_table,filename)])
					if i.startswith(j): == 'NYSE':
						subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (nyse_table,filename)])
						subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (nyse_table)])
						subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (nyse_table,j)])

					if i.startswith(j): == 'NASDAQ':
						subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (nasdaq_table,filename)])
						subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (nasdaq_table)])
						subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (nasdaq_table,j)])
					if i.startswith(j): == 'AMEX':
							subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (amex_table,filename)])
							subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (amex_table)])
							subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (amex_table,j)])

	subprocess.call(["psql", "-d",options.dbname,"-c","create table raw_stocks_all as select * from raw_stocks_amex union select * from raw_stocks_nyse union select * from raw_stocks_nasdaq"])
	subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_amex"])
	subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_nyse"])
	subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_nasdaq"])	
	else:
		subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (options.tablename,options.filename)])


#    """Return statistics for the date of `dateobj`"""
#    _datestr = dateobj.strftime('%Y-%m-%d')
#    for i in exchange:
#
#    	filename = "/stocks/raw_data/%s_%s" %(exchange,_datestr)
#    #sql = SQL % _datestr
#    	filepath = os.path.join(options.filename, 'DateLoop-%s.txt' % _datestr)
#    	subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (options.tablename,options.filename)])
#    return 

	
def main(options, args):
    """"""
    _date = options.startdate
    #while _date <= options.enddate:
    rs = copy_cmd(options, _date)
    _date += datetime.timedelta(days=1)


if __name__ == '__main__':
    parser = OptionParser(version="%prog 1.0")
    parser.add_option('-s', '--startdate', type='string', dest='startdate', 
        help='the start date (format: yyyymmdd)')

    parser.add_option('-e', '--enddate', type='string', dest='enddate', 
        help='the end date (format: yyyymmdd)')

    parser.add_option('--filename', type='string', dest='filename', default='NULL',
        help='target directory for output files')

    parser.add_option('--dbhost', type='string', dest='dbhost', default='localhost', 
        help='Postgres host address')

    parser.add_option('--dbname', type='string', dest='dbname', default='raw_stocks', 
        help='db name')

    parser.add_option('--tablename', type='string', dest='tablename', default='raw_option', 
        help='Postgres Table to load')

    options, args = parser.parse_args()

    ## Process the date args
    if not options.startdate:
        options.startdate = datetime.datetime.today()
    else:
        try:
            options.startdate = datetime.datetime.strptime('%Y%m%d', options.startdate)
        except ValueError:
            parser.error("Invalid value for startdate (%s)" % options.startdate)

    if not options.enddate:
        options.enddate = options.startdate + datetime.timedelta(days=7)
    else:
        try:
            options.enddate = datetime.datetime.strptime('%Y%m%d', options.enddate)
        except ValueError:
            parser.error("Invalid value for enddate (%s)" % options.enddate)

    main(options, args)#