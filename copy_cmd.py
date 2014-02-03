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
def get_stats(options, dateobj):
    """Return statistics for the date of `dateobj`"""
    _datestr = dateobj.strftime('%Y-%m-%d')
    #sql = SQL % _datestr
    filepath = os.path.join(options.filename, 'DateLoop-%s.txt' % _datestr)
    subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (options.tablename,options.filename)])
    return 

	
def main(options, args):
    """"""
    _date = options.startdate
    #while _date <= options.enddate:
    rs = get_stats(options, _date)
    _date += datetime.timedelta(days=1)


if __name__ == '__main__':
    parser = OptionParser(version="%prog 1.0")
    parser.add_option('-s', '--startdate', type='string', dest='startdate', 
        help='the start date (format: yyyymmdd)')

    parser.add_option('-e', '--enddate', type='string', dest='enddate', 
        help='the end date (format: yyyymmdd)')

    parser.add_option('--filename', type='string', dest='filename', default='/stocks/raw_data/OPRA_20140121_test.csv',
        help='target directory for output files')

    parser.add_option('--dbhost', type='string', dest='dbhost', default='localhost', 
        help='Postgres host address')

    parser.add_option('--dbname', type='string', dest='dbname', default='stocks', 
        help='db name')

    parser.add_option('--tablename', type='string', dest='tablename', default='raw_option_test', 
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