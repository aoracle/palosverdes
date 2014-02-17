#!/usr/bin/python
import psycopg2
import sys
import datetime
import os
import subprocess
import shutil
from optparse import OptionParser
import ftplib
from ftplib import FTP
ftp = ftplib.FTP("ftp.eoddata.com")
ftp.login("amahajan1981", "gurukul1")


amex_table = 'raw_stocks_amex'
nyse_table = 'raw_stocks_nyse'
nasdaq_table = 'raw_stocks_nasdaq'
opra_table = 'raw_option'
filepath = '/stocks/raw_data/test/'
archivepath = '/stocks/raw_data/test/archive'
source="/"
#dest="/stocks/raw_data/"

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

def check_word_type(filename,exchange):
    words = []
    if exchange == None:
      words = ['AMEX','OPRA','NYSE','NASDAQ'] #I am not sure if adj and adv are variables
    else:
      words.append(exchange)
      #print "words : %s " % (words)
    count = 0
    for i in words:
        if i in filename:
            word_type = str(i) #just make sure its string
            count=+1
        else:
            word_type = ''
    if count == 1:
        return count

def downloadFiles(path,destination,exchange):
#path & destination are str of the form "/dir/folder/something/"
#path should be the abs path to the root FOLDER of the file tree to download
    try:
        ftp.cwd(path)
        #clone path to destination
        os.chdir(destination)
        os.mkdir(destination[0:len(destination)-1]+path)
        print destination[0:len(destination)-1]+path+" built"
    except OSError:
        #folder already exists at destination
        pass
    except ftplib.error_perm:
        #invalid entry (ensure input form: "/dir/folder/something/")
        print "error: could not change to "+path
        sys.exit("ending session")

    #list children:
    filelist=ftp.nlst()

    for file in filelist:
        try:
            if check_word_type(file,exchange):
                print "File Download : %s" % (file)             
            #this will check if file is folder:
                ftp.cwd(path+file+"/")
            #if so, explore it:
                downloadFiles(path+file+"/",destination)
        except ftplib.error_perm:
            #not a folder with accessible content
            #download & return
            os.chdir(destination[0:len(destination)-1]+path)
            #possibly need a permission exception catch:
            ftp.retrbinary("RETR "+file, open(os.path.join(destination,file),"wb").write)
            print file + " downloaded"
    return



def create_table():

  for i in ['raw_stocks_amex','raw_stocks_nyse','raw_stocks_nasdaq','raw_option']:
    if i != 'raw_option':    
      subprocess.call(["psql", "-d",options.dbname,"-c","  create table IF NOT EXISTS %s (symbol varchar(50),eod varchar(50),open varchar(50),high varchar(50),low varchar(50),close varchar(50),volume varchar(50))" % (i)])
    else:
      subprocess.call(["psql", "-d",options.dbname,"-c","  create table if not exists %s (symbol varchar(50),eod  varchar(50),open varchar(50),high varchar(50),low varchar(50),close varchar(50),volume varchar(50),openint varchar(50))" % (i)])
 
def copy_cmd(options, dateobj):
  #create_table
  print "I am in 112"
  subprocess.call(["psql", "-d",options.dbname,"-c","drop table if exists raw_stocks_all"])
  subprocess.call(["psql", "-d",options.dbname,"-c","truncate table raw_option"])
  j = ['OPRA','AMEX','NYSE','NASDAQ']
  if options.filename == 'NULL':
   for i in os.listdir(filepath):
    #print "I am in 113 File Name : %s" % (i)
    if i.endswith(".txt"):
      filename = filepath + '/' + i
      #for j in exchange:
      #print "Printing J : %s" % (j)
      print "Printing i : %s" % (i)
      if i.startswith(j[0]):
        print "opra_table : %s  i : %s" % (opra_table,i) 
        ret1 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (opra_table,filename)])
      elif i.startswith(j[1]):
        ret2 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (nyse_table,filename)])

      elif i.startswith(j[2]):
        ret3 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (nasdaq_table,filename)])

      if i.startswith(j[3]):
          ret4 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (amex_table,filename)])
          
  
   subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (amex_table)])
   subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (amex_table,j[3])])
   subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (nasdaq_table)])
   subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (nasdaq_table,j[2])])
   subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (nyse_table)])
   subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (nyse_table,j[1])])
   subprocess.call(["psql", "-d",options.dbname,"-c","create table raw_stocks_all as select * from raw_stocks_amex union select * from raw_stocks_nyse union select * from raw_stocks_nasdaq"])
   subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_amex"])
   subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_nyse"])
   subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_nasdaq"])
  
  if ret1 + ret2 + ret3 + ret4 == 0:
    for i in os.listdir(filepath):
      filename = filepath + '/' + i
      if os.path.exists(archivepath)==False:
        os.mkdir(archivepath)
        os.chmod(archivepath, 0755)
      shutil.move(filename,archivepath)

    


	#else:
  #  pass
  #:
		#subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (options.tablename,options.filename)])


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


  _date = options.startdate
  print "I am in 1"
  #while _date <= options.enddate:
  #downloadFiles(source,filepath)
  create_table()
  rs = copy_cmd(options, _date)
  

  print "I am in 2"
  _date += datetime.timedelta(days=1)


if __name__ == '__main__':



  print "I am in 11"
  parser = OptionParser(version="%prog 1.0")
  parser.add_option('-s', '--startdate', type='string', dest='startdate', help='the start date (format: yyyymmdd)')
  parser.add_option('-e', '--enddate', type='string', dest='enddate', help='the end date (format: yyyymmdd)')
  parser.add_option('--filename', type='string', dest='filename', default='NULL',help='target directory for output files')
  parser.add_option('--dbhost', type='string', dest='dbhost', default='localhost', help='Postgres host address')
  parser.add_option('--dbname', type='string', dest='dbname', default='raw_stocks', help='db name')
  parser.add_option('--tablename', type='string', dest='tablename', default='raw_option', help='Postgres Table to load')
  parser.add_option('--exchange', type='string', dest='exchange',default=None, help='NYSE AMEX NASDAQ OPTION')
  parser.add_option("-f",type="string",dest="function",help="Function to run")
  options, args = parser.parse_args()

  if options.function:
    print "exchange : %s" % (options.exchange)
    if options.function == 'downloadFiles':
      globals()[options.function](source,filepath,options.exchange)
    #target_conn.commit()
    sys.exit(0)

 ## Process the date args
  if not options.startdate:
   options.startdate = datetime.datetime.today()
  else:
    try:
      options.startdate = datetime.datetime.strptime('%Y%m%d', options.startdate)
    except ValueError:
      parser.error("Invalid value for startdate (%s)") % (options.startdate)
      if not options.enddate:
        options.enddate = options.startdate + datetime.timedelta(days=7)
      else:
        try:
         options.enddate = datetime.datetime.strptime('%Y%m%d', options.enddate)
        except ValueError:
         parser.error("Invalid value for enddate (%s)" % options.enddate)
         print "Priting Options : %s" % (options)
  main(options, args)#