#!/usr/bin/python
import psycopg2
import sys
import datetime
import os
import subprocess
import shutil
from optparse import OptionParser
import logging
import ftplib
from ftplib import FTP
import yaml   
from constants import *
from multiprocessing import Pool


ftp = ftplib.FTP(ftp_site)
ftp.login(ftp_username,ftp_password)


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
    if exchange == 'ALL':
      words = ['AMEX','OPRA','NYSE','NASDAQ'] #I am not sure if adj and adv are variables
    else:
      words = words + exchange.split(',')
      #print "Printing Words : %s" % (words)
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
        logger.info(destination[0:len(destination)-1]+path+" built")
    except OSError:
        #folder already exists at destination
        pass
    except ftplib.error_perm:
        #invalid entry (ensure input form: "/dir/folder/something/")
        logger.info("error: could not change to "+path)
        sys.exit("ending session")

    #list children:
    filelist=ftp.nlst()
    file_names = []
    for file in filelist:
        try:
            if check_word_type(file,exchange):
                logger.info("File Download : %s" % (file))    
            #this will check if file is folder:
                ftp.cwd(path+file+"/")
            #if so, explore it:
                file_names.append(file)
                downloadFiles(path+file+"/",destination)
        except ftplib.error_perm:
            #not a folder with accessible content
            #download & return
            os.chdir(destination[0:len(destination)-1]+path)
            #possibly need a permission exception catch:
            ftp.retrbinary("RETR "+file, open(os.path.join(destination,file),"wb").write)
            logger.info(file + " downloaded")
    return




def create_table():

  for i in ['raw_stocks_amex','raw_stocks_nyse','raw_stocks_nasdaq','raw_option']:
    if i != 'raw_option':    
      subprocess.call(["psql", "-d",options.dbname,"-c","  create table IF NOT EXISTS %s (symbol varchar(50),eod varchar(50),open varchar(50),high varchar(50),low varchar(50),close varchar(50),volume varchar(50))" % (i)])
    else:
      subprocess.call(["psql", "-d",options.dbname,"-c","  create table if not exists %s (symbol varchar(50),eod  varchar(50),open varchar(50),high varchar(50),low varchar(50),close varchar(50),volume varchar(50),openint varchar(50))" % (i)])
 
def move_files(status,filename,name):
    if status  == 0:
    #for i in os.listdir(filepath):
      #filename = filepath + '/' + i
      if os.path.exists(archivepath)==False:
        os.mkdir(archivepath)
        os.chmod(archivepath, 0755)
      logger.info("Filepath : %s" % (os.path.join(archivepath,name)))
      if os.path.isfile(os.path.join(archivepath,name)):
        os.remove(os.path.join(archivepath,name))
      shutil.move(filename,archivepath)

def copy_cmd(options, dateobj):
  ret1 = ret11 = ret2 = ret3 = ret4 = 1
  #create_table
  logger.debug("I am in 112")
  subprocess.call(["psql", "-d",options.dbname,"-c","drop table if exists raw_stocks_all"])
  subprocess.call(["psql", "-d",options.dbname,"-c","truncate table raw_option"])
  j = ['OPRA','AMEX','NYSE','NASDAQ']
  if options.filename == 'NULL':
   for i in os.listdir(filepath):
    #print "I am in 113 File Name : %s" % (i)
    if i.endswith(".csv") or i.endswith(".txt"):
      filename = filepath + '/' + i
      #for j in exchange:
      #print "Printing J : %s" % (j)
      logger.debug("Printing i : %s" % (i))
      if i.startswith(j[0]):
        logger.info("opra_table : %s  i : %s" % (opra_table,i)) 
        ret1 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (opra_table,filename)])
        ret11 = subprocess.call(["psql", "-d",options.dbname,"-c","update raw_option set eod = to_char(to_date(eod,'DD-MON-YYYY'),'YYYYMMDD') where length(eod) > 8"])
        move_files(ret1,filename,i)
      elif i.startswith(j[1]):
        ret2 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (nyse_table,filename)])
        move_files(ret2,filename,i)
      elif i.startswith(j[2]):
        ret3 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (nasdaq_table,filename)])
        move_files(ret3,filename,i)
      if i.startswith(j[3]):
        ret4 = subprocess.call(["psql", "-d",options.dbname,"-c","COPY %s FROM '%s' DELIMITER ',' CSV" % (amex_table,filename)])
        move_files(ret4,filename,i)
  
   subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (amex_table)])
   subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (amex_table,j[3])])
   subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (nasdaq_table)])
   subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (nasdaq_table,j[2])])
   subprocess.call(["psql", "-d",options.dbname,"-c","alter table %s add column exchange varchar(10)" % (nyse_table)])
   subprocess.call(["psql", "-d",options.dbname,"-c","update  %s set exchange = '%s'" % (nyse_table,j[1])])
   subprocess.call(["psql", "-d",options.dbname,"-c","create table raw_stocks_all as select * from raw_stocks_amex union select * from raw_stocks_nyse union select * from raw_stocks_nasdaq"])
   subprocess.call(["psql", "-d",options.dbname,"-c","delete from raw_stocks_all where eod = 'Date'"])
   subprocess.call(["psql", "-d",options.dbname,"-c","delete from raw_option where eod = 'Date'"])
   subprocess.call(["psql", "-d",options.dbname,"-c","update raw_stocks_all set eod = to_char(to_date(eod,'DD-MON-YYYY'),'YYYYMMDD') where length(eod) > 8"])
   #update raw_stocks_all set eod = to_char(to_date(eod,'YYYY-MM-DD'),'YYYYMMDD') where length(eod) > 8;
   subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_amex"])
   subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_nyse"])
   subprocess.call(["psql", "-d",options.dbname,"-c","drop table raw_stocks_nasdaq"])
   return ret1 + ret11 + ret2 + ret3 + ret4

def create_stage_final_table():
  
  s1=subprocess.call(["psql", "-d",options.dbname,"-c","drop table if exists stage_option"])
  
  create_stage_option_str = "create table stage_option as select substring(symbol,1,length(symbol)-15) strike_symbol,eod,substring(symbol,length(substring(symbol,1,length(symbol)-15))+3,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+5,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+1,2) EXPIRY_DATE,substring(symbol,length(substring(symbol,1,length(symbol)-15))+7,1) option_type,\
  trim(leading '0' from substring(symbol,length(substring(symbol,1,length(symbol)-15))+8,5))||'.'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+13,3) STRIKE,symbol,open,high,low,close,volume,openint from raw_option"
  
  s2=subprocess.call(["psql", "-d",options.dbname,"-c",create_stage_option_str])
  
  create_option_str = "create table if not exists options \
  (id bigserial NOT NULL,strike_symbol varchar(10),expiry_date date,option_type varchar(2),strike numeric,eod date,symbol character varying(50),open numeric,high numeric,low numeric,close numeric,volume int,openint int,money numeric);"
  
  s3=subprocess.call(["psql", "-d",options.dbname,"-c",create_option_str])
  
  insert_option_str = "insert into options(strike_symbol,expiry_date,option_type,strike,eod,symbol,open,high,low,close,volume,openint,money) \
  select strike_symbol,to_date(expiry_date,'MM/DD/YY'),option_type,cast(strike as numeric),to_date(eod,'YYYYMMDD'),symbol,cast(open as numeric),cast(high as numeric),cast(low as numeric),cast(close as numeric),cast(volume as int),cast(openint as int),cast(volume as int)*cast(close as numeric) from stage_option" 
  #where to_date(eod,'YYYYMMDD') > (select coalesce(max(eod),'20050201') from options)"
  
  s4=subprocess.call(["psql", "-d",options.dbname,"-c",insert_option_str])
 
  create_table_stocks_str = "create table if not exists stocks( id bigserial NOT NULL,symbol character varying(50),eod date,open numeric,high numeric,low numeric,close numeric,volume int,exchange character varying(50))"

  s5=subprocess.call(["psql", "-d",options.dbname,"-c",create_table_stocks_str])
 
  insert_stocks_str = "insert into stocks(symbol,eod,open,high,low,close,volume,exchange) select symbol,to_date(eod,'YYYYMMDD'),cast(open as numeric),cast(high as numeric),cast(low as numeric),cast(close as numeric),cast(volume as int),exchange from raw_stocks_all" 
  #where to_date(eod,'YYYYMMDD') > (select coalesce(max(eod),'20050201') from stocks)"

  s6=subprocess.call(["psql", "-d",options.dbname,"-c",insert_stocks_str])
  return s1 + s2 + s3 + s4 + s5 + s6 

#def validate_data():
# This will give count of reords 
  #select count(*) from options;select count(*) from raw_option;select count(*) from raw_stocks_all;select count(*) from stage_option;select count(*) from stocks;
  #select count(*),eod from options group by eod order by eod;
  #select count(*),exchange,eod from stocks group by exchange,eod order by exchange,eod;

  #def truncate_table():
  #truncate table options;truncate table raw_option;truncate table raw_stocks_all;truncate table stage_option;truncate table stocks;

def init_options():
  parser = OptionParser()
  parser = OptionParser(version="%prog 1.0")
  parser.add_option('-s', '--startdate', type='string', dest='startdate', help='the start date (format: yyyymmdd)')
  parser.add_option('-e', '--enddate', type='string', dest='enddate', help='the end date (format: yyyymmdd)')
  parser.add_option('--filename', type='string', dest='filename', default='NULL',help='target directory for output files')
  parser.add_option('--dbhost', type='string', dest='dbhost', default='localhost', help='Postgres host address')
  parser.add_option('--dbname', type='string', dest='dbname', default=default_dbname, help='db name')
  parser.add_option('--tablename', type='string', dest='tablename', default='raw_option', help='Postgres Table to load')
  parser.add_option('--exchange', type='string', dest='exchange',default='ALL', help='suppy list as [NYSE AMEX NASDAQ OPTION]')
  parser.add_option("-d",action="store_true",dest="download",default=False,help="Download File before start of run ")
  parser.add_option("-l", "--log", type="string",default='info',dest="level_name",help="Set the log level for the launcher. Acceptable values: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default: DEBUG")
  parser.add_option("-f",type="string",dest="function",help="Function to run")
  options, args = parser.parse_args()

  return options

def main(options):
  status = 1
  #print "I am in 1"
  logger.debug("I am in 1")
  #while _date <= options.enddate:
  if options.download:
    
    logger.debug("I am in main.download")
    logger.info("source : %s , filepath : %s , Options.exchange : %s" % (ftp_source,filepath,options.exchange)) 
    downloadFiles(ftp_source,filepath,options.exchange)
  _date = options.startdate
  create_table()
  rs = copy_cmd(options, _date)
  #status = create_stage_final_table()
  print "printing status : %s " % (status) 
  print "printing ret status : %s" % (rs)
  if int(rs) + int(status) == 0:
    logger.info("Loading Process finished successfully")
  logger.debug("I am in main")
  _date += datetime.timedelta(days=1)


if __name__ == '__main__':

  options = init_options() 
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
         logger.info("Priting Options : %s" % (options))

  if os.path.exists(log_dir)==False:
    os.mkdir(log_dir)
    os.chmod(archivepath, 0755)
  levels = {'info':logging.INFO,'warning':logging.WARNING,'error':logging.ERROR,'critical':logging.CRITICAL,'debug':logging.DEBUG}
  log_id = datetime.datetime.now().strftime('%Y%m%dT%H%M')
  log_file_name = log_dir + 'push_data_db' + '_' + options.dbname + '_' + log_id + '.log'
  logger = logging.getLogger()
  handler = logging.FileHandler(log_file_name)
  formatter = logging.Formatter('[%(asctime)s] -%(name)s - p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
  handler.setFormatter(formatter)
  logger.addHandler(handler) 
  logger.setLevel(levels.get(options.level_name, logging.NOTSET))
  logger.info('Starting....')
  logger.debug('LOG DIR : %s' % log_dir)
  logger.debug("I am in __name__.main()")  

  if options.function:
    logger.info("exchange : %s" % (options.exchange))
    if options.function == 'downloadFiles':
      globals()[options.function](source,filepath,options.exchange)
    else:
      globals()[options.function]()
    sys.exit(0)

  main(options)
