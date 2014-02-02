create table RAW_OPTION ( Symbol  varchar(50),
  EOD   varchar(50),
  Open varchar(50),
  High varchar(50),
  Low varchar(50),
  Close varchar(50),
  Volume varchar(50),
  OpenInt varchar(50) );


cd /stocks/raw_data;cat /stocks/raw_data/OPRA_* > OPRA_week.csv

COPY RAW_OPTION FROM '/stocks/raw_data/OPRA_week.csv' DELIMITER ',' CSV;

COPY RAW_OPTION FROM '/stocks/raw_data/OPRA_2014/OPRA_2014.csv' DELIMITER ',' CSV;

COPY RAW_OPTION FROM '/stocks/raw_data/OPRA_20140120.csv' DELIMITER ',' CSV;

select count(*),EOD from raw_option group by EOD order by to_date(eod,'YYYYMMDD');


#select to_char(eod,'DD-MON-YY') from raw_option where eod = '20140130' limit 100;

#update raw_option set eod = to_char(to_date(eod,'YYYYMMDD'),'DD-MON-YY') where eod = '20140130';

#update raw_option set eod = to_char(to_date(eod,'DD-MON-YY'),'DD-MON-YYYY') where eod = '27-JAN-14';

create table stage_option_month as 
select 
substring(symbol,1,length(symbol)-15) strike_symbol,eod,
substring(symbol,length(substring(symbol,1,length(symbol)-15))+3,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+5,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+1,2) EXPIRY_DATE,
substring(symbol,length(substring(symbol,1,length(symbol)-15))+7,1) option_type,
trim(leading '0' from substring(symbol,length(substring(symbol,1,length(symbol)-15))+8,5))||'.'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+13,3) STRIKE,
symbol,open,high,low,close,volume,openint from raw_option;



create table stage_option_final_month
( 
    id bigserial NOT NULL,
    strike_symbol varchar(10),
    expiry_date date,
    option_type varchar(2),
    strike numeric,
    eod date,
    symbol character varying(50),
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume int,
    openint int,
    money numeric
);


insert into stage_option_final_month
(strike_symbol,expiry_date,option_type,strike,eod,symbol,open,high,low,close,volume,openint,money)
 select 
strike_symbol,to_date(expiry_date,'MM/DD/YY'),
option_type,cast(strike as numeric),to_date(eod,'DD-MON-YYYY'),symbol,
cast(open as numeric),cast(high as numeric),
cast(low as numeric),cast(close as numeric),
cast(volume as int),cast(openint as int),
cast(volume as int)*cast(close as numeric)  
from stage_option_month;

# Dump Table from Palos to stocks
#/usr/lib/postgresql/9.1/bin/pg_dump -t option_jan_2014 palos | psql stocks

# Finalize Stocks Based on Volume 
# select count(*),strike_symbol from option_jan_2014 where volume > 100 group by strike_symbol having count(*) > 100 order by count(*) desc;

# This table has only half million records for the whole month. Here we are just trying to seprate Noise from signal.
# By removing all rows where money < 10K we are just removing noise. 
#create table option_m100_jan_2014 as select * from option_jan_2014 where money > 100; 

#All of our calculations now should rely only on this table option_m100_jan_2014









