/*create table RAW_OPTION ( Symbol  varchar(50),
  EOD   varchar(50),
  Open varchar(50),
  High varchar(50),
  Low varchar(50),
  Close varchar(50),
  Volume varchar(50),
  OpenInt varchar(50) );
*/

create table stage_option as 
select 
substring(symbol,1,length(symbol)-15) strike_symbol,eod,
substring(symbol,length(substring(symbol,1,length(symbol)-15))+3,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+5,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+1,2) EXPIRY_DATE,
substring(symbol,length(substring(symbol,1,length(symbol)-15))+7,1) option_type,
trim(leading '0' from substring(symbol,length(substring(symbol,1,length(symbol)-15))+8,5))||'.'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+13,3) STRIKE,
symbol,open,high,low,close,volume,openint from raw_option;

create table options
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


insert into options(strike_symbol,expiry_date,option_type,strike,eod,symbol,open,high,low,close,volume,openint,money) 
select 
strike_symbol,to_date(expiry_date,'MM/DD/YY'),
option_type,cast(strike as numeric),
to_date(eod,'YYYYMMDD'),symbol,cast(open as numeric),
cast(high as numeric),cast(low as numeric),
cast(close as numeric),cast(volume as int),
cast(openint as int),cast(volume as int)*cast(close as numeric) from stage_option;

create table stocks
( 
    id bigserial NOT NULL,
    symbol character varying(50),
    eod date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume int,
    exchange character varying(50)
);

insert into stocks(symbol,eod,open,high,low,close,volume,exchange) 
select 
symbol,
to_date(eod,'YYYYMMDD'),cast(open as numeric),
cast(high as numeric),cast(low as numeric),
cast(close as numeric),cast(volume as int),
exchange from raw_stocks_all;




# For trina solar : find out where Money went order by accordingly
select o.*,o.money*100 from options o where strike_symbol = 'TSL' and money > 100 order by money desc,symbol,eod,strike,expiry_date,option_type;
# Query to get HOT Options  
# This means when TOTAL option money exchange for that symbol is greater than 10 million on single day
# TO get for a given Day where eod ='2014-01-23'

select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from options group by strike_symbol,option_type,eod having sum(money) > 100000 order by eod,sum(money) desc;


#This means Return only when INDIVIDUAL Single Option symbol money_exch is greater than 10 Million on that day
select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from options where money > 100000 group by strike_symbol,option_type,eod order by eod,sum(money) desc;


#select to_char(eod,'DD-MON-YY') from raw_option where eod = '20140130' limit 100;

#update raw_option set eod = to_char(to_date(eod,'YYYYMMDD'),'DD-MON-YY') where eod = '20140130';

#update raw_option set eod = to_char(to_date(eod,'DD-MON-YY'),'DD-MON-YYYY') where eod = '27-JAN-14';

# Dump Table from Palos to stocks
#/usr/lib/postgresql/9.1/bin/pg_dump -t option_jan_2014 palos | psql stocks

# Finalize Stocks Based on Volume 
# select count(*),strike_symbol from option_jan_2014 where volume > 100 group by strike_symbol having count(*) > 100 order by count(*) desc;

# This table has only half million records for the whole month. Here we are just trying to seprate Noise from signal.
# By removing all rows where money < 10K we are just removing noise. 
#create table option_m100_jan_2014 as select * from option_jan_2014 where money > 100; 

#All of our calculations now should rely only on this table option_m100_jan_2014

# Analysis of Unusual Stock Options 
# I found Unsual Money Exchanged for GD when i started digging I found that 245 Millions worth of option got exchanged and most of them were calls on 1/14/2014
# If you look at Volume on other days its very small except on 1/14/2014

select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from stage_option_final_week where strike_symbol = 'GD' group by strike_symbol,option_type,eod  order by eod,sum(money) desc;

select strike_symbol,strike,open,close,option_type,expiry_date,eod,money*100 MONEY_EXCH from stage_option_final_week where strike_symbol = 'GD' and eod = '2014-01-14' and money > 100 order by eod,money desc; 

select strike_symbol,strike,open,close,option_type,expiry_date,eod,money*100 MONEY_EXCH from stage_option_final_week where strike_symbol = 'GD' and eod = '2014-01-14' and money > 100 order by eod,money desc; 



select count(*),substring(strike_symbol,1,1) from stage_option_final group by substring(strike_symbol,1,1) order by substring(strike_symbol,1,1);


# Best partitioning Ranges for Stocks option table
select count,cast((count*100/3232632) as numeric) Percent_OF,ALPHA from 
(
select count(*) count,substring(strike_symbol,1,1) ALPHA from stage_option_final group by substring(strike_symbol,1,1) order by substring(strike_symbol,1,1)
) X
;

/*
 count  | percent_of | alpha 
--------+------------+---------------
 280899 |          8 | A (2 group)
 138692 |          4 | B (1 group)
 265320 |          8 | C (2 group) 
 140162 |          4 | D (1 group)
 152966 |          4 | E (1 group)
 130102 |          4 | F (1 group)
 152034 |          4 | G (1 group)
  93066 |          2 | H (1 group)
 149579 |          4 | I (1 group)
  47650 |          1 | J K L (1 GROUP)
  53802 |          1 | K
  92865 |          2 | L
 184408 |          5 | M (1 group)
 139368 |          4 | N (1 group)
  56765 |          1 | O P (1 GROUP)
 150713 |          4 | P
  40932 |          1 | Q R (1 group)
  99220 |          3 | R
 308371 |          9 | S (2 group)
 179780 |          5 | T (1 group)
  94103 |          2 | U V (1 group)
  98892 |          3 | V 
  75084 |          2 | W X Y Z (1 group)
  71216 |          2 | X
  22401 |          0 | Y
  14242 |          0 | Z


CREATE TABLE stage_option_final_A ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_B ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_C ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_D ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_E ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_F ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_G ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_H ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_I ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_J ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_K ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_L ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_M ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_N ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_O ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_P ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_Q ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_R ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_S ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_T ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_U ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_V ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_W ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_X ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_Y ( ) INHERITS (measurement);
CREATE TABLE stage_option_final_Z ( ) INHERITS (measurement);
*/
