
#Schema Name = raw_data

create table raw_option_test 
(
  symbol  varchar(50),
  eod   varchar(50),
  open varchar(50),
  high varchar(50),
  low varchar(50),
  close varchar(50),
  volume varchar(50),
  openint varchar(50)
  );

cat /stocks/raw_data/OPRA_* > option_total.csv


COPY raw_option_test FROM '/stocks/raw_data/OPRA/OPRA_total.csv' DELIMITER ',';

create table stage_option as 
select substring(symbol,1,length(symbol)-15) strike_symbol,substring(symbol,length(substring(symbol,1,length(symbol)-15))+3,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+5,2)||'/'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+1,2) EXPIRY_DATE,substring(symbol,length(substring(symbol,1,length(symbol)-15))+7,1) option_type,trim(leading '0' from substring(symbol,length(substring(symbol,1,length(symbol)-15))+8,5))||'.'||substring(symbol,length(substring(symbol,1,length(symbol)-15))+13,3) strike,eod,symbol,open,high,low,close,volume,openint from raw_option;


create table stage_option_final
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


insert into stage_option_final(strike_symbol,expiry_date,option_type,strike,eod,symbol,open,high,low,close,volume,openint,money) 
select 
strike_symbol,to_date(expiry_date,'MM/DD/YY'),option_type,cast(strike as numeric),to_date(eod,'YYYYMMDD'),symbol,cast(open as numeric),cast(high as numeric),cast(low as numeric),cast(close as numeric),cast(volume as int),cast(openint as int),cast(volume as int)*cast(close as numeric) from stage_option;


# Query to get HOT Options  
# This means when TOTAL option money exchange for that symbol is greater than 10 million on single day
# TO get for a given Day where eod ='2014-01-23'

select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from stage_option_final group by strike_symbol,option_type,eod having sum(money) > 100000 order by eod,sum(money) desc;


#This means Return only when INDIVIDUAL Single Option symbol money_exch is greater than 10 Million on that day
select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from stage_option_final where money > 100000 group by strike_symbol,option_type,eod order by eod,sum(money) desc;


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
