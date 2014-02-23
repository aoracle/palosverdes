select count(*),strike_symbol from option_jan_2014 where volume > 100 group by strike_symbol having count(*) > 100 order by count(*) desc;


select count(*),strike_symbol from option_jan_2014 where volume > 100 and money > 100 group by strike_symbol having count(*) > 100 order by count(*) desc;



select count(*) from option_jan_2014 where money > 10000;



select strike_symbol,(SUM(CASE WHEN option_TYPE = 'P' THEN money ELSE 0 END)  /SUM(CASE WHEN option_TYPE = 'C' THEN money ELSE 0 END))   from option_jan_2014 where money > 1000 group by strike_symbol order by 
(SUM(CASE WHEN option_TYPE = 'P' THEN money ELSE 0 END)  / SUM(CASE WHEN option_TYPE = 'C' THEN money ELSE 0 END)) DESC ; 

select strike_symbol,eod,(SUM(CASE WHEN option_TYPE = 'P' THEN money ELSE 0.001 END)  /SUM(CASE WHEN option_TYPE = 'C' THEN money ELSE 0.001 END))   from option_jan_2014 where strike_symbol = 'NVDQ' and money > 100 group by strike_symbol,eod order by 
(SUM(CASE WHEN option_TYPE = 'P' THEN money ELSE 0.001 END) /SUM(CASE WHEN option_TYPE = 'C' THEN money ELSE 0.001 END)) DESC ; 


select * from option_jan_2014 where strike_symbol = 'NVDQ' and money > 100 order by eod asc;


select * from option_jan_2014  where (high - low ) > low and money > 100;


select sum(money),strike,close,symbol,eod,expiry_date  from option_m100_jan_2014  where expiry_date > to_date ('2014-02-15','YYYY-MM-DD') and expiry_date < to_date ('2014-05-20','YYYY-MM-DD')
group by symbol,eod,strike,expiry_date,close  order by symbol,eod asc;



select sum(money),strike,close,symbol,eod,expiry_date,option_type  from option_m100_jan_2014  where expiry_date > to_date ('2014-02-15','YYYY-MM-DD') and expiry_date < to_date ('2014-05-20','YYYY-MM-DD') and strike_symbol = 'TSL'
group by symbol,eod,strike,expiry_date,close,option_type  order by symbol,strike,expiry_date,eod asc;


 where strike_symbol not in ('AAPL','AMZN', 'VIX','SPX.XO','SPX.XO','SPY','FB','GOOG','TSLA','QQQ','EWZ','RUT.X','XLF','DIA','TWTR','VXX','VIX.XO','LNKD','GE' ) 


select count(ct),ct from (
select count(strike_symbol) ct,strike_symbol from (
select sum(money),strike_symbol,eod,option_type  
from option_m100_jan_2014 group by strike_symbol,eod,option_type  having sum(money) > 1000 
order by eod desc,sum(money) desc
) A group by strike_symbol order by count(strike_symbol)) B group by ct order by count(ct);



select count(strike_symbol) ct,strike_symbol from (
select sum(money),strike_symbol,eod,option_type  
from option_m100_jan_2014 group by strike_symbol,eod,option_type  having sum(money) > 1000 
order by eod desc,sum(money) desc
) A group by strike_symbol having count(strike_symbol) > 35 order by count(strike_symbol) 






select count(*)  from option_m100_jan_2014 group by strike_symbol,eod,option_type;
