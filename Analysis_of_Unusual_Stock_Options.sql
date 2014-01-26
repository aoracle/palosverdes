# Analysis of Unusual Stock Options 
# I found Unsual Money Exchanged for GD when i started digging I found that 245 Millions worth of option got exchanged and most of them were calls on 1/14/2014
# If you look at Volume on other days its very small except on 1/14/2014

select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from stage_option_final where strike_symbol = 'GD' group by strike_symbol,option_type,eod  order by eod,sum(money) desc;

select strike_symbol,strike,open,close,option_type,expiry_date,eod,money*100 MONEY_EXCH from stage_option_final where strike_symbol = 'GD' and eod = '2014-01-14' and money > 100 order by eod,money desc; 

select strike_symbol,strike,open,close,option_type,expiry_date,eod,money*100 MONEY_EXCH from stage_option_final where strike_symbol = 'GD' and eod = '2014-01-14' and money > 100 order by eod,money desc; 



select count(*),substring(strike_symbol,1,1) from stage_option_final group by substring(strike_symbol,1,1) order by substring(strike_symbol,1,1);


# Best partitioning Ranges for Stocks option table
select count,cast((count*100/3232632) as numeric) Percent_OF,ALPHA from 
(
select count(*) count,substring(strike_symbol,1,1) ALPHA from stage_option_final group by substring(strike_symbol,1,1) order by substring(strike_symbol,1,1)
) X
;


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
