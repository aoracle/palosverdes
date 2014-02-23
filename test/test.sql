select * from raw_option limit 100;
select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000) MONEY_EXCH from stage_option_final_week group by strike_symbol,option_type,eod having sum(money) > 100000 order by eod,sum(money) desc;


select option_type,eod,trunc((sum(money)*100) / 1000000) MONEY_EXCH from stage_option_final_month group by option_type,eod order by eod,sum(money) desc;



select strike_symbol,option_type,eod,trunc((sum(money)*100) / 1000000)||' Million' MONEY_EXCH from stage_option_final_month group by strike_symbol,option_type,eod having sum(money) > 100000 order by eod,sum(money) desc;
