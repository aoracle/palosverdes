
select * from raw_stocks where symbol = 'TSL' and CAST(coalesce(volume, '0') AS integer) > 10000000 order by CAST(coalesce(volume, '0') AS integer) desc;


