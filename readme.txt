1) update FILES_STORE value in settings.py - it's where historical prices will be stored

2) call parameters is almost exactly the same as it was
    was: python getyahoo.py --symbol='AAPL', --date='2012-07'
    now: scrapy crawl yopt -a symbol='AAPL' -a date='2012-07'
    
    was: python yetyahoo.py --symbol='AAPL'
    now: scrapy crawl yopt -a symbol='AAPL'
    
    was: python getyahoo.py --exact='stocks.txt'
    now: scrapy crawl yopt -a exact='stocks.txt'

3) to get prices:
    - without historical:
    scrapy crawl prices -a hist=False 
    or simple 
    scrapy crawl prices
    - with historical:
    scrapy crawl prices -a hist=True

4) run script from finyahoo folder (where scrapy.cfg places). 

5) Obviously scrapy installed required. use 'pip install scrapy' or 'easy_install -U scrapy'. 
There could be lxml issues so that some additional actions required (just google or let me know to fix it)

6) logging into console or into LOG_FILE (in settings.py) file (log placed in project's home). log level varying with LOG_LEVEL (DEBUG, ERROR, WARNING)

7) output files are almost the same that is was before, placed in project's home as well. Timestamp in filename to avoid rewrites.

8) vary multythreading with CONCURRENT_REQUESTS_PER_DOMAIN and CONCURRENT_REQUESTS options in settings.py