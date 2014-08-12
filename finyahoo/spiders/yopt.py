#! /usr/bin/python
#! -*- coding: utf-8 -*-
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import HtmlXPathSelector, XmlXPathSelector
from scrapy.item import Item
from scrapy.http import Response, Request
from scrapy.http import FormRequest
from scrapy.http.cookies import CookieJar
import re
from finyahoo.items import FinyahooItem
from scrapy import log, signals
import datetime
import calendar
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from scrapy.http import HtmlResponse

class FinyahooSpider(CrawlSpider):
    name = "yopt"
    sites = ['finance.yahoo.com']
    allowed_domains = ['http://www.' + '%s' % (site) for site in sites] + ['www.' + '%s' % (site) for site in sites] + [site for site in sites]
    handle_httpstatus_list = [404]
    curr_dict = {}
    start_urls = ['http://finance.yahoo.com']

    def __init__(self, exact='stocks.csv', symbol='', date=datetime.date.today(), *args, **kwargs):
        super(FinyahooSpider, self).__init__(*args, **kwargs)
        self.exact = [i.strip().rstrip() for i in open(exact,'r').read().split('\n') if i]
        self.symbol = symbol
        if date <> datetime.date.today():
            DateTemp=map(int, str(date)[1:-1].split('-'))
            self.date=datetime.date(DateTemp[0], DateTemp[1], 1)
        else:        
            self.date = date
    
    def parse_start_url(self, response):
        if self.symbol:
            yield Request('http://finance.yahoo.com/q/os?s=' + str(self.symbol),callback=self.parse_item,meta={'symbol':self.symbol}) 
        else:
            for s in self.exact:
                yield Request('http://finance.yahoo.com/q/os?s=' + str(s),callback=self.parse_item,meta={'symbol':s}) 
    
    def parse_item(self,response):
        soup = BeautifulSoup(response.body)
        items = []
        table = response.selector.xpath('//table[@border="0" and @cellpadding="3" and @cellspacing="1" and @width="100%"]/tr')[2:]
        if len(table) < 1:
            item = FinyahooItem()
            item['date'] = self.date.strftime("%m/%d/%Y")
            item['instrument'] = response.request.meta['symbol']
            item['option_symbol'] = '--'
            item['symbol'] = '--'
            item['expiration'] = '--'
            item['type'] = '--'
            item['strike'] = '--'
            item['last'] = '--'
            item['change'] = '--'
            item['bid'] = '--'
            item['ask'] = '--'
            item['volume'] = '--'
            item['open_int'] = '--'
            return item
            
        for t in table:
            for type in ['Call','Put']:
                if 'Call' in type:
                    stcol = 2
                elif 'Put' in type:
                    stcol = 10
                item = FinyahooItem()
                item['date'] = self.date.strftime("%m/%d/%Y")
                item['instrument'] = response.request.meta['symbol']
                
                try:
                    item['option_symbol'] = t.xpath('.//td[1]/a/text()')[0].extract()
                except:
                    item['option_symbol'] = None

                item['symbol'] = response.request.meta['symbol']
                    
                try:
                    exDatePattern=re.compile(r'''Options Expiring \w{6,13}, \w{3,10} \d{2}, \d{1,4}''', re.IGNORECASE) 
                    exDateStr=''.join(exDatePattern.findall(str(response.body)))
                    exDate=exDateStr[''.join(exDateStr).find(',')+1:].strip()
                    vMonthTemp=str(exDate[:exDate.find(' ')]).strip()
                    vDay=str(exDate[len(vMonthTemp):exDate.find(',')]).strip()
                    vYear=str(exDate[exDate.rfind(',')+1:]).strip()
                    months=['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                    vMonth=str(months.index(vMonthTemp)+1)
                    expirDate=vMonth+'/'+vDay+'/'+vYear                
                    
                    item['expiration'] = expirDate
                except:
                    item['expiration'] = None
                    
                item['type'] = type
                
                try:
                    item['strike'] = t.xpath('.//td[8]/b/a/text()')[0].extract()
                except:
                    item['strike'] = None
                try:
                    item['last'] = re.sub(',','.',re.sub(r'<([^>]+)>','',t.xpath('.//td['+str(stcol)+']')[0].extract())).strip()
                except:
                    item['last'] = None
                try:
                    item['change'] = re.sub(',','.',re.sub(r'<([^>]+)>','',t.xpath('.//td['+str(stcol+1)+']')[0].extract())).strip()
                except:
                    item['change'] = None
                try:
                    item['bid'] = re.sub(',','.',re.sub(r'<([^>]+)>','',t.xpath('.//td['+str(stcol+2)+']')[0].extract())).strip()
                except:
                    item['bid'] = None
                try:
                    item['ask'] = re.sub(',','.',re.sub(r'<([^>]+)>','',t.xpath('.//td['+str(stcol+3)+']')[0].extract())).strip()
                except:
                    item['ask'] = None
                try:
                    item['volume'] = re.sub(',','.',re.sub(r'<([^>]+)>','',t.xpath('.//td['+str(stcol+4)+']')[0].extract())).strip()
                except:
                    item['volume'] = None
                try:
                    item['open_int'] = re.sub(',','.',re.sub(r'<([^>]+)>','',t.xpath('.//td['+str(stcol+5)+']')[0].extract())).strip()
                except:
                    item['open_int'] = None
                items.append(item)
        return items
        