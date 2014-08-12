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
from finyahoo.items import FinyahooItemPrices
from scrapy import log, signals
from datetime import date, timedelta, datetime
import calendar
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from finyahoo.settings import FILES_STORE
import urllib, urllib2

class FinyahooSpider(CrawlSpider):
    name = "prices"
    sites = ['finance.yahoo.com']
    allowed_domains = ['http://www.' + '%s' % (site) for site in sites] + ['www.' + '%s' % (site) for site in sites] + [site for site in sites]
    handle_httpstatus_list = [404]
    symbols = [i.strip().rstrip() for i in open('stocks.csv','r').read().split('\n') if i]
    start_urls = ['http://finance.yahoo.com']
    
    def __init__(self, symbols='stocks.csv', hist=False, *args, **kwargs):
        super(FinyahooSpider, self).__init__(*args, **kwargs)
        self.symbols = [i.strip().rstrip() for i in open(symbols,'r').read().split('\n') if i]
        self.hist = hist
        print 'hist ',hist
        print 'FILES_STORE ',FILES_STORE
        
    def parse_start_url(self, response):
        if self.hist:
            for s in self.symbols:
                yield Request('http://finance.yahoo.com/q/hp?s=' + str(s),callback=self.get_hist,meta={'symbol':s}) 
        for s in self.symbols:
            yield Request('http://finance.yahoo.com/q?s=' + str(s),callback=self.get_todays,meta={'symbol':s}) 

    def get_hist(self, response):
        s = response.request.meta['symbol']
        csvlink = response.selector.xpath('//a[contains(@href,"http://real-chart.finance.yahoo.com/table.csv?s=")]/@href')[0].extract()
        urllib.urlretrieve(csvlink, FILES_STORE + '/history_'+s+'.csv')
            
    def get_todays(self,response):
        item = FinyahooItemPrices()
        item['date'] = date.today().strftime("%m/%d/%Y")
        try:
            item['open'] = response.selector.xpath('//th[contains(text(),"Open:")]/following::td/text()')[0].extract()
        except:
            item['open'] = None
        try:
            item['high'] = response.selector.xpath('//th[contains(text(),"s Range:")]/following::td/span[2]/text()')[0].extract()
        except:
            item['high'] = None
        try:
            item['low'] = response.selector.xpath('//th[contains(text(),"s Range:")]/following::td/span[1]/text()')[0].extract()
        except:
            item['low'] = None
        try:
            item['close'] = response.selector.xpath('//span[@class="time_rtq_ticker"]/span/text()')[0].extract()
        except:
            item['close'] = None
        try:
            item['volume'] = re.sub(',','',response.selector.xpath('//th[contains(text(),"Volume:")]/following::td/text()')[0].extract())
        except:
            item['volume'] = None
            
        item['adj_close'] = None

        return item
        