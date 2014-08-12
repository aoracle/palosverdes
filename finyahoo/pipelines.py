from scrapy.xlib.pydispatch import dispatcher
from scrapy.contrib.exporter import CsvItemExporter
from scrapy.exceptions import DropItem
from scrapy import log, signals
import MySQLdb
import traceback
from scrapy.contrib.pipeline.images import ImagesPipeline
import sqlite3
from os import path
import os
import time
import unicodedata
from scrapy.http import Request
        
class CsvExportPipeline(object):

    def __init__(self):
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.files = {}

    def spider_opened(self, spider):
        file = open('%s_%s.csv' % (spider.name, int(time.time())), 'w+b')
        self.files[spider] = file
        if 'yopt' in spider.name:
            self.exporter = CsvItemExporter(file,fields_to_export = ['date','instrument','option_symbol','symbol','expiration','type','strike','last','change','bid','ask','volume','open_int'],dialect='excel')
        elif 'prices' in spider.name:
            self.exporter = CsvItemExporter(file,fields_to_export = ['date','open','high','low','close','volume','adj_close'],dialect='excel')
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()

    def process_item(self, item, spider):
        if item is None:
            raise DropItem("None")
        self.exporter.export_item(item)
        return item 
