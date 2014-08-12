from scrapy.command import ScrapyCommand
import urllib
import urllib2
from scrapy import log

class AllCrawlCommand(ScrapyCommand):

    requires_project = True
    default_settings = {'LOG_ENABLED': False}

    def short_desc(self):
        return "Schedule a run for all available spiders"

    def run(self, args, opts):
        url = 'http://localhost:6800/schedule.json'
        for s in self.crawler.spiders.list():
            if 'options' in s:
                addoption = 'CONCURRENT_REQUESTS=5'
            elif 'stocks' in s:
                addoption = 'CONCURRENT_REQUESTS=5'
            else:
                addoption = ''
            if addoption:
                values = {'project' : 'prices', 'spider' : s, 'setting':addoption}
            else:
                values = {'project' : 'prices', 'spider' : s}
            data = urllib.urlencode(values)
            log.msg(" *** data = " + str(data), log.DEBUG)
            print " *** data = " + str(data)
            req = urllib2.Request(url, data)
            response = urllib2.urlopen(req)
            log.msg(response, log.DEBUG)