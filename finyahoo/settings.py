COMMANDS_MODULE = 'finyahoo.commands'
SPIDER_MODULES = ['finyahoo.spiders']
NEWSPIDER_MODULE = 'finyahoo.spiders'
DEFAULT_ITEM_CLASS = 'finyahoo.items.FinyahooItem'
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.65 Safari/537.36'
DOWNLOAD_DELAY = 0.8

ITEM_PIPELINES = [
    'finyahoo.pipelines.CsvExportPipeline'
]
WEBSERVICE_ENABLED = True
EXTENSIONS = {
    'scrapy.contrib.corestats.CoreStats': 500,
    'scrapy.webservice.WebService': 500,
}

DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 110,
}
CONCURRENT_REQUESTS_PER_DOMAIN=5
CONCURRENT_REQUESTS=5
RANDOMIZE_DOWNLOAD_DELAY = False
LOG_FILE = 'finyahoo.log'
LOG_LEVEL = 'DEBUG'
#COOKIES_DEBUG = True
URLLENGTH_LIMIT = 5000

FILES_STORE = 'f:/_GD/ODesk/yahoo/finyahoo/finyahoo/files'
