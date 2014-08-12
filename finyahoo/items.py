from scrapy.item import Item, Field

class FinyahooItem(Item):
    date = Field()
    instrument = Field()
    option_symbol = Field()
    symbol = Field()
    expiration = Field()
    type = Field()
    strike = Field()
    last = Field()
    change = Field()
    bid = Field()
    ask = Field()
    volume = Field()
    open_int = Field()

class FinyahooItemPrices(Item):
    #Date,Open,High,Low,Close,Volume,Adj Close    
    date = Field()
    open = Field()
    high = Field()
    low = Field()
    close = Field()
    volume = Field()
    adj_close = Field()
