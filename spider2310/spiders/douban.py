import os
import sys

import scrapy
from scrapy import Selector, Request
from scrapy.cmdline import execute
from scrapy.http import HtmlResponse

from spider2310.items import MovieItem


class DoubanSpider(scrapy.Spider):
    name = "douban"
    allowed_domains = ["movie.douban.com"]

    def start_requests(self):
        for page in range(10):
            yield Request(url=f'https://movie.douban.com/top250?start={page * 25}&filter=',
                          meta={'proxy': 'socket5://127.0.0.1:7890'})

    def parse(self, response: HtmlResponse, **kwargs):
        sel = Selector(response)
        list_items = sel.css('#content > div > div.article > ol > li')
        for list_item in list_items:
            detail_url = list_item.css('div.info > div.hd > a::attr(href)').extract_first()
            print(detail_url)
            movie_item = MovieItem()
            movie_item['title'] = list_item.css('span.title::text').extract_first()
            movie_item['rank'] = list_items.css('span.rating_num::text').extract_first()
            movie_item['subject'] = list_items.css('span.inq::text').extract_first() or ''
            yield Request(
                url=detail_url,
                callback=self.parse_detail,
                cb_kwargs={'item': movie_item}
            )

    def parse_detail(self, response, **kwargs):
        movie_item = kwargs['item']
        sel = Selector(response)
        movie_item['duration'] = sel.css('span[property="v:runtime"]::attr(content)').extract_first()
        movie_item['intro'] = sel.css('span[property="v:summary"]::text').extract_first() or ''
        yield movie_item


if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    execute(['scrapy', 'crawl', 'douban'])
    # execute(['scrapy', 'crawl', 'douban', '-o', 'douban.csv'])
    # execute(['scrapy', 'crawl', 'douban', '--nolog'])
