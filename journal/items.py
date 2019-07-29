# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import re
from scrapy.loader.processors import Identity, MapCompose


def prevent_spec_chars(some_string):
    danger_chars = r'\/:*?"<>|'
    for char in danger_chars:
        some_string = some_string.replace(char, '')
    return some_string


class JournalItem(scrapy.Item):
    journal = scrapy.Field()
    year = scrapy.Field(
        input_processor=MapCompose(
            lambda x: re.search(r'\d{4}', x).group()
        )
    )
    file_name = scrapy.Field(
        input_processor=MapCompose(
            lambda x: x.strip(),
            prevent_spec_chars,
        )
    )
    files = scrapy.Field()
    file_urls = scrapy.Field(
        output_processor=Identity()
    )
    issue = scrapy.Field(
        input_processor=MapCompose(
            lambda x: x.strip(),
            lambda x: x.replace('Most Recent Issue: ', ''),
            lambda x: x.replace(',', '').replace(':', ''),
        )
    )
    cookies = scrapy.Field(
        output_processor=Identity(),
    )


class TaylorItem(scrapy.Item):
    journal_name = scrapy.Field()
    volume_title = scrapy.Field()
    issue_number = scrapy.Field()
    article_title = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()


class WileyItem(scrapy.Item):
    journal_name = scrapy.Field()
    volume_title = scrapy.Field()
    issue_number = scrapy.Field()
    article_title = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
