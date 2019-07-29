# -*- coding: utf-8 -*-
import scrapy
import csv
import os.path
import re

from scrapy import FormRequest
from scrapy.utils.project import get_project_settings
from w3lib.html import replace_escape_chars, replace_entities
from scrapy.utils.response import open_in_browser
from journal.items import TaylorItem
from time import sleep
from random import randint


# User configuration parameters
LIMIT_YEARS = 3  # None - all years
LIMIT_ISSUES = 2  # None - all issues
LIMIT_ARTICLES = None  # None - all articles
MIN_PAUSE_SECONDS = 5
MAX_PAUSE_SECONDS = 7

# Spider settings
settings = get_project_settings()

# Spider folders
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_WITH_URLS = os.path.join(ROOT_DIR, 'journals.csv')

# Log configuration
LOG_HEADERS = ['Journal_Name', 'URL']
LOG_FOLDER = os.path.join(ROOT_DIR, 'logs')

# Path to log files
LOG_FILE_JOURNAL_WITHOUT_ISSUES = os.path.join(LOG_FOLDER, 'log_journals_without_issues.csv')
# LOG_FILE_ISSUE_MIX_ARTICLES = os.path.join(LOG_FOLDER, 'log_mix_pdf_and_no_pdf_issues.csv')
# LOG_FILE_ISSUE_NO_PDF_ARTICLES = os.path.join(LOG_FOLDER, 'log_no_pdf_issues.csv')
LOG_FILE_ISSUE_NO_ATRICLES = os.path.join(LOG_FOLDER, 'log_no_article_issues.csv')
LOG_FILE_ARTICLE_NO_PDF = os.path.join(LOG_FOLDER, 'log_article_no_pdf_file.csv')
LOG_FILE_ARTICLE_NO_RIS = os.path.join(LOG_FOLDER, 'log_article_no_ris_file.csv')


def remove_garbage(val):
    val = replace_escape_chars(val)
    val = replace_entities(val)
    val = re.sub(r'\s{2,}', ' ', val)
    return val.strip()


def prevent_spec_chars(some_string):
    danger_chars = r'\/:*?"<>|'
    for char in danger_chars:
        some_string = some_string.replace(char, '')
    return some_string


class TaylorFrancisDownloadAuthSpider(scrapy.Spider):
    name = 'taylor_francis_download_auth'
    # allowed_domains = ['carleton.ca']
    # start_urls = ['https://www-tandfonline-com.proxy.library.carleton.ca/loi/calr20']

    custom_settings = {
        'FILES_STORE': settings.get('JOURNALS_STORAGE'),
        'ITEM_PIPELINES': {
            'journal.pipelines.TaylorPdfPipeline': 300,
        },
    }

    def init_folders_path(self, folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def save_to_log_csv(self, file_path, list_values):
        row = dict(zip(LOG_HEADERS, list_values))
        file_exists = os.path.exists(file_path)

        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=LOG_HEADERS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    def save_ris_file(self, response, meta):
        storage = self.settings['JOURNALS_STORAGE']
        journal_name = meta['journal_name']
        volume_title = meta['volume_title']
        issue_number = meta['issue_number']
        article_title = meta['article_title']

        storage_path = os.path.join(storage, journal_name, volume_title, issue_number)
        ris_file = article_title + '.txt'
        ris_file_path = os.path.join(storage_path, ris_file)

        self.init_folders_path(storage_path)
        with open(ris_file_path, 'w', encoding='utf-8') as f:
            f.write(response.text)

    def __init__(self, limit_years=LIMIT_YEARS, limit_issues=LIMIT_ISSUES, limit_articles=LIMIT_ARTICLES, min_p=MIN_PAUSE_SECONDS, max_p=MAX_PAUSE_SECONDS, *args, **kwargs):
        super(TaylorFrancisDownloadAuthSpider, self).__init__(*args, **kwargs)
        self.settings = settings
        self.limit_years = limit_years
        self.limit_issues = limit_issues
        self.limit_articles = limit_articles
        self.min_p = min_p
        self.max_p = max_p
        self.login_form_xpath = '//form[@id="mc1" and @action="/login"]'
        self.init_folders_path(LOG_FOLDER)

    # Getting all start_urls from csv file
    # def start_requests(self):
    #     with open(CSV_FILE_WITH_URLS) as csv_file:
    #         csv_reader = csv.DictReader(csv_file)
    #         start_urls = [line['Journal_URL'] for line in csv_reader]
    #     return [scrapy.Request(url, dont_filter=True) for url in start_urls]

    # Getting all start_urls from csv file
    def start_requests(self):
        with open(CSV_FILE_WITH_URLS) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            start_urls = [line['Journal_URL'] for line in csv_reader]
        yield scrapy.Request(start_urls[0], dont_filter=True, meta={'start_urls': start_urls}, callback=self.login_to_library)

    def login_to_library(self, response):
        start_urls = response.meta['start_urls']
        login_form = response.xpath(self.login_form_xpath)
        if login_form:
            credentials = {
                'user': self.settings['CREDENTIALS']['user'],
                'pass': self.settings['CREDENTIALS']['pass'],
            }
            yield FormRequest.from_response(response, formxpath=self.login_form_xpath, formdata=credentials, meta={'start_urls': start_urls}, dont_filter=True, callback=self.parse_journals)
        else:
            # If no login required
            yield scrapy.Request(response.url, dont_filter=True, meta={'start_urls': start_urls}, callback=self.parse_journals)

    def parse_journals(self, response):
        for journal_url in response.meta['start_urls']:
            # Pause between request to journals
            sleep(randint(self.min_p, self.max_p))
            yield scrapy.Request(journal_url, dont_filter=True, callback=self.parse_journal)

    def parse_journal(self, response):
        # journal_name = response.xpath('//title/text()').get()
        meta = {
            'journal_name': response.xpath('//title/text()').get(),
        }

        volumes = response.xpath('//ul[@class="list-of-issues"]/li[@class="vol_li "]/a')

        # Log journal without content
        if not volumes:
            self.save_to_log_csv(LOG_FILE_JOURNAL_WITHOUT_ISSUES, [meta['journal_name'], response.meta.get('redirect_urls', [response.url])[0]])
            print('>>>>>>>>>>>>>>> SAVING JOURNAL WITH NO CONTENT TO LOG ...')
            return False

        for volume in volumes[:self.limit_years]:
            volume_title = volume.xpath('./h3/text()').get()
            meta['volume_title'] = remove_garbage(volume_title)
            yield response.follow(volume, callback=self.parse_volume, dont_filter=True, meta=meta)

    def parse_volume(self, response):
        meta = {
            'journal_name': response.meta['journal_name'],
            'volume_title': response.meta['volume_title'],
        }

        issues = response.xpath('//li[@class="vol_li active"]/ul/li/a')

        # Log journal without issues
        if not issues:
            self.save_to_log_csv(LOG_FILE_JOURNAL_WITHOUT_ISSUES, [meta['journal_name'], response.url])
            print('>>>>>>>>>>>>>>> SAVING JOURNAL WITHOUT ISSUES TO LOG ...')
            return False

        for issue in issues[:self.limit_issues]:
            # Pause between issues
            sleep(randint(self.min_p, self.max_p))
            meta['issue_number'] = issue.xpath('./div[contains(@class, "issue-num")]/text()').get()
            yield response.follow(issue, callback=self.parse_issue, dont_filter=True, meta=meta)

    def parse_issue(self, response):
        meta = {
            'journal_name': response.meta['journal_name'],
            'volume_title': response.meta['volume_title'],
            'issue_number': response.meta['issue_number'],
        }

        articles = response.xpath('//table[@class="articleEntry"]//div[@class="art_title linkable"]/a')

        # Log issue without articles
        if not articles:
            self.save_to_log_csv(LOG_FILE_ISSUE_NO_ATRICLES, [meta['journal_name'], response.url])
            print('>>>>>>>>>>>>>>> SAVING ISSUE WITHOUT ARTICLES TO LOG ...')
            return False

        for article in articles[:self.limit_articles]:
            # Pause between requsts to articles
            sleep(randint(self.min_p, self.max_p))
            meta['article_title'] = article.xpath('./span/text()').get()
            yield response.follow(article, callback=self.parse_article, dont_filter=True, meta=meta)

    def parse_article(self, response):
        meta = {
            'journal_name': response.meta['journal_name'],
            'volume_title': response.meta['volume_title'],
            'issue_number': response.meta['issue_number'],
            'article_title': response.meta['article_title'],
        }

        pdf_url = response.xpath('//a[@class="show-pdf"]/@href').get()

        # Log article without pdf
        if not pdf_url:
            self.save_to_log_csv(LOG_FILE_ARTICLE_NO_PDF, [meta['journal_name'], response.url])
            print('>>>>>>>>>>>>>>> SAVING ARTICLE WITHOUT PDF TO LOG ...')

        meta['pdf_url'] = response.urljoin(pdf_url)
        meta['article_url'] = response.url

        citation_url = response.xpath('//li[@class="downloadCitations"]/a/@href').get()

        # Log article without ris
        if not citation_url:
            self.save_to_log_csv(LOG_FILE_ARTICLE_NO_RIS, [meta['journal_name'], response.url])
            print('>>>>>>>>>>>>>>> SAVING ARTICLE WITHOUT RIS FILE TO LOG ...')

            # Check ability to download PDF
            if meta['pdf_url']:
                item = TaylorItem()
                item['journal_name'] = prevent_spec_chars(response.meta['journal_name'])
                item['volume_title'] = prevent_spec_chars(response.meta['volume_title'])
                item['issue_number'] = prevent_spec_chars(response.meta['issue_number'])
                item['article_title'] = prevent_spec_chars(response.meta['article_title'])
                item['file_urls'] = [response.meta['pdf_url']]
                yield item
            else:
                # No PDF and RIS for downloading
                return False

        yield response.follow(citation_url, callback=self.parse_citation, dont_filter=True, meta=meta)

    def parse_citation(self, response):
        meta = {
            'journal_name': response.meta['journal_name'],
            'volume_title': response.meta['volume_title'],
            'issue_number': response.meta['issue_number'],
            'article_title': response.meta['article_title'],
            'pdf_url': response.meta['pdf_url'],
            'article_url': response.meta['article_url'],
        }

        formdata = {
            'include': 'abs',
        }
        yield FormRequest.from_response(response, formxpath='//form[@action="/action/downloadCitation"]', formdata=formdata, dont_filter=True, meta=meta, callback=self.save_data)

    def save_data(self, response):
        # meta = {
        #     'journal_name': response.meta['journal_name'],
        #     'volume_title': response.meta['volume_title'],
        #     'issue_number': response.meta['issue_number'],
        #     'article_title': response.meta['article_title'],
        #     'pdf_url': response.meta['pdf_url'],
        # }

        item = TaylorItem()
        item['journal_name'] = prevent_spec_chars(response.meta['journal_name'])
        item['volume_title'] = prevent_spec_chars(response.meta['volume_title'])
        item['issue_number'] = prevent_spec_chars(response.meta['issue_number'])
        item['article_title'] = prevent_spec_chars(response.meta['article_title'])
        item['file_urls'] = [response.meta['pdf_url']]

        # self.save_ris_file(response, meta)
        if response.status == 200:
            self.save_ris_file(response, item)
        else:
            # Log article without ris
            self.save_to_log_csv(LOG_FILE_ARTICLE_NO_RIS, [item['journal_name'], response.meta['article_url']])
            print('>>>>>>>>>>>>>>> SAVING ARTICLE WITHOUT RIS FILE TO LOG ...')

        # yield meta
        yield item
