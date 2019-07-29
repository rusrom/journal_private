# -*- coding: utf-8 -*-
import scrapy
import csv
import os.path
import re

from scrapy import FormRequest
from scrapy.utils.project import get_project_settings
from w3lib.html import replace_escape_chars, replace_entities
from scrapy.utils.response import open_in_browser
from time import sleep
from random import randint


# User configuration parameters
LIMIT_DISCIPLINE_JOURNALS = None
# MIN_PAUSE_SECONDS = 1
# MAX_PAUSE_SECONDS = 2

# Spider settings
settings = get_project_settings()

# Spider folders
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_WITH_URLS = os.path.join(ROOT_DIR, 'discipline_input.csv')

# Log configuration
LOG_HEADERS = ['Journal_Name', 'URL']
LOG_FOLDER = os.path.join(ROOT_DIR, 'logs')
LOG_FILE_NO_CONTENT = os.path.join(LOG_FOLDER, 'taylor_francis_discipline_no_content.csv')


def remove_garbage(val):
    val = replace_escape_chars(val)
    val = replace_entities(val)
    val = re.sub(r'\s{2,}', ' ', val)
    return val.strip()


class TaylorFrancisScrapeDisciplineSpider(scrapy.Spider):
    name = 'taylor_francis_scrape_discipline'
    # allowed_domains = ['carleton.ca']

    custom_settings = {
        'FEED_EXPORT_FIELDS': ['Discipline_Tree', 'Discipline_Name', 'Journal_Name', 'Publisher', 'Journal_History', 'Print_ISSN', 'Online_ISSN', 'Journal_URL', 'Abstract'],
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

    # def __init__(self, limit_discipline_journals=LIMIT_DISCIPLINE_JOURNALS, min_p=MIN_PAUSE_SECONDS, max_p=MAX_PAUSE_SECONDS, *args, **kwargs):
    def __init__(self, limit_discipline_journals=LIMIT_DISCIPLINE_JOURNALS, *args, **kwargs):
        super(TaylorFrancisScrapeDisciplineSpider, self).__init__(*args, **kwargs)
        self.settings = settings
        self.limit_discipline_journals = limit_discipline_journals
        # self.min_p = min_p
        # self.max_p = max_p
        self.login_form_xpath = '//form[@id="mc1" and @action="/login"]'
        self.init_folders_path(LOG_FOLDER)

    def start_requests(self):
        with open(CSV_FILE_WITH_URLS) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_lines = [{
                'Discipline URL': line['Discipline URL'],
                'Discipline_Tree': line['Discipline_Tree'],
                'Discipline_Name': line['Discipline_Name'],
            } for line in csv_reader]

        yield scrapy.Request(csv_lines[0]['Discipline URL'], dont_filter=True, meta={'csv_lines': csv_lines}, callback=self.login_to_library)

    def login_to_library(self, response):
        csv_lines = response.meta['csv_lines']

        login_form = response.xpath(self.login_form_xpath)
        if login_form:
            credentials = {
                'user': self.settings['CREDENTIALS']['user'],
                'pass': self.settings['CREDENTIALS']['pass'],
            }
            yield FormRequest.from_response(response, formxpath=self.login_form_xpath, formdata=credentials, meta={'csv_lines': csv_lines}, dont_filter=True, callback=self.parse_csv_lines)
        else:
            # If no login required
            yield scrapy.Request(response.url, dont_filter=True, meta={'csv_lines': csv_lines}, callback=self.parse_csv_lines)

    def parse_csv_lines(self, response):
        for line in response.meta['csv_lines']:
            discipline_url = line['Discipline URL'] + '&pageSize=1000'
            meta = {
                'Discipline_Tree': line['Discipline_Tree'],
                'Discipline_Name': line['Discipline_Name']
            }

            yield scrapy.Request(discipline_url, dont_filter=True, callback=self.parse_discipline, meta=meta)

    def parse_discipline(self, response):
        meta = {
            'Discipline_Tree': response.meta['Discipline_Tree'],
            'Discipline_Name': response.meta['Discipline_Name'],
        }

        journals = response.xpath('//article//h4[contains(@class, "art_title")]/a')

        # Log if discipline search is empty
        if not journals:
            self.save_to_log_csv(LOG_FILE_NO_CONTENT, [meta['Discipline_Name'], response.meta.get('redirect_urls', [response.url])[0]])
            print('>>>>>>>>>>>>>>> SAVING DISCIPLINE WITH NO CONTENT TO LOG ...')
            return False

        for journal in journals[:self.limit_discipline_journals]:
            # Pause between request to journals
            # sleep(randint(self.min_p, self.max_p))

            meta['Journal_Name'] = journal.xpath('./text()').get()
            journal_href = journal.xpath('./@href').get()
            yield response.follow(journal_href, dont_filter=True, meta=meta, callback=self.parse_journal)

    def parse_journal(self, response):
        meta = {
            'Discipline_Tree': response.meta['Discipline_Tree'],
            'Discipline_Name': response.meta['Discipline_Name'],
            'Journal_Name': response.meta['Journal_Name'],
            'Journal_URL': response.url,
            'Publisher': 'Taylor and Francis',
        }

        journal_information_href = response.xpath('//ul[@role="menulist"]//a[contains(., "Journal information")]/@href').get()
        if journal_information_href:
            journal_information_href = response.urljoin(journal_information_href)

        aims_and_scope_url = response.xpath('//ul[@role="menulist"]//a[contains(., "Aims and scope")]/@href').get()
        if aims_and_scope_url:
            aims_and_scope_url = response.urljoin(aims_and_scope_url)

        if journal_information_href:
            if aims_and_scope_url:
                meta['aims_and_scope_url'] = aims_and_scope_url
            yield response.follow(journal_information_href, dont_filter=True, meta=meta, callback=self.parse_journal_information)

        elif aims_and_scope_url:
            yield response.follow(aims_and_scope_url, dont_filter=True, meta=meta, callback=self.parse_aims_and_scope)

        else:
            # Write all available information abou journal
            yield meta

    def parse_journal_information(self, response):
        meta = {
            'Discipline_Tree': response.meta['Discipline_Tree'],
            'Discipline_Name': response.meta['Discipline_Name'],
            'Journal_Name': response.meta['Journal_Name'],
            'Journal_URL': response.meta['Journal_URL'],
            'Publisher': response.meta['Publisher'],
            # 'aims_and_scope_url': response.meta['aims_and_scope_url'],
        }

        print_issn = response.xpath('//span[contains(text(), "Print ISSN:")]/following-sibling::text()').get()
        if print_issn:
            print_issn = print_issn.strip()
            meta['Print_ISSN'] = print_issn

        online_issn = response.xpath('//span[contains(text(), "Online ISSN:")]/following-sibling::text()').get()
        if online_issn:
            online_issn = online_issn.strip()
            meta['Online_ISSN'] = online_issn

        curently_known = response.xpath('string(//h3[contains(., "Currently known as:")]/following-sibling::ul[1]/li)').getall()
        if curently_known:
            curently_known = [remove_garbage(val) for val in curently_known if val.strip()]

        formerly_known = response.xpath('string(//h3[contains(., "Formerly known as")]/following-sibling::ul[1]/li)').getall()
        if formerly_known:
            formerly_known = [remove_garbage(val) for val in formerly_known if val.strip()]

        journal_history = '\n'.join(curently_known + formerly_known)
        if journal_history:
            meta['Journal_History'] = journal_history

        if response.meta.get('aims_and_scope_url'):
            yield response.follow(response.meta['aims_and_scope_url'], dont_filter=True, meta=meta, callback=self.parse_aims_and_scope)
        else:
            # Write all available information abou journal
            yield meta

    def parse_aims_and_scope(self, response):
        meta = {
            'Discipline_Tree': response.meta.get('Discipline_Tree'),
            'Discipline_Name': response.meta.get('Discipline_Name'),
            'Journal_Name': response.meta.get('Journal_Name'),
            'Journal_URL': response.meta.get('Journal_URL'),
            'Publisher': response.meta.get('Publisher'),
            'Print_ISSN': response.meta.get('Print_ISSN'),
            'Online_ISSN': response.meta.get('Online_ISSN'),
            'Journal_History': response.meta.get('Journal_History'),
        }

        aim_and_scope_lines = response.xpath('//h1[contains(text(), "Aims and scope")]/following-sibling::div[1]/*')
        if aim_and_scope_lines:
            aim_and_scope_text = [line.xpath('string(.)').get() for line in aim_and_scope_lines]
            aim_and_scope_text = [el.strip() for el in aim_and_scope_text if el.strip()]
            meta['Abstract'] = '\n'.join(aim_and_scope_text)

        yield meta
