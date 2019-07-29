# -*- coding: utf-8 -*-
import scrapy
import csv
import os.path
import re

from scrapy import FormRequest
from scrapy.utils.project import get_project_settings
from scrapy.utils.response import open_in_browser
from time import sleep
from random import randint


# Spider folders
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# User configuration parameters
LIMIT_DISCIPLINE_JOURNALS = None
MIN_PAUSE_SECONDS = 1
MAX_PAUSE_SECONDS = 2
CSV_FILE_WITH_URLS = os.path.join(ROOT_DIR, 'discipline_input_wiley.csv')

# Log configuration
LOG_HEADERS = ['Journal_Name', 'URL']
LOG_FOLDER = os.path.join(ROOT_DIR, 'logs')
LOG_FILE_NO_CONTENT = os.path.join(LOG_FOLDER, 'taylor_francis_discipline_no_content.csv')

# Spider settings
settings = get_project_settings()


class WileyScrapeDiscilineSpider(scrapy.Spider):
    name = 'wiley_scrape_disciline'
    custom_settings = {
        'FEED_EXPORT_FIELDS': [
            'Discipline_Tree',
            'Discipline_Name',
            'Journal_Name',
            'Currently_known_as',
            'Publisher',
            'Impact_Factor',
            'ISI_Journal_Citation_Reports_and_Rankings',
            'Journal_URL',
            'Online_ISSN',
            'Start_Year',
            'Latest_Year',
        ],
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

    # def __init__(self, limit_discipline_journals=LIMIT_DISCIPLINE_JOURNALS, *args, **kwargs):
    def __init__(self, *args, **kwargs):
        super(WileyScrapeDiscilineSpider, self).__init__(*args, **kwargs)
        self.settings = settings
        # self.limit_discipline_journals = limit_discipline_journals
        self.limit_discipline_journals = LIMIT_DISCIPLINE_JOURNALS
        self.min_p = MIN_PAUSE_SECONDS
        self.max_p = MAX_PAUSE_SECONDS
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
        # open_in_browser(response)
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
        # open_in_browser(response)
        for line in response.meta['csv_lines']:
            discipline_url = line['Discipline URL']
            meta = {
                'Discipline_Tree': line['Discipline_Tree'],
                'Discipline_Name': line['Discipline_Name']
            }

            yield scrapy.Request(discipline_url, dont_filter=True, callback=self.parse_discipline, meta=meta)

    def parse_discipline(self, response):
        # open_in_browser(response)
        meta = {
            'Discipline_Tree': response.meta['Discipline_Tree'],
            'Discipline_Name': response.meta['Discipline_Name'],
        }

        journals = response.xpath('//li[contains(@class, "search__item")]')

        # Log if discipline search is empty
        if not journals:
            self.save_to_log_csv(LOG_FILE_NO_CONTENT, [meta['Discipline_Name'], response.meta.get('redirect_urls', [response.url])[0]])
            print('>>>>>>>>>>>>>>> SAVING DISCIPLINE WITH NO CONTENT TO LOG ...')
            return False

        for journal in journals[:self.limit_discipline_journals]:
            # Pause between request to journals
            sleep(randint(self.min_p, self.max_p))

            currently_known_as = journal.xpath('.//span[contains(@class, "meta__title__currentVersion")]/a')
            if currently_known_as:
                meta['Currently_known_as'] = currently_known_as.xpath('string(.)').get()
                journal_name = journal.xpath('string(.//h3)')
                if journal_name:
                    meta['Journal_Name'] = journal_name.get()
                journal_href = currently_known_as.attrib['href']
            else:
                journal_name = journal.xpath('.//h3/a')
                meta['Currently_known_as'] = None
                meta['Journal_Name'] = journal_name.xpath('string(.)').get()
                journal_href = journal_name.attrib['href']

            start_year = journal.xpath('.//a[@class="meta__date"][1]/text()')
            latest_year = journal.xpath('.//a[@class="meta__date"][2]/text()')
            meta['Start_Year'] = start_year.get() if start_year else None
            meta['Latest_Year'] = latest_year.get() if latest_year else None

            yield response.follow(journal_href, dont_filter=True, meta=meta, callback=self.parse_journal)

        # Pagination: Next page
        next_page = response.xpath('//div[@class="pagination"]/span/a[@title="Next page"]')
        if next_page:
            yield response.follow(url=next_page.attrib['href'], callback=self.parse_discipline)

    def parse_journal(self, response):
        meta = {
            'Discipline_Tree': response.meta['Discipline_Tree'],
            'Discipline_Name': response.meta['Discipline_Name'],
            'Journal_Name': response.meta['Journal_Name'],
            'Currently_known_as': response.meta['Currently_known_as'],
            'Journal_URL': response.url,
            'Publisher': 'Wiley',
            'Start_Year': response.meta['Start_Year'],
            'Latest_Year': response.meta['Latest_Year'],
        }

        impact_factor = response.xpath('//span[contains(text(), "Impact factor:")]//following-sibling::span[1]/text()')
        if impact_factor:
            meta['Impact_Factor'] = impact_factor.get()

        isi_ranking = response.xpath('//span[contains(text(), "ISI Journal Citation")]//following-sibling::span[1]/text()')
        if isi_ranking:
            meta['ISI_Journal_Citation_Reports_and_Rankings'] = '\n'.join(isi_ranking.getall())

        online_issn = response.xpath('//span[contains(text(), "Online ISSN:")]//following-sibling::span[1]/text()')
        if online_issn:
            meta['Online_ISSN'] = online_issn.get()

        yield meta
