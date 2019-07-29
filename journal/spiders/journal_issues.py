# -*- coding: utf-8 -*-
import scrapy
import pickle
import csv
import os.path
import re

from scrapy.http import FormRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from time import sleep
from scrapy.selector import Selector
from scrapy.utils.project import get_project_settings
from journal.items import JournalItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from random import randint


settings = get_project_settings()


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_WITH_URLS = os.path.join(ROOT_DIR, 'journals.csv')
COOKIES_FILE = os.path.join(ROOT_DIR, 'cookies.pkl')

# LOG_FILE_JOURNAL_WITHOUT_ISSUES = os.path.join(ROOT_DIR, 'journals_without_issues.log')
# LOG_FILE_ISSUE_MIX_ARTICLES = os.path.join(ROOT_DIR, 'mix_pdf_and_no_pdf_issues.log')
# LOG_FILE_ISSUE_NO_PDF_ARTICLES = os.path.join(ROOT_DIR, 'no_pdf_issues.log')
# LOG_FILE_ISSUE_NO_ATRICLES = os.path.join(ROOT_DIR, 'no_article_issues.log')
# LOG_FILE_ARTICLE_NO_PDF = os.path.join(ROOT_DIR, 'article_no_pdf_file.log')
# LOG_FILE_ARTICLE_NO_RIS = os.path.join(ROOT_DIR, 'article_no_ris_file.log')

LOG_HEADERS = ['ISSN', 'Journal_Name', 'URL']
LOG_FILE_JOURNAL_WITHOUT_ISSUES = os.path.join(ROOT_DIR, 'log_journals_without_issues.csv')
LOG_FILE_ISSUE_MIX_ARTICLES = os.path.join(ROOT_DIR, 'log_mix_pdf_and_no_pdf_issues.csv')
LOG_FILE_ISSUE_NO_PDF_ARTICLES = os.path.join(ROOT_DIR, 'log_no_pdf_issues.csv')
LOG_FILE_ISSUE_NO_ATRICLES = os.path.join(ROOT_DIR, 'log_no_article_issues.csv')
LOG_FILE_ARTICLE_NO_PDF = os.path.join(ROOT_DIR, 'log_article_no_pdf_file.csv')
LOG_FILE_ARTICLE_NO_RIS = os.path.join(ROOT_DIR, 'log_article_no_ris_file.csv')


def load_cookies():
    if os.path.exists(COOKIES_FILE):
        return pickle.load(open(COOKIES_FILE, 'rb'))
    return False


def save_to_log(file_path, val):
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write('{}\n'.format(val))


def save_to_log_csv(file_path, list_values):
    row = dict(zip(LOG_HEADERS, list_values))
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=LOG_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


class JournalIssuesSpider(scrapy.Spider):
    name = 'journal_issues'
    allowed_domains = ['carleton.ca']

    custom_settings = {
        'FILES_STORE': settings.get('JOURNALS_STORAGE'),
        'ITEM_PIPELINES': {
            'journal.pipelines.JournalPdfPipeline': 300,
        },
    }

    def set_cookies(self):
            self.driver.get('https://www.petsmart.ca/')
            for cookie in self.cookies:
                self.driver.add_cookie(cookie)

    def __init__(self, use_auth=True, limit_years=None, limit_issues=None, min_p=5, max_p=7, *args, **kwargs):
        super(JournalIssuesSpider, self).__init__(*args, **kwargs)
        self.settings = settings
        self.use_auth = use_auth
        self.limit_years = limit_years
        self.limit_issues = limit_issues
        self.min_p = min_p
        self.max_p = max_p
        self.driver = webdriver.Chrome(self.settings.get('CHROME_PATH'))

        if self.use_auth:
            self.cookies = load_cookies()
            if self.cookies:
                self.set_cookies()
        else:
            self.cookies = False

    def login_to_library(self):
        # Check need login or not
        try:
            user = self.driver.find_element_by_xpath('//form[@id="mc1"]/input[@name="user"]')
            need_login = True
        except NoSuchElementException:
            need_login = False

        # Login block
        if need_login:
            user = self.driver.find_element_by_xpath('//form[@id="mc1"]/input[@name="user"]')
            user.send_keys(self.settings.get('CREDENTIALS')['user'])
            sleep(randint(2, 4))

            password = self.driver.find_element_by_xpath('//form[@id="mc1"]/input[@name="pass"]')
            sleep(randint(2, 4))
            password.send_keys(self.settings.get('CREDENTIALS')['pass'])

            login = self.driver.find_element_by_xpath('//form[@id="mc1"]/input[@value="Login"]')
            login.click()
            sleep(randint(2, 4))

    # Getting all start_urls from csv file
    def start_requests(self):
        with open(CSV_FILE_WITH_URLS) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            start_urls = [line['URL'] for line in csv_reader]
        return [scrapy.Request(url, dont_filter=True) for url in start_urls]

    def parse(self, response):
        # Go to journal URL
        self.driver.get(response.url)
        sleep(randint(self.min_p, self.max_p))

        # Login to library and save cookies
        if self.use_auth:
            self.login_to_library()
            self.cookies = self.driver.get_cookies()
            pickle.dump(self.cookies, open(COOKIES_FILE, 'wb'))

        latest_issue = True

        # Getting html markup for Scrapy
        html_code = self.driver.page_source
        art_response = Selector(text=html_code)

        # Get journal ISSN
        issn = art_response.xpath('//div[@id="journal-details"]/div[@class="issn"]/text()').get()
        if issn:
            issn = re.search(r'\d+', issn).group()
        else:
            issn = 'issn not found'

        # Get journal name
        journal_name = art_response.xpath('//div[@id="journal-details"]/h3/text()').get()

        # Get all years divs
        all_years = art_response.xpath('//div[@id="issues"]//div[@class="accordion-group" and descendant::ul]')

        # Log journal without issues
        if not all_years:
            # save_to_log(LOG_FILE_JOURNAL_WITHOUT_ISSUES, response.meta.get('redirect_urls', [response.url])[0])
            save_to_log_csv(LOG_FILE_JOURNAL_WITHOUT_ISSUES, [issn, journal_name, response.meta.get('redirect_urls', [response.url])[0]])
            print('>>>>>>>>>>>>>>> SAVING JOURNAL URL WITH NO CONTENT TO LOG ...')
            print('-----------------------> SKIP JOURNAL <----------------------')
            return False

        # Go through all years
        for year in all_years[:self.limit_years]:
            sleep(randint(self.min_p, self.max_p))

            # Geto all issues links
            all_issue_links = year.xpath('.//li/a/@href').extract()

            # Correction for scraping latest issue
            if latest_issue:
                all_issue_links.insert(0, self.driver.current_url)
                latest_issue = False

            # Go through all issues
            for issue_link in all_issue_links[:self.limit_issues]:
                sleep(randint(self.min_p, self.max_p))

                # Go to issue page
                if issue_link != self.driver.current_url:
                    self.driver.get(issue_link)
                    sleep(5)
                    # html_code = (self.driver.page_source).encode('utf-8')
                    html_code = self.driver.page_source

                    # Save cookies if need auth
                    self.cookies = self.driver.get_cookies()
                    if self.use_auth:
                        pickle.dump(self.cookies, open(COOKIES_FILE, 'wb'))

                    art_response = Selector(text=html_code)

                # Check if PDF Download articles on the page
                check_pdf_links = art_response.xpath('//div[@id="result-list"]/ol[@id="toc"]/li/div[@class="journal-result"]//div[@class="clear links"]//a[contains(text(), "PDF Download")]')
                # Check if NO PDF articles
                check_no_pdf_link = art_response.xpath('//div[@id="result-list"]/ol[@id="toc"]/li/div[@class="journal-result"]//div[@class="clear links"]//a[contains(text(), "Find Full-Text @ My Library")]')

                if check_pdf_links or check_no_pdf_link:

                    # Log URL of the issue that has a MIX of both types of articles
                    if check_pdf_links and check_no_pdf_link:
                        # save_to_log(LOG_FILE_ISSUE_MIX_ARTICLES, self.driver.current_url)
                        save_to_log_csv(LOG_FILE_ISSUE_MIX_ARTICLES, [issn, journal_name, self.driver.current_url])
                        print('>>>>>>>>>>>>>>> SAVING JOURNAL ISSUE URL WITH MIX OF PDF AND NOT PDF TO LOG ...')

                    # Log URL of the issue that has NO PDF articles
                    if not check_pdf_links:
                        # save_to_log(LOG_FILE_ISSUE_NO_PDF_ARTICLES, self.driver.current_url)
                        save_to_log_csv(LOG_FILE_ISSUE_NO_PDF_ARTICLES, [issn, journal_name, self.driver.current_url])
                        print('>>>>>>>>>>>>>>> SAVING JOURNAL ISSUE URL WITH NO PDF TO LOG ...')

                    articles = art_response.xpath('//div[@id="result-list"]/ol[@id="toc"]/li[contains(@class, "journal-item row-")]/div[@class="journal-result"]')
                    for article in articles:
                        l = ItemLoader(item=JournalItem(), selector=article)
                        l.default_output_processor = TakeFirst()

                        l.add_value('journal', art_response.xpath('//div[@id="journal-details"]/h3/text()').extract())
                        l.add_xpath('file_name', './/h4//span[@class="article-title"]/text()')
                        l.add_xpath('journal', '//div[@id="journal-details"]/h3/text()')
                        l.add_value('issue', art_response.xpath('//div[@id="journal-details"]/following-sibling::div[@style]//span/text()').extract())
                        l.add_value('year', art_response.xpath('//div[@id="journal-details"]/following-sibling::div[@style]//span/text()').extract())

                        # Detail page URL
                        detail_article_url = article.xpath('.//h4//a/@href').get()
                        sleep(randint(2, 4))

                        # Go to detailed page
                        self.driver.get(detail_article_url)
                        sleep(5)

                        # Login to library and save cookies
                        if self.use_auth:
                            self.login_to_library()
                            self.cookies = self.driver.get_cookies()
                            pickle.dump(self.cookies, open(COOKIES_FILE, 'wb'))

                        l.add_value('cookies', self.driver.get_cookies())

                        # Get link to PDF document
                        try:
                            pdf_url = self.driver.find_element_by_xpath('//div[@class="download-btn"]/a[contains(., "PDF Download")]').get_attribute('href')
                            l.add_value('file_urls', pdf_url)
                        except NoSuchElementException:
                            # Log no PDF file for article
                            # save_to_log(LOG_FILE_ARTICLE_NO_PDF, self.driver.current_url)
                            save_to_log_csv(LOG_FILE_ARTICLE_NO_PDF, [issn, journal_name, self.driver.current_url])
                            print('>>>>>>>>>>>>>>> SAVING URL ARTICLE WITH NO PDF FILE FOR DOWNLOAD TO LOG ...')

                        # Get link to RIS file
                        try:
                            ris_url = self.driver.find_element_by_xpath('//li/a[contains(text(), "RIS (EndNote)")]').get_attribute('href')
                            l.add_value('file_urls', ris_url)
                        except NoSuchElementException:
                            # Log no RIS file for article
                            # save_to_log(LOG_FILE_ARTICLE_NO_RIS, self.driver.current_url)
                            save_to_log_csv(LOG_FILE_ARTICLE_NO_RIS, [issn, journal_name, self.driver.current_url])
                            print('>>>>>>>>>>>>>>> SAVING URL ARTICLE WITH NO RIS FILE FOR DOWNLOAD TO LOG ...')

                        # yield scrapy.Request(url=detail_article_url, callback=self.datailed_page, meta={'item': item}, dont_filter=True)
                        yield l.load_item()
                else:
                    # Log URL of the issue that has no articles
                    # save_to_log(LOG_FILE_ISSUE_NO_ATRICLES, self.driver.current_url)
                    save_to_log_csv(LOG_FILE_ISSUE_NO_ATRICLES, [issn, journal_name, self.driver.current_url])
                    print('>>>>>>>>>>>>>>> SAVING JOURNAL ISSUE URL WITHOUT ATRICLES TO LOG ...')
                    print('-------------------------> GO TO NEXT JOURNAL ISSUE <--------------------------')
                    continue

    def close(self, reason):
        self.driver.close()
