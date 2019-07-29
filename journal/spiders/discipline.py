# -*- coding: utf-8 -*-
import scrapy
import pickle
import csv
import os.path
import re
import glob

from selenium import webdriver
from time import sleep
from selenium.common.exceptions import NoSuchElementException
from random import randint
from scrapy.selector import Selector
from scrapy.utils.project import get_project_settings


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_WITH_URLS = os.path.join(ROOT_DIR, 'discipline_input.csv')
COOKIES_FILE = os.path.join(ROOT_DIR, 'cookies.pkl')
LOG_FILE_NO_CONTENT = os.path.join(ROOT_DIR, 'no_discipline_content.log')


def load_cookies():
    if os.path.exists(COOKIES_FILE):
        return pickle.load(open(COOKIES_FILE, 'rb'))
    return False


def save_to_log(file_path, val):
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write('{}\n'.format(val))


class DisciplineSpider(scrapy.Spider):
    name = 'discipline'
    allowed_domains = ['carleton.ca']

    def set_cookies(self):
            self.driver.get('https://www.petsmart.ca/')
            for cookie in self.cookies:
                self.driver.add_cookie(cookie)

    def __init__(self, use_auth=True, *args, **kwargs):
        super(DisciplineSpider, self).__init__(*args, **kwargs)
        self.settings = get_project_settings()
        self.use_auth = use_auth
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

    def start_requests(self):
        with open(CSV_FILE_WITH_URLS) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            start_urls = [line for line in csv_reader]
        return [scrapy.Request(url['Discipline URL'], meta={
            'Discipline Tree': url['Discipline Tree'],
            'Discipline Name': url['Discipline Name']
        }, dont_filter=True) for url in start_urls]

    def parse(self, response):
        discipline_tree = response.meta['Discipline Tree']
        discipline_name = response.meta['Discipline Name']

        self.driver.get(response.url)
        sleep(5)

        if self.use_auth:
            self.login_to_library()

        # Save cookies
        if self.use_auth:
            self.cookies = self.driver.get_cookies()
            pickle.dump(self.cookies, open(COOKIES_FILE, 'wb'))

        # Get journals elements
        journals = self.driver.find_elements_by_xpath('//li[div[@class="journal"]]//div[@class="title"]/a')

        # Write to log if no content inside journal
        if not journals:
            save_to_log(LOG_FILE_NO_CONTENT, self.driver.current_url)
            # save_to_log(LOG_FILE_NO_CONTENT, response.meta.get('redirect_urls', [response.url])[0])
            print('>>>>>>>>>>>>>>> SAVING JOURNAL URL WITH NO CONTENT TO LOG ...')
            print('---------------------> SKIP JOURNAL <---------------------')
            return False

        journals_urls = [url.get_attribute("href") for url in journals]
        for journal_url in journals_urls:
            sleep(randint(3, 5))
            item = {}
            self.driver.get(journal_url)

            html_code = (self.driver.page_source).encode('utf-8')
            journal_markup = Selector(text=html_code)

            item['Discipline Tree'] = discipline_tree
            item['Discipline Name'] = discipline_name
            item['Journal Name'] = journal_markup.xpath('//h3/text()').get('')

            issn = journal_markup.xpath('//div[@id="journal-details"]/div[@class="issn"]/text()').get('')
            item['ISSN'] = re.sub(r'ISSN[^\d]+', '', issn)
            item['Publisher'] = journal_markup.xpath('//div[@id="journal-details"]/div[@class="publisher"]/a/text()').get('')

            coverage = journal_markup.xpath('string(//div[@id="journal-details"]/div[@class="coverage"])').get('')
            if coverage:
                coverage_years = re.findall(r'\d+', coverage)
                if len(coverage_years) == 2:
                    year_from, year_to = coverage_years
                elif len(coverage_years) == 1:
                    year_from = coverage_years[0]
                    year_to = ''
            else:
                year_from = ''
                year_to = ''
            item['Year From'] = year_from
            item['Year To'] = year_to

            most_recent = journal_markup.xpath('string(//h4[contains(string(), "Most Recent Issue:")])').get('')
            if most_recent:
                most_recent = most_recent.replace('Most Recent Issue:', '').strip()
            item['Most Recent Issue'] = most_recent

            abstract = journal_markup.xpath('string(//div[@id="journal-details"]/div[@class="description"])').get('')
            if abstract:
                abstract = re.sub(r'\s{2,}', ' ', abstract)
                abstract = abstract.strip()
            item['Abstract'] = abstract

            extra_info = journal_markup.xpath('//h3/following-sibling::div[@class="linked-title"]')
            data = []
            for info_line in extra_info:
                res = info_line.xpath('.//text()').getall()
                if res:
                    res = ' '.join(res)
                    data.append(res.strip())
            if data:
                data = list(map(lambda x: re.sub(r'\s{2,}', ' ', x), data))
                data = '\n'.join(data)
            else:
                data = ''
            item['Formerly known as'] = data

            item['Journal URL'] = self.driver.current_url

            yield item

    def close(self, reason):
        self.driver.close()

        current_file = max(glob.iglob('*.csv'), key=os.path.getctime)

        with open(current_file, encoding='utf-8') as f:
            reader = csv.reader(f)
            good_lines = [line for line in reader if line]

        with open(current_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            for line in good_lines:
                writer.writerow(line)
