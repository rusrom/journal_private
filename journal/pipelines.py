# -*- coding: utf-8 -*-

from scrapy.http import Request
from scrapy.pipelines.files import FilesPipeline

# class JournalPipeline(object):
#     def process_item(self, item, spider):
#         return item


class JournalPdfPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        return [Request(pdf_url, meta={
            'file_ext': 'txt' if 'risfile' in pdf_url else 'pdf',
            'file_name': item['file_name'],
            'issue': item['issue'],
            'year': item['year'],
            'journal': item['journal'],
        }, cookies=item['cookies'], dont_filter=True) for pdf_url in item.get('file_urls')]

    def file_path(self, request, response=None, info=None):
        return '{journal}/{year}/{issue}/{file_name}.{extension}'.format(
            journal=request.meta['journal'],
            year=request.meta['year'],
            issue=request.meta['issue'],
            file_name=request.meta['file_name'],
            extension=request.meta['file_ext'],
        )


class TaylorPdfPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        return [Request(pdf_url, meta={
            'file_ext': 'pdf',
            'file_name': item['article_title'],
            'issue': item['issue_number'],
            'year': item['volume_title'],
            'journal': item['journal_name'],
        }, dont_filter=True) for pdf_url in item.get('file_urls')]

    def file_path(self, request, response=None, info=None):
        return '{journal}/{year}/{issue}/{file_name}.{extension}'.format(
            journal=request.meta['journal'],
            year=request.meta['year'],
            issue=request.meta['issue'],
            file_name=request.meta['file_name'],
            extension=request.meta['file_ext'],
        )


class WileyPdfPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        return [Request(pdf_url, meta={
            'file_ext': 'pdf',
            'file_name': item['article_title'],
            'issue': item['issue_number'],
            'year': item['volume_title'],
            'journal': item['journal_name'],
        }, dont_filter=True) for pdf_url in item.get('file_urls')]

    def file_path(self, request, response=None, info=None):
        return '{journal}/{year}/{issue}/{file_name}.{extension}'.format(
            journal=request.meta['journal'],
            year=request.meta['year'],
            issue=request.meta['issue'],
            file_name=request.meta['file_name'],
            extension=request.meta['file_ext'],
        )
