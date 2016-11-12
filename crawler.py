#!/usr/bin/env python2.7


import argparse
import codecs
import HTMLParser 
import json
import logging
import os
import os.path
import requests
import time
import urlparse


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(message)s')
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("crawler").setLevel(logging.DEBUG)
logger = logging.getLogger('crawler')

RESULT_DIR = 'data'


def save_file(file_name, content):
    file_path = os.path.join(RESULT_DIR, file_name)
    temp_file_path = file_path + '.temp'
    with codecs.open(temp_file_path, 'w', 'utf-8') as file:
        file.write(content)
    os.rename(temp_file_path, file_path)

def load_file(file_name):
    file_path = os.path.join(RESULT_DIR, file_name)
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return file.read().decode('utf-8')
    else:
        return None


class Storage(object):
    FILE_NAME = 'index.txt'

    def __init__(self):
        self.valid = dict()
        self.wrong = dict()
        self.next = 1
        self.saved = 0

    @property
    def file_name(self):
        return 'url-map.txt'

    def check_before_add(self, url):
        if self.get_valid(url):
            raise RuntimeError('url {} already in valid'.format(url))
        if self.get_wrong(url):
            raise RuntimeError('url {} already in wrong'.format(url))

    def add_valid(self, url, body):
        self.check_before_add(url)
        file_name = '{}.page'.format(self.next)

        logger.info("save %s to %s", url, file_name)
        save_file(file_name, body)

        self.valid[url] = file_name
        self.next += 1
        self.save_if_required()

    def add_wrong(self, url, code=None, content_type=None):
        self.check_before_add(url)
        self.wrong[url] = dict(code=code, content_type=content_type)
        self.save_if_required()

    def get_valid(self, url):
        file_name = self.valid.get(url)
        if file_name:
            return load_file(file_name)

    def get_wrong(self, url):
        return self.wrong.get(url)

    def save_if_required(self):
        now = int(time.time())
        if self.saved < now:
            self.save()
            self.saved = now

    def save(self):
        logger.info('Storage.save start')
        data = dict(valid=self.valid, wrong=self.wrong, next=self.next)
        data = json.dumps(data)
        save_file(self.file_name, data)
        self.saved = int(time.time())
        logger.info('Storage.save done')

    def load(self):
        logger.info('Storage.load start %s', self.file_name)
        data = load_file(self.file_name)
        if data is None:
            logger.info('Storage.load ommitted %s', self.file_path)
            return
        data = json.loads(data)
        self.valid = data.get('valid', {})
        self.wrong = data.get('wrong', {})
        self.next = data.get('next', 1)
        logger.info('Storage.load done %s', self.file_name)


class Scheduler(object):
    def __init__(self):
        self.set = set()
        self.list = []

    def add(self, url):
        if url in self.set:
            return
        self.set.add(url)
        self.list.append(url)

    def get(self):
        if not self.list:
            return
        result = self.list[0]
        self.list = self.list[1:]
        return result


class LinkExtractor(HTMLParser.HTMLParser):
    CATCH = {
        'a': 'href',
        'link': 'href',
        'script': 'src',
    }
 
    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self.result = []

    def handle_starttag(self, tag, attrs):
        target_attr = self.CATCH.get(tag)
        if target_attr:
            for (attr_name, attr_value) in attrs:
                if attr_name == target_attr:
                    if attr_value:
                        self.result.append(attr_value)


def extract_urls(body):
    parser = LinkExtractor()
    parser.feed(body)
    return parser.result


def valid_content_type(content_type):
    if content_type.startswith('text/html'):
        return True
    if content_type.startswith('text/xml'):
        return True
    if content_type.startswith('application/rss+xml'):
        return True
    return False

class Crawler(object):
    def __init__(self, url, parallel):
        target = urlparse.urlparse(url)
        self.scheme = target.scheme
        self.netloc = target.netloc
        self.storage = Storage()
        self.storage.load()
        self.scheduler = Scheduler()
        self.scheduler.add(url)

    def process(self, url):
        logger.debug("url %s start processing", url)
        wrong = self.storage.get_wrong(url)
        if wrong:
            logger.debug("url %s is wrong (code=%s, content_type",
                url, wrong.get('code'), wrong.get('content_type'))
            return
        body = self.storage.get_valid(url)

        if body:
            logger.debug("url %s found saved", url)
        else:
            logger.debug("url %s request", url)
            response = requests.get(url)
            code = response.status_code
            content_type = response.headers.get('content-type')

            # analyze status code
            if code != 200:
                logger.debug("url %s wrong status code", url, code)
                self.index.add_wrong(url, code=code, content_type=content_type)
                return

            # analyze content-type
            if not valid_content_type(content_type):
                logger.debug("url %s wrong content-type %s", url, content_type)
                self.storage.add_wrong(url, code=code, content_type=content_type)
                return

            # # get body
            body = response.content.decode('utf-8')
            self.storage.add_valid(url, body)

        for url in extract_urls(body):
            parsed_url = urlparse.urlparse(url)

            # check domain
            if parsed_url.netloc != self.netloc:
                continue

            # substitute scheme for '//:...'
            if not parsed_url.scheme:
                url = self.scheme + ':' + url

            self.scheduler.add(url)

    def loop(self):
        while True:
            url = self.scheduler.get()
            if not url:
                break
            self.process(url)


def main():
    parser = argparse.ArgumentParser(description='Crawler')
    parser.add_argument('url', type=str)
    parser.add_argument('--parallel', default=4, type=int)
    parser.add_argument('--result', default='data', type=str)

    result = parser.parse_args()

    global RESULT_DIR
    RESULT_DIR = os.path.abspath(result.result)
    if not os.path.exists(RESULT_DIR):
        os.mkdir(RESULT_DIR)

    crawler = Crawler(result.url, result.parallel)
    crawler.loop()


if __name__ == '__main__':
    main()
