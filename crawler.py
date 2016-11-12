#!/usr/bin/env python2.7


import argparse
import codecs
import HTMLParser
import logging
import os
import os.path
import requests
import gevent.queue
import gevent.pool
import gevent.monkey
import time
import urlparse
import yaml


logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s %(levelname)s %(filename)s:%(lineno)s %(message)s'
)
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


class Index(object):
    def __init__(self):
        self.valid = dict()
        self.wrong = dict()
        self.next = 1
        self.saved = 0

    @property
    def file_name(self):
        return 'index.yaml'

    def check_before_add(self, url):
        if self.get_valid(url):
            raise RuntimeError('url {} already in valid'.format(url))
        if self.get_wrong(url):
            raise RuntimeError('url {} already in wrong'.format(url))

    def add_valid(self, url, body):
        self.check_before_add(url)
        file_name = '{}.page'.format(self.next)
        save_file(file_name, body)

        self.valid[url] = file_name
        self.next += 1
        self.save_if_required()

        return file_name

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
        logger.info('save index start')
        data = dict(valid=self.valid, wrong=self.wrong, next=self.next)
        data = yaml.dump(data)
        save_file(self.file_name, data)
        self.saved = int(time.time())
        logger.info('save index done')

    def load(self):
        logger.info('load index start')
        data = load_file(self.file_name)
        if data is None:
            logger.info('load index empty %s', self.file_name)
            return
        try:
            data = yaml.load(data)
        except BaseException as error:
            logger.error('load index %s problem %s', self.file_name, error)
        self.valid = data.get('valid', {})
        self.wrong = data.get('wrong', {})
        self.next = data.get('next', 1)
        logger.info('load index done')


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
    for prefix in ['text/html', 'text/xml', 'application/rss+xml', 'text/css']:
        if content_type.startswith(prefix):
            return True
    return False


class Crawler(object):
    def __init__(self, url):
        target = urlparse.urlparse(url)
        self.scheme = target.scheme
        self.netloc = target.netloc
        self.url = url

        self.index = Index()
        self.index.load()

        self.todo = gevent.queue.Queue()
        self.cancel = False

    # ugly boilerplate for manage workers
    def run(self, parallel):
        pool = gevent.pool.Pool(parallel)
        known = set()
        left = 1
        self.todo.put((None, [self.url]))

        try:
            for (url, children) in self.todo:
                left -= 1
                for url in children:
                    if self.cancel:
                        break
                    if url not in known:
                        left += 1
                        pool.spawn(self.process, url)
                    known.add(url)
                if left == 0:
                    break
                if self.cancel:
                    break
        except KeyboardInterrupt:
            pass

        pool.join()
        self.index.save()

    def _parse_(self, body):
        result = []

        for url in extract_urls(body):
            parsed_url = urlparse.urlparse(url)

            # check domain
            if parsed_url.netloc != self.netloc:
                continue

            # substitute scheme for '//:...'
            if not parsed_url.scheme:
                url = self.scheme + ':' + url

            result.append(url)

        return result

    def _fetch_(self, url):
        logger.debug("url fetch %s", url)
        response = requests.get(url)
        code = response.status_code
        content_type = response.headers.get('content-type')

        # analyze status code
        if code != 200:
            logger.debug("url wrong status code %s %s ", code, url)
            self.index.add_wrong(url, code=code, content_type=content_type)
            return

        # analyze content-type
        if not valid_content_type(content_type):
            logger.debug("url wrong content-type %s %s", content_type, url)
            self.index.add_wrong(url, code=code, content_type=content_type)
            return

        # # get body
        body = response.text
        file_name = self.index.add_valid(url, body)
        logger.debug('url %s saved to %s', url, file_name)

        return body

    def _process_(self, url):
        logger.debug("start %s", url)

        # check in index for wrong
        wrong = self.index.get_wrong(url)
        if wrong:
            logger.debug("url %s is wrong (code=%s, content_type",
                         url, wrong.get('code'), wrong.get('content_type'))
            return

        # check in index for valid
        body = self.index.get_valid(url)
        if body:
            logger.debug("url %s found", url)
        else:
            # fetch
            body = self._fetch_(url)

        # parse
        children = self._parse_(body)

        self.todo.put((url, children))

    def process(self, url):
        try:
            self._process_(url)
        except KeyboardInterrupt:
            self.cancel = True


def main():
    gevent.monkey.patch_all()

    parser = argparse.ArgumentParser(description='Crawler')
    parser.add_argument('url', type=str)
    parser.add_argument('--parallel', default=4, type=int)
    parser.add_argument('--result', default='data', type=str)

    result = parser.parse_args()

    global RESULT_DIR
    RESULT_DIR = os.path.abspath(result.result)
    if not os.path.exists(RESULT_DIR):
        os.mkdir(RESULT_DIR)

    crawler = Crawler(result.url)
    crawler.run(result.parallel)


if __name__ == '__main__':
    main()
