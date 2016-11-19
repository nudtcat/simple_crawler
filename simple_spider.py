#!/usr/bin/env python
# -*- coding:utf-8 -*-

from gevent import monkey

monkey.patch_all()
import gevent
import Queue
import lxml.html
import hashlib
import requests
import logging
import traceback
import time
import re
import datetime


class spider_setting():
	def __init__(self, url, deep, thread_num=10, filter_str=None, filter_re=None,
				 log_name='spider_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '.log', referer=""):
		logging.basicConfig(format='%(levelname)s:(%(asctime)s) %(message)s', filename=log_name, level=logging.WARNING)
		self.logger = logging.getLogger(__name__)
		self.logger.warn("\n\n\n\n===========start===========")

		self.url = url
		self.deep = deep
		self.filter_str = filter_str
		self.filter_re = filter_re
		self.thread_num = thread_num

		self.url_queue = Queue.Queue()
		self.url_queue.put((url, 1, referer))
		self.hash_set = set()
		self.hash_set.add(hashlib.md5(url).hexdigest())


class spider():
	def __init__(self, spider_setting):
		self.url_queue = spider_setting.url_queue
		self.hash_set = spider_setting.hash_set
		self.logger = spider_setting.logger
		self.thread_num = spider_setting.thread_num

		self.deep = spider_setting.deep
		self.filter_str = spider_setting.filter_str
		self.filter_re = spider_setting.filter_re

		self.gevent_list = []

	def run(self):
		while True:
			try:
				url_pair = self.url_queue.get(timeout=5)
				url = url_pair[0]
				cur_deep = url_pair[1]
				referer = url_pair[2]
				self.logger.warn("Get From Queue" + str(url_pair))

			except Queue.Empty:
				self.logger.warn("Queue_len:" + str(self.url_queue.qsize()) + "\tspider end!")
				break

			try:
				start_time = time.time()
				r = requests.get(url, headers=self.set_headers(referer=referer), timeout=5)
				end_time = time.time()
				self.logger.warn(
					"Queue_len:" + str(self.url_queue.qsize()) + "\t" + str(
						len(self.hash_set)) + "\t" + str(end_time - start_time) + "\t" + url + "\tReferer: " + referer)
				self.save_resource(url, r, cur_deep, referer)
				html = r.text

				urls = self.filter_links(self.parser_html(url, html))
				if cur_deep < self.deep:
					for new_url in urls:
						if not self.is_repeat(new_url):
							self.url_queue.put((new_url, cur_deep + 1, url))
			except:
				self.logger.error(traceback.format_exc())
			self.url_queue.task_done()

	def parser_html(self, url, html):
		doc = lxml.html.document_fromstring(html)
		doc.make_links_absolute(url)
		return [link[2] for link in doc.iterlinks()]

	def filter_links(self, links):
		# url_parser_re = r"^(\w*):\/\/(?:([^:]*)?(?::(.*))?@)?([0-9.\-A-Za-z]+)(?::(\d+))?(?:\/([^?#]*))?(?:\?([^#]*))?(?:#(.*))?$"
		# r = re.compile(url_parser_re)
		blacklist = ['doc', 'pdf', 'mp4', 'jpg', 'jpeg', 'docx', 'xls', 'xlsx', 'mp3', 'apk', 'rar', 'zip', 'flv',
					 'swf', 'gif', 'png', 'css'
										  'exe']
		results = []
		for link in links:
			if (link.split(".")[-1] not in blacklist):
				# url = re.match(r, link)
				results.append(link)
		return results

	def is_repeat(self, link):
		md5sum = hashlib.md5(link).hexdigest()
		if md5sum in self.hash_set:
			return True
		else:
			self.hash_set.add(md5sum)
			return False

	def save_resource(self, url, req, cur_deep, referer):
		if (self.filter_str is not None) and (self.filter_str not in url):
			return
		if (self.filter_re is not None) and (not re.search(self.filter_re, url)):
			return
		print url, referer

	def set_headers(self, referer=""):
		return {
			"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
			"Connection": "keep-alive",
			"Referer": referer
		}

	def start(self):
		for i in range(self.thread_num):
			self.gevent_list.append(gevent.spawn(self.run))

	def join(self):
		gevent.joinall(self.gevent_list)


if __name__ == "__main__":
	s = spider_setting(r"http://www.sina.com.cn", 3)
	a = spider(s)
	a.start()
	a.join()
