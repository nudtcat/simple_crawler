#!/usr/bin/env python
# -*- coding:utf-8 -*-

import Queue
import threading
import lxml.html
import hashlib
import requests
import logging
import traceback


logging.basicConfig(format='%(levelname)s:(%(asctime)s) %(message)s',filename='spider.log', level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.warn("start")

url_queue = Queue.Queue()
hash_set = set()  # 并发问题,加锁
lock = threading.RLock()

headers = {
	"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0",
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	"Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
	"Connection": "keep-alive"
}


def parser_html(url, html):
	doc = lxml.html.document_fromstring(html)
	doc.make_links_absolute(url)
	return doc.iterlinks()


def filter_links(links):
	tags = ['a', 'iframe', 'frame']
	blacklist = ['doc', 'pdf', 'mp4', 'jpg', 'jpeg']
	results = []
	for link in links:
		if (link[0].tag in tags) and (link[2].split(".")[-1] not in blacklist):
			results.append(link[2])
	return results


def is_repeat(link):
	lock.acquire()
	md5sum = hashlib.md5(link).hexdigest()
	if md5sum in hash_set:
		lock.release()
		return True
	else:
		hash_set.add(md5sum)
		lock.release()
		return False


def save_resource(url, req, cur_deep):
	#多线程
	print url, cur_deep, hashlib.md5(url).hexdigest()


class Spider(threading.Thread):
	def __init__(self, url_queue, deep):
		threading.Thread.__init__(self)
		self.url_queue = url_queue
		self.deep = deep

	def run(self):
		while True:
			try:
				url_pair = self.url_queue.get(timeout=5)
				url = url_pair[0]
				cur_deep = url_pair[1]
				logger.warn(self.name + " Get From Queue: " + url)
			except Queue.Empty:
				logger.warn(self.name + " timeout,thread end.")
				break

			try:
				r = requests.get(url, headers, timeout=5)
				html = r.text
				save_resource(url, r, cur_deep)

				urls = filter_links(parser_html(url, html))
				if cur_deep < self.deep:
					for url in urls:
						if not is_repeat(url):
							self.url_queue.put([url, cur_deep + 1])
			except:
				logger.error(traceback.format_exc())
				pass
			self.url_queue.task_done()


if __name__ == "__main__":
	url_queue.put([r'http://www.sina.com.cn/', 1])
	list = []
	for i in range(10):
		u = Spider(url_queue, 2)
		list.append(u)
		u.start()
	for u in list:
		u.join()
	logger.warn("==== The End ====")
