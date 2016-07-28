#!/usr/bin/env python
# -*- coding:utf-8 -*-

import threading
import random
import time

warehouse = []
max_num = 10
condition = threading.Condition()


class ProducerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		while True:
			condition.acquire()
			if len(warehouse) == max_num:
				print "Warehouse is full"
				condition.wait()  # Release the lock
				print "Producer is notified by consumer"
			else:
				warehouse.append(1)
				print "Producer is working, NUM: " + str(len(warehouse))
			condition.notify()
			condition.release()
			time.sleep(random.random())


class ConsumerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		while True:
			condition.acquire()
			if len(warehouse) == 0:
				print "Warehouse is empty"
				condition.wait()
				print "Consumer is notified by producer"
			else:
				warehouse.pop()
				print "Consumer is working, NUM: " + str(len(warehouse))
			condition.notify()
			condition.release()
			time.sleep(random.random())


if __name__ == "__main__":
	c_list = []
	p_list = []
	for i in range(5):
		p = ProducerThread()
		p_list.append(p)
		p.start()
	for i in range(6):
		c = ConsumerThread()
		c_list.append(c)
		c.start()
