#! /usr/bin/env python3
#encoding: utf-8

"""
A crawler framework using pycurl.
"""

import signal
signal.signal(signal.SIGPIPE, signal.SIG_IGN)

import time
import pycurl

pycurl.global_init(pycurl.GLOBAL_ALL)

class BaseRequest(object):
	"""对pycurl的curl的封装，最基本的请求类型，有对curl的特殊的操
	作需求可以继承该类并对self.curl进行操作。
	"""
	def __init__(self):
		self.curl = pycurl.Curl()
		self.prior = False	# 该值为True时请求插入缓冲区前部
		self.retry = 3		# 当设置有retry次数时，请求执行失败后将会再次放入缓冲区
		self.type = type	

	def __del__(self):
		self.curl.close()
		self.curl = None

	def reset(self):
		"""请求失败并重试时会直接在multicurl中perform，所以当请求
		中定义有一次性的资源时多次perform会出现问题，例如StringIO，
		多次往其中写入数据会造成数据的叠加，得到的并非希望的结果。
		因此在每次perform之前需要重新申请这类资源。在该函数中释放
		上次申请的资源并为下次perform申请新的资源。
		"""
		pass


class BaseCrawler(object):
	"""最基础的爬虫框架，基于Pycurl的异步机制和协程实现。"""

	def __init__(self, max_concurrent, loop_interval):
		"""max_concurrent:最大异步并发数
		   loop_interval:当没有得到下载结果时主循环的延时
		"""
		self.max_curls = max_concurrent
		self.__loop_interval = loop_interval
		self.__free_curl_cnt = self.max_curls
		self.__curls = [None for i in range(self.max_curls)]
		self.__multi_curl = pycurl.CurlMulti()
		self.__send_buffer = []			# 发送缓冲区
		self.__looper = None
		self.__suspending = False		# 控制dispatch是否正在挂起
		self.__dispatching = False		# 控制send函数
		self.__dispatch_closed = False	# dispatch是否已经退出
	
	def len_buf(self):
		return len(self.__send_buffer)

	def __del__(self):
		self.__multi_curl.close()
		self.__multi_curl = None
		self.__send_buffer = None
	
	def send(self, request):
		"""Put a request into the buffer."""
		request.reset()
		if request.prior is True: self.__send_buffer.insert(0, request)
		else: self.__send_buffer.append(request)
		if self.__dispatching: self.__looper.send(None)

	def suspend(self):
		"""挂起dispatch进程，调用resume将恢复dispatch的执行。
		   该函数只能在dispatch中调用。
		"""
		self.__suspending = True
		self.__looper.send(None)

	def resume(self):
		"""恢复dispatch的执行。该函数不能在dispatch中调用。"""
		self.__suspending = False

	def dispatch(self):
		"""自定义函数，控制爬虫的爬取流程。"""
		pass

	def handle_ok(self, req):
		"""Handle success request."""
		pass

	def handle_error(self, req, errno, errmsg):
		"""Handle failure request."""
		pass

	def __loop(self):
		"""爬虫执行主循环，基本流程为：从请求缓冲区中获取下载请求，
		送入pycurl.multicurl中进行下载，等待下载结果并调用自定义回
		调函数分别处理下载结果。
		"""
		while True:
			# dispatch自定义函数已经退出，且发送缓冲区已经没有request，
			# 且没有request正在下载
			if self.__dispatch_closed and not self.__send_buffer and \
			self.__free_curl_cnt == self.max_curls: break
			# multicurl从发送缓冲区接受尽可能多的request直至达到最大并发
			while self.__free_curl_cnt > 0:
				req = None
				try:
					req = self.__send_buffer.pop(0)
				# 当缓冲区中没有请求时先调用dispatch填充发送缓冲区，
				# 直到达到最大并发数或dispatch挂起
				except IndexError:
					if self.__suspending: break
					else:
						try:
							self.__dispatching = True
							cpu = yield
							self.__dispatching = False
						except GeneratorExit:
							self.__dispatch_closed = True
							self.__suspending = True
							self.__dispatching = False
							break
				if req is None: continue
				for i,k in enumerate(self.__curls):
					if k is None:
						req.curl.id = i
						self.__curls[i] = req
						self.__multi_curl.add_handle(req.curl)
						self.__free_curl_cnt -= 1
						break
			while True:
				ret, active_num = self.__multi_curl.perform()
				if ret != pycurl.E_CALL_MULTI_PERFORM: break
			while True:
				if self.__multi_curl.select(self.__loop_interval)==-1: continue
				queued_num, ok_list, err_list = self.__multi_curl.info_read()
				for c in ok_list:
					self.__multi_curl.remove_handle(c)
					self.__free_curl_cnt += 1
					self.handle_ok(self.__curls[c.id])
					self.__curls[c.id] = None
				for c, errno, errmsg in err_list:
					self.__multi_curl.remove_handle(c)
					self.__free_curl_cnt += 1
					req = self.__curls[c.id]
					self.__curls[c.id] = None
					if req.retry > 0:
						req.retry -= 1
						self.send(req)
					else: self.handle_error(req, errno, errmsg)
				if queued_num == 0: break

	def run(self):
		"""Start Crawler"""
		self.__looper = self.__loop()
		next(self.__looper)
		self.dispatch()
		try:
			self.__looper.close()
		except RuntimeError as e: print(e)


###############################################################################
# Example
###############################################################################

class ForumRequest(BaseRequest):
	"""一种常用的请求类型，根据URL获取内容，可以设置HTTP代理， 
	并可以利用HTTP PROXY TUNNEL模拟HTTPS代理。
	"""
	def __init__(self, url, timeout=25, proxy=None, https=False, httpsproxytunnel=False):
		super(Request, self).__init__()
		self.curl.setopt(pycurl.NOSIGNAL, 1)
		self.curl.setopt(pycurl.URL, url)
		self.curl.setopt(pycurl.CONNECTTIMEOUT, 50)
		self.curl.setopt(pycurl.TIMEOUT, timeout)
		if proxy: c.setopt(pycurl.PROXY, proxy)
		if https:
			self.curl.setopt(pycurl.SSL_VERIFYPEER, 0)
			self.curl.setopt(pycurl.SSL_VERIFYHOST, 0)
		if httpsproxytunnel: self.curl.setopt(pycurl.HTTPPROXYTUNNEL, 1)
		self.fp = None
		self.url = url

	def __del__(self):
		if self.fp is not None: self.fp.close()
		super(Request, self).__del__()

	def reset(self):
		if self.fp is not None: self.fp.close()
		self.fp = StringIO()
		self.curl.setopt(pycurl.WRITEFUNCTION, self.fp.write)

	def set_proxy(self, proxy):
		"""重新设置请求的代理。
		   注意请求重试时不会自动更换代理。
		"""
		c.setopt(pycurl.PROXY, proxy)

	def get_content(self):
		"""获取下载后的文本内容
		"""
		return self.fp.getvalue()

class ForumCrawler(BaseCrawler):
	"""这是一个针对最常见的论坛新闻类站点的爬虫样例，其特点包括： 
	1、页面结构分为topic和thread两类，thread页面是具体的一个帖子
	主题或是一个具体的新闻，topic页面是thread页面URL的汇总，并按
	照发表或最新更新时间对thread进行了排序；
	2、使用Request类，不考虑代理，不考虑HTTP头等特殊情况。
	"""
	def __init__(self, board_ids, max_concurrent=30, loop_interval=1):
		"""board_ids：标识所要爬取的board的ID列表，ID用于从get_next_topic_url
		函数中获得Topic页面的URL。
		"""
		super(ForumCrawler, self).__init__(max_concurrent, loop_interval)
		self.board_ids = board_ids
		self.stop = False

	def dispatch(self):
		for board_id in self.board_ids:
			self.stop = False
			while True:
				req = Request('http://example.com')
				self.send(req)
				self.suspend()	# 等待该topic页面下载解析完成再翻页，
				#因为需要根据该topic页的解析结果决定是否需要继续翻页
				# if some condition: break

	def handle_ok(self, req):
		content1 = req.get_content()
		if req.type == "topic":
			for url, args in self.parse_topic(content1):
				req = Request(url)
				req.type = "thread"
				req.args = args
				self.send(req)
			self.resume()
		elif req.type == "thread": return self.parse_thread(content1, req.args)

	def handle_error(self, req, errno, errmsg):
		if req.type == "topic":
			print("Get topic-url:%s failed,error is %d,%s" % (req.url, errno, errmsg))
			print("Get topic page failed,crawler exit unnormally")
		elif req.type == "thread":
			print("Get thread-url:%s failed,error is %d,%s" % (req.url, errno, errmsg))

	def parse_topic(self, content1):
		"""自定义生成器函数，解析topic页面并yield每一条thread的信息，
		yield数据的格式为(url,[附带数据1，附带数据2，...])，附带数据
		列表会作为args参数传递给相应的parse_thread函数。
		注意，当在解析过程中发现帖子的最新更新时间早于爬虫上次启动时
		间时，停止yield数据，并设置self.stop为True。
		"""
		pass

	def parse_thread(self, content1, args):
		"""自定义函数，解析thread页面并把解析结果持久化。args为parse_topic
		中的附带数据列表，用于从parse_topic传递数据给parse_thread。
		"""
		pass



