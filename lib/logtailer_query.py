#!/usr/local/bin/python2.7
# coding: utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
from os import path
import ConfigParser
import urllib2
from urllib import unquote
from datetime import date, timedelta, datetime
import time
from collections import defaultdict
import logging
import logging.config

basedir = path.realpath(path.join(path.dirname(__file__), '..'))
os.chdir(basedir)
sys.path.append(path.join(basedir, 'lib'))


sys.path.append(basedir+'/conf')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_settings")
from db.logtailer_models import DbLogtailerThread
from db.logtailer_models import DbLogtailerThreadStatHourly
from db.logtailer_models import DbStream
from django.db import transaction


class LogtailerRecordQuery:
	
	def __init__(self, stream_name, hr):
		self.stream_name = stream_name 
		self.hr = hr
		d = datetime.strptime(str(hr),"%Y%m%d%H")         
		self.snap_time = int(time.mktime(d.timetuple()))
		self.record_count = 0
		self.logger = logging.getLogger('xxxxxxxx.logtailer_query.monitor')
		self.logger.info('LogtailerRecordQuery inited')



	def run(self):
		stream_rows = [r for r in DbStream.objects.using("logtailer").filter(name=self.stream_name)]
		if len(stream_rows) == 0:
			self.logger.info('error: stream %s lost' % stream_name)
		streamId = stream_rows[0].id

		thread_rows = [r for r in DbLogtailerThread.objects.using("logtailer").filter(stream_id=int(streamId))]
		if len(thread_rows) == 0:
			self.logger.info('error: stream %s has no thread' % self.stream_name)  
		for row in thread_rows:
			try:
				stat_hourly_row = DbLogtailerThreadStatHourly.objects.using("logtailer").get(thread_id=row.id, snap_time=self.snap_time)
			except DbLogtailerThreadStatHourly.DoesNotExist, e:
				self.logger.info('error: stream %s, thread: %s  has no hourly(%s) data, time(%s)' % (self.stream_name, row.thread_name, self.hr, self.snap_time))
				continue
			self.record_count += stat_hourly_row.message_sent

		print self.record_count



if __name__ == '__main__':
	loggingConfigFile = path.join(basedir, 'conf/logtailer_logging.conf')
	logging.config.fileConfig(loggingConfigFile)
	oMaker = LogtailerRecordQuery(sys.argv[1], sys.argv[2])
	oMaker.run()

