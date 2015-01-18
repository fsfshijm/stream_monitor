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
from xxxxxxxx_pyutils.email_tools import SmtpEmailTool
import logging
import logging.config
import subprocess

basedir = path.realpath(path.join(path.dirname(__file__), '..'))
os.chdir(basedir)
sys.path.append(path.join(basedir, 'lib'))
sys.path.append(path.join(basedir, 'lib/gen-py'))

import monitor_util as MNU


sys.path.append(basedir+'/conf')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_settings") 
from db.event_report_models import EventReportSdk3X   
from db.event_report_models import UeAderDever  
from django.db import transaction
from django.db import connection
from file_helper import FileInOut


class EventReportMonitorHourly:
	
	def __init__(self):
		self.logger = logging.getLogger('xxxxxxxx.event_report_monitor_hourly.monitor')
		self.logger.info('EventReportMonitorHourly inited')
		stamp = os.environ.get("DOMINO_STAMP", None)
		if not stamp:
			localtime = datetime.today() - timedelta(hours=3)
		else: 
			localtime = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S") - timedelta(hours=1)
		self.one_hour_ago = localtime.strftime("%Y%m%d%H")
		self.dt = localtime.strftime("%Y-%m-%d")
		self.hr = int(localtime.strftime("%H"))
		self.average_time = 0
		self.sdk3x_logtailer_data = {
					"nodes": ["nginx", "logtailer_nginx_to_kafka", "logtailer_kafka_to_eventarchiver"], 
					"values":{"nginx": 0, "logtailer_nginx_to_kafka": 0, "logtailer_kafka_to_eventarchiver":0}
				}
		self.sdk3x_normal_data = {
					"nodes": ["nginx", "eventarchiver", "mysql"], 
					"values":{"nginx": 0, "eventarchiver":0 ,"mysql": 0}
				}

		self.dbox_normal_data = {
					"nodes": ["nginx", "eventarchiver", "mysql"], 
					"values":{"nginx": 0, "eventarchiver":0 ,"mysql": 0}
				}
		self.appwall_normal_data = {
					"nodes": ["nginx", "eventarchiver", "mysql"], 
					"values":{"nginx": 0, "eventarchiver":0 ,"mysql": 0}
				}
		self.sdk3x_anti_data = {
					"nodes": ["nginx", "anti", "mysql"], 
					"values":{"nginx": 0, "anti":0 ,"mysql": 0}
				}
		self.appwall_anti_data = {
					"nodes": ["nginx", "anti", "mysql"], 
					"values":{"nginx": 0, "anti":0 ,"mysql": 0}
				}
		self.sdk1x2x_data = {
					"nodes": ["nginx",  "mysql"], 
					"values":{"nginx": 0, "mysql": 0}
				}

	def sdk3x_normal_nginx(self, server, data_dir, timestamp):
		cmd = """ssh monitor@%s 'wc -l %s/event_report.log.%s' | awk 'END{print echo $1}'""" % (server, data_dir, timestamp)
		total_count = int(subprocess.check_output(cmd, shell=True))
		self.sdk3x_logtailer_data["values"]["nginx"] += total_count

	#多宝屋事件报告
		cmd = """ssh monitor@%s grep -E SdkSdkType.{4}31 %s/event_report.log.%s -c | awk 'END{print echo $0}'""" % (server, data_dir, timestamp)
		dbox_count = int(subprocess.check_output(cmd, shell=True))

	#推广墙事件报告
		cmd = """ssh monitor@%s grep -E SdkSdkType.{4}30 %s/event_report.log.%s -c | awk 'END{print echo $0}'""" % (server, data_dir, timestamp)
		appwall_count = int(subprocess.check_output(cmd, shell=True))
		self.sdk3x_normal_data["values"]["nginx"] += total_count - appwall_count - dbox_count

	#多宝屋展现报告，一个tr带多个报告，为了数量能对上，这里需要解析下，通过%7D%2C (},) 来进行分割即可确定每个tr带的报告的个数
		cmd = """ssh monitor@%s grep -E SdkRt.{4}4.*SdkSdkType.{4}31 %s/event_report.log.%s | awk -F '%%7D%%2C' 'BEGIN{count = 0} {count = count + NF - 1} END{print echo count}'""" % (server, data_dir, timestamp)
		dbox_imp = int(subprocess.check_output(cmd, shell=True))

	#原本的多宝屋纪录 ＋ rt＝4时展开后的报告数（上面之所以用NF－1，是因为dbox_count本身是多了一些rt＝4的数据）
		self.dbox_normal_data["values"]["nginx"] += dbox_count + dbox_imp

	#推广墙展现报告，一个tr带多个报告，为了数量能对上，这里需要解析下，通过%7D%2C (},) 来进行分割即可确定每个tr带的报告的个数
		cmd = """ssh monitor@%s grep -E SdkRt.{4}4.*SdkSdkType.{4}30 %s/event_report.log.%s | awk -F '%%7D%%2C' 'BEGIN{count = 0} {count = count + NF - 1} END{print echo count}'""" % (server, data_dir, timestamp)
		appwall_imp = int(subprocess.check_output(cmd, shell=True))
		#原本的推广墙纪录 ＋ rt＝4时展开后的报告数（上面之所以用NF－1，是因为appwall_countb本身是多了一些rt＝4的数据）
		self.appwall_normal_data["values"]["nginx"] += appwall_count + appwall_imp 

	def sdk3x_anti_nginx(self, server, data_dir, timestamp):
		cmd = """ssh monitor@%s 'wc -l %s/dai_report.log.%s*' | awk 'END{print echo $1}'""" % (server, data_dir, timestamp)
		total_count = int(subprocess.check_output(cmd, shell=True))
		cmd = """ssh monitor@%s grep SdkType=30 %s/dai_report.log.%s* -c | awk -F : 'BEGIN{count = 0} {count = count + $2} END{print echo count}'""" % (server, data_dir, timestamp)
		appwall_count = int(subprocess.check_output(cmd, shell=True))
		self.sdk3x_anti_data["values"]["nginx"] += total_count - appwall_count
		self.appwall_anti_data["values"]["nginx"] += appwall_count 

	def sdk1x2x_nginx(self, server, data_dir, timestamp):
		cmd = """ssh monitor@%s 'wc -l %s/report.log.%s' | awk 'END{print echo $1}'""" % (server, data_dir, timestamp)
		count = int(subprocess.check_output(cmd, shell=True))
		self.sdk1x2x_data["values"]["nginx"] += count

	def statistics_nginx(self):
		print 'statistics_nginx start ...'
		self.logger.info('statistics_nginx start ...')
		servers = ["192.168.10.20", "192.168.10.21", "192.168.10.22", "192.168.10.23", "192.168.10.24", "192.168.10.25", "192.168.10.26"]
		#servers = ["192.168.10.21", "192.168.10.22", "192.168.10.25", "192.168.10.20", "192.168.10.24", "192.168.10.23"]
		nginx_sdk3x_normal_data_dir = "/data/remote_bak/day_rsync/log_bak/nginx" #event_report.log.*
		nginx_sdk3x_anti_data_dir = "/data/search/data/advertise_log/backup" #dai_report.log.
		nginx_sdk1x2x_data_dir = "/data/remote_bak/day_rsync/log_bak/nginx" #report.log.*
		for server in servers:
			self.sdk3x_normal_nginx(server, nginx_sdk3x_normal_data_dir, self.one_hour_ago)
			self.sdk3x_anti_nginx(server, nginx_sdk3x_anti_data_dir, self.one_hour_ago)
			self.sdk1x2x_nginx(server, nginx_sdk1x2x_data_dir, self.one_hour_ago)

			self.logger.info('nginx: logtailer-%s, sdk3x_normal-%s, appwall_normal-%s,dbox_normal-%s, sdk3x_anti-%s, appwall_anti-%s, sdk1x2x-%s' % (self.sdk3x_logtailer_data["values"]["nginx"], self.sdk3x_normal_data["values"]["nginx"], self.appwall_normal_data["values"]["nginx"],self.dbox_normal_data["values"]["nginx"], self.sdk3x_anti_data["values"]["nginx"], self.appwall_anti_data["values"]["nginx"], self.sdk1x2x_data["values"]["nginx"]))


	def statistics_antiserver(self):
		print 'start statistics_antiserver ....'
		filepath = "/data/search/data/online_shared/hourly_log_event.%s" % self.one_hour_ago
		cmd = "ssh monitor@192.168.10.30 %s/lib/thrift_file_read.py %s" % (basedir, filepath)
		values =  subprocess.check_output(cmd, shell=True)
		records = values.split(',')
		self.sdk3x_anti_data["values"]["anti"] += int(records[0])
		self.appwall_anti_data["values"]["anti"] += int(records[1])
		self.logger.info('anti: sdk3x_anti-%s, appwall_anti-%s' % (self.sdk3x_anti_data["values"]["anti"], self.appwall_anti_data["values"]["anti"]))

	def statistics_eventarchiver(self):
		print 'start statistics_eventarchiver ....'
		#data_dir = "/data/business/event_report_archive/"
		#cmd = """ssh monitor@192.168.10.30 find %s -name "ad_event_report.%s*" """ % (data_dir, self.one_hour_ago)
		data_dir = "/data1/monitor/event_report/30/event_report_archive"
		cmd = """find %s -name "ad_event_report.%s*" """ % (data_dir, self.one_hour_ago)
 		file_str = subprocess.check_output(cmd, shell=True)
 		file_list = file_str.split(' ')[0].split('\n')
 		file_list = [e for e in file_list if e != '']

		for filepath in file_list:
			#filepath = "/data/business/event_report_archive/ad_event_report.%s" % self.one_hour_ago
			#cmd = "ssh monitor@192.168.10.30 %s/lib/thrift_file_read.py %s" % (basedir, filepath)
			cmd = "%s/lib/thrift_file_read.py %s" % (basedir, filepath)
			values =  subprocess.check_output(cmd, shell=True)
			records = values.split(',')
			self.sdk3x_normal_data["values"]["eventarchiver"] += int(records[0])
			self.appwall_normal_data["values"]["eventarchiver"] += int(records[1])
			self.dbox_normal_data["values"]["eventarchiver"] += int(records[2])
		self.logger.info('eventarchiver: sdk3x_normal-%s, appwall_normal-%s, dbox_normal-%s' % (self.sdk3x_normal_data["values"]["eventarchiver"], self.appwall_normal_data["values"]["eventarchiver"], self.dbox_normal_data["values"]["eventarchiver"]))

	def statistics_logtailer(self):
		print 'start statistics_logtailer ....'
		cmd = "%s/lib/logtailer_query.py ad_event_report %s" % (basedir, self.one_hour_ago)
		count = subprocess.check_output(cmd, shell=True)
		self.sdk3x_logtailer_data["values"]["logtailer_nginx_to_kafka"] += int(count)

		cmd = "%s/lib/logtailer_query.py ad_event_report_archive %s" % (basedir, self.one_hour_ago)
		count = subprocess.check_output(cmd, shell=True)
		self.sdk3x_logtailer_data["values"]["logtailer_kafka_to_eventarchiver"] += int(count)
		self.logger.info('logtailer: nginx_to_kafka-%s, kafka_to_eventarchiver-%s' % (self.sdk3x_logtailer_data["values"]["logtailer_nginx_to_kafka"], self.sdk3x_logtailer_data["values"]["logtailer_kafka_to_eventarchiver"]))

	def statistics_mysql(self):
		print 'start statistics_mysql ....'
		self.logger.info('start statistics_mysql ....')
		cursor = connection.cursor()

		sql = """select sum(action_count) from event_report_sdk3x where dt='%s' and hr=%s and sdk_type not in (30,31)""" % (self.dt, self.hr)
		cursor.execute(sql)
		row = cursor.fetchone()
		if row !=None and row[0] != None: 
			self.sdk3x_normal_data["values"]["mysql"] += int(row[0])

		sql = """select sum(action_count) from event_report_sdk3x where dt='%s' and hr=%s and sdk_type = 30""" % (self.dt, self.hr)
		cursor.execute(sql)
		row = cursor.fetchone()
		if row !=None and row[0] != None: 
			self.appwall_normal_data["values"]["mysql"] += int(row[0])

		sql = """select sum(action_count) from event_report_sdk3x where dt='%s' and hr=%s and sdk_type = 31""" % (self.dt, self.hr)
		cursor.execute(sql)
		row = cursor.fetchone()
		if row !=None and row[0] != None: 
			self.dbox_normal_data["values"]["mysql"] += int(row[0])

		#mysql 数据是从event_archiver出来得install_success, download_finish, 不是从anti出来得，这里取这个数据和anti对比，理论上他们的数量应该是一样的
		sql = """select sum(action_count) from event_report_sdk3x where dt='%s' and hr=%s and sdk_type <> 30 and action_type in ('install_success', 'download_finish')""" % (self.dt, self.hr)
		cursor.execute(sql)
		row = cursor.fetchone()
		if row !=None and row[0] != None: 
			self.sdk3x_anti_data["values"]["mysql"] += int(row[0])
		sql = """select sum(action_count) from event_report_sdk3x where dt='%s' and hr=%s and sdk_type = 30 and action_type in ('install_success', 'download_finish')""" % (self.dt, self.hr)
		cursor.execute(sql)
		row = cursor.fetchone()
		if row !=None and row[0] != None: 
			self.appwall_anti_data["values"]["mysql"] += int(row[0])

		sql = """select sum(action_count) from event_report_sdk1x2x where dt='%s' and hr=%s""" % (self.dt, self.hr)
		cursor.execute(sql)
		row = cursor.fetchone()
		if row !=None and row[0] != None: 
			self.sdk1x2x_data["values"]["mysql"] += int(row[0])

		self.logger.info('mysql: sdk3x_normal-%s, appwall_normal-%s, dbox_normal-%s, sdk3x_anti-%s, appwall_anti-%s, sdk1x2x-%s' % (self.sdk3x_normal_data["values"]["mysql"], self.appwall_normal_data["values"]["mysql"], self.dbox_normal_data["values"]["mysql"], self.sdk3x_anti_data["values"]["mysql"], self.appwall_anti_data["values"]["mysql"], self.sdk1x2x_data["values"]["mysql"]))


	def send_report(self):
		print 'send_report ...' 
		data_sort = ["sdk3x_logtailer_data", "sdk3x_normal_data", "sdk3x_anti_data", "appwall_normal_data", "appwall_anti_data","dbox_normal_data", "sdk1x2x_data"]
		event_report_data = {
			"sdk3x_normal_data": [self.sdk3x_normal_data, "事件流sdk3.0以上非anti数据流量监控：nginx -> event-archiver -> mysql "],
			"sdk3x_anti_data": [self.sdk3x_anti_data, "事件流sdk3.0以上anti数据流量监控：nginx -> anti -> mysql "],
			"sdk1x2x_data": [self.sdk1x2x_data, "事件流sdk3.0以下数据流量监控：nginx -> mysql "],
			"appwall_normal_data": [self.appwall_normal_data, "推广墙非anti数据流量监控：nginx ->  event-archiver -> mysql "],
			"appwall_anti_data": [self.appwall_anti_data, "推广墙anti数据流量监控：nginx -> anti -> mysql "],
			"dbox_normal_data": [self.dbox_normal_data, "多宝屋非anti数据流量监控：nginx ->  event-archiver -> mysql "],
			"sdk3x_logtailer_data": [self.sdk3x_logtailer_data, "针对sdk3.0以上非anti流里的logtailer数据流量监控：nginx -> logtailer_to_kafka -> logtailer_kafka_to_eventarchiver"],
			}
		html = ""
		html += '<h2><事件数据流各环节流量监控 %s></h2>' % self.one_hour_ago
		html += '<h3><说明：非‘丢失率’列记录的是各个环节这个小时总的log数，丢失率 = (前一个环节log数 - 当前环节log数) / 前一个环节log数 > </h3>'
		html += '<hr/>'
		html += '<hr/>'
		html += '<style> .list {padding: 0px; margin: 0px; border-spacing:0px; border-collapse: collapse; width: 100%;border:1px solid #d2d6d9;}.list th {background-color: #8EB31A; color: #fff; font-size:14px; font-weight: bold; line-height: 20px; padding-top: 4px;border:1px solid #d2d6d9;} .list td {border-top: 1px solid #d2d6d9; padding-top: 1x; font-size: 12px; color: #000;height: 20px; text-align: center;border-right:1px solid #8eb31a;} .list .odd td {background-color: #FFF;} .list .even td {background-color: #F0F9D6;}</style>'

		alert_message = ''
		obj_name = ''
		for key in data_sort:
			item = event_report_data[key]
			html += '<h3>%s</h3>' % item[1]
			html += '<table class="list" border="1" align="center" style="width:800px;">'

			html += '<tr>'
			for field in item[0]["nodes"]:
				html += '<th  colspan="2">%s</th>' % field
			html += '</tr>'
			html += '<tr>'
			for field in item[0]["nodes"]:
				html += '<th>数量</th><th>丢失率</th>'
			html += '</tr>'

			pre_node_count = 0
			html += '<tr>'
			for field in item[0]["nodes"]:
				cur_node_count = item[0]["values"][field]
				if pre_node_count == 0 :
					rate = 0
				else :
					rate = round((pre_node_count - cur_node_count) * 100.0 / pre_node_count, 2)
				if rate >= 30:
					alert_message = 'warning: 事件数据流 %s, %s 环节, 丢失率为%s%s,超过30%s;' % (key, field, rate,'%', '%')
					obj_name = "event_report_%s" % key
				html += '<td>%s</td><td>%s%s</td>' % (cur_node_count, rate, '%')
				pre_node_count = cur_node_count
			html += '</tr>'
			html += '</table>'
		#send phone message
		if alert_message != '':
			#phone_num = "15801523700 13581626155 18810826526 18600504392"
			phone_num = "15801523700 18310951253"
			cmd = '/usr/local/xxxxxxxx/current/quaked/bin/sendsms.py -t %s -c "%s" -s qa -n event_report' % (phone_num, alert_message)
			subprocess.check_output(cmd, shell=True)

			MNU.insert_alarm_data('qa', obj_name, alert_message, 'shijingmeng@xxxxxxxx.cn', self.dt, self.hr)

		for key in data_sort:
			print key
			item = event_report_data[key]
			MNU.insert_reports_data(item[0]['values'], "event_report_%s" % key, self.dt, self.hr)

		config = ConfigParser.ConfigParser()
		config.read(path.join(basedir, 'data', 'email.conf'))
		emailTools = SmtpEmailTool(
				config.get('email','server'),
				config.get('email','user'),
				config.get('email','password')
		)
		mail_to = ['shijingmeng@xxxxxxxx.cn', 'yuxiang@xxxxxxxx.cn', 'pangtingting@xxxxxxxx.cn', 'chenzhiming@xxxxxxxx.cn', 'hexue@xxxxxxxx.cn']
		#mail_to = ['shijingmeng@xxxxxxxx.cn']
		emailTools.sendEmail(mail_to, '[qa event_report] 事件数据流各环节流量监控--小时级(%s)' % self.one_hour_ago, html.encode('utf8'))



	def run(self):
		self.statistics_nginx()
		self.statistics_eventarchiver()
		self.statistics_logtailer()
		self.statistics_antiserver()
		self.statistics_mysql()
		self.send_report()
		


if __name__ == '__main__':
	loggingConfigFile = path.join(basedir, 'conf/logging.conf')
	logging.config.fileConfig(loggingConfigFile)
	o = EventReportMonitorHourly()
	o.run()

