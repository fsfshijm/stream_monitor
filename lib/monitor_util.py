#!/usr/local/bin/python2.7
# -*- coding: utf8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
from datetime import datetime, timedelta
from urllib import unquote
import argparse
import re
import subprocess
import xxxxxxxx_pyutils.mysql_util as MU


#MU.registerConnection('monitor', host="dreportm.db.in.xxxxxxxx.cn", port=3306, user='monitor', passwd='dmqa714monitor', db='monitor', pool_size=8)
MU.registerConnection('monitor', host="dreportm.db.in.xxxxxxxx.cn", port=3306, user='monitor', passwd='dmqa714monitor', db='monitor', pool_size=8)

def load_into_mysql(row, insert_sql):
    try:
            sql = insert_sql % tuple(row)
            MU.execute('monitor', sql)
    except Exception,e:
            print "load exception", row
            print e

def insert_alarm_data(sender, object, content, alarm_email, dt, hr):
	"""
	功能：插入报警数据
	参数说明：
		sender: 发送人，随便写，表明是哪个组的即可，eg，qa
		object: 某条数据支流或者模块的名字
		content：报警内容
		alarm_email: 报警联系人，是邮箱，字符串传进来, 逗号分割
		dt, hr: 报警时的时间
	"""
	try:
		insert_sql = """INSERT INTO alarm_stat SET sender = '%s', content = '%s', alarm_email = '%s', obj_id = %s, dt = '%s', hr = %s"""
		obj_id_sql = """SELECT obj_id from objects where obj_name = '%s';"""
		cur_row = MU.fetchone('monitor', obj_id_sql % (object))
		if cur_row == None:
			print 'object is no exit, please create it first!'
		row = [str(sender), str(content), str(alarm_email), cur_row[0], dt, hr]
		load_into_mysql(row, insert_sql)
	except Exception, e:
		print e


def insert_reports_data(content, object, dt, hr):
	"""
	功能：插入各监控报告数据，作为历史数据存储起来;object 不存在，不会插入, 同一时间的数据，会更新, 主要是处理重跑
	参数说明：
		content: 报告数据，是一个一级map，eg.{'key1':10, 'key2':20}
		object: 该报告对应的数据流或者模块的一个名字，需要在http://qmm.xxxxxxxx-inc.cn:8675/index.php?r=objects/index  上面创建，然后再使用
		dt, hr: 统计的数据对应的时间（不是报告发出来的时间）
	"""
	try:
		tmp_content = eval(str(content))
		pattern = re.compile(r"^\d+(\.\d*)?$")
		for key in tmp_content:
			match = pattern.match(str(tmp_content[key]))
			if not match:
				print 'the value of your map which must be numeric is wrong!'
				return	
		new_content = {key.encode('utf8') : tmp_content[key] for key in tmp_content}
		insert_sql = """INSERT INTO reports SET content_map = "%s", obj_id = %s, dt = '%s', hr = %s""" 
		update_sql = """update reports SET content_map = "%s" where obj_id = %s and dt = '%s' and hr = %s""" 
		record_sql = """SELECT * from reports where obj_id = %s and dt = '%s' and hr=%s;"""
		obj_id_sql = """SELECT obj_id from objects where obj_name = '%s';"""
		cur_row = MU.fetchone('monitor', obj_id_sql % (object))
		if cur_row == None:
			print 'object is no exit, please create it first!'
			return

		cur_record = MU.fetchone('monitor', record_sql % (cur_row[0], dt, hr))
		if cur_record != None:
			MU.execute('monitor', update_sql % (str(new_content), cur_row[0], dt, hr))	
		else:
			row = [str(new_content), str(cur_row[0]), dt, hr]
			load_into_mysql(row, insert_sql)

		checkData(object, dt, hr)
	except Exception, e:
		print e

def select_reports_content(object, dt, hr):
	"""
	功能：查找某条流（或某个模块）某个时间的数据
	参数说明：
		object: 该报告对应的数据流或者模块的一个名字
		dt, hr: 时间
	返回值：一个map，eg. {'key1':10, 'key2':20}
	"""
	try:
		obj_id_sql = """SELECT obj_id from objects where obj_name = '%s';"""
		content_sql = """select content_map from reports where dt = '%s' and hr = %s and obj_id = %s"""
		cur_row = MU.fetchone('monitor', obj_id_sql % (object))
		if cur_row == None:
			print 'object is no exit, please create it first!'
		else:
			obj_id = cur_row[0]
			cur_content_row = MU.fetchone('monitor', content_sql % (dt, hr, obj_id))
			if cur_content_row == None:
				return None
			else:
				return eval(cur_content_row[0])
	except Exception, e:
		print e


def checkData(object, dt, hr):
	"""
	功能：计算同比和环比数据
	参数说明：
		object: 该报告对应的数据流或者模块的一个名字
		dt, hr: 时间
	"""
	try:
		obj_id_sql = """SELECT obj_id, type, obj_name, group_id, phone_nums,watcher from objects where obj_name = '%s';"""
		group_sql = """SELECT group_name from groups where id = %s;"""
		cur_row = MU.fetchone('monitor', obj_id_sql % (object))
		if cur_row == None:
			print 'object is no exit, please create it first!'
		else:
			obj_info = {}
			obj_info['obj_id'] = cur_row[0]
			obj_info['type'] = cur_row[1]
			obj_info['obj_name'] = cur_row[2]
			obj_info['group_id'] = cur_row[3]
			obj_info['phone_nums'] = cur_row[4]
			obj_info['watcher'] = cur_row[5]
			group_row = MU.fetchone('monitor', group_sql % (obj_info['group_id']))
			obj_info['group_name'] = group_row[0]

			if obj_info['type'] == 2: #天级
				cycle_huanbi = 1
				cycle_tongbi = 7
				calc_ratio_day(obj_info, dt, cycle_huanbi, cycle_tongbi)
			else: #小时级
				cycle_huanbi = 1
				cycle_tongbi = 24
				calc_ratio_hr(obj_info, dt, hr, cycle_huanbi, cycle_tongbi)
				
	except Exception, e:
		print e




def calc_ratio_hr(obj_info, dt, hr, cycle_huanbi, cycle_tongbi):
	"""
	功能：计算某条流或者模块数据的同比环比，小时级
	参数说明：
		object: 该报告对应的数据流或者模块的一个名字
		dt, hr: 时间
	"""
	try:
		obj_id = obj_info['obj_id']
		content_sql = """select content_map from reports where dt = '%s' and hr = %s and obj_id = %s"""
		checker_sql ="select id, keyword, type, threshold, is_running from checker where obj_id = %s"
		checker_update_sql ="update checker set real_rate = %s, pre_cycle =%s, cur_cycle = %s, status = '%s'  where id = %s"

		cur_content_row = MU.fetchone('monitor', content_sql % (dt, hr, obj_id))
		cur_content = {}
		if cur_content_row != None:
			cur_content = eval(cur_content_row[0])
		cur_time = datetime.strptime("%s %s:10:10" % (dt, hr), "%Y-%m-%d %H:%M:%S")

		checker_rows = MU.fetchall('monitor', checker_sql % (obj_id))
		for row in checker_rows:
			if row[4] == 0: #是否在运行 0-停止  1-运行中
				MU.execute('monitor', checker_update_sql % (0, 0, 0, 'Stoped', row[0]))
				continue
			if row[2] in [0, 1] : #type 0-同比增, 1-同比减, 2-环比增, 3-环比减
				pre_time = cur_time - timedelta(hours=cycle_tongbi)
			else: #1-环比
				pre_time = cur_time - timedelta(hours=cycle_huanbi)
			pre_dt = pre_time.strftime('%Y-%m-%d')
			pre_hr = pre_time.strftime('%H')
			pre_content_row = MU.fetchone('monitor', content_sql % (pre_dt, pre_hr, obj_id))
			pre_content = {}
			if pre_content_row != None:
				pre_content = eval(pre_content_row[0])
			cur_cycle = cur_content.get(row[1], 0)
			pre_cycle = pre_content.get(row[1], 0)
			if cur_cycle == 0  or pre_cycle == 0:
				MU.execute('monitor', checker_update_sql % (0, pre_cycle, cur_cycle, 'DataLost', row[0]))
				continue
			real_rate = 0
			if row[2] in [0,2]: #0-同比增, 2-环比增
				real_rate = round((float(cur_cycle) - float(pre_cycle)) / float(pre_cycle) * 100.0, 2)
			else: # 1-同比减, 3-环比减
				real_rate = round((float(pre_cycle) - float(cur_cycle)) / float(pre_cycle) * 100.0, 2)

			if real_rate > float(row[3]):
				MU.execute('monitor', checker_update_sql % (real_rate, pre_cycle, cur_cycle, 'Error', row[0]))
				type_map = {0:'同比增加', 1:"同比减少", 2:"环比增加", 3:"环比减少"}
				alert_message = "项目%s, %s数据异常，%s 比例为%s,超过%s" % (obj_info['obj_name'], row[1], type_map[row[2]], real_rate, float(row[3]))

				cmd = '/usr/local/xxxxxxxx/current/quaked/bin/sendsms.py -t %s -c "%s" -s %s -n %s' % (obj_info['phone_nums'], alert_message, obj_info['group_name'], obj_info['obj_name'])
				subprocess.check_output(cmd, shell=True)
				insert_alarm_data(obj_info['group_name'], obj_info['obj_name'], alert_message, obj_info['watcher'], dt, hr)
			else: 
				MU.execute('monitor', checker_update_sql % (real_rate, pre_cycle, cur_cycle, 'Normal', row[0]))

	except Exception, e:
		print e


def calc_ratio_day(obj_info, dt, cycle_huanbi, cycle_tongbi):
	"""
	功能：计算某条流或者模块数据的同比环比，天级
	参数说明：
		object: 该报告对应的数据流或者模块的一个名字
		dt, hr: 时间
	"""
	try:
		obj_id = obj_info['obj_id']
		content_sql = """select content_map from reports where dt = '%s' and obj_id = %s"""
		checker_sql ="select id, keyword, type, threshold, is_running from checker where obj_id = %s"
		checker_update_sql ="update checker set real_rate = %s, pre_cycle =%s, cur_cycle = %s, status = '%s'  where id = %s"

		cur_content_row = MU.fetchone('monitor', content_sql % (dt, obj_id))
		cur_content = {}
		if cur_content_row != None:
			cur_content = eval(cur_content_row[0])
		cur_time = datetime.strptime("%s 10:10:10" % (dt), "%Y-%m-%d %H:%M:%S")

		checker_rows = MU.fetchall('monitor', checker_sql % (obj_id))
		for row in checker_rows:
			if row[4] == 0:
				MU.execute('monitor', checker_update_sql % (0, 0, 0, 'Stoped', row[0]))
				continue
			if row[2] in [0, 1] : #type 0-同比增, 1-同比减, 2-环比增, 3-环比减
				pre_time = cur_time - timedelta(days=cycle_tongbi)
			else: #1-环比
				pre_time = cur_time - timedelta(days=cycle_huanbi)
			pre_dt = pre_time.strftime('%Y-%m-%d')
			pre_content_row = MU.fetchone('monitor', content_sql % (pre_dt, obj_id))
			pre_content = {}
			if pre_content_row != None:
				pre_content = eval(pre_content_row[0])
			cur_cycle = cur_content.get(row[1], 0)
			pre_cycle = pre_content.get(row[1], 0)
			if cur_cycle == 0  or pre_cycle == 0:
				MU.execute('monitor', checker_update_sql % (0, pre_cycle, cur_cycle, 'DataLost', row[0]))
				continue

			real_rate = 0
			if row[2] in [0,2]: #0-同比增, 2-环比增
				real_rate = round((float(cur_cycle) - float(pre_cycle)) / float(pre_cycle) * 100.0, 2)
			else: # 1-同比减, 3-环比减
				real_rate = round((float(pre_cycle) - float(cur_cycle)) / float(pre_cycle) * 100.0, 2)

			if real_rate > float(row[3]):
				MU.execute('monitor', checker_update_sql % (real_rate, pre_cycle, cur_cycle, 'Error', row[0]))

				type_map = {0:'同比增加', 1:"同比减少", 2:"环比增加", 3:"环比减少"}
				alert_message = "项目%s, %s数据异常，%s 比例为%s,超过%s" % (obj_info['obj_name'], row[1], type_map[row[2]], real_rate, float(row[3]))
				cmd = '/usr/local/xxxxxxxx/current/quaked/bin/sendsms.py -t %s -c "%s" -s %s -n %s' % (obj_info['phone_nums'], alert_message, obj_info['group_name'], obj_info['obj_name'])
				subprocess.check_output(cmd, shell=True)
				insert_alarm_data(obj_info['group_name'], obj_info['obj_name'], alert_message, obj_info['watcher'], dt, hr)
			else: 
				MU.execute('monitor', checker_update_sql % (real_rate, pre_cycle, cur_cycle, 'Normal', row[0]))

	except Exception, e:
		print e





if __name__ == "__main__":
	sender = 'qa'
	obj_name = 'click_hourly_monitor'
	content = 'error: aaa'
	alarm_to = 'qa@xxxxxxxx.cn,sdk@xxxxxxxx.cn'
	dt = '2014-09-11'
	hr = '10'
	#insert_alarm_data(sender, obj_name, content, alarm_to, dt, hr)

	content = {'logtailer': 40, 'nginx': 40, 'dispatcher': 40, 'mysql': 40}
	content = {'aw_parse_json_error_': 0, 'ad_validate_vcode_error_': 48433, 'ad_decode_tracker_error_': 306, 'aw_decode_tracker_error_': 3, 'ad_check_tracker_empty_error_': 364604, 'aw_check_tracker_empty_error_': 2, 'aw_validate_vcode_error_': 16330, 'ad_parse_json_error_': 98130}

	object = 'click_hourly_monitor'
	object = 'event_archiver_dm303'
	insert_reports_data(content, object, dt, hr)

	#calc_chain_ratio_hr('event_report_sdk3x_normal_data', '2014-07-24', 8)
	#calc_chain_ratio_day('clk_hourly_monitor', '2014-07-25')
