#!/usr/local/bin/python2.7 
# coding: utf-8

import sys
import os
from os import path

basedir = path.realpath(path.join(path.dirname(__file__), '..'))
os.chdir(basedir)
sys.path.append(path.join(basedir, 'lib'))
sys.path.append(path.join(basedir, 'lib/gen-py'))


from searchui_types.ttypes import AdEventReport
from file_helper import FileInOut

		

		
def statistics_antiserver(filepath):
	if not os.path.isfile(filepath): 
		print 0
	o = AdEventReport()
	fileinout = FileInOut()
	fileinout.open(filepath, 'r')
	event_count, appwall_count, dbox_count  = fileinout.getRecordcount(o)
	fileinout.close
	print "%s,%s,%s" % (event_count, appwall_count, dbox_count)



if __name__ == '__main__':
	statistics_antiserver(sys.argv[1])
