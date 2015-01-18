#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
offerwall antispam
"""

import sys
import os
import argparse
import ConfigParser
import logging.config

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

basepath = os.path.realpath(os.path.dirname(__file__)+'/../')
sys.path.append(basepath+'/lib/gen-py')

from xxxxxxxx_pyutils import FacebookBase

class fbcli(object):
	def req(self, host, port, key=''):
		trans, cli = FacebookBase.client(host, port)
		c = cli.getCounters()
		trans.close()
		if (key != '') and (c.has_key(key)):
			print c[key]
		else:
			print c


if __name__ == '__main__':
	ap = argparse.ArgumentParser(description = 'xxxxxxxx offerwall antispam')
	ap.add_argument('-d', '--executeDir', type=str,
		help='execute directory', default = basepath)
	ap.add_argument('host', type=str,
		help='host', default = '127.0.0.1')
	ap.add_argument('port', type=int,
		help='port', default=12312)
	ap.add_argument('key', type=string,
		help='key', default='')

	args = ap.parse_args()

	os.chdir(args.executeDir)

	fbcli().req(args.host, args.port, args.key)


# extreme ways are back again
# vim: set noexpandtab ts=4 sts=4 sw=4 :
