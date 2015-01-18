# -*- coding: utf-8 -*-

"""thrift文件处理的通用工具"""

__author__ = ["zhaolingzhi@xxxxxxxx.cn", 'PangBo <pangbo@xxxxxxxx.cn>']


from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class FileInOut(object):

	def open(self, filepath, mode):
		"""打开文件，准备用thrift TBinaryProtocol 读或者写

		如果文件名是.bz2结尾，将会自动使用bzip2压缩或者解压缩

		Args:
			filepath str - 文件路径
			mode - 文件读写模式
		"""
		if filepath.endswith('.bz2'):
			import bz2
			f = bz2.BZ2File(filepath, mode)
		else:
			f = open(filepath, mode)
		self.__trans = TTransport.TFileObjectTransport(f)
		self.__prot = TBinaryProtocol.TBinaryProtocol(self.__trans)

	def read(self, t):
		"""读一个t对象

		Args:
			t - a thrift object
		Returns:
			bool - True读取成功，False读取失败
		"""
		try:
			t.read(self.__prot)
			return True
		except EOFError, e:
			return False

	def write(self, t):
		t.write(self.__prot)


	def close(self):
		if hasattr(self, '__trans'):
			self.__trans.flush()
			self.__trans.close()
			del self.__trans
		if hasattr(self, '__prot'):
			del self.__prot

	def getRecordcount(self, t):
		event_count = 0
		appwall_count = 0
		dbox_count = 0
		while 1 == 1 :
			if self.read(t):
				if int(t.sdkType) == 30:
					appwall_count += 1
				elif int(t.sdkType) == 31:
					dbox_count += 1
				else:
					event_count += 1
			else :
				break	
		return event_count, appwall_count,dbox_count

class FastFileInOut(FileInOut):
	'''快速，可能丢数据 '''

	def open(self, p, mode):
		if p.endswith('.bz2'):
			import bz2
			f = bz2.BZ2File(p, mode)
		else:
			f = open(p, mode)
		self.__trans = TTransport.TFileObjectTransport(f)
		self.__trans = TTransport.TBufferedTransport(self.__trans, 256 * 1024)
		self.__prot = TBinaryProtocol.TBinaryProtocolAccelerated(self.__trans)
