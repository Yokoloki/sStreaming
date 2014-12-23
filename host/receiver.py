import socket
import struct
import eventlet
import logging
from eventlet import greenthread

BUFF_SIZE = 1024
WINDOW_SIZE = 500
PRINT_F = 1.0
PERCENT_SYMBLE = '%'

struct1 = struct.Struct('2q6i984s')

flow_map = {}


class LiveStream(object):
	window_begin = 0L
	window_end = 0L
	__RECBuffer = []
	currentLossCount = 0L
	totalLossCount = 0L
	currentBlock_id = 0L
	remainPacket = 0L
	__currentBitrate = 0.0			#The current bitrate of the flow
	__globalPacketLossRate = 0.0	#Globle packet loss rate
	__currentPacketLossRate = 0.0	#In the last minute period, the packet loss rate
	__currentJitter = 0.0

	def __init__(self, flow_id, remainPacket):
		self.__RECBuffer = []
		self.flow_id = flow_id
		self.remainPacket = remainPacket

	def setWindow(self, window_begin):
		self.window_begin = window_begin
		self.window_end = self.window_begin + WINDOW_SIZE

	def __setRECBuffer(self):
		for i in range(self.window_begin, self.window_end + 1):
			if i in self.__RECBuffer:
				self.__RECBuffer.remove(i)
			else:
				self.setWindow(window_begin = i)
				break

	#to be continued, because the function need to be optimised
	def calPacketLoss(self):
		if self.currentBlock_id < self.window_end:
			self.__RECBuffer.append(self.currentBlock_id)
			self.__setRECBuffer()
			if self.currentBlock_id != self.window_begin:
				myLoggging.info('flow_id: %d packet disordering, block_id: %d'%(self.flow_id, 
					self.currentBlock_id))
			
			return
		else:
			lossPackets = 0
			old_window_begin = self.window_begin
			old_window_end = self.window_end

			new_window_begin = self.currentBlock_id - WINDOW_SIZE + 1
			self.setWindow(new_window_begin)
			self.__RECBuffer.append(self.currentBlock_id)
			self.__setRECBuffer()

			if new_window_begin < old_window_end:
				for i in range(old_window_begin, new_window_begin):
					if i not in self.__RECBuffer:
						myLoggging.info('flow_id: %d loss packet, block_id: %d'%(self.flow_id, i))
						lossPackets += 1
					else:
						self.__RECBuffer.remove(i)
			else:
				for i in range(old_window_begin, old_window_end):
					if i not in self.__RECBuffer:
						myLoggging.info('flow_id: %d loss packet, block_id: %d'%(self.flow_id, i))
						lossPackets += 1
					else:
						self.__RECBuffer.remove(i)

				lossPackets += new_window_begin - old_window_begin
				myLoggging.info('flow_id: %d loss packet, block_id: [%d, %d]'%(self.flow_id, 
					new_window_begin, old_window_begin - 1))

			self.currentLossCount += lossPackets
			self.totalLossCount += lossPackets
						
	def __setCurrentPacketLossRate(self):
		self.__currentPacketLossRate = self.currentLossCount / (self.__currentBitrate * PRINT_F) * 100


	def __setGlobalPacketLossRate(self):
		pass

	def __setCurrentBitrate(self, previousBlock_id = -1):
		if previousBlock_id != -1:
			self.__currentBitrate =  (self.currentBlock_id - previousBlock_id) / PRINT_F
		
	def __setCurrentJitter(self, previousBitrate):
		self.__currentJitter = self.__currentBitrate - previousBitrate

	def getGlobalPacketLossRate(self):
		self.__setGlobalPacketLossRate()
		return self.__globalPacketLossRate

	def getCurrentPacketLossRate(self):
		self.__setCurrentPacketLossRate()
		return self.__currentPacketLossRate

	def getCurrentBitrate(self, previousBlock_id = -1):
		self.__setCurrentBitrate(previousBlock_id)
		return self.__currentBitrate

	def getCurrentJitter(self, previousBitrate):
		if previousBitrate == 0:
			return 0.0
		self.__setCurrentJitter(previousBitrate)
		return self.__currentJitter

def printThread(liveStream):
	while True:
		previousBlock_id = liveStream.currentBlock_id
		previousBitrate = liveStream.getCurrentBitrate()
		liveStream.currentLossCount = 0

		greenthread.sleep(PRINT_F)
		print 'stream%d current bitrate: %.2fkb/s, jitter: %.2fkb/s, packet loss rate: %.2f%s'%(liveStream.flow_id, 
			liveStream.getCurrentBitrate(previousBlock_id), 
			liveStream.getCurrentJitter(previousBitrate), 
			liveStream.getCurrentPacketLossRate(), 
			PERCENT_SYMBLE)
		print liveStream.totalLossCount
 
if __name__ == '__main__':
	#setup logging
	formatter = logging.Formatter('%(asctime)s - %(levelname)s: [line%(lineno)d - %(message)s]')
	ip = 'test'
	fileHandler = logging.FileHandler('../log/%s.log'%ip)
	fileHandler.setFormatter(formatter)

	myLoggging = logging.getLogger()
	myLoggging.addHandler(fileHandler)
	myLoggging.setLevel(logging.INFO)

	listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	listener.bind(('', 9998))
	listener.settimeout(1)

	pool = eventlet.GreenPool(10000)
	while True:
		try:
			message, addr = listener.recvfrom(BUFF_SIZE)
			unpack_data = struct1.unpack(message)

			flow_id = unpack_data[2]
			block_id = unpack_data[0]
			remainPacket = unpack_data[1]

			if flow_id in flow_map:
				liveStream = flow_map.get(flow_id)
				liveStream.currentBlock_id = block_id
				liveStream.calPacketLoss()
			else:
				liveStream = LiveStream(flow_id, remainPacket)
				liveStream.currentBlock_id = block_id
				liveStream.setWindow(window_begin = block_id + 1)

				thread = pool.spawn(printThread, liveStream)

				flow_map[flow_id] = liveStream

		except socket.timeout:
			print 'no more stream or network error'

		except (SystemExit, KeyboardInterrupt):
			print 'error'
			listener.close()
			break

		finally:
			greenthread.sleep()
