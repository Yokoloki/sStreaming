import socket
import struct
import eventlet
from eventlet import greenthread

BUFF_SIZE = 1024
WINDOW_SIZE = 100
PRINT_F = 5.0
PERCENT_SYMBLE = '%'

struct1 = struct.Struct('2q6i984s')

flow_map = {}


class LiveStream(object):
	currentBuffer = []				#The packet buffer
	__currentReceivedPacket = []
	currentLossCount = 0L			#
	totalLossCount = 0L
	currentBlock_id = 0L
	remainPacket = 0L
	__currentBitrate = 0.0			#The current bitrate of the flow
	__globalPacketLossRate = 0.0	#Globle packet loss rate
	__currentPacketLossRate = 0.0	#In the last minute period, the packet loss rate
	__currentJitter = 0.0

	def __init__(self, flow_id, remainPacket):
		self.__currentReceivedPacket = []
		currentBuffer = []
		self.flow_id = flow_id
		self.remainPacket = remainPacket

	def calPacketLoss(self):
		if self.currentBlock_id in self.currentBuffer:
			self.__currentReceivedPacket.append(self.currentBlock_id)

			return
		else:
			if self.currentBlock_id == self.currentBuffer[-1] + 1:
				if len(self.__currentReceivedPacket) == WINDOW_SIZE:
					self.currentBuffer = range(self.currentBlock_id, 
					self.currentBlock_id + WINDOW_SIZE)
					self.__currentReceivedPacket = []
					self.__currentReceivedPacket.append(self.currentBlock_id)
					return
			
			threshold = self.currentBlock_id - self.currentBuffer[WINDOW_SIZE/2] + self.currentBuffer[0]
			previousBuffer = range(self.currentBuffer[0], threshold + 1)
			self.currentBuffer = range(threshold, threshold + WINDOW_SIZE)

			lossPackets = self.currentBlock_id - self.currentBuffer[0] + 1

			for i in previousBuffer:
				if i in self.__currentReceivedPacket:
					lossPackets -= 1
					self.__currentReceivedPacket.remove(i)
				else:
					myLoggging.info('flow_id: %d loss packet, block_id: %d'%(self.flow_id, i))

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
 
if __name__ == '__main__':
	#setup logging
	formatter = logging.Formatter('%(asctime)s - %(levelname)s: [line%(lineno)d - %(message)s]')
	ip = ''
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
				liveStream.currentBuffer = range(block_id + 1, block_id + WINDOW_SIZE + 1)

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
