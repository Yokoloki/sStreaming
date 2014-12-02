import socket
import struct
import eventlet
from eventlet import greenthread

BUFF_SIZE = 1024
WINDOW_SIZE = 10
flow_map = {}
struct1 = struct.Struct('2q6i984s')
packet_count = {}
loss_count = {}

class LiveStream(object):
	currentBuffer = []				#The packet buffer 
	currentBlock_id = 0L
	remainPacket = 0L
	__currentBitrate = 0			#The current bitrate of the flow
	__globalPacketLossRate = 0.0	#Globle packet loss rate
	__currentPacketLossRate = 0.0	#In the last minute period, the packe loss rate	


	def __init__(self, flow_id, remainPacket):
		self.flow_id = flow_id
		self.remainPacket = remainPacket


	def __setCurrentPacketLossRate(self):

		pass
	def __setGlobalPacketLossRate(self):
		pass

	def __setCurrentBitrate(self, previousBlock_id):
		self.__currentBitrate =  self.currentBlock_id - previousBlock_id
		pass


	def getGlobalPacketLossRate(self):
		self.__setGlobalPacketLossRate()
		return self.__globalPacketLossRate

	def getCurrentPacketLossRate(self):
		self.__setCurrentPacketLossRate()
		return self.__currentPacketLossRate

	def getCurrentBitrate(self, previousBlock_id):
		self.__setCurrentBitrate(previousBlock_id)
		return self.__currentBitrate


def calCurrentBitrate(liveStream):
	previousBlock_id = liveStream.currentBlock_id
	while True:
		greenthread.sleep(5)
		print 'stream%d current bitrate: %dkb/s'%(liveStream.flow_id, 
			liveStream.getCurrentBitrate(previousBlock_id) / 5.0)
		previousBlock_id = liveStream.currentBlock_id
	pass

def calCurrentPacketLossRate(liveStream):
	'''While True:
		print 'stream%d current packet loss rate: %d\%'%(liveStream.flow_id, 
			liveStream.getCurrentPacketLossRate())
		sleep(1)'''
	pass

if __name__ == '__main__':
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
			else:
				liveStream = LiveStream(flow_id, remainPacket)

				calBitrateThread = pool.spawn(calCurrentBitrate, liveStream)

				calPacketLossThread = pool.spawn(calCurrentPacketLossRate, liveStream)

				flow_map[flow_id] = liveStream

		except socket.timeout:
			print 'no more stream or network error'

		except (SystemExit, KeyboardInterrupt):
			print 'error'
			listener.close()
			break

		finally:
			greenthread.sleep()


    	