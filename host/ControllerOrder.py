import struct
import time
import logging
from eventlet import greenthread

BUFF_SIZE = 1024

#logging set up
formatter = logging.Formatter('%(asctime)s - %(levelname)s: [line%(lineno)d - %(message)s]')
fileHandler = logging.FileHandler('../log/generateStream.log')
fileHandler.setFormatter(formatter)

myLoggging = logging.getLogger()
myLoggging.addHandler(fileHandler)
myLoggging.setLevel(logging.INFO)

    

class ControllerOrder(object):
    def __init__(self, flow_id, bitrate, period, dstPort, 
        dstIP1, dstIP2, dstIP3, dstIP4, message = ""):
        self.flow_id = flow_id
        self.bitrate = bitrate
        self.period = period
        self.dstPort = dstPort
        self.dstIP1 = dstIP1
        self.dstIP2 = dstIP2
        self.dstIP3 = dstIP3
        self.dstIP4 = dstIP4
        self.message = message

    def getData(self):
        return [self.flow_id, self.bitrate, self.period, self.dstPort, 
        self.dstIP1, self.dstIP2, self.dstIP3, self.dstIP4, self.message]

class streamPacket(object):
	def __init__(self, flow_id, block_id, srcPort = 9998, 
        srcIP1 = 127, srcIP2 = 0, srcIP3 = 0, srcIP4 = 1, message = ""):
		self.flow_id = flow_id
		self.block_id = block_id
		self.srcIP1 = srcIP1
		self.srcIP2 = srcIP2
		self.srcIP3 = srcIP3
		self.srcIP4 = srcIP4
		self.srcPort = srcPort
		self.message = message

	def getData(self):
		return [self.flow_id, self.block_id, self.srcPort, 
        self.srcIP1, self.srcIP2, self.srcIP3, self.srcIP4, self.message]


def generatePacket(flow_id, block_id, srcPort = 9998, 
    srcIP1 = 127, srcIP2 = 0, srcIP3 = 0, srcIP4 = 1, message = ""):

	packet = streamPacket(flow_id = flow_id, block_id = block_id, srcPort = srcPort, 
        srcIP1 = srcIP1, srcIP2 = srcIP2, srcIP3 = srcIP3, 
        srcIP4 = srcIP4, message = message)
	return packet

def generateAndSendNewStream(order, clientSocket, threadMap):
    myLoggging.info('Generating stream. flow_id: %d'%(order.flow_id))
    block_id = 0L
    struct1 = struct.Struct('q6i992s')

    dstIP = "%d.%d.%d.%d"%(order.dstIP1, order.dstIP2, 
        order.dstIP3, order.dstIP4)

    #when generate a packet, we can add the paras: srcPort, srcIP and message
    value = generatePacket(order.flow_id, block_id).getData()

    pack_data = struct1.pack(*value)

    if order.period < 0:
        pass
    else:
        for i in range(order.period):
            startTime = time.clock()
            for j in range(order.bitrate):   
                clientSocket.sendto(pack_data, (dstIP, order.dstPort))
                block_id += 1
                
            endTime = time.clock()

            if 1 - endTime + startTime > 0:
                #time.sleep(1 - endTime + startTime)
                pass
            print 'flow: %d sending %d packets'%(order.flow_id, block_id)

            greenthread.sleep(0)

    #Take away the finished greenThread
    del threadMap[order.flow_id]

    myLoggging.info('Generating stream finished. flow_id: %d'%(order.flow_id))
    return block_id

def stopThread(stopThread, flow_id):
    if flow_id in stopThread:
        myLoggging.info('flow_id: %d stop streaming'%(flow_id))
        greenThread = stopThread.get(flow_id)
        greenThread.kill()
        return
    else:
        return