import struct
import time
import logging
from eventlet import greenthread

BUFF_SIZE = 1024
SOCKET_SLEEP_TIME = 0.7

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
    srcPort = 9999
    srcIP1 = 127
    srcIP2 = 0
    srcIP3 = 0
    srcIP4 = 1
	
    def __init__(self, block_id, remainPacket, flow_id, message = ""):
        self.block_id = block_id
        self.remainPacket = remainPacket
        self.flow_id = flow_id
        self.message = message

    def getData(self):
        return [self.block_id, self.remainPacket, self.flow_id, self.srcPort, 
        self.srcIP1, self.srcIP2, self.srcIP3, self.srcIP4, self.message]


def generatePacket(block_id, remainPacket, flow_id, message = ""):
	packet = streamPacket(block_id = block_id, remainPacket = remainPacket, 
        flow_id = flow_id, message = message)
	return packet

def generateAndSendNewStream(order, clientSocket, threadMap):
    myLoggging.info('Generating stream. flow_id: %d'%(order.flow_id))
    struct1 = struct.Struct('2q6i984s')

    dstIP = "%d.%d.%d.%d"%(order.dstIP1, order.dstIP2, 
        order.dstIP3, order.dstIP4)

    block_id = 0L
    remainPacket = order.period * order.bitrate - block_id
    #when generate a packet, we can add the paras: srcPort, srcIP and message
    packet = generatePacket(block_id, remainPacket, order.flow_id)
    value = packet.getData()

    pack_data = struct1.pack(*value)
    clientSocket.sendto(pack_data, (dstIP, order.dstPort))

    value[3] = clientSocket.getsockname()[1]

    time_slot = 0
    while True:
        if order.period != 0 & time_slot > order.period:
            break
        time_slot += 1

        time_slice = 50
        time_sleep = SOCKET_SLEEP_TIME
        time_interval = order.bitrate / time_slice

        for j in range(order.bitrate):
            pack_data = struct1.pack(*value)   
            clientSocket.sendto(pack_data, (dstIP, order.dstPort))
            block_id += 1
            remainPacket -= 1
            value[0] = block_id
            value[1] = remainPacket
            if j % time_interval == 0:
                greenthread.sleep(time_sleep / time_slice)

        endTime = time.clock()

        #here we should add some code adjust the bitrate
        pass
        print 'flow: %d sending %d packets'%(order.flow_id, block_id)
        #greenthread.sleep(0)
        

    #Take away the finished greenThread
    if order.flow_id in threadMap:
        del threadMap[order.flow_id]
    
    clientSocket.close()
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