import sys
import socket
import time
import struct
from ControllerOrder import *

if __name__ == "__main__":
	BUFF_SIZE = 1024
	hostIP = '172.18.219.60'
	hostPort = 9999

	startTime = time.clock()

	#[flow_id, bitrate, period, dstPort, dstIP1, dstIP2, dstIP3, dstIP4, message = ""]
	order = ControllerOrder(flow_id = 1, bitrate = 1000, period = 5, 
		dstPort = 9999, dstIP1 = 172, dstIP2 = 18, 
		dstIP3 = 219, dstIP4 = 5)
	value = order.getData()
	struct1 = struct.Struct('8I992s')

	pack_data = struct1.pack(*value)

	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	#s.setsockopt(socket.SOL_IP, socket.IP_HDRINCL, 1)
	s.sendto(pack_data, (hostIP, hostPort))

	message, addr = s.recvfrom(BUFF_SIZE)
	print message
	endTime = time.clock();
	s.close()
	print (endTime - startTime)




