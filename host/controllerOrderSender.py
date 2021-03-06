import sys
import socket
import time
import struct
from ControllerOrder import *

if __name__ == "__main__":
	hostIP = '172.18.219.5'
	hostPort = 9999

	startTime = time.clock()

	#[flow_id, bitrate, period, dstPort, dstIP1, dstIP2, dstIP3, dstIP4, message = ""]
	L = range(5)
	for i in L[::1]:
		order = ControllerOrder(flow_id = i + 5, bitrate = 50 + i * 50, period = 0, 
			dstPort = 9998, dstIP1 = 172, dstIP2 = 18, 
			dstIP3 = 219, dstIP4 = 200)
		value = order.getData()
		print value
		struct1 = struct.Struct('8i992s')

		pack_data = struct1.pack(*value)

		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		#s.setsockopt(socket.SOL_IP, socket.IP_HDRINCL, 1)
		s.sendto(pack_data, (hostIP, hostPort))

		message, addr = s.recvfrom(BUFF_SIZE)
		print message
		endTime = time.clock();
		print (endTime - startTime)