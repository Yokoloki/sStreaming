import sys
import socket
import io
import time
import struct
import ctypes
from ControllerOrder import *

BUFF_SIZE = 1024
hostIP = '172.18.219.60'
hostPort = 9999

startTime = time.clock()

#[flow_id, bitrate, period, dstPort, dstIP1, dstIP2, dstIP3, dstIP4, message = ""]
order = ControllerOrder(1, 200, 5, 9999, 172, 18, 219, 5)
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




