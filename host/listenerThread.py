import socket
import struct
import eventlet
import time
import sys
from ControllerOrder import *

if __name__ == "__main__":
    struct1 = struct.Struct('8I992s')

    BUFF_SIZE = 1024

    print 'Controller Order listener socket listening on port 9999'
    listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener.bind(("172.18.219.60", 9999))

    pool = eventlet.GreenPool()

    while True:
        try:
            message, addr = listener.recvfrom(BUFF_SIZE)
            unpacked_data = struct1.unpack(message)
            print 'received new controller order'
            print 'from IP: %s\tport: %d'%(addr[0], addr[1])
            print 'new flow_id: %d\tbitrate: %d\tperiod: %ds'%(unpacked_data[0], 
                unpacked_data[1], unpacked_data[2])
            listener.sendto("got it", addr)

            #Here, we should do some progress over unpacked_data
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            order = ControllerOrder(flow_id = unpacked_data[0], 
                bitrate = unpacked_data[1], period = unpacked_data[2], 
                dstPort = unpacked_data[3], dstIP1 = unpacked_data[4], 
                dstIP2 = unpacked_data[5], dstIP3 = unpacked_data[6], 
                dstIP4 = unpacked_data[7], message = unpacked_data[8])

            greenThread = pool.spawn(generateNewStream, order, clientSocket)
            greenThread.wait()
            

        except (SystemExit, KeyboardInterrupt):
            print 'error'
            listener.close()
            break
