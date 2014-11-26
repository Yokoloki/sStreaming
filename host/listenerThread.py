import socket
import struct
import eventlet
import time
import sys
import logging
from ControllerOrder import *
from eventlet import greenthread


if __name__ == "__main__":
    struct1 = struct.Struct('8i992s')
    threadMap = {}
    
    print 'Controller Order listener socket listening on port 9999'

    listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener.bind(("172.18.219.60", 9999))
    listener.settimeout(0.95)
    #listener = eventlet.listen(("172.18.219.60", 9999))
    pool = eventlet.GreenPool(10000)
    counter = 0
    while True:
        try:
            message, addr = listener.recvfrom(BUFF_SIZE)
            unpacked_data = struct1.unpack(message)
            print 'received new controller order'
            print 'from IP: %s\tport: %d'%(addr[0], addr[1])

            if pool.free() == 0:
                print 'GreenThred pool full. Reject order.'
                listener.sendto("Server busy, Reject order", addr)
                continue
            #if period <= 0 , try to stop the specific streaming with flow_id
            if unpacked_data[2] <=0 :
                print 'Stop Streaming order received, try to stop stream: %d'%(unpacked_data[0])
                
                stopThread(threadMap, unpacked_data[0])
                listener.sendto("stop streaming order archieved", addr)
                continue;

            #if flow_id exsists, ignore the order
            if unpacked_data[0] in threadMap:
                print 'flow_id: %d exsists, ignore this order.'%(unpacked_data[0])
                listener.sendto("ignore order", addr)
                continue

            print 'new flow_id: %d\tbitrate: %d\tperiod: %ds'%(unpacked_data[0], 
                unpacked_data[1], unpacked_data[2])

            listener.sendto("start streaming", addr)
            #Here, we should do some progress over unpacked_data
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            order = ControllerOrder(flow_id = unpacked_data[0], 
                bitrate = unpacked_data[1], period = unpacked_data[2], 
                dstPort = unpacked_data[3], dstIP1 = unpacked_data[4], 
                dstIP2 = unpacked_data[5], dstIP3 = unpacked_data[6], 
                dstIP4 = unpacked_data[7], message = unpacked_data[8])

            greenThread = pool.spawn(generateAndSendNewStream, order, clientSocket, threadMap)
            
            #And put the greenThread into the map
            threadMap[order.flow_id] = greenThread

            #greenthread.sleep(0)
            
        except socket.timeout:
            print 'socket time out'
            #greenthread.sleep(0)
        except (SystemExit, KeyboardInterrupt):
            print 'error'
            listener.close()
            break
        finally:
            greenthread.sleep(0)
