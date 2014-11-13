import pymongo
import random
import re

from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def getPortNo(intf):
    return int(re.match("s(\d+)-eth(\d+)", str(intf)).group(2))


def deploy():
    conn = pymongo.Connection("127.0.0.1")
    conn.drop_database("sStreaming")
    db = conn["sStreaming"]
    db.Version.insert({"Version": random.randint(1, 2**32)})

    net = Mininet(controller=RemoteController)
    c0 = net.addController('c0')

    info("*** Adding switch\n")
    switches = {}
    for i in xrange(4):
        switch = net.addSwitch("s%d" % (i+1))
        db.Switch.insert({"dpid": switch.dpid, "as": 1, "type": "ext"})
        switches[i] = switch

    for i in xrange(3):
        host_ip = "172.18.1.%d/24" % (i+1)
        host = net.addHost("h%d" % (i+1), ip=host_ip)
        link = net.addLink(host, switches[i])

        db.Port.insert({"dpid": switches[i].dpid,
                        "port": getPortNo(link.intf2),
                        "mac": link.intf2.MAC()})
        db.ARP.insert({"ip": host_ip, "mac": link.intf1.MAC()})

    info("*** Creating links\n")
    for i in xrange(3):
        link = net.addLink(switches[i],
                           switches[3])
        src_bson = {"dpid": switches[i].dpid,
                    "port": getPortNo(link.intf1),
                    "mac": link.intf1.MAC(),
                    "adj-dpid": switches[3].dpid,
                    "adj-port": getPortNo(link.intf2)}
        dst_bson = {"dpid": switches[3].dpid,
                    "port": getPortNo(link.intf2),
                    "mac": link.intf2.MAC(),
                    "adj-dpid": switches[i].dpid,
                    "adj-port": getPortNo(link.intf1)}
        db.Port.insert(src_bson)
        db.Port.insert(dst_bson)
    conn.close()

    info("*** Starting network\n")
    net.start()
    c0.start()
    for k, v in switches.items():
        v.start([c0])
        v.cmd("ovs-vsctl set Bridge s%d protocols=OpenFlow13" % k)
    CLI(net)

    info("*** Stopping network\n")
    net.stop()

if __name__ == "__main__":
    setLogLevel("info")
    deploy()
