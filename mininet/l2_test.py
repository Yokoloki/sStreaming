import pymongo
import random
import re

from mininet.net import Mininet
from mininet.node import RemoteController, Switch
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def getPortNo(intf):
    return int(re.match("[h|s](\d+)-eth(\d+)", str(intf)).group(2))

def exportToDB(net, as_map):
    conn = pymongo.Connection("127.0.0.1")
    conn.drop_database("sStreaming")
    db = conn["sStreaming"]
    db.Version.insert({"Version": random.randint(1, 2**32)})

    for host in net.hosts:
        name = str(host)
        as_ = as_map[name]
        db.Node.insert({"name": name,
                        "type": "host",
                        "as": as_})
        for port_no in host.intfs.keys():
            intf = host.intfs[port_no]
            intf_info = {"node": name,
                         "port_no": port_no,
                         "mac": intf.mac}
            if intf.ip != None:
                intf_info["ip"] = intf.ip
                intf_info["prefixLen"] = intf.prefixLen
                db.ARP.insert({"ip": intf.ip, "mac": intf.mac})
            db.Intf.insert(intf_info)
    for switch in net.switches:
        name = str(switch)
        as_ = as_map[name]
        db.Node.insert({"name": name,
                        "type": "inn",
                        "as": as_,
                        "dpid": int(switch.dpid)})
        for port_no in switch.intfs.keys():
            intf = switch.intfs[port_no]
            intf_info = {"node": name,
                         "dpid": int(switch.dpid),
                         "port_no": port_no,
                         "mac": intf.mac}
            if intf.ip != None:
                intf_info["ip"] = intf.ip
                intf_info["prefixLen"] = intf.prefixLen
                db.ARP.insert({"ip": intf.ip, "mac": intf.mac})
            db.Intf.insert(intf_info)
    for link in net.links:
        intf1 = link.intf1
        intf2 = link.intf2
        db.Link.insert({"src_name": str(intf1.node),
                        "src_port": getPortNo(intf1),
                        "dst_name": str(intf2.node),
                        "dst_port": getPortNo(intf2)})
        if type(intf1.node) != type(intf2.node):
            intf = intf1 if type(intf1.node) == type(Switch) else intf2
            db.Node.update({"name": str(intf.node)},
                           {"$set": {"type": "ext"}})



def deploy():
    net = Mininet(controller=RemoteController)
    c0 = net.addController('c0')

    as_map = {}
    info("*** Adding switch\n")
    switches = {}
    for i in xrange(4):
        switch = net.addSwitch("s%d" % (i+1))
        as_map[str(switch)] = 1
        switches[i] = switch

    for i in xrange(3):
        host_ip = "172.18.1.%d/24" % (i+1)
        host = net.addHost("h%d" % (i+1), ip=host_ip)
        as_map[str(host)] = 1
        link = net.addLink(host, switches[i])

    info("*** Creating links\n")
    for i in xrange(3):
        link = net.addLink(switches[i],
                           switches[3])

    info("*** Starting network\n")
    net.start()
    c0.start()
    for k, v in switches.items():
        v.start([c0])
        v.cmd("ovs-vsctl set Bridge s%d protocols=OpenFlow13" % (k+1))

    exportToDB(net, as_map)
    CLI(net)

    info("*** Stopping network\n")
    net.stop()

if __name__ == "__main__":
    setLogLevel("info")
    deploy()
