import argparse
import json
import pymongo
import re
import random

from mininet.net import Mininet
from mininet.link import TCLink
from mininet.node import RemoteController, Switch, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def parseJSON(args):
    with file(args.f) as f:
        topo = json.load(f)
    return topo

def genNet(args):
    print "Generator Mode has not been implemented yet."
    exit(1)

def getPortNo(intf):
    return int(re.match("[s|h](\d+)-eth(\d+)", str(intf)).group(2))

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
            if intf.ip and intf.mac:
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
                        "dpid": int(switch.dpid, 16)})
        for port_no in switch.intfs.keys():
            intf = switch.intfs[port_no]
            intf_info = {"node": name,
                         "dpid": int(switch.dpid, 16),
                         "port_no": port_no,
                         "mac": intf.mac}
            if intf.ip and intf.mac:
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

def deploy(topo):
    info("Description: %s\n" % topo["summary"]["Description"])
    info("Gen-time:    %s\n" % topo["summary"]["Gen-time"])
    info("Node-count:  %s\n" % topo["summary"]["Node-count"])
    info("Link-count:  %s\n" % topo["summary"]["Link-count"])
    info("AS-count:    %s\n" % topo["summary"]["As-count"])

    net = Mininet(controller=RemoteController)
    c0 = net.addController('c0')

    info("*** Adding switch\n")
    switches = {}
    as_map = {}
    ext_switches = set()
    as_ip_pool = {}
    inter_as_ip_pool = {}
    for node_info in topo["nodes"]:
        name = "s%d" % node_info["id"]
        hex_mac = "%012x" % node_info["id"]
        mac = ':'.join([hex_mac[0:2],
                        hex_mac[2:4],
                        hex_mac[4:6],
                        hex_mac[6:8],
                        hex_mac[8:10],
                        hex_mac[10:12]])
        switch = net.addSwitch(name, dpid=mac, cls=OVSSwitch, protocols="OpenFlow13")
        as_map[str(switch)] = node_info["as"]
        if node_info["type"] == "ext":
            base = as_ip_pool.get(node_info["as"], 0)
            assert(base + 4 < 255)
            as_ip_pool[node_info["as"]] = base + 4
            host_ip = "172.19.%d.%d/30" % (node_info["as"], base+1)
            switch_ip = "172.19.%d.%d/30" % (node_info["as"], base+2)
            route = "dev h%d-eth0 via %s" % (node_info["id"], switch_ip.split("/")[0])
            host = net.addHost('h%d' % node_info["id"],
                               ip=host_ip,
                               defaultRoute=route)
            as_map[str(host)] = node_info["as"]
            link = net.addLink(host, switch)
            ext_switches.add(node_info["id"])
            link.intf2.setIP(switch_ip)
        switches[node_info["id"]] = switch

    info("*** Creating links\n")
    for link_info in topo["links"]:
        if "args" not in link_info:
            link = net.addLink(switches[link_info["src"]],
                        switches[link_info["dst"]],
                        cls=TCLink,
                        **topo["defaults"]["link"])
        else:
            link = net.addLink(switches[link_info["src"]],
                        switches[link_info["dst"]],
                        cls=TCLink,
                        **link_info["args"])
        if (link_info["src"] in ext_switches) \
                and (link_info["dst"] in ext_switches):
            as1 = as_map["s%d" % link_info["src"]]
            as2 = as_map["s%d" % link_info["dst"]]
            if as1 == as2:
                base = as_ip_pool.get(as1, 1)
                assert(base + 1 < 255)
                as_ip_pool[as1] = base + 2
                switch_ip1 = "172.18.%d.%d/24" % (as1, base)
                switch_ip2 = "172.18.%d.%d/24" % (as1, base+1)
            else:
                as1, as2 = min(as1, as2), max(as1, as2)
                base = inter_as_ip_pool.get((as1, as2), 0)
                assert(base + 4 < 255)
                inter_as_ip_pool[(as1, as2)] = base + 4
                switch_ip1 = "10.%d.%d.%d/30" % (as1, as2, base+1)
                switch_ip2 = "10.%d.%d.%d/30" % (as1, as2, base+2)
            link.intf1.setIP(switch_ip1)
            link.intf2.setIP(switch_ip2)
        elif link_info["src"] in ext_switches:
            as1 = as_map["s%d" % link_info["src"]]
            base = as_ip_pool.get(as1, 1)
            assert(base < 255)
            as_ip_pool[as1] = base + 1
            switch_ip1 = "172.18.%d.%d/24" % (as1, base)
            link.intf1.setIP(switch_ip1)
        elif link_info["dst"] in ext_switches:
            as2 = as_map["s%d" % link_info["dst"]]
            base = as_ip_pool.get(as2, 1)
            assert(base < 255)
            as_ip_pool[as2] = base + 1
            switch_ip2 = "172.18.%d.%d/24" % (as2, base)
            link.intf2.setIP(switch_ip2)

    exportToDB(net, as_map)

    info("*** Starting network\n")
    net.start()
    c0.start()
    CLI(net)

    info("*** Stopping network\n")
    net.stop()

if __name__ == "__main__":
    setLogLevel("info")
    parser = argparse.ArgumentParser(description="SDN Demo - Topology")
    subparsers = parser.add_subparsers()
    parser_i = subparsers.add_parser("parse", help="Parser Mode")
    parser_i.add_argument("f",
                          metavar="json_network_file",
                          help="Use the input network file to construct topology")
    parser_i.set_defaults(func=parseJSON)
    parser_g = subparsers.add_parser("gen", help="Generator Mode")
    parser_g.add_argument("-a",
                          metavar="arguments",
                          help="Generate and use a new network topology")
    parser_g.set_defaults(func=genNet)
    args = parser.parse_args()
    topo = args.func(args)
    deploy(topo)
