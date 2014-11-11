import argparse
import json
import pymongo

from mininet.net import Mininet
from mininet.link import TCLink
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def parseJSON(args):
    with file(args.f) as f:
        topo = json.load(f)
    return topo

def genNet(args):
    print "Generator Mode has not been implemented yet."
    exit(1)

def deploy(topo):
    info("Description: %s\n" % topo["summary"]["Description"])
    info("Gen-time:    %s\n" % topo["summary"]["Gen-time"])
    info("Node-count:  %s\n" % topo["summary"]["Node-count"])
    info("Link-count:  %s\n" % topo["summary"]["Link-count"])
    info("AS-count:    %s\n" % topo["summary"]["As-count"])

    conn = pymongo.Connection("localhost")
    conn.drop_database('sStreaming')
    db = conn['sStreaming']

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
        as_map[node_info["id"]] = node_info["as"]
        hex_mac = "%012x" % node_info["id"]
        mac = ':'.join([hex_mac[0:2],
                        hex_mac[2:4],
                        hex_mac[4:6],
                        hex_mac[6:8],
                        hex_mac[8:10],
                        hex_mac[10:12]])
        switch = net.addSwitch(name, dpid=mac)
        if node_info["type"] == "ext":
            base = as_ip_pool.get(node_info["as"], 0)
            assert(base + 4 < 255)
            as_ip_pool[node_info["as"]] = base + 4
            host_ip = "172.19.%d.%d/30" % (node_info["as"], base+1)
            switch_ip = "172.19.%d.%d/30" % (node_info["as"], base+2)
            host = net.addHost('h%d' % node_info["id"],
                               ip=host_ip,
                               defaultRoute="h%d-eth0" % node_info["id"])
            link = net.addLink(host, switch)
            ext_switches.add(node_info["id"])
            link.intf2.setIP(switch_ip)
            db.Port.insert({"name": str(link.intf2),
                            "dpid": node_info["id"],
                            "ip": switch_ip})
            db.ARP.insert({"ip": host_ip, "mac": link.intf1.MAC()})
            db.ARP.insert({"ip": switch_ip, "mac": link.intf2.MAC()})
        switches[node_info["id"]] = switch
        db.Switch.insert({"dpid": node_info["id"],
                          "as": node_info["as"],
                          "type": node_info["type"]})

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
        src_bson = {"name": str(link.intf1),
                    "dpid": link_info["src"],
                    "adj": str(link.intf2)}
        dst_bson = {"name": str(link.intf2),
                    "dpid": link_info["dst"],
                    "adj": str(link.intf1)}
        if (link_info["src"] in ext_switches) \
                and (link_info["dst"] in ext_switches):
            as1 = as_map[link_info["src"]]
            as2 = as_map[link_info["dst"]]
            as1, as2 = min(as1, as2), max(as1, as2)
            base = inter_as_ip_pool.get((as1, as2), 0)
            assert(base + 4 < 255)
            inter_as_ip_pool[(as1, as2)] = base + 4
            switch_ip1 = "10.%d.%d.%d/30" % (as1, as2, base+1)
            switch_ip2 = "10.%d.%d.%d/30" % (as1, as2, base+2)
            link.intf1.setIP(switch_ip1)
            link.intf2.setIP(switch_ip2)
            src_bson["ip"] = switch_ip1
            dst_bson["ip"] = switch_ip2
            db.ARP.insert({"ip": switch_ip1, "mac": link.intf1.MAC()})
            db.ARP.insert({"ip": switch_ip2, "mac": link.intf2.MAC()})
        elif link_info["src"] in ext_switches:
            as1 = as_map[link_info["src"]]
            base = as_ip_pool.get(as1, 1)
            assert(base < 255)
            as_ip_pool[as1] = base + 1
            switch_ip1 = "172.18.%d.%d/24" % (as1, base)
            link.intf1.setIP(switch_ip1)
            src_bson["ip"] = switch_ip1
            db.ARP.insert({"ip": switch_ip1, "mac": link.intf1.MAC()})
        elif link_info["dst"] in ext_switches:
            as2 = as_map[link_info["dst"]]
            base = as_ip_pool.get(as2, 1)
            assert(base < 255)
            as_ip_pool[as2] = base + 1
            switch_ip2 = "172.18.%d.%d/24" % (as2, base)
            link.intf2.setIP(switch_ip2)
            dst_bson["ip"] = switch_ip2
            db.ARP.insert({"ip": switch_ip2, "mac": link.intf2.MAC()})
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
