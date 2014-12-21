import argparse
import json
import pymongo
import re
import random

from mininet.net import Mininet
from mininet.link import TCLink
from mininet.node import RemoteController, Switch, UserSwitch, OVSSwitch, Node
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


class TopoBuilder(object):
    def __init__(self, topo, export=False, ext_access=True):
        super(TopoBuilder, self).__init__()
        self.topo = topo
        self.export = export
        self.ext_access = True
        self.net = Mininet()
        self.switches = {}
        self.hosts = {}
        self.as_map = {}
        self.ext_switches = set()
        self.inn_net = "192.18.%d.%d/16"
        self.ext_net = "10.1.%d.%d/16"
        self.ext_gw = "10.1.255.254/16"


    def exportToDB():
        net = self.net
        as_map = self.as_map
        conn = pymongo.Connection("127.0.0.1")
        conn.drop_database("sStreaming")
        db = conn["sStreaming"]
        db.Version.insert({"Version": random.randint(1, 2 ** 32)})

        for host in net.hosts:
            name = str(host)
            as_ = as_map[name]
            db.Node.insert({"name": name,
                            "type": "host",
                            "as": as_})
            for port_no in host.intfs.keys():
                intf = host.intfs[port_no]
                if not intf.mac:
                    continue
                ip, pLen = host.params["ip"].split("/")
                db.Intf.insert({"node": name,
                                "port_no": port_no,
                                "mac": intf.mac,
                                "ip": ip})
                db.ARP.insert({"ip": ip, "mac": intf.mac})
        for switch in net.switches:
            name = str(switch)
            as_ = as_map[name]
            db.Node.insert({"name": name,
                            "type": "inn",
                            "as": as_,
                            "dpid": int(switch.dpid, 16)})
            for port_no in switch.intfs.keys():
                intf = switch.intfs[port_no]
                if not intf.mac:
                    continue
                db.Intf.insert({"node": name,
                                "dpid": int(switch.dpid, 16),
                                "port_no": port_no,
                                "mac": intf.mac})
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

    def add_switches(self):
        info("*** Adding switches\n")
        for node_info in self.topo["nodes"]:
            name = "s%d" % node_info["id"]
            hex_mac = "%012x" % node_info["id"]
            mac = ':'.join([hex_mac[0:2],
                            hex_mac[2:4],
                            hex_mac[4:6],
                            hex_mac[6:8],
                            hex_mac[8:10],
                            hex_mac[10:12]])
            switch = self.net.addSwitch(name, dpid=mac, cls=UserSwitch, protocols="OpenFlow13")
            self.as_map[str(switch)] = node_info["as"]
            if node_info["type"] == "ext":
                self.ext_switches.add(node_info["id"])
            self.switches[node_info["id"]] = switch

    def add_hosts(self):
        info("*** Adding hosts\n")
        ip_base = 1
        for dpid in self.ext_switches:
            switch = self.switches[dpid]
            inn_host_ip = self.inn_net % (ip_base/256, ip_base%256)
            ip_base += 1
            host = self.net.addHost('h%d' % dpid, ip=inn_host_ip)
            self.hosts[dpid] = host
            self.as_map[str(host)] = self.as_map[str(switch)]

    def add_links(self):
        info("*** Creating links\n")
        for dpid in self.ext_switches:
            switch = self.switches[dpid]
            host = self.hosts[dpid]
            self.net.addLink(host, switch)
        
        for link_info in self.topo["links"]:
            if "args" not in link_info:
                link = self.net.addLink(self.switches[link_info["src"]],
                                        self.switches[link_info["dst"]],
                                        cls=TCLink,
                                        **topo["defaults"]["link"])
            else:
                link = self.net.addLink(self.switches[link_info["src"]],
                                        self.switches[link_info["dst"]],
                                        cls=TCLink,
                                        **link_info["args"])

    def fixNetworkManager(self, root, intf):
        # Prevent network-manager from messing with our interface
        cfile = "/etc/network/interfaces"
        line = "\niface %s inet manual\n" % intf
        config = open(cfile).read()
        if line not in config:
            print "*** Adding", line.strip(), "to", cfile
            with open(cfile, 'a') as f:
                f.write(line)
            root.cmd("service network-manager restart")

    def setup_nat(self):
        self.nat_switch = self.net.addSwitch("s%d" % 0xffffff, 
                                             cls=OVSSwitch, 
                                             protocols="OpenFlow13")
        ip_base = 1
        for host in self.hosts.values():
            host_ip = self.ext_net % (ip_base/256, ip_base%256)
            ip_base += 1
            link = self.net.addLink(host, self.nat_switch)
            link.intf1.setIP(host_ip)
        self.nat_root = Node("root", inNamespace=False)
        self.fixNetworkManager(self.nat_root, "root-eth0")
        link = self.net.addLink(self.nat_root, self.nat_switch)
        link.intf1.setIP(self.ext_gw)

    def config_hosts(self):
        info("*** Configuring hosts\n")
        for host in self.hosts.values():
            host.cmd("sysctl net.ipv6.conf.all.disable_ipv6=1")
            host.cmd("sysctl net.ipv4.icmp_echo_ignore_broadcasts=0")
            host.cmd("route add -net 224.1.0.0/16 %s-eth0" % str(host))
            if self.ext_access:
                host.cmd("ip route fulsh root 0/0")
                host.cmd("route add default gw", self.ext_gw.split("/")[0])


    def start_nat(self, inetIntf="em1", subnet="10.1.0.0/16"):
        localIntf = self.nat_root.defaultIntf()

        # Flush any currently active rules
        self.nat_root.cmd("iptables -F")
        self.nat_root.cmd("iptables -t nat -F")

        # Create default entries for unmatched traffic
        self.nat_root.cmd("iptables -P INPUT ACCEPT")
        self.nat_root.cmd("iptables -P OUTPUT ACCEPT")
        self.nat_root.cmd("iptables -P FORWARD DROP")

        # Configure NAT
        self.nat_root.cmd("iptables -I FORWARD -i", localIntf, "-d", subnet, "-j DROP")
        self.nat_root.cmd("iptables -A FORWARD -i", localIntf, "-s", subnet, "-j ACCEPT")
        self.nat_root.cmd("iptables -A FORWARD -i", inetIntf, "-d", subnet, "-j ACCEPT")
        self.nat_root.cmd("iptables -t nat -A POSTROUTING -o ", inetIntf, "-j MASQUERADE")

        # Instruct the kernel to perform forwarding
        self.nat_root.cmd("sysctl net.ipv4.ip_forward=1")

        self.nat_switch.start([self.controller])

    def stop_nat(self):
        # Flush any currently active rules
        self.nat_root.cmd("iptables -F")
        self.nat_root.cmd("iptables -t nat -F")
        
        # Instruct the kernel to stop forwarding
        self.nat_root.cmd("sysctl net.ipv4.ip_forward=0")

    def deploy(self):
        info("Description: %s\n" % topo["summary"]["Description"])
        info("Gen-time:    %s\n" % topo["summary"]["Gen-time"])
        info("Node-count:  %s\n" % topo["summary"]["Node-count"])
        info("Link-count:  %s\n" % topo["summary"]["Link-count"])
        info("AS-count:    %s\n" % topo["summary"]["As-count"])

        self.controller = self.net.addController('c1', 
                                                 RemoteController, 
                                                 port=6633)
        self.add_switches()
        self.add_hosts()
        self.add_links()

        if self.export:
            self.exportToDB()
        if self.ext_access:
            self.setup_nat()

    def start(self):
        info("*** Starting network\n")
        self.net.build()
        self.controller.start()
        for switch in self.switches.values():
            switch.start([self.controller])
        if self.ext_access:
            self.start_nat()

        self.config_hosts()
        CLI(self.net)

        info("*** Stopping network\n")
        if self.ext_access:
            self.stop_nat()
        self.net.stop()

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
    builder = TopoBuilder(topo)
    builder.deploy()
    builder.start()
