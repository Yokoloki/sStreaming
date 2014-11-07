import argparse
import json
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

    net = Mininet(controller=RemoteController)
    c0 = net.addController('c0')

    info("*** Adding switch\n")
    switches = {}
    for node_info in topo["nodes"]:
        name = "s%d" % node_info["id"]
        if node_info["type"] == "ext":
            hex_mac = "F%011x" % node_info["id"]
        else:
            hex_mac = "0%011x" % node_info["id"]
        mac = ':'.join([hex_mac[0:2],
                        hex_mac[2:4],
                        hex_mac[4:6],
                        hex_mac[6:8],
                        hex_mac[8:10],
                        hex_mac[10:12]])
        switch = net.addSwitch(name, dpid=mac)
        if node_info["type"] == "ext":
            host = net.addHost('h%d' % node_info["id"])
            net.addLink(switch, host)
        switches[node_info["id"]] = switch

    info("*** Creating links\n")
    for link_info in topo["links"]:
        if "args" not in link_info:
            net.addLink(switches[link_info["src"]],
                        switches[link_info["dst"]],
                        cls=TCLink,
                        **topo["defaults"]["link"])
        else:
            net.addLink(switches[link_info["src"]],
                        switches[link_info["dst"]],
                        cls=TCLink,
                        **link_info["args"])

    info("*** Starting network\n")
    net.build()
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
