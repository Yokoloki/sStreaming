import pymongo
import logging
import struct
import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event, event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ipv4
from ryu.lib import addrconv

class EventPacketIn(event.EventBase):
    def __init__(self, msg, pkt):
        super(EventPacketIn, self).__init__()
        self.msg = msg
        self.pkt = pkt

class EventReload(event.EventBase):
    def __init__(self):
        super(EventReload, self).__init__()

class EventRegDp(event.EventBase):
    def __init__(self, datapath):
        super(EventRegDp, self).__init__()
        self.datapath = datapath

class Routing(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Routing, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('Routing')
        self.logger.setLevel(logging.DEBUG)
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]
        self.logger.debug("Routing: init")
        self.graph = nx.Graph()
        self.subnets = set()
        #dpid -> datapath
        self.dps = {}
        #dpid -> name
        self.name_map = {}
        #name -> dpid
        self.dpid_map = {}
        #name -> ip
        self.ip_map = {}
        #(name, net) -> [(port_no, ip)]
        self.ports_map = {}
        #ip -> mac
        self.arp_table = {}

    @set_ev_cls(EventReload, CONFIG_DISPATCHER)
    def _reload_handler(self, ev):
        self.logger.debug("Routing: _reload_handler")
        del self.graph
        del self.subnets
        del self.dps
        del self.name_map
        del self.dpid_map
        del self.ip_map
        del self.ports_map
        del self.arp_table

        self.graph = nx.Graph()
        self.subnets = set()
        self.dps = {}
        self.name_map = {}
        self.dpid_map = {}
        self.ip_map = {}
        self.ports_map = {}
        self.arp_table = {}

        nodes = self.db.Node.find()
        for node in nodes:
            self.graph.add_node(node["name"])
            if "dpid" in node:
                self.dpid_map[node["name"]] = node["dpid"]
                self.name_map[node["dpid"]] = node["name"]
        intfs = self.db.Intf.find()
        for intf in intfs:
            if "ip" not in intf: continue
            name = intf["node"]
            port_no = intf["port_no"]
            self.arp_table[intf["ip"]] = intf["mac"]
            self.ip_map[intf["ip"]] = name
            subnet = ipv4_apply_mask(intf["ip"], int(intf["prefixLen"]))
            subnet += "/" + intf["prefixLen"]
            self.subnets.add(subnet)
            self.graph.add_edge(name, subnet)
            ports = self.ports_map.setdefault((name, subnet), [])
            ports.append((port_no, intf["ip"]))

    @set_ev_cls(EventPacketIn, MAIN_DISPATCHER)
    def _routing_handler(self, ev):
        self.logger.debug("Routing: _routing_handler")
        msg = ev.msg
        pkt = ev.pkt

        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        dpid = datapath.id
        eth_protocol = pkt.get_protocol(ethernet.ethernet)
        ip_protocol = pkt.get_protocol(ipv4.ipv4)

        dst_ip = ip_protocol.dst
        dst_mac = eth_protocol.dst

        src_name = self.name_map[dpid]
        dst_name = self.ip_map[dst_ip]

        subnet = self.get_subnet(dst_ip)

        path = nx.shortest_path(self.graph, source=src_name, target=dst_name)
        if len(path) > 2:
            self.logger.info("Routing: no path to %s" % dst_ip)
            return False

        for i in xrange(0, len(path)-2, 2):
            out_port, out_port_ip = self.ports_map[(path[i], path[i+1])][0]
            _, next_hop = self.ports_map[(path[i+2], path[i+1])][0]
            self.add_route(self.dpid_map[path[i]], subnet,
                           out_port, out_port_ip, next_hop)

        out_port, out_port_ip = self.ports_map[(path[0], path[1])][0]
        _, next_hop = self.ports_map[(path[2], path[1])][0]
        eth_protocol.src = self.arp_table[out_port_ip]
        eth_protocol.dst = self.arp_table[next_hop]
        ip_protocol.ttl -= 1
        pkt.serialize()

        actions = [parser.OFPActionOutput(out_port)]
        out = parser.OFPPacketOut(datapath=datapath,
                                  buffer_id=ofproto.OFP_NO_BUFFER,
                                  in_port=ofproto.OFPP_CONTROLLER,
                                  actions=actions,
                                  data=pkt.data)
        datapath.send_msg(out)
        return True

    @set_ev_cls(EventRegDp, CONFIG_DISPATCHER)
    def _regdp_handler(self, ev):
        datapath = ev.datapath
        self.dps[datapath.id] = datapath
        self.logger.debug("Routing reg dp %d" % datapath.id)

    def add_route(self, dpid, subnet, out_port, out_port_ip, next_hop):
        datapath = self.dps[dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        ip, prefixLen = subnet.split("/")
        src_mac = self.arp_table[out_port_ip]
        dst_mac = self.arp_table[next_hop]

        priority = 3
        match = parser.OFPMatch()
        match.set_ipv4_dst_masked(ipv4_text_to_int(ip),
                                  mask_ntob(prefixLen))

        actions = []
        actions.append(parser.OFPActionDecNwTtl())
        actions.append(parser.OFPActionSetField(eth_src=src_mac))
        actions.append(parser.OFPActionSetField(eth_dst=dst_mac))
        actions.append(parser.OFPActionOutput(out_port))

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def get_subnet(self, addr):
        for (subnet, mask) in self.subnets:
            if ipv4_apply_mask(addr, mask) == subnet:
                return (subnet, mask)
        return None


def ipv4_text_to_int(ip_text):
    if ip_text == 0:
        return ip_text
    assert isinstance(ip_text, (str, unicode))
    return struct.unpack("!I", addrconv.ipv4.text_to_bin(ip_text))[0]

def ipv4_int_to_text(ip_int):
    assert isinstance(ip_int, (int, long))
    return addrconv.ipv4.bin_to_text(struct.pack('!I', ip_int))

UINT32_MAX = 0xffffffff
def mask_ntob(mask):
    assert (mask >= 8) and (mask <= 32)
    return (UINT32_MAX << (32 - mask)) & UINT32_MAX

def ipv4_apply_mask(addr, prefixLen):
    assert isinstance(addr, (str, unicode))
    addr_int = ipv4_text_to_int(addr)
    return ipv4_int_to_text(addr_int & mask_ntob(prefixLen))
