import pymongo
import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event, event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet

IPV4_STREAMING = "224.1.0.0"

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

class Switching(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Switching, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('Switching')
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Switching: init")
        #dpid -> datapath
        self.dps = {}
        #nx.Graph for path calculation
        self.graph = nx.Graph()

    @set_ev_cls(EventReload, CONFIG_DISPATCHER)
    def _reload_handler(self, ev):
        self.logger.debug("Switching: _reload_handler")
        del self.dps
        del self.graph
        del self.name_map
        del self.dpid_map
        del self.mac_map
        del self.port_map

        self.dps = {}
        self.graph = nx.Graph()
        self.name_map = {}
        self.dpid_map = {}
        self.mac_map = {}
        self.port_map = {}

        nodes = self.db.Node.find()
        for node in nodes:
            self.graph.add_node(node["name"])
            if "dpid" in node:
                self.dpid_map[node["name"]] = node["dpid"]
                self.name_map[node["dpid"]] = node["name"]
        intfs = self.db.Intf.find()
        for intf in intfs:
            self.mac_map[intf["mac"]] = intf["node"]

        links = self.db.Link.find()
        for link in links:
            src_name = link["src_name"]
            dst_name = link["dst_name"]
            self.graph.add_edge(src_name, dst_name)
            self.port_map[(src_name, dst_name)] = link["src_port"]
            self.port_map[(dst_name, src_name)] = link["dst_port"]

    @set_ev_cls(EventPacketIn, MAIN_DISPATCHER)
    def _switching_handler(self, ev):
        msg = ev.msg
        pkt = ev.pkt

        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        dpid = datapath.id
        dst_ip = pkt.get_protocol(ipv4.ipv4)
        stream_id = get_stream_id(dst_ip)

        if not self.streamSimulator.hasReceiver(stream_id):
            #DROP ENTRY
            pass
        #GROUP ENTRY
        self.cal_paths_for_stream(stream_id)
        #FORWARD DATA
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        actions = [parser.OFPActionGroup(stream_id, ofproto.OFPGT_ALL)])]
        out = parser.OFPPacketOut(datapath=datapath,
                                  buffer_id=msg.buffer_id,
                                  in_port=in_port,
                                  actions=actions,
                                  data=data)
        datapath.send_msg(out)
        return True

    @set_ev_cls(EventRegDp, CONFIG_DISPATCHER)
    def _regdp_handler(self, ev):
        datapath = ev.datapath
        self.dps[datapath.id] = datapath

    def add_stream(self, dpid, stream_id, eth_dst, out_ports):
        datapath = self.dps[dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        buckets = []
        for port in out_ports:
            actions = [parser.OFPActionOutput(port)]
            buckets.append(parser.OFPBucket(actions=actions))
        if self.streamSimulator.isReceiver(stream_id, dpid):
            eth_src = ""
            eth_dst = ""
            ipv4_dst = ""
            actions = [parser.OFPActionSetField(eth_src=eth_src),
                       parser.OFPActionSetField(eth_dst=eth_dst),
                       parser.OFPActionSetField(ipv4_dst=ipv4_dst),
                       parser.OFPActionOutput(1)]
            buckets.append(parser.OFPBucket(actions=actions))

        gmod = parser.OFPGroupMod(datapath=datapath,
                                  command=ofproto.OFPGC_ADD,
                                  type_=ofproto.OFPGT_ALL,
                                  group_id=stream_id,
                                  buckets=buckets)
        datapath.send_msg(gmod)

        priority = 5
        match = parser.OFPMatch(in_port=in_port, eth_dst=eth_dst)
        actions = [parser.OFPActionGroup(stream_id, ofproto.OFPGT_ALL)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst, idle_timeout=300)
        datapath.send_msg(mod)

    def cal_paths_for_stream(self, stream_id):
        return
        #path calculation algorithm
        for i in xrange(path_len-1):
            rules = {}
            for j in xrange(path_count):
                out_ports = rules.setdefault(paths[j][i], set())
                port = self.port_map[(paths[j][i], paths[j][i+1])]
                out_ports.add(port)
            for src in rules.keys():
                self.add_multiswitch_flow(self.dpid_map[src],
                                               eth_dst,
                                               rules[src])

def ipv4_text_to_int(ip_text):
    if ip_text == 0:
        return 0
    assert isinstance(ip_text, str)
    return struct.unpack('!I', addrconv.ipv4.text_to_bin(ip_text))[0]

def ipv4_int_to_text(ip_int):
    assert isinstance(ip_int, (int, long))
    return addrconv.ipv4.bin_to_text(struct.pack('!I', ip_int))

def is_streaming(addr):
    addr_int = ipv4_text_to_int(addr)
    masked_int = addr_bin & 0xffff0000
    masked_text = ipv4_int_to_text(masked_int)
    return masked_text == IPV4_STREAMING

def get_stream_id(addr):
    assert is_streaming(addr)
    addr_int = ipv4_text_to_int(addr)
    stream_id = addr_bin & 0x0000ffff
    return stream_id
