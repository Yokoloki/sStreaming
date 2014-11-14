import pymongo
import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event, event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet

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
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]
        self.logger.debug("Switching: init")
        #dpid -> datapath
        self.dps = {}
        #nx.Graph for path calculation
        self.graph = nx.Graph()
        #dpid -> name
        self.name_map = {}
        #name -> dpid
        self.dpid_map = {}
        #mac -> name
        self.mac_map = {}
        #(name1, name2) -> out_port
        self.port_map = {}

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
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst

        if eth_dst not in self.mac_map:
            return False

        src_name = self.name_map[dpid]
        dst_name = self.mac_map[eth_dst]

        path = nx.shortest_path(self.graph, source=src_name, target=dst_name)
        assert(len(path) > 1)
        for i in xrange(len(path)-1):
            out_port = self.port_map[(path[i], path[i+1])]
            self.add_switch_flow(self.dpid_map[path[i]], eth_dst, out_port)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        actions = [parser.OFPActionOutput(self.port_map[(path[0], path[1])])]
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

    def add_switch_flow(self, dpid, eth_dst, out_port):
        datapath = self.dps[dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        priority = 5
        match = parser.OFPMatch(eth_dst=eth_dst)
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst, idle_timeout=300)
        datapath.send_msg(mod)

