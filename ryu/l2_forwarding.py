import pymongo
import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event, event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp

class EventPacketIn(event.EventBase):
    def __init__(self, msg, decoded_pkt):
        super(EventPacketIn, self).__init__()
        self.msg = msg
        self.decoded_pkt = decoded_pkt

class EventReload(event.EventBase):
    def __init__(self):
        super(EventReload, self).__init__()


class L2Forwarding(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(L2Forwarding, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('L2Forwarding')
        self.logger.setLevel(logging.DEBUG)
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]
        self.logger.debug("L2Forwarding: init")
        self.dps = {}
        self.graphs = {}
        self.as_map = {}
        self.mac_map = {}
        self.port_map = {}

    @set_ev_cls(EventReload, CONFIG_DISPATCHER)
    def _reload_handler(self, ev):
        self.logger.debug("L2Forwarding: _reload_handler")
        del self.dps
        del self.graphs
        del self.as_map
        del self.mac_map
        del self.port_map

        self.dps = {}
        self.graphs = {}
        self.as_map = {}
        self.mac_map = {}
        self.port_map = {}

        switches = self.db.Switch.find()
        for sw in switches:
            graph = self.graphs.setdefault(sw["as"], nx.Graph())
            graph.add_node(sw["dpid"])
            self.as_map[sw["dpid"]] = sw["as"]
        ports = self.db.Port.find()
        for port in ports:
            self.mac_map[port["mac"]] = port["dpid"]
            if "adj-dpid" in port:
                dpid1 = port["dpid"]
                dpid2 = port["adj-dpid"]
                as1 = self.as_map[dpid1]
                as2 = self.as_map[dpid2]
                if as1 == as2:
                    self.graphs[as1].add_edge(dpid1, dpid2)
                    self.port_map[(dpid1, dpid2)] = port["port"]

    @set_ev_cls(EventPacketIn, MAIN_DISPATCHER)
    def _l2_forwarding_handler(self, ev):
        self.logger.debug("L2Forwarding: _l2_forwarding_handler")
        msg = ev.msg
        decoded_pkt = ev.decoded_pkt

        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        dpid = datapath.id
        eth_dst = decoded_pkt['ethernet'].dst

        if eth_dst not in self.mac_map:
            self.logger.info("L2Forwarding: dst %s not in mac_map" % eth_dst)
            return False

        dst_dpid = self.mac_map[eth_dst]
        as1 = self.as_map[dpid]
        as2 = self.as_map[dst_dpid]
        if as1 != as2:
            self.logger.info("L2Forwarding: dst %s is not in the same AS as dp%s" %
                    (eth_dst, dpid))
            return False
        graph = self.graphs[as1]
        path = graph.shortest_path(source=dpid, target=dst_dpid)
        assert(len(path) > 1)
        for i in xrange(len(path)-1):
            out_port = self.port_map[(path[i], path[i+1])]
            self.add_l2_flow(dpid, eth_dst, out_port)

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
        self.logger.debug("ARPProxy: %s - %s" % (dst_ip, dst_mac))
        return True

    def add_l2_flow(self, dpid, eth_dst, out_port):
        datapath = self.dps[dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        priority = 5
        match = parser.OFPMatch(eth_dst=eth_dst)
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def reg_dp(self, datapath):
        self.dps[datapath.id] = datapath
