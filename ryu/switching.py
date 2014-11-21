import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event, event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology.event import *
from events import *
from ryu.lib.packet import packet, ethernet
from ryu.lib.port_no import port_no_to_str
from ryu.lib.dpid import dpid_to_str


class Switching(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Switching, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('Switching')
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Switching: init")
        self.multipath = False
        #dpid -> datapath
        self.dps = {}
        #dpid -> [{hw_addr, name, port_no, dpid}]
        self.dp_ports = {}
        #mac -> host
        self.hosts = {}
        #(src, dst) -> out_port
        self.link_outport = {}
        #flow_id - > [{"dpid", "match", "action"}]
        self.flows = {}
        #link -> flow_id_involved
        self.link_to_flows = {}
        #flow_id -> links_used_by_flow
        self.graph = nx.Graph()

    def enable_multipath(self):
        self.multipath = True

    @set_ev_cls(EventDpReg, CONFIG_DISPATCHER)
    def _dp_reg_handler(self, ev):
        datapath = ev.datapath
        self.dps[datapath.id] = datapath

    @set_ev_cls(EventHostReg, MAIN_DISPATCHER)
    def _host_reg_handler(self, ev):
        self.hosts[ev.host.mac] = ev.host

    @set_ev_cls(EventHostRequest, MAIN_DISPATCHER)
    def _host_request_handler(self, req):
        dpid = req.dpid
        hosts = []
        if dpid is None:
            for host in self.hosts.values():
                hosts.append(host)
        elif dpid in self.dps:
            for host in self.hosts.values():
                if host.dpid == dpid:
                    hosts.append(host)

        rep = EventHostReply(req.src, hosts)
        self.reply_to_request(req, rep)

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"])
        self.dp_ports[dpid] = msg["ports"]
        self.graph.add_node(dpid)

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"])
        if dpid in self.dps:
            del self.dps[dpid]
        if dpid in self.dp_ports:
            del self.dp_ports[dpid]
        if dpid in self.graph.nodes():
            self.graph.remove_node(dpid)
        for mac in self.hosts.keys():
            if dpid == self.hosts[mac].dpid:
                del self.hosts[mac]
        for (src, dst) in self.link_to_flows.keys():
            if src == dpid or dst == dpid:
                self.del_related_flows(src, dst)

    @set_ev_cls(EventLinkAdd)
    def _link_add_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"])
        src_port_no = int(msg["src"]["port_no"])
        dst_dpid = int(msg["dst"]["dpid"])
        dst_port_no = int(msg["dst"]["port_no"])
        self.link_outport[(src_dpid, dst_dpid)] = src_port_no
        self.link_outport[(dst_dpid, src_dpid)] = dst_port_no
        self.graph.add_edge(src_dpid, dst_dpid)

    @set_ev_cls(EventLinkDelete)
    def _link_del_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"])
        src_port_no = int(msg["src"]["port_no"])
        dst_dpid = int(msg["dst"]["dpid"])
        dst_port_no = int(msg["dst"]["port_no"])
        if (src_dpid, dst_dpid) in self.link_outport:
            del self.link_outport[(src_dpid, dst_dpid)]
        if (dst_dpid, src_dpid) in self.link_outport:
            del self.link_outport[(dst_dpid, src_dpid)]
        if (src_dpid, dst_dpid) in self.graph.edges() or \
                (dst_dpid, src_dpid) in self.graph.edges():
                    self.graph.remove_edge(src_dpid, dst_dpid)
        self.del_related_flows(src_dpid, dst_dpid)

    def del_related_flows(self, src, dst):
        if (src, dst) in self.link_to_flows or \
                (dst, src) in self.link_to_flows:
                    if (src, dst) not in self.link_to_flows:
                        src, dst = dst, src
                    flow_ids = self.link_to_flows[(src, dst)]
                    for flow_id in flow_ids:
                        self.del_flow_by_id(flow_id)
                    del self.link_to_flows[(src, dst)]

    def del_flow_by_id(self, flow_id):
        if flow_id not in self.flows:
            return
        for entry in self.flows[flow_id]:
            dpid = entry["dpid"]
            if dpid not in self.dps:
                continue
            out_port = entry["out_port"]
            out_group = entry["out_group"]
            match = entry["match"]
            self.del_switch_flow(dpid, out_port, out_group, match)
        del self.flows[flow_id]

    @set_ev_cls(Event_Switching_PacketIn, MAIN_DISPATCHER)
    def _switching_handler(self, ev):
        msg = ev.msg
        pkt = ev.pkt

        in_port = msg.match["in_port"]

        dpid = msg.datapath.id
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst

        if eth_dst not in self.hosts:
            self.logger.debug("Switching: %s has not been discovered" % eth_dst)
            return False

        host = self.hosts[eth_dst]
        dst_dpid = host.dpid
        dst_out_port = host.port_no
        if not nx.has_path(self.graph, dpid, dst_dpid):
            return False

        if self.multipath:
            self.create_mp_flow(dpid, eth_dst, dst_dpid, dst_out_port)
        else:
            self.create_flow(dpid, eth_dst, dst_dpid, dst_out_port)
        #Send to last switch directly
        datapath = self.dps[dst_dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(dst_out_port)]
        out = parser.OFPPacketOut(datapath=datapath,
                                  buffer_id=ofproto.OFP_NO_BUFFER,
                                  in_port=ofproto.OFPP_CONTROLLER,
                                  actions=actions,
                                  data=msg.data)
        datapath.send_msg(out)
        self.logger.debug("Switching: flow{eth_dst:%s} created" % eth_dst)
        return True

    def create_flow(self, src_dpid, eth_dst, dst_dpid, dst_out_port):
        if src_dpid == dst_dpid:
            return

        path = nx.shortest_path(self.graph, source=src_dpid, target=dst_dpid)

        flow_id = hash(eth_dst) % (2**30)
        self.flows[flow_id] = []
        for i in xrange(len(path)-1):
            out_port = self.link_outport[(path[i], path[i+1])]
            self.add_switch_flow(flow_id, path[i], eth_dst, out_port)

            flow_ids = self.link_to_flows.setdefault((path[i], path[i+1]), set())
            flow_ids.add(flow_id)
        self.add_switch_flow(flow_id, dst_dpid, eth_dst, dst_out_port)

    def create_mp_flow(self, src_dpid, eth_dst, dst_dpid, dst_out_port):
        if src_dpid == dst_dpid:
            return

        paths = nx.all_shortest_paths(self.graph, source=src_dpid, target=dst_dpid)
        paths = list(paths)
        path_count = len(paths)
        path_len = len(paths[0])

        flow_id = hash(eth_dst) % (2**30)
        self.flows[flow_id] = []
        for i in xrange(path_len-1):
            rules = {}
            #Aggregrate rules
            for j in xrange(path_count):
                out_ports = rules.setdefault(paths[j][i], set())
                port = self.link_outport[(paths[j][i], paths[j][i+1])]
                out_ports.add(port)

                flow_ids = self.link_to_flows.setdefault((paths[j][i], paths[j][i+1]), set())
                flow_ids.add(flow_id)
            for src_dpid in rules.keys():
                if len(rules[src_dpid]) > 1:
                    self.add_mp_switch_flow(flow_id, src_dpid, eth_dst, rules[src_dpid])
                else:
                    self.add_switch_flow(flow_id, src_dpid, eth_dst, rules[src_dpid].pop())
        self.add_switch_flow(flow_id, dst_dpid, eth_dst, dst_out_port)

    def add_switch_flow(self, flow_id, dpid, eth_dst, out_port):
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
        self.flows[flow_id].append({"dpid": dpid,
                                    "out_port": out_port,
                                    "out_group": ofproto.OFPG_ANY,
                                    "match": match})

    def add_mp_switch_flow(self, flow_id, dpid, eth_dst, out_ports):
        datapath = self.dps[dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        group_id = flow_id
        buckets = []
        for port in out_ports:
            actions = [parser.OFPActionOutput(port)]
            buckets.append(parser.OFPBucket(actions=actions))

        gmod = parser.OFPGroupMod(datapath=datapath,
                                  command=ofproto.OFPGC_ADD,
                                  type_=ofproto.OFPGT_SELECT,
                                  group_id=group_id,
                                  buckets=buckets)
        datapath.send_msg(gmod)

        priority = 5
        match = parser.OFPMatch(eth_dst=eth_dst)
        actions = [parser.OFPActionGroup(group_id, ofproto.OFPGT_SELECT)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst, idle_timeout=300)
        datapath.send_msg(mod)
        self.flows[flow_id].append({"dpid": dpid,
                                    "out_port": ofproto.OFPP_ANY,
                                    "out_group": group_id,
                                    "out_ports": list(out_ports),
                                    "match": match})

    def del_switch_flow(self, dpid, out_port, out_group, match):
        if dpid not in self.dps:
            return
        datapath = self.dps[dpid]
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        if out_group != ofproto.OFPG_ANY:
            gmod = parser.OFPGroupMod(datapath=datapath,
                                      command=ofproto.OFPGC_DELETE,
                                      type_=ofproto.OFPGT_SELECT,
                                      group_id=out_group)
            datapath.send_msg(gmod)

        mod = parser.OFPFlowMod(datapath=datapath,
                                command=ofproto.OFPFC_DELETE,
                                out_port=out_port,
                                out_group=out_group,
                                match=match,
                                instructions=[])
        datapath.send_msg(mod)

