import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology.event import *
from events import *
from ryu.lib.packet import ethernet


class Switching(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Switching, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        self.multipath = False
        # mac -> host
        self.hosts = {}
        # (src, dst) -> out_port
        self.link_outport = {}
        # flow_id - > [{"dpid", "match", "action"}]
        self.flows = {}
        # link(src<dst) -> flows
        self.link_to_flows = {}
        # flow -> links(src<dst)
        self.flow_to_links = {}
        self.graph = nx.Graph()

    def reg_DPSet(self, dpset):
        self.dpset = dpset

    def set_wrapper(self, wrapper):
        self.wrapper = wrapper

    def enable_multipath(self):
        self.multipath = True

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
        elif self.dpset.get(dpid):
            for host in self.hosts.values():
                if host.dpid == dpid:
                    hosts.append(host)

        rep = EventHostReply(req.src, hosts)
        self.reply_to_request(req, rep)

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        self.graph.add_node(dpid)

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        if dpid in self.graph.nodes():
            self.graph.remove_node(dpid)
        for mac in self.hosts.keys():
            if dpid == self.hosts[mac].dpid:
                del self.hosts[mac]
        for (src, dst) in self.link_to_flows.keys():
            if (src == dpid) or (dst == dpid):
                self.del_related_flows(src, dst)

    @set_ev_cls(EventLinkAdd)
    def _link_add_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"], 16)
        src_port_no = int(msg["src"]["port_no"], 16)
        dst_dpid = int(msg["dst"]["dpid"], 16)
        dst_port_no = int(msg["dst"]["port_no"], 16)
        self.link_outport[(src_dpid, dst_dpid)] = src_port_no
        self.link_outport[(dst_dpid, src_dpid)] = dst_port_no
        self.graph.add_edge(src_dpid, dst_dpid)
        if src_dpid < dst_dpid:
            self.link_to_flows[(src_dpid, dst_dpid)] = set()

    @set_ev_cls(EventLinkDelete)
    def _link_del_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"], 16)
        # src_port_no = int(msg["src"]["port_no"], 16)
        dst_dpid = int(msg["dst"]["dpid"], 16)
        # dst_port_no = int(msg["dst"]["port_no"], 16)
        if (src_dpid, dst_dpid) in self.link_outport:
            del self.link_outport[(src_dpid, dst_dpid)]
        if (dst_dpid, src_dpid) in self.link_outport:
            del self.link_outport[(dst_dpid, src_dpid)]
        if (src_dpid, dst_dpid) in self.graph.edges() or \
                (dst_dpid, src_dpid) in self.graph.edges():
                    self.graph.remove_edge(src_dpid, dst_dpid)
        if (src_dpid, dst_dpid) in self.link_to_flows:
            self.del_related_flows(src_dpid, dst_dpid)

    def del_related_flows(self, src, dst):
        inf_flows = self.link_to_flows[(src, dst)].copy()
        for flow_id in inf_flows:
            self.del_flow_by_id(flow_id)

    def del_flow_by_id(self, flow_id):
        if flow_id not in self.flows:
            return
        for entry in self.flows[flow_id]:
            dpid = entry["dpid"]
            datapath = self.dpset.get(dpid)
            if datapath is None:
                continue
            out_port = entry["out_port"]
            out_group = entry["out_group"]
            match = entry["match"]
            self.del_switch_flow(dpid, out_port, out_group, match)
        del self.flows[flow_id]
        for link in self.flow_to_links[flow_id]:
            self.link_to_flows[link].discard(flow_id)
        del self.flow_to_links[flow_id]

    @set_ev_cls(Event_Switching_PacketIn, MAIN_DISPATCHER)
    def _switching_handler(self, ev):
        msg = ev.msg
        pkt = ev.pkt

        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id
        eth_src = pkt.get_protocol(ethernet.ethernet).src
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst
        in_port = msg.match["in_port"]

        if eth_dst not in self.hosts:
            self.logger.debug("%s has not been discovered" % eth_dst)
            ports = self.wrapper.get_flood_ports()
            for dpid, out_port in ports:
                if (dpid, out_port) == (datapath.id, in_port):
                    continue
                self.logger.info("flood to port:%d:%d", dpid, out_port)
                dp = self.dpset.get(dpid)
                actions = [parser.OFPActionOutput(out_port)]
                out = dp.ofproto_parser.OFPPacketOut(datapath=dp,
                                                     buffer_id=dp.ofproto.OFP_NO_BUFFER,
                                                     in_port=dp.ofproto.OFPP_CONTROLLER,
                                                     actions=actions,
                                                     data=msg.data)
                dp.send_msg(out)
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
        # Send to last switch directly
        datapath = self.dpset.get(dst_dpid)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(dst_out_port)]
        out = parser.OFPPacketOut(datapath=datapath,
                                  buffer_id=ofproto.OFP_NO_BUFFER,
                                  in_port=ofproto.OFPP_CONTROLLER,
                                  actions=actions,
                                  data=msg.data)
        datapath.send_msg(out)
        self.logger.debug("flow{%s -> %s} created" % (eth_src, eth_dst))
        return True

    def create_flow(self, src_dpid, eth_dst, dst_dpid, dst_out_port):
        flow_id = hash(eth_dst) % (2 ** 30)
        self.flows[flow_id] = []
        self.flow_to_links[flow_id] = set()
        if src_dpid != dst_dpid:
            path = nx.shortest_path(self.graph, source=src_dpid, target=dst_dpid)
            for i in xrange(len(path)-1):
                out_port = self.link_outport[(path[i], path[i+1])]
                self.add_switch_flow(flow_id, path[i], eth_dst, out_port)
                src, dst = path[i], path[i+1]
                if src > dst:
                    src, dst = dst, src
                self.link_to_flows[(src, dst)].add(flow_id)
                self.flow_to_links[flow_id].add((src, dst))
        self.add_switch_flow(flow_id, dst_dpid, eth_dst, dst_out_port)

    def create_mp_flow(self, src_dpid, eth_dst, dst_dpid, dst_out_port):
        flow_id = hash(eth_dst) % (2 ** 30)
        self.flows[flow_id] = []
        self.flow_to_links[flow_id] = set()
        if src_dpid != dst_dpid:
            paths = nx.all_shortest_paths(self.graph, source=src_dpid, target=dst_dpid)
            paths = list(paths)
            path_count = len(paths)
            path_len = len(paths[0])
            for i in xrange(path_len-1):
                rules = {}
                # Aggregrate rules
                for j in xrange(path_count):
                    out_ports = rules.setdefault(paths[j][i], set())
                    port = self.link_outport[(paths[j][i], paths[j][i+1])]
                    out_ports.add(port)
                    src, dst = paths[j][i], paths[j][i+1]
                    if src > dst:
                        src, dst = dst, src
                    self.link_to_flows[(src, dst)].add(flow_id)
                    self.flow_to_links[flow_id].add((src, dst))
                for src_dpid in rules.keys():
                    if len(rules[src_dpid]) > 1:
                        self.add_mp_switch_flow(flow_id, 
                                                src_dpid, 
                                                eth_dst, 
                                                rules[src_dpid])
                    else:
                        self.add_switch_flow(flow_id, 
                                             src_dpid, 
                                             eth_dst, 
                                             rules[src_dpid].pop())
        self.add_switch_flow(flow_id, dst_dpid, eth_dst, dst_out_port)

    def add_switch_flow(self, flow_id, dpid, eth_dst, out_port):
        datapath = self.dpset.get(dpid)
        if datapath is None: 
            return
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
        datapath = self.dpset.get(dpid)
        if datapath is None: 
            return
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
        datapath = self.dpset.get(dpid)
        if datapath is None:
            return
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
