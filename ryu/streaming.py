import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3

from ryu.topology.events import *
from events import *
from addrs import *
from algorithms import *


class Streaming(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Streaming, self).__init__(*args, **kwargs)
        self.logger = logging.basicConfig(format="Streaming: %(message)s")
        self.logger.setLevel(logging.DEBUG)
        # nx.Graph for path calculation
        self.graph = nx.Graph()
        # Shortest path for the whole network
        self.paths = None
        self.pathlens = None
        # dpid -> port -> host
        self.hosts = {}
        # (src, dst) -> out_port
        self.link_outport = {}
        # Dict containing information about stream
        # stream_id -> src{dpid, in_port}
        #              eth_dst
        #              ip_dst
        #              rate
        #              clients{dpid->out_ports}
        #              curr_flows{dpid->(prev, in_port, out_ports)}
        #              links
        self.streams = {}
        # link -> stream_id
        self.link_to_streams = {}
        # streams that fail to build due to topology change
        self.failed_streams = set()
        self.algorithm = Shortest_Path_Heuristic()

    def reg_DPSet(self, dpset):
        self.dpset = dpset

    @set_ev_cls(EventHostReg)
    def _host_reg_handler(self, ev):
        dpid = ev.host.dpid
        port_no = ev.host.port_no
        self.hosts[dpid][port_no] = host

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        self.graph.add_node(dpid)
        self.hosts[dpid] = {}

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        if dpid in self.graph.nodes():
            self.graph.remove_node(dpid)
        if dpid in self.hosts:
            del self.hosts[dpid]
        inf_streams = set()
        for (src, dst) in self.link_to_streams.keys():
            if (src == dpid) or (dst == dpid):
                inf_streams |= self.link_to_streams[(src, dst)]
        for stream_id in inf_stream:
            self.cal_flows_for_stream(stream_id, ev)

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
            self.link_to_streams[(src_dpid, dst_dpid)] = set()
            for stream_id in self.failed_streams.copy():
                self.cal_flows_for_stream(stream_id, ev)

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
        inf_stream = self.link_to_streams.get((src_dpid, dst_dpid), set()).copy()
        for stream_id in inf_stream:
            self.cal_flows_for_stream(stream_id, ev)

    @set_ev_cls(EventStreamSourceEnter)
    def _source_enter_handler(self, ev):
        self.streams[ev.stream_id] = {"rate": ev.rate,
                                      "eth_dst": ev.eth_dst,
                                      "ip_dst": ev.ip_dst,
                                      "clients": {},
                                      "curr_flows": {},
                                      "links": set()}
        self.streams[ev.stream_id]["src"] = {"dpid": ev.src_dpid,
                                             "in_port": ev.src_in_port}

    @set_ev_cls(EventStreamSourceLeave)
    def _source_leave_handler(self, ev):
        stream_id = ev.stream_id
        if stream_id not in self.streams:
            self.logger.info("source leaving a non-existing stream%d",
                             stream_id)
            return False
        curr_flows = self.streams[ev.stream_id]["curr_flows"]
        # Clean up
        for dpid, flow in curr_flows.items():
            self.mod_stream_flow(dpid, stream_id, flow, None)
        for link in self.streams[stream_id]["links"]:
            if link in self.link_to_streams:
                self.link_to_streams[link].discard(stream_id)
        del self.streams[ev.stream_id]

    @set_ev_cls(EventStreamClientEnter)
    def _client_enter_handler(self, ev):
        stream_id = ev.stream_id
        if stream_id not in self.streams:
            self.logger.info("client joining a non-existing stream%d",
                             stream_id)
            return False
        client_ports = self.streams[stream_id]["clients"].setdefault(ev.dpid, set())
        client_ports.add(ev.out_port)
        self.cal_flows_for_stream(stream_id, ev)

    @set_ev_cls(EventStreamClientLeave)
    def _client_leave_handler(self, ev):
        stream_id = ev.stream_id
        if stream_id not in self.streams:
            self.logger.info("client leaving a non-existing stream%d",
                             stream_id)
            return False
        self.cal_flows_for_stream(stream_id, ev)
        client_ports = self.streams[stream_id]["clients"][ev.dpid]
        client_ports.remove(ev.out_port)
        if len(client_ports) == 0:
            del self.streams[stream_id]["clients"][ev.dpid]

    @set_ev_cls(EventPacketIn, MAIN_DISPATCHER)
    def _streaming_handler(self, ev):
        msg = ev.msg
        pkt = ev.pkt

        # datapath = msg.datapath
        # ofproto = datapath.ofproto
        # parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        # dpid = datapath.id
        dst_ip = pkt.get_protocol(ipv4.ipv4).dst
        stream_id = get_stream_id(dst_ip)

        if stream_id not in self.streams:
            self.logger.info("recv unregistered stream%d", stream_id)
            return False
        if dpid not in self.streams[stream_id]["curr_flows"]:
            self.logger.info("packet of stream %d is not "
                             "supposed to recv in dp%d",
                             stream_id, dpid)
            return False

        flow = self.streams[stream_id]["curr_flows"][dpid]
        if in_port != flow["in_port"]:
            self.logger.info("packet of stream%d is supposed "
                             "to recv from %d:%d, not %d:%d",
                             stream_id, dpid, flow["in_port"], dpid, in_port)
            return False
        self.logger.info("flow of stream%d is not installed as excepted",
                         stream_id)
        return False

    def update_paths(self):
        self.paths = nx.shortest_path(self.graph)
        self.pathlens = {}
        for src in self.paths:
            self.pathlens[src] = {}
            for dst in self.paths[src]:
                self.pathlens[src][dst] = len(self.paths[src][dst])

    def cal_flows_for_stream(self, stream_id, ev):
        if isinstance(EventSwitchBase, ev) or isinstance(EventLinkBase, ev):
            self.update_paths()
        for link in self.streams[stream_id]["links"]:
            if link in self.link_to_streams[link]:
                self.link_to_streams[link].discard(stream_id)
        self.streams[stream_id]["links"] = set()
        new_flows = self.algorithm.cal(self.streams[stream_id],
                                       self.link_outport,
                                       self.paths, self.pathlens, ev)
        if new_flows is not None:
            to_mod_set = set()
            to_mod_set.update(self.streams[stream_id]["curr_flows"].keys())
            to_mod_set.update(new_flows.keys())
            for dpid in to_mod_set:
                prev_flow = self.streams[stream_id]["curr_flows"].get(dpid)
                curr_flow = new_flows.get(dpid)
                self.mod_stream_flow(dpid, stream_id, prev_flow, curr_flow)
            for dpid, flow in new_flows.items():
                if flow["prev"] != -1:
                    src, dst = dpid, flow["prev"]
                    if src > dst:
                        src, dst = dst, src
                    self.streams[stream_id]["links"].add((src, dst))
                    self.link_to_streams[(src, dst)].add(stream_id)
            if stream_id in self.failed_streams:
                self.failed_streams.remove(stream_id)
        else:
            new_flows = {}
            src_dpid = self.streams[stream_id]["src"]["dpid"]
            src_in_port = self.streams[stream_id]["src"]["in_port"]
            new_flows[src_dpid] = {"prev": -1,
                                   "in_port": src_in_port,
                                   "out_ports": []}
            for dpid in self.streams[stream_id]["curr_flows"].keys():
                prev_flow = self.streams[stream_id]["curr_flows"][dpid]
                curr_flow = new_flows.get(dpid)
                self.mod_stream_flow(dpid, stream_id, prev_flow, curr_flow)
            self.failed_streams.add(stream_id)
        self.streams[stream_id]["curr_flows"] = new_flows

    # flow = {in_port, set(out_ports)}
    def mod_stream_flow(self, dpid, stream_id, prev_flow, curr_flow):
        datapath = self.dpset.get(dpid)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        if datapath is None:
            return False
        if prev_flow is None:
            prev_flow = {"in_port": -1, "out_ports": []}
        if curr_flow is None:
            curr_flow = {"in_port": -1, "out_ports": []}

        prev_in_port = prev_flow["in_port"]
        curr_in_port = curr_flow["in_port"]
        prev_out_ports = prev_flow["out_ports"]
        curr_out_ports = curr_flow["out_ports"]

        if len(prev_out_ports) != 0:
            # Del existing group
            gmod = parser.OFPGroupMod(datapath=datapath,
                                      command=ofproto.OFPGC_DEL,
                                      type_=ofproto.OFPGT_ALL,
                                      group_id=stream_id)
            datapath.send_msg(gmod)
        if len(curr_out_ports) != 0:
            # Add new group
            buckets = []
            for port in curr_out_ports:
                actions = [parser.OFPActionOutput(port)]
                if port in self.hosts[dpid]:
                    host = self.hosts[dpid][port]
                    eth_dst = host.mac
                    ipv4_dst = host.ip
                    actions.append(parser.OFPActionSetField(eth_dst=eth_dst))
                    actions.append(parser.OFPActionSetField(ipv4_dst=ipv4_dst))
                buckets.append(parser.OFPBucket(actions=actions))
            gmod = parser.OFPGroupMod(datapath=datapath,
                                      command=ofproto.OFPGC_ADD,
                                      type_=ofproto.OFPGT_ALL,
                                      group_id=stream_id,
                                      buckets=buckets)
            datapath.send_msg(gmod)

        if prev_in_port != -1:
            # Del first
            out_port = ofproto.OFPP_ANY
            out_group = ofproto.OFPG_ANY
            if len(prev_out_ports) != 0:
                out_group = stream_id
            match = parser.OFPMatch(in_port=curr_in_port, eth_dst=eth_dst)
            mod = parser.OFPFlowMod(datapath=datapath,
                                    command=ofproto.OFPFC_DELETE,
                                    out_port=out_port,
                                    out_group=out_group,
                                    match=match,
                                    instructions=[])
            datapath.send_msg(mod)

        if curr_in_port != -1:
            # Add new flow
            priority = 5
            match = parser.OFPMatch(in_port=in_port, eth_dst=eth_dst)
            actions = []
            if len(curr_out_ports) != 0:
                actions.append(parser.OFPActionGroup(stream_id,
                                                     ofproto.OFPGT_ALL))
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                                 actions)]
            mod = parser.OFPFlowMod(datapath=datapath,
                                    priority=priority,
                                    match=match,
                                    instructions=inst)
            datapath.send_msg(mod)

        return True
