import logging
import networkx as nx
from ryu.base import app_manager
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import ethernet, ipv4

from ryu.topology.event import *
from events import *
from addrs import *
from algorithms import *


DEFAULT_BANDWIDTH = 10
class Streaming(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _EVENTS = [EventHostStatChanged,
               EventSwitchStatChanged,
               EventStreamSourceEnter,
               EventStreamSourceLeave]

    def __init__(self, *args, **kwargs):
        super(Streaming, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        # nx.Graph for path calculation
        self.graph = nx.Graph()
        # Shortest path for the whole network
        self.paths = None
        self.pathlens = None
        # dpid -> port -> host
        self.port_to_host = {}
        # mac -> {host, sourcing, receving}
        self.host_table = {}
        # dpid -> {stream_id -> {dist, bandwidth}}
        self.switch_table = {}
        # dpids
        self.sw_to_update = set()
        # (src, dst) -> out_port
        self.link_outport = {}
        # Dict containing information about stream
        # stream_id -> src{dpid, in_port}
        #              eth_dst
        #              ip_dst
        #              rate
        #              clients{dpid->out_ports}
        #              m_tree{dpid->(parent, children)}
        #              bandwidth{dpid->pri}
        #              links
        self.streams = {}
        # link -> stream_id
        self.link_to_streams = {}
        # streams that fail to build due to topology change
        self.failed_streams = set()
        self.algorithm = Shortest_Path_Heuristic()

    def reg_DPSet(self, dpset):
        self.dpset = dpset

    @set_ev_cls(EventHostStatRequest, MAIN_DISPATCHER)
    def _host_stat_request_handler(self, req):
        mac = req.mac
        if mac is None:
            host_stat = self.host_table
        else:
            host_stat = {mac: self.host_table[mac]}
        rep = EventHostStatReply(req.src, host_stat)
        self.reply_to_request(req, rep)

    @set_ev_cls(EventSwitchStatRequest, MAIN_DISPATCHER)
    def _switch_stat_request_handler(self, req):
        dpid = req.dpid
        if dpid is None:
            sw_stat = self.switch_table
        else:
            sw_stat = {dpid: self.switch_table[dpid]}
        rep = EventSwitchStatReply(req.src, sw_stat)
        self.reply_to_request(req, rep)

    @set_ev_cls(EventHostReg)
    def _host_reg_handler(self, ev):
        dpid = ev.host.dpid
        port_no = ev.host.port_no
        self.port_to_host[dpid][port_no] = ev.host
        self.host_table[ev.host.mac] = {"host": ev.host,
                                        "sourcing": set(),
                                        "receving": set()}

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        self.graph.add_node(dpid)
        self.port_to_host[dpid] = {}
        self.switch_table[dpid] = {}
        self.update_paths()

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        if dpid in self.graph.nodes():
            self.graph.remove_node(dpid)
        if dpid in self.port_to_host:
            for host in self.port_to_host[dpid].values():
                del self.host_table[host.mac]
            del self.port_to_host[dpid]
        if dpid in self.switch_table:
            del self.switch_table[dpid]
        self.update_paths()
        for stream_id in self.streams:
            if self.streams[stream_id]["src"]["dpid"] == dpid:
                self.send_event_to_observers(EventStreamSourceLeave(stream_id))
        inf_streams = set()
        for (src, dst) in self.link_to_streams.keys():
            if (src == dpid) or (dst == dpid):
                inf_streams |= self.link_to_streams[(src, dst)]
        for stream_id in inf_streams:
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
        self.update_paths()
        if src_dpid < dst_dpid:
            self.link_to_streams[(src_dpid, dst_dpid)] = set()
            for stream_id in self.failed_streams.copy():
                self.cal_flows_for_stream(stream_id, ev)

    @set_ev_cls(EventLinkDelete)
    def _link_del_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"], 16)
        dst_dpid = int(msg["dst"]["dpid"], 16)
        # src_port_no = int(msg["src"]["port_no"], 16)
        # dst_port_no = int(msg["dst"]["port_no"], 16)
        # DO NOT CLEAR UP FOR PORT LOOKUP
        # if (src_dpid, dst_dpid) in self.link_outport:
        #    del self.link_outport[(src_dpid, dst_dpid)]
        # if (dst_dpid, src_dpid) in self.link_outport:
        #     del self.link_outport[(dst_dpid, src_dpid)]
        if (src_dpid, dst_dpid) in self.graph.edges() or \
                (dst_dpid, src_dpid) in self.graph.edges():
                    self.graph.remove_edge(src_dpid, dst_dpid)
        self.update_paths()
        inf_stream = self.link_to_streams.get((src_dpid, dst_dpid), set()).copy()
        for stream_id in inf_stream:
            self.cal_flows_for_stream(stream_id, ev)

    @set_ev_cls(EventStreamSourceEnter)
    def _source_enter_handler(self, ev):
        self.logger.info("EventStreamSourceEnter")
        stream_id = ev.stream_id
        if stream_id in self.streams:
            self.logger.info("source of stream%d already exist", stream_id)
            return
        self.streams[stream_id] = {"rate": ev.rate,
                                   "eth_dst": ev.eth_dst,
                                   "ip_dst": ev.ip_dst,
                                   "clients": {},
                                   "m_tree": {},
                                   "bandwidth": {},
                                   "links": set()}
        self.streams[stream_id]["src"] = {"mac": ev.src_mac,
                                          "dpid": ev.src_dpid,
                                          "in_port": ev.src_in_port}
        self.cal_flows_for_stream(stream_id, ev)
        self.update_host_table(ev.src_mac, "add", "sourcing", stream_id)

    @set_ev_cls(EventStreamSourceLeave)
    def _source_leave_handler(self, ev):
        stream_id = ev.stream_id
        if stream_id not in self.streams:
            self.logger.info("source leaving a non-existing stream%d",
                             stream_id)
            return False
        m_tree = self.streams[ev.stream_id]["m_tree"]
        # Clean up
        for dpid in m_tree.keys():
            self.mod_stream_flow(dpid, stream_id, None)
            self.update_switch_table(dpid, "del", stream_id)
        for link in self.streams[stream_id]["links"]:
            if link in self.link_to_streams:
                self.link_to_streams[link].discard(stream_id)
        for dpid, ports in self.streams[ev.stream_id]["clients"].items():
            if dpid not in self.port_to_host: continue
            for port in ports:
                mac = self.port_to_host[dpid][port]
                self.update_host_table(mac, "del", "receving", stream_id)
        self.update_host_table(ev.src_mac, "del", "sourcing", stream_id)
        del self.streams[stream_id]

    @set_ev_cls(EventStreamClientEnter)
    def _client_enter_handler(self, ev):
        stream_id = ev.stream_id
        if stream_id not in self.streams:
            self.logger.info("client joining a non-existing stream%d",
                             stream_id)
            return False
        client_ports = self.streams[stream_id]["clients"].setdefault(ev.dpid, 
                                                                     set())
        client_ports.add(ev.out_port)
        self.cal_flows_for_stream(stream_id, ev)
        self.update_host_table(ev.mac, "add", "receving", stream_id)

    @set_ev_cls(EventStreamClientLeave)
    def _client_leave_handler(self, ev):
        stream_id = ev.stream_id
        if stream_id not in self.streams:
            self.logger.info("client leaving a non-existing stream%d",
                             stream_id)
            return False
        client_ports = self.streams[stream_id]["clients"][ev.dpid]
        client_ports.remove(ev.out_port)
        self.cal_flows_for_stream(stream_id, ev)
        if len(client_ports) == 0:
            del self.streams[stream_id]["clients"][ev.dpid]
        self.update_host_table(ev.mac, "del", "receving", stream_id)

    @set_ev_cls(EventStreamBandwidthChange)
    def _bandwidth_change_handler(self, ev):
        stream_id = ev.stream_id
        dpid = ev.dpid
        bandwidth = ev.bandwidth
        curr_stat = self.streams[stream_id]["m_tree"].get(dpid)
        self.mod_stream_flow(dpid, stream_id, curr_stat, bandwidth)
        self.streams[stream_id]["bandwidth"][dpid] = bandwidth
        self.update_topology(stream_id)

    @set_ev_cls(Event_Streaming_PacketIn, MAIN_DISPATCHER)
    def _streaming_handler(self, ev):
        msg = ev.msg
        pkt = ev.pkt

        dpid = msg.datapath.id
        in_port = msg.match["in_port"]

        eth_src = pkt.get_protocol(ethernet.ethernet).src
        ip_dst = pkt.get_protocol(ipv4.ipv4).dst
        stream_id = get_stream_id(ip_dst)

        if stream_id not in self.streams:
            self.logger.info("reg stream%d through packets", stream_id)
            self.send_event_to_observers(\
                    EventStreamSourceEnter(stream_id, eth_src, dpid, in_port))
            return True
        if dpid not in self.streams[stream_id]["m_tree"]:
            self.logger.info("packet of stream %d is not "
                             "supposed to recv in dp%d",
                             stream_id, dpid)
            return False

        flow = self.streams[stream_id]["m_tree"][dpid]
        if in_port != flow.get("in_port"):
            self.logger.info("packet of stream%d is supposed "
                             "to recv from %s:%s, not %s:%s",
                             stream_id,
                             dpid, flow.get("in_port"),
                             dpid, in_port)
            return False
        self.logger.info("flow of stream%d is not installed properly",
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
        for link in self.streams[stream_id]["links"]:
            if link in self.link_to_streams[link]:
                self.link_to_streams[link].discard(stream_id)
        self.streams[stream_id]["links"] = set()
        new_tree, mod_dpids = self.algorithm.cal(self.streams[stream_id],
                                                 self.paths, 
                                                 self.pathlens, ev)
        if new_tree is None:
            new_tree = {}
            src_dpid = self.streams[stream_id]["src"]["dpid"]
            new_tree[src_dpid] = {"parent": -1,
                                  "children": set()}
            mod_dpids = self.streams[stream_id]["m_tree"].keys()
            self.failed_streams.add(stream_id)
        else:
            if stream_id in self.failed_streams:
                self.failed_streams.remove(stream_id)

        for dpid in mod_dpids:
            new_stat = new_tree.get(dpid)
            self.mod_stream_flow(dpid, stream_id, new_stat)
            if new_stat is None:
                self.update_switch_table(dpid, "del", stream_id)
        for dpid, stat in new_tree.items():
            if stat["parent"] != -1:
                src, dst = dpid, stat["parent"]
                if src > dst:
                    src, dst = dst, src
                self.streams[stream_id]["links"].add((src, dst))
                self.link_to_streams[(src, dst)].add(stream_id)
        self.streams[stream_id]["m_tree"] = new_tree
        self.update_topology(stream_id)

    def mod_stream_flow(self, dpid, stream_id, new_stat, new_band=None):
        datapath = self.dpset.get(dpid)
        if datapath is None: return
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        if datapath is None:
            return False

        prev_stat = self.streams[stream_id]["m_tree"].get(dpid)
        prev_band = self.streams[stream_id]["bandwidth"].get(dpid)
        if prev_stat is not None:
            if self.streams[stream_id]["src"]["dpid"] == dpid:
                prev_in_port = self.streams[stream_id]["src"]["in_port"]
            else:
                prev_in_port = self.link_outport.get((dpid, prev_stat["parent"]), -1)
            prev_out_ports = map(lambda c: self.link_outport[(dpid, c)],\
                    prev_stat["children"])
        if new_stat is not None:
            if self.streams[stream_id]["src"]["dpid"] == dpid:
                curr_in_port = self.streams[stream_id]["src"]["in_port"]
            else:
                curr_in_port = self.link_outport.get((dpid, new_stat["parent"]), -1)
            curr_out_ports = map(lambda c: self.link_outport[(dpid, c)],\
                    new_stat["children"])
            if dpid in self.streams[stream_id]["clients"]:
                for port in self.streams[stream_id]["clients"][dpid]:
                    curr_out_ports.append(port)

        if prev_stat is not None and len(prev_out_ports) != 0:
            # Del existing group
            gmod = parser.OFPGroupMod(datapath=datapath,
                                      command=ofproto.OFPGC_DELETE,
                                      type_=ofproto.OFPGT_ALL,
                                      group_id=stream_id)
            datapath.send_msg(gmod)

        if new_stat is not None and len(curr_out_ports) != 0:
            # Add new group
            buckets = []
            for port in curr_out_ports:
                actions = [parser.OFPActionOutput(port)]
                if port in self.port_to_host[dpid]:
                    host = self.port_to_host[dpid][port]
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

        if prev_band is not None:
            mmod = parser.OFPMeterMod(datapath=datapath,
                                      command=ofproto.OFPMC_DELETE,
                                      meter_id=stream_id)
            datapath.send_msg(mmod)

        if new_band is not None:
            bands = [parser.OFPMeterBandDrop(rate=new_band*1000)]
            mmod = parser.OFPMeterMod(datapath=datapath,
                                      command=ofproto.OFPMC_ADD,
                                      flags=ofproto.OFPMF_KBPS,
                                      meter_id=stream_id,
                                      bands=bands)
            datapath.send_msg(mmod)

        if prev_stat is not None:
            # Del first
            out_port = ofproto.OFPP_ANY
            out_group = ofproto.OFPG_ANY
            if len(prev_out_ports) != 0:
                out_group = stream_id
            eth_dst = self.streams[stream_id]["eth_dst"]
            match = parser.OFPMatch(in_port=prev_in_port, eth_dst=eth_dst)
            mod = parser.OFPFlowMod(datapath=datapath,
                                    command=ofproto.OFPFC_DELETE,
                                    out_port=out_port,
                                    out_group=out_group,
                                    match=match,
                                    instructions=[])
            datapath.send_msg(mod)

        if new_stat is not None:
            # Add new flow
            priority = 5
            eth_dst = self.streams[stream_id]["eth_dst"]
            match = parser.OFPMatch(in_port=curr_in_port, eth_dst=eth_dst)
            actions = []
            if len(curr_out_ports) != 0:
                actions.append(parser.OFPActionGroup(stream_id,
                                                     ofproto.OFPGT_ALL))
            inst = []
            if new_band is not None:
                inst.append(parser.OFPInstructionMeter(meter_id=stream_id))
            inst.append(parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                                     actions))
            mod = parser.OFPFlowMod(datapath=datapath,
                                    priority=priority,
                                    match=match,
                                    instructions=inst)
            datapath.send_msg(mod)
        return True

    def update_topology(self, stream_id):
        src_dpid = self.streams[stream_id]["src"]["dpid"]
        m_tree = self.streams[stream_id]["m_tree"]
        bw_setting = self.streams[stream_id]["bandwidth"]
        bw_real = {}
        stack = [(src_dpid, 1)]
        while len(stack) > 0:
            dpid, dist = stack.pop()
            parent = m_tree[dpid]["parent"]
            children = m_tree[dpid]["children"]
            [stack.append((child, dist+1)) for child in children]
            p_bw = bw_real.get(parent, DEFAULT_BANDWIDTH)
            m_bw = bw_setting.get(dpid, DEFAULT_BANDWIDTH)
            bw_real[dpid] = min(p_bw, m_bw)
            self.update_switch_table(dpid, "add", stream_id, dist, bw_real[dpid])
        return bw_real

    def update_host_table(self, mac, op, category, stream_id):
        if op == "add":
            self.host_table[mac][category].add(stream_id)
        elif op == "del":
            self.host_table[mac][category].discard(stream_id)
        self.send_event_to_observers(\
                EventHostStatChanged(self.host_table[mac]["host"],
                                     self.host_table[mac]["sourcing"],
                                     self.host_table[mac]["receving"]))

    def update_switch_table(self, dpid, op, stream_id, dist=-1, bw=-1):
        if dpid not in self.switch_table:
            return
        send_event = True
        if op == "add":
            if stream_id in self.switch_table[dpid]:
                entry = self.switch_table[dpid][stream_id]
                if entry["bandwidth"] == bw and entry["distance"] == dist:
                    send_event = False
            self.switch_table[dpid][stream_id] = {"bandwidth": bw, 
                                                  "distance": dist}
        elif op == "del":
            del self.switch_table[dpid][stream_id]
        if send_event:
            self.send_event_to_observers(\
                    EventSwitchStatChanged(dpid,
                                           self.switch_table[dpid]))
