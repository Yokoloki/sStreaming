import logging
import json

from ryu import cfg
from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3, inet
from ryu.controller import ofp_event, dpset
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.topology import switches
from ryu.topology.event import *
from ryu.app.wsgi import WSGIApplication
from ryu.lib.packet import packet, ethernet, arp, ipv4, icmp
from ryu.lib.mac import haddr_to_bin
from ryu.lib.dpid import dpid_to_str
from ryu.lib.port_no import port_no_to_str

from arp_proxy import ARPProxy
from switching import Switching
from streaming import Streaming
from visual import VisualServer

from events import *
from addrs import *

cfg.CONF.observe_links = True
cfg.CONF.explicit_drop = False
cfg.CONF.wsapi_port = 80
CONFIG_FILE = "config.json"


class Wrapper(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {
        "switches": switches.Switches,
        "dpset": dpset.DPSet,
        "wsgi": WSGIApplication,
        "ARPProxy": ARPProxy,
        "Switching": Switching,
        "Streaming": Streaming,
        "Visual": VisualServer
    }
    _EVENTS = [Event_ARP_PacketIn,
               Event_Switching_PacketIn,
               Event_Streaming_PacketIn,
               EventHostReg]

    def __init__(self, *args, **kwargs):
        super(Wrapper, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        self.dpset = kwargs["dpset"]
        self._wsgi = kwargs["wsgi"]
        self._arp_proxy = kwargs["ARPProxy"]
        self._switching = kwargs["Switching"]
        self._streaming = kwargs["Streaming"]
        self._visual = kwargs["Visual"]

        with file(CONFIG_FILE) as f:
            conf = json.load(f)
            for app in conf.keys():
                kwargs[app].config(conf[app])
    
        self._arp_proxy.reg_DPSet(self.dpset)
        self._arp_proxy.set_wrapper(self)
        self._switching.reg_DPSet(self.dpset)
        self._switching.set_wrapper(self)
        self._switching.enable_multipath()
        self._streaming.reg_DPSet(self.dpset)
        self._visual.reg_DPSet(self.dpset)
        self._visual.set_wrapper(self)
        self._visual.reg_controllers(self._wsgi)

        # dpid -> set(mac)
        self.hostmac = {}
        # dpid -> set(port)
        self.flood_ports = {}

        self._arp_proxy.insert_entry(HOST_DIS_IP_SRC, HOST_DIS_ETH_SRC)

    def get_flood_ports(self):
        port_list = []
        for dpid, ports in self.flood_ports.items():
            for port in ports:
                port_list.append((dpid, port))
        return port_list

    def trigger_host_discovery(self):
        # In order to enable active host discovery
        # net.ipv4.icmp_echo_ignore_broadcasts should be set to 0
        icmp_packet = packet.Packet()
        icmp_packet.add_protocol(ethernet.ethernet(
            src=HOST_DIS_ETH_SRC,
            dst=HOST_DIS_ETH_DST))
        icmp_packet.add_protocol(ipv4.ipv4(
            dst=HOST_DIS_IP_DST,
            src=HOST_DIS_IP_SRC,
            proto=inet.IPPROTO_ICMP))
        icmp_packet.add_protocol(icmp.icmp(data=icmp.echo()))
        icmp_packet.serialize()

        for dpid, out_port in self.get_flood_ports():
            dp = self.dpset.get(dpid)
            actions = [dp.ofproto_parser.OFPActionOutput(out_port)]
            out = dp.ofproto_parser.OFPPacketOut(
                    datapath=dp,
                    buffer_id=dp.ofproto.OFP_NO_BUFFER,
                    in_port=dp.ofproto.OFPP_CONTROLLER,
                    actions=actions,
                    data=icmp_packet.data)
            dp.send_msg(out)

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        # {"dpid", "ports": [{"hw_addr", "name", "port_no", "dpid"}]}
        dpid = int(msg["dpid"], 16)
        self.hostmac[dpid] = set()
        self.flood_ports[dpid] = set()
        for port in msg["ports"]:
            port_no = int(port["port_no"], 16)
            self.flood_ports[dpid].add(port_no)

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        del self.hostmac[int(msg["dpid"], 16)]
        if dpid in self.flood_ports:
            del self.flood_ports[dpid]

    @set_ev_cls(EventLinkAdd)
    def _link_add_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"], 16)
        src_port_no = int(msg["src"]["port_no"], 16)
        dst_dpid = int(msg["dst"]["dpid"], 16)
        dst_port_no = int(msg["dst"]["port_no"], 16)
        if src_dpid in self.flood_ports:
            if src_port_no in self.flood_ports[src_dpid]:
                self.flood_ports[src_dpid].remove(src_port_no)
        if dst_dpid in self.flood_ports:
            if dst_port_no in self.flood_ports[dst_dpid]:
                self.flood_ports[dst_dpid].remove(dst_port_no)

    @set_ev_cls(EventLinkDelete)
    def _link_del_handler(self, ev):
        msg = ev.link.to_dict()
        src_dpid = int(msg["src"]["dpid"], 16)
        src_port_no = int(msg["src"]["port_no"], 16)
        dst_dpid = int(msg["dst"]["dpid"], 16)
        dst_port_no = int(msg["dst"]["port_no"], 16)
        if src_dpid in self.flood_ports:
            self.flood_ports[src_dpid].add(src_port_no)
        if dst_dpid in self.flood_ports:
            self.flood_ports[dst_dpid].add(dst_port_no)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.flush_flows(datapath)
        dpid = datapath.id
        # table-miss flow entry
        miss_match = parser.OFPMatch()
        if dpid == NAT_SW_DPID:
            miss_actions = [parser.OFPActionOutput(ofproto.OFPP_NORMAL)]
        else:
            miss_actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                                   ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, miss_match, miss_actions)

        # ipv6 discovery entry
        ipv6_match = parser.OFPMatch()
        ipv6_match.set_dl_dst_masked(haddr_to_bin(IPV6_HOST_DIS_DST),
                                     haddr_to_bin(IPV6_HOST_DIS_MASK))
        ipv6_actions = []
        self.add_flow(datapath, 1, ipv6_match, ipv6_actions)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        arp_protocol = pkt.get_protocol(arp.arp)
        ip_protocol = pkt.get_protocol(ipv4.ipv4)

        eth_src = pkt.get_protocol(ethernet.ethernet).src
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst

        if eth_dst == LLDP:
            return

        hosts = self.hostmac.get(datapath.id)
        if hosts is not None and eth_src not in hosts:
            src_ip = None
            if arp_protocol is not None:
                src_ip = arp_protocol.src_ip
            elif ip_protocol is not None:
                src_ip = ip_protocol.src
            if src_ip is not None:
                print "disc ip%s mac%s" % (src_ip, eth_src)
                hosts.add(eth_src)
                host = Host(eth_src, src_ip, datapath.id, in_port)
                self.send_event_to_observers(EventHostReg(host))
 
        # Active Host Discovery
        if eth_dst == HOST_DIS_ETH_SRC:
            self.logger.info("recv HOST_DIS_ETH_SRC")
            return

        # Passive Host Discovery
        if arp_protocol:
            self.send_event_to_observers(Event_ARP_PacketIn(msg, pkt))
            return

        if eth_dst == ETHERNET_FLOOD:
            # Ignore LLDP packets and flooding packets
            return
        elif is_multicast(eth_dst):
            # Streaming
            print "Streaming"
            self.send_event_to_observers(Event_Streaming_PacketIn(msg, pkt))
        else:
            # Switching
            print "Switching"
            self.send_event_to_observers(Event_Switching_PacketIn(msg, pkt))

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def flush_flows(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        mod = parser.OFPFlowMod(datapath=datapath,
                                command=ofproto.OFPFC_DELETE,
                                out_port=ofproto.OFPP_ANY,
                                out_group=ofproto.OFPG_ANY,
                                match=parser.OFPMatch(),
                                instructions=[])
        datapath.send_msg(mod)


class Host(object):
    # This is data class passed by EventHostXXX

    def __init__(self, mac, ip, dpid, port_no):
        super(Host, self).__init__()
        self.mac = mac
        self.ip = ip
        self.dpid = dpid
        self.port_no = port_no

    def to_dict(self):
        d = {
            "mac": self.mac,
            "ip": self.ip,
            "dpid": dpid_to_str(self.dpid),
            "port_no": port_no_to_str(self.port_no)
        }
        return d

    def __str__(self):
        msg = 'Host<mac=%s,ip=%s>' % (self.mac, self.ip)
        return msg
