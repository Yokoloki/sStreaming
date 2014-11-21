import pymongo
import logging

from ryu import cfg
from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.topology import switches
from ryu.topology.event import *
from ryu.app.wsgi import WSGIApplication
from ryu.lib.packet import packet, ethernet, arp
from ryu.lib.mac import haddr_to_bin

from arp_proxy import ARPProxy
from switching import Switching
from visual import VisualServer
from events import *

ETHERNET_FLOOD = "ff:ff:ff:ff:ff:ff"
ETHERNET_MULTICAST = "ee:ee:ee:ee:ee:ee"
LLDP = "01:80:c2:00:00:0e"

cfg.CONF.observe_links = True

class Wrapper(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {
        "switches": switches.Switches,
        "wsgi": WSGIApplication,
        "ARPProxy": ARPProxy,
        "Switching": Switching,
        "Visual": VisualServer
    }
    _EVENTS = [Event_ARP_PacketIn,
               Event_Switching_PacketIn,
               EventDpReg, EventHostReg]

    def __init__(self, *args, **kwargs):
        super(Wrapper, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        kwargs["Switching"].enable_multipath()
        kwargs["Visual"].reg_controllers(kwargs["wsgi"])
        #dpid -> set(mac)
        self.hosts = {}

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        self.hosts[int(msg["dpid"])] = set()

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        del self.hosts[int(msg["dpid"])]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.flush_flows(datapath)
        # table-miss flow entry
        miss_match = parser.OFPMatch()
        miss_actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, miss_match, miss_actions)

        # ipv6 discovery entry
        ipv6_dl_dst = "33:33:00:00:00:00"
        ipv6_dl_mask = "ff:ff:00:00:00:00"
        ipv6_match = parser.OFPMatch()
        ipv6_match.set_dl_dst_masked(haddr_to_bin(ipv6_dl_dst),
                                     haddr_to_bin(ipv6_dl_mask))
        ipv6_actions = []
        self.add_flow(datapath, 1, ipv6_match, ipv6_actions)

        self.send_event_to_observers(EventDpReg(datapath))

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        arp_protocol = pkt.get_protocol(arp.arp)

        eth_src = pkt.get_protocol(ethernet.ethernet).src
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst
        if arp_protocol:
            self.send_event_to_observers(Event_ARP_PacketIn(msg, pkt))
            if eth_src not in self.hosts[datapath.id]:
                self.hosts[datapath.id].add(eth_src)
                self.send_event_to_observers(EventHostReg(eth_src, datapath.id, in_port))

        if eth_dst == LLDP or eth_dst == ETHERNET_FLOOD:
            #Ignore LLDP packets and flooding packets
            return
        #elif eth_dst == ETHERNET_MULTICAST:
            #Streaming
            #self.send_event_to_observers(Event_Streaming_PacketIn(msg, decoded_pkt))
            #self.logger.debug("Streaming")
        else:
            #Switching
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
