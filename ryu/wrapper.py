import pymongo
import logging

from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib.packet import packet, ethernet
from arp_proxy import ARPProxy
from arp_proxy import EventPacketIn as Event_ARPProxy_PacketIn
from arp_proxy import EventReload as Event_ARPProxy_Reload
from switching import Switching
from switching import EventPacketIn as Event_Switching_PacketIn
from switching import EventReload as Event_Switching_Reload
from switching import EventRegDp as Event_Switching_RegDp
#from streaming import EventPacketIn as Event_Streaming_PacketIn
#from streaming import EventReload as Event_Streaming_Reload

ETHERNET_FLOOD = "ff:ff:ff:ff:ff:ff"
ETHERNET_MULTICAST = "ee:ee:ee:ee:ee:ee"

class Wrapper(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"ARPProxy": ARPProxy,
                 "Switching": Switching}
    _EVENTS = [Event_ARPProxy_PacketIn, Event_ARPProxy_Reload,
               Event_Switching_PacketIn, Event_Switching_Reload, Event_Switching_RegDp]

    def __init__(self, *args, **kwargs):
        super(Wrapper, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        self.version = 0
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]

    def reload(self):
        self.version = self.db.Version.find_one()["Version"]

        self.send_event_to_observers(Event_ARPProxy_Reload())
        self.send_event_to_observers(Event_Switching_Reload())
        #self.send_event_to_observers(Event_Streaming_Reload())

    def chkVersion(self):
        new_version = self.db.Version.find_one()["Version"]
        return new_version == self.version

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        if not self.chkVersion():
            self.reload()

        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # table-miss flow entry
        miss_match = parser.OFPMatch()
        miss_actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, miss_match, miss_actions)

        self.send_event_to_observers(Event_Switching_RegDp(datapath))

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst

        if eth_dst == ETHERNET_FLOOD:
            #ARP proxy
            self.send_event_to_observers(Event_ARPProxy_PacketIn(msg, pkt))
        #elif eth_dst == ETHERNET_MULTICAST:
            #Streaming
            #self.send_event_to_observers(Event_Streaming_PacketIn(msg, decoded_pkt))
            #self.logger.debug("Streaming")
        else:
            #Switching
            self.send_event_to_observers(Event_Switching_PacketIn(msg, pkt))
