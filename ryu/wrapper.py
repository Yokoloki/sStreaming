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
from routing import Routing
from routing import EventPacketIn as Event_Routing_PacketIn
from routing import EventReload as Event_Routing_Reload
from routing import EventRegDp as Event_Routing_RegDp
#from streaming import EventPacketIn as Event_Streaming_PacketIn
#from streaming import EventReload as Event_Streaming_Reload

ETHERNET_FLOOD = "ff:ff:ff:ff:ff:ff"
ETHERNET_MULTICAST = "ee:ee:ee:ee:ee:ee"
ETHERNET_IPV6_DISC = "33:33:00:00:00:02"

class Wrapper(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"ARPProxy": ARPProxy,
                 "Switching": Switching,
                 "Routing": Routing}
    _EVENTS = [Event_ARPProxy_PacketIn, Event_ARPProxy_Reload,
               Event_Switching_PacketIn, Event_Switching_Reload, Event_Switching_RegDp,
               Event_Routing_PacketIn, Event_Routing_Reload, Event_Routing_RegDp]

    def __init__(self, *args, **kwargs):
        super(Wrapper, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        self.switches = {}
        self.version = 0
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]
        self._switching = kwargs["Switching"]

    def reload(self):
        self.version = self.db.Version.find_one()["Version"]
        del self.switches
        self.switches = {}
        nodes = self.db.Node.find()
        for node in nodes:
            if node["type"]=="host": continue
            self.switches[node["dpid"]] = {
                    "name": node["name"],
                    "type": node["type"],
                    "as": node["as"],
                    "mac": {}}
        intfs = self.db.Intf.find()
        for intf in intfs:
            if "dpid" not in intf: continue
            self.switches[intf["dpid"]]["mac"][intf["port_no"]] = intf["mac"]
        self.send_event_to_observers(Event_ARPProxy_Reload())
        self.send_event_to_observers(Event_Switching_Reload())
        self.send_event_to_observers(Event_Routing_Reload())
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

        # ignore ipv6 discovery message
        ipv6_match = parser.OFPMatch(eth_dst=ETHERNET_IPV6_DISC)
        ipv6_actions = [parser.OFPActionOutput(ofproto.OFPP_NORMAL)]
        self.add_flow(datapath, 1, ipv6_match, ipv6_actions)

        self.send_event_to_observers(Event_Switching_RegDp(datapath))
        self.send_event_to_observers(Event_Routing_RegDp(datapath))

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

        self.logger.debug("_packet_in_handler")
        pkt = packet.Packet(msg.data)
        eth_dst = pkt.get_protocol(ethernet.ethernet).dst
        self.logger.debug("_packet_in_handler: get_protocol.dst")

        if eth_dst == ETHERNET_FLOOD:
            #ARP proxy
            self.send_event_to_observers(Event_ARPProxy_PacketIn(msg, pkt))
            self.logger.debug("ARP Proxy")
        #elif eth_dst == ETHERNET_MULTICAST:
            #Streaming
            #self.send_event_to_observers(Event_Streaming_PacketIn(msg, decoded_pkt))
            #self.logger.debug("Streaming")
        elif eth_dst == ETHERNET_IPV6_DISC:
            #IPV6 Neighbor Discovery
            self.logger.debug("IPv6 Discovery")
        elif eth_dst == self.switches[datapath.id]["mac"][in_port]:
            #Routing
            self.send_event_to_observers(Event_Routing_PacketIn(msg, pkt))
            self.logger.debug("Routing")
        else:
            #Switching
            self.send_event_to_observers(Event_Switching_PacketIn(msg, pkt))
            self.logger.debug("Switching mac = %s" % eth_dst)
        self.logger.debug("left _packet_in_handler")
