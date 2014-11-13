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
from l2_forwarding import L2Forwarding
from l2_forwarding import EventPacketIn as Event_L2Forwarding_PacketIn
from l2_forwarding import EventReload as Event_L2Forwarding_Reload
#from l3_forwarding import EventPacketIn as Event_L3Forwarding_PacketIn
#from l3_forwarding import EventReload as Event_L3Forwarding_Reload
#from streaming import EventPacketIn as Event_Streaming_PacketIn
#from streaming import EventReload as Event_Streaming_Reload

ETHERNET_FLOOD = "ff:ff:ff:ff:ff:ff"
ETHERNET_MULTICAST = "ee:ee:ee:ee:ee:ee"

class Wrapper(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"ARPProxy": ARPProxy, "L2Forwarding": L2Forwarding}
    _EVENTS = [Event_ARPProxy_PacketIn, Event_ARPProxy_Reload,
               Event_L2Forwarding_PacketIn, Event_L2Forwarding_Reload]

    def __init__(self, *args, **kwargs):
        super(Wrapper, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        self.switches = {}
        self.version = 0
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]

    def reload(self):
        self.version = self.db.Version.find_one()["Version"]
        del self.switches
        self.switches = {}
        switches = self.db.Switch.find()
        for sw in switches:
            self.switches[sw["dpid"]] = {
                    "type": sw["type"],
                    "as": sw["as"],
                    "mac": {}}
        ports = self.db.Port.find()
        for port in ports:
            self.switches[port["dpid"]]["mac"][port["port"]] = port["mac"]
        self.send_event_to_observers(Event_ARPProxy_Reload())
        self.send_event_to_observers(Event_L2Forwarding_Reload())
        #self.send_event_to_observers(Event_L3Forwarding_Reload())
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

        #self._l2_forwarding.reg_dp(datapath)
        # table-miss flow entry
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        self.logger.info("_packet_in_handler")
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        decoded_pkt = dict((p.protocol_name, p) for p in pkt.protocols if type(p) != str )

        eth_dst = decoded_pkt['ethernet'].dst

        if eth_dst == ETHERNET_FLOOD:
            #ARP proxy
            self.send_event_to_observers(Event_ARPProxy_PacketIn(msg, decoded_pkt))
            self.logger.debug("ARP Proxy")
        #elif eth_dst == ETHERNET_MULTICAST:
            #Streaming
            #self.send_event_to_observers(Event_Streaming_PacketIn(msg, decoded_pkt))
            #self.logger.debug("Streaming")
        #elif eth_dst == self.switches[datapath.id]["mac"][in_port]:
            #L3 Forwarding
            #self.send_event_to_observers(Event_L3Forwarding_PacketIn(msg, decoded_pkt))
            #self.logger.debug("L3Forwarding")
        else:
            #L2 Forwarding
            self.send_event_to_observers(Event_L2Forwarding_PacketIn(msg, decoded_pkt))
            self.logger.debug("L2Forwarding mac = %s" % eth_dst)
