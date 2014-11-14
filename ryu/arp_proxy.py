import pymongo
import logging
from ryu.base import app_manager
from ryu.controller import ofp_event, event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp

class EventPacketIn(event.EventBase):
    def __init__(self, msg, pkt):
        super(EventPacketIn, self).__init__()
        self.msg = msg
        self.pkt = pkt

class EventReload(event.EventBase):
    def __init__(self):
        super(EventReload, self).__init__()


class ARPProxy(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ARPProxy, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('ARPProxy')
        self.logger.setLevel(logging.DEBUG)
        self.conn = pymongo.Connection("127.0.0.1")
        self.db = self.conn["sStreaming"]
        self.arp_table = {}
        self.logger.debug("ARPProxy: init")

    @set_ev_cls(EventReload, CONFIG_DISPATCHER)
    def _reload_handler(self, ev):
        self.logger.debug("ARP_Proxy: _reload_handler")
        del self.arp_table
        self.arp_table = {}
        arp_entries = self.db.ARP.find()
        for entry in arp_entries:
            self.arp_table[entry["ip"]] = entry["mac"]


    @set_ev_cls(EventPacketIn, MAIN_DISPATCHER)
    def _arp_proxy_handler(self, ev):
        self.logger.debug("ARP_Proxy: _arp_proxy_handler")
        msg = ev.msg
        pkt = ev.pkt

        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        eth_protocol = pkt.get_protocol(ethernet.ethernet)
        eth_src = eth_protocol.src

        arp_protocol = pkt.get_protocol(arp.arp)
        if not arp_protocol or \
                arp_protocol.opcode != arp.ARP_REQUEST:
            return False
        src_ip = arp_protocol.src_ip
        dst_ip = arp_protocol.dst_ip
        dst_mac = self.arp_table.get(dst_ip)

        if dst_mac == None:
            self.logger.info("ARP_Proxy: %s not in arp table" % dst_ip)
            return False

        ARP_Reply = packet.Packet()
        ARP_Reply.add_protocol(
                ethernet.ethernet(
                    ethertype=eth_protocol.ethertype,
                    dst=eth_src,
                    src=dst_mac))
        ARP_Reply.add_protocol(
                arp.arp(
                    opcode=arp.ARP_REPLY,
                    src_mac=dst_mac,
                    src_ip=dst_ip,
                    dst_mac=eth_src,
                    dst_ip=src_ip))
        ARP_Reply.serialize()

        actions = [parser.OFPActionOutput(in_port)]
        out = parser.OFPPacketOut(datapath=datapath,
                                  buffer_id=ofproto.OFP_NO_BUFFER,
                                  in_port=ofproto.OFPP_CONTROLLER,
                                  actions=actions,
                                  data=ARP_Reply.data)
        datapath.send_msg(out)
        self.logger.debug("ARPProxy: %s - %s" % (dst_ip, dst_mac))
        return True
