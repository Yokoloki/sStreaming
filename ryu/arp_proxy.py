import logging
import struct
from ryu.base import app_manager
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp
from ryu.lib import addrconv
from ryu.topology.event import *
from events import Event_ARP_PacketIn

IPV4_STREAMING = "224.1.0.0"


class ARPProxy(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ARPProxy, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)
        self.arp_table = {}
        self.dp_to_ip = {}
        self.logger.debug("init")

    def reg_DPSet(self, dpset):
        self.dpset = dpset

    def set_wrapper(self, wrapper):
        self.wrapper = wrapper

    def insert_entry(self, ip, mac):
        self.arp_table[ip] = mac

    @set_ev_cls(EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        # {"dpid", "ports": [{"hw_addr", "name", "port_no", "dpid"}]}
        dpid = int(msg["dpid"], 16)
        self.dp_to_ip[dpid] = set()

    @set_ev_cls(EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        dpid = int(msg["dpid"], 16)
        if dpid in self.dp_to_ip:
            for ip in self.dp_to_ip[dpid]:
                del self.arp_table[ip]
            del self.dp_to_ip[dpid]

    @set_ev_cls(Event_ARP_PacketIn, MAIN_DISPATCHER)
    def _arp_proxy_handler(self, ev):
        msg = ev.msg
        pkt = ev.pkt

        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        eth_protocol = pkt.get_protocol(ethernet.ethernet)
        eth_src = eth_protocol.src

        arp_protocol = pkt.get_protocol(arp.arp)

        src_ip = arp_protocol.src_ip
        dst_ip = arp_protocol.dst_ip

        self.arp_table[src_ip] = eth_src

        self.dp_to_ip[datapath.id].add(src_ip)
        if arp_protocol.opcode == arp.ARP_REPLY:
            return

        dst_mac = self.arp_table.get(dst_ip)
        if dst_mac is None:
            # Flood to flood_ports
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
        else:
            ARP_Reply = packet.Packet()
            ARP_Reply.add_protocol(ethernet.ethernet(ethertype=eth_protocol.ethertype,
                                                     dst=eth_src,
                                                     src=dst_mac))
            ARP_Reply.add_protocol(arp.arp(opcode=arp.ARP_REPLY,
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
            self.logger.debug("%s - %s" % (dst_ip, dst_mac))
            return True


def ipv4_text_to_int(ip_text):
    if ip_text == 0:
        return 0
    assert isinstance(ip_text, str)
    return struct.unpack('!I', addrconv.ipv4.text_to_bin(ip_text))[0]


def ipv4_int_to_text(ip_int):
    assert isinstance(ip_int, (int, long))
    return addrconv.ipv4.bin_to_text(struct.pack('!I', ip_int))


def is_streaming(addr):
    addr_int = ipv4_text_to_int(addr)
    masked_int = addr_int & 0xffff0000
    masked_text = ipv4_int_to_text(masked_int)
    return masked_text == IPV4_STREAMING
