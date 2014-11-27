import struct
from ryu.lib import addrconv

LLDP = "01:80:c2:00:00:0e"

IPV6_HOST_DIS_DST = "33:33:00:00:00:00"
IPV6_HOST_DIS_MASK = "ff:ff:00:00:00:00"

ETHERNET_FLOOD = "ff:ff:ff:ff:ff:ff"
ETHERNET_MULTICAST = "ee:ee:ee:ee:ee:ee"

HOST_DIS_ETH_SRC = "e0:e1:e2:e3:e4:e5"
HOST_DIS_ETH_DST = "ff:ff:ff:ff:ff:ff"
HOST_DIS_IP_SRC = "172.18.255.254"
HOST_DIS_IP_DST = "172.18.255.255"

IPV4_STREAMING = "224.1.0.0"


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


def get_stream_id(addr):
    assert is_streaming(addr)
    addr_int = ipv4_text_to_int(addr)
    stream_id = addr_int & 0x0000ffff
    return stream_id
