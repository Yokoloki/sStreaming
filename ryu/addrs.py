import struct
from ryu.lib import addrconv

NAT_SW_DPID = 0xffffff

LLDP = "01:80:c2:00:00:0e"

IPV6_HOST_DIS_DST = "33:33:00:00:00:00"
IPV6_HOST_DIS_MASK = "ff:ff:00:00:00:00"

ETHERNET_FLOOD = "ff:ff:ff:ff:ff:ff"
ETHERNET_MULTICAST = "ee:ee:ee:ee:ee:ee"

HOST_DIS_ETH_SRC = "e0:e1:e2:e3:e4:e5"
HOST_DIS_ETH_DST = "ff:ff:ff:ff:ff:ff"
HOST_DIS_IP_SRC = "172.18.255.254"
HOST_DIS_IP_DST = "172.18.255.255"

ETH_STREAMING_MASK = 0xffffffff0000 
ETH_STREAMING_ADDR_INT = "0x01005e010000"

IPV4_STREAMING_MASK = 0xffff0000
IPV4_STREAMING_WILDCARD = 0x0000ffff
IPV4_STREAMING_ADDR = "224.1.0.0"


def ipv4_text_to_int(ip_text):
    if ip_text == 0:
        return 0
    assert isinstance(ip_text, str)
    return struct.unpack('!I', addrconv.ipv4.text_to_bin(ip_text))[0]


def ipv4_int_to_text(ip_int):
    assert isinstance(ip_int, (int, long))
    return addrconv.ipv4.bin_to_text(struct.pack('!I', ip_int))


def eth_text_to_int(eth_text):
    if eth_text == 0:
        return 0
    assert isinstance(eth_text, str)
    array = struct.unpack('6B', addrconv.mac.text_to_bin(eth_text))
    eth_int = 0
    for i in array:
        eth_int <<= 8
        eth_int += i
    return eth_int


def is_multicast(haddr):
    addr_int = eth_text_to_int(haddr)
    masked_int = addr_int & ETH_STREAMING_MASK
    return masked_int == ETH_STREAMING_ADDR_INT


def is_streaming(addr):
    addr_int = ipv4_text_to_int(addr)
    masked_int = addr_int & IPV4_STREAMING_MASK
    masked_text = ipv4_int_to_text(masked_int)
    return masked_text == IPV4_STREAMING_ADDR


def get_stream_id(addr):
    assert is_streaming(addr)
    addr_int = ipv4_text_to_int(addr)
    stream_id = addr_int & IPV4_STREAMING_WILDCARD
    return stream_id
