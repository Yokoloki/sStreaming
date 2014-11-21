from ryu.controller import event
from ryu.lib.port_no import port_no_to_str
from ryu.lib.dpid import dpid_to_str


class EventPacketIn(event.EventBase):
    def __init__(self, msg, pkt):
        super(EventPacketIn, self).__init__()
        self.msg = msg
        self.pkt = pkt

class Event_ARP_PacketIn(EventPacketIn):
    def __init__(self, *args, **kwargs):
        super(Event_ARP_PacketIn, self).__init__(*args, **kwargs)

class Event_Switching_PacketIn(EventPacketIn):
    def __init__(self, *args, **kwargs):
        super(Event_Switching_PacketIn, self).__init__(*args, **kwargs)

class EventDpReg(event.EventBase):
    def __init__(self, datapath):
        super(EventDpReg, self).__init__()
        self.datapath = datapath

class EventHostReg(event.EventBase):
    def __init__(self, mac, dpid, port):
        super(EventHostReg, self).__init__()
        self.host = Host(mac, dpid, port)

class EventHostRequest(event.EventRequestBase):
    def __init__(self, dpid=None):
        super(EventHostRequest, self).__init__()
        self.dst = 'Switching'
        self.dpid = dpid

    def __str__(self):
        return "EventHostRequest<src=%s, dpid=%s>" % \
            (self.src, self.dpid)

class EventHostReply(event.EventReplyBase):
    def __init__(self, dst, hosts):
        super(EventHostReply, self).__init__(dst)
        self.hosts = hosts

    def __str__(self):
        return "EventHostReply<dst=%s, %s>" % \
            (self.dst, self.hosts)

class Host(object):
    # This is data class passed by EventHostXXX
    def __init__(self, mac, dpid, port_no):
        super(Host, self).__init__()

        self.mac = mac
        self.dpid = dpid
        self.port_no = port_no

    def to_dict(self):
        d = {
            "mac": self.mac,
            "dpid": dpid_to_str(self.dpid),
            "port_no": port_no_to_str(self.port_no)
        }
        return d

    def __str__(self):
        msg = 'Host<mac=%s>' % self.mac
        return msg
