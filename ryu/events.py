from ryu.controller import event
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


class Event_Streaming_PacketIn(EventPacketIn):

    def __init__(self, *args, **kwargs):
        super(Event_Streaming_PacketIn, self).__init__(*args, **kwargs)


class EventHostReg(event.EventBase):

    def __init__(self, host):
        super(EventHostReg, self).__init__()
        self.host = host


class EventHostRequest(event.EventRequestBase):

    def __init__(self, dpid=None):
        super(EventHostRequest, self).__init__()
        self.dst = "Switching"
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


class EventHostStatRequest(event.EventRequestBase):

    def __init__(self, mac=None):
        super(EventHostStatRequest, self).__init__()
        self.dst = "Streaming"
        self.mac = mac

    def __str__(self):
        return "EventHostStatRequest<src=%s, mac=%s>" % \
            (self.src, self.mac)


class EventHostStatReply(event.EventReplyBase):

    def __init__(self, dst, host_stat):
        super(EventHostStatReply, self).__init__(dst)
        self.host_stat = host_stat

    def __str__(self):
        return "EventHostStatReply<dst=%s, %s>" % \
            (self.dst, self.host_stat)


class EventSwitchStatRequest(event.EventRequestBase):

    def __init__(self, dpid=None):
        super(EventSwitchStatRequest, self).__init__()
        self.dst = "Streaming"
        self.dpid = dpid

    def __str__(self):
        return "EventSwitchStatRequest<src=%s, mac=%s>" % \
            (self.src, self.dpid)


class EventSwitchStatReply(event.EventReplyBase):

    def __init__(self, dst, sw_stat):
        super(EventSwitchStatReply, self).__init__(dst)
        self.sw_stat = sw_stat
        self.bandwidth = {}
        self.distance = {}
        for dpid in sw_stat:
            self.bandwidth[dpid] = {}
            self.distance[dpid] = {}
            for stream_id, info in sw_stat[dpid].items():
                self.bandwidth[dpid][stream_id] = info["bandwidth"]
                self.distance[dpid][stream_id] = info["distance"]

    def __str__(self):
        return "EventSwitchStatReply<dst=%s, %s, %s>" % \
            (self.dst, self.bandwidth, self.distance)


class EventSwitchStatChanged(event.EventBase):

    def __init__(self, dpid, sw_stat):
        super(EventSwitchStatChanged, self).__init__()
        self.dpid = dpid
        self.bandwidth = {}
        self.distance = {}
        for stream_id, info in sw_stat.items():
            self.bandwidth[stream_id] = info["bandwidth"]
            self.distance[stream_id] = info["distance"]

    def to_dict(self):
        d = {
            "dpid": dpid_to_str(self.dpid),
            "bandwidth": self.bandwidth,
            "distance": self.distance
        }
        return d


class EventHostStatChanged(event.EventBase):

    def __init__(self, host, sourcing, receving):
        super(EventHostStatChanged, self).__init__()
        self.host = host
        self.sourcing = sourcing
        self.receving = receving

    def to_dict(self):
        d = self.host.to_dict()
        d["sourcing"] = list(self.sourcing)
        d["receving"] = list(self.receving)
        return d


class EventStreamSourceEnter(event.EventBase):

    def __init__(self, stream_id, src_mac, src_dpid, src_in_port, fname, rate=100):
        super(EventStreamSourceEnter, self).__init__()
        self.stream_id = stream_id
        self.src_mac = src_mac
        self.src_dpid = src_dpid
        self.src_in_port = src_in_port
        self.fname = fname
        self.rate = rate
        self.eth_dst = "01:00:5e:01:%02x:%02x" % (stream_id/256, stream_id%256)
        self.ip_dst = "225.1.%d.%d" % (stream_id/256, stream_id%256)


class EventStreamSourceLeave(event.EventBase):

    def __init__(self, stream_id):
        super(EventStreamSourceLeave, self).__init__()
        self.stream_id = stream_id


class EventStreamClientEnter(event.EventBase):

    def __init__(self, stream_id, mac, dpid, out_port):
        super(EventStreamClientEnter, self).__init__()
        self.stream_id = stream_id
        self.mac = mac
        self.dpid = dpid
        self.out_port = out_port


class EventStreamClientLeave(event.EventBase):

    def __init__(self, stream_id, mac, dpid, out_port):
        super(EventStreamClientLeave, self).__init__()
        self.stream_id = stream_id
        self.mac = mac
        self.dpid = dpid
        self.out_port = out_port


class EventStreamBandwidthChange(event.EventBase):

    def __init__(self, stream_id, dpid, bandwidth):
        super(EventStreamBandwidthChange, self).__init__()
        self.stream_id = stream_id
        self.dpid = dpid
        self.bandwidth = bandwidth
