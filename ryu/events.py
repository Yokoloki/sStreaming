from ryu.controller import event


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


class EventHostReg(event.EventBase):

    def __init__(self, host):
        super(EventHostReg, self).__init__()
        self.host = host


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


class EventStreamSourceEnter(event.EventBase):

    def __init__(self, stream_id, src_dpid, src_in_port, rate):
        super(EventStreamSourceEnter, self).__init__()
        self.stream_id = stream_id
        self.src_dpid = src_dpid
        self.src_in_port = src_in_port
        self.rate = rate


class EventStreamSourceLeave(event.EventBase):

    def __init__(self, stream_id):
        super(EventStreamSourceLeave, self).__init__()
        self.stream_id = stream_id


class EventStreamClientEnter(event.EventBase):

    def __init__(self, stream_id, dpid, out_port):
        super(EventStreamClientEnter, self).__init__()
        self.stream_id = stream_id
        self.dpid = dpid
        self.out_port = out_port


class EventStreamClientLeave(event.EventBase):

    def __init__(self, stream_id, dpid, out_port):
        super(EventStreamClientLeave, self).__init__()
        self.stream_id = stream_id
        self.dpid = dpid
        self.out_port = out_port
