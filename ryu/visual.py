import os
import json
import logging
from webob import Response
from webob.static import DirectoryApp

from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.app.wsgi import ControllerBase, route
from ryu.app.wsgi import WebSocketRPCClient, websocket
from ryu.contrib.tinyrpc.exc import InvalidReplyError
from socket import error as SocketError
from ryu.controller.handler import set_ev_cls
from ryu.lib.dpid import DPID_PATTERN, str_to_dpid
from ryu.lib.port_no import str_to_port_no
from ryu.topology.event import *
from events import *

PATH = os.path.dirname(__file__)


class VisualServer(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _EVENTS = [EventStreamSourceEnter,
               EventStreamSourceLeave,
               EventStreamClientEnter,
               EventStreamClientLeave,
               EventStreamBandwidthChange]

    def __init__(self, *args, **kwargs):
        super(VisualServer, self).__init__(*args, **kwargs)
        self.rpc_clients = []
        self.logger.setLevel(logging.DEBUG)

    def set_wrapper(self, wrapper):
        self.wrapper = wrapper

    def reg_DPSet(self, dpset):
        self.dpset = dpset

    def reg_controllers(self, wsgi):
        wsgi.register(TopologyController, {"visual_server": self})
        wsgi.register(StreamController, {"visual_server": self})
        wsgi.register(WebSocketTopologyController, {"visual_server": self})
        wsgi.register(StaticFileController)

    def get_switches(self):
        sw_rep = self.send_request(EventSwitchRequest())
        sw_stat_rep = self.send_request(EventSwitchStatRequest())
        switches = [switch.to_dict() for switch in sw_rep.switches]
        [s.pop("ports") for s in switches]
        bandwidth = sw_stat_rep.bandwidth
        distance = sw_stat_rep.distance
        for i in xrange(len(switches)):
            dpid = str_to_dpid(switches[i]["dpid"])
            if dpid in bandwidth:
                switches[i]["bandwidth"] = bandwidth[dpid]
                switches[i]["distance"] = distance[dpid]
        return switches

    def get_links(self):
        rep = self.send_request(EventLinkRequest(None))
        links = [link.to_dict() for link in rep.links]
        return links

    def get_hosts(self):
        host_rep = self.send_request(EventHostRequest())
        host_stat_rep = self.send_request(EventHostStatRequest())
        hosts = [host.to_dict() for host in host_rep.hosts]
        host_stat = host_stat_rep.host_stat
        for i in xrange(len(hosts)):
            mac = hosts[i]["mac"]
            if mac in host_stat:
                hosts[i]["sourcing"] = list(host_stat[mac]["sourcing"])
                hosts[i]["receving"] = list(host_stat[mac]["receving"])
        return hosts

    @set_ev_cls(EventSwitchEnter)
    def _event_switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        del msg["ports"]
        self._rpc_broadcall("event_switch_enter", msg)

    @set_ev_cls(EventSwitchLeave)
    def _event_switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        del msg["ports"]
        self._rpc_broadcall("event_switch_leave", msg)

    @set_ev_cls(EventLinkAdd)
    def _event_link_add_handler(self, ev):
        msg = ev.link.to_dict()
        self._rpc_broadcall("event_link_add", msg)

    @set_ev_cls(EventLinkDelete)
    def _event_link_delete_handler(self, ev):
        msg = ev.link.to_dict()
        self._rpc_broadcall("event_link_delete", msg)

    @set_ev_cls(EventHostReg)
    def _event_host_reg_handler(self, ev):
        msg = ev.host.to_dict()
        self._rpc_broadcall("event_host_reg", msg)

    @set_ev_cls(EventSwitchStatChanged)
    def _event_switch_stat_changed_handler(self, ev):
        msg = ev.to_dict()
        self._rpc_broadcall("event_switch_stat_changed", msg)

    @set_ev_cls(EventHostStatChanged)
    def _event_host_stat_changed_handler(self, ev):
        msg = ev.to_dict()
        self._rpc_broadcall("event_host_stat_changed", msg)

    def _rpc_broadcall(self, func_name, msg):
        disconnected_clients = []
        for rpc_client in self.rpc_clients:
            rpc_server = rpc_client.get_proxy()
            try:
                getattr(rpc_server, func_name)(msg)
            except SocketError:
                self.logger.debug("WebSocket disconnected: %s" % rpc_client.ws)
                disconnected_clients.append(rpc_client)
            except InvalidReplyError as e:
                self.logger.error(e)
        for client in disconnected_clients:
            self.rpc_clients.remove(client)


class TopologyController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(TopologyController, self).__init__(req, link, data, **config)
        self.visual_server = data["visual_server"]

    @route("topology", "/topology/disc", methods=["GET"])
    def _disc_hosts(self, req, **kwargs):
        self.visual_server.wrapper.trigger_host_discovery()
        body = json.dumps({"stat": "succ"})
        return Response(content_type="application/json", body=body)

    @route("topology", "/topology/switches", methods=["GET"])
    def _list_switches(self, req, **kwargs):
        switches = self.visual_server.get_switches()
        body = json.dumps(switches)
        return Response(content_type="application/json", body=body)

    @route("topology", "/topology/links", methods=["GET"])
    def _list_links(self, req, **kwargs):
        links = self.visual_server.get_links()
        body = json.dumps(links)
        return Response(content_type="application/json", body=body)

    @route("topology", "/topology/hosts", methods=["GET"])
    def _list_hosts(self, req, **kwargs):
        hosts = self.visual_server.get_hosts()
        body = json.dumps(hosts)
        return Response(content_type="application/json", body=body)


class WebSocketTopologyController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(WebSocketTopologyController, self).__init__(req, link, data, **config)
        self.visual_server = data["visual_server"]

    @websocket("wstopology", "/topology/ws")
    def _websocket_handler(self, ws):
        rpc_client = WebSocketRPCClient(ws)
        self.visual_server.rpc_clients.append(rpc_client)
        rpc_client.serve_forever()


class StreamController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(StreamController, self).__init__(req, link, data, **config)
        self.visual_server = data["visual_server"]

    @route("stream", "/streaming/source_for", methods=["POST"])
    def _source_for_handler(self, req, **kwargs):
        try:
            data = eval(req.body) if req.body else {}
        except SyntaxError:
            self.visual_server.logger.info("source_for_handler: "
                                           "invalid syntax %s" %  req.body)
            return Response(status=400)
        try:
            mac = data["mac"]
            dpid = str_to_dpid(data["dpid"])
            port_no = str_to_port_no(data["port_no"])
            stream_id = data["stream_id"]
        except KeyError, message:
            return Response(status=400, body=str(message))
        self.visual_server.send_event_to_observers(\
                EventStreamSourceEnter(stream_id, mac, dpid, port_no))
        body = json.dumps({"stat": "succ"})
        return Response(content_type="application/json", body=body)

    @route("stream", "/streaming/receive_from", methods=["POST"])
    def _receive_from_handler(self, req, **kwargs):
        try:
            data = eval(req.body) if req.body else {}
        except SyntaxError:
            self.visual_server.logger.info("source_for_handler: "
                                           "invalid syntax %s" %  req.body)
            return Response(status=400)
        try:
            mac = data["mac"]
            dpid = str_to_dpid(data["dpid"])
            port_no = str_to_port_no(data["port_no"])
            stream_id = data["stream_id"]
        except KeyError, message:
            return Response(status=400, body=str(message))
        self.visual_server.send_event_to_observers(\
                EventStreamClientEnter(stream_id, mac, dpid, port_no))
        body = json.dumps({"stat": "succ"})
        return Response(content_type="application/json", body=body)

    @route("stream", "/streaming/bandwidth_change", methods=["POST"])
    def _bandwidth_change_handler(self, req, **kwargs):
        try:
            data = eval(req.body) if req.body else {}
        except SyntaxError:
            self.visual_server.logger.info("source_for_handler: "
                                           "invalid syntax %s" %  req.body)
            return Response(status=400)
        try:
            stream_id = data["stream_id"]
            dpid = str_to_dpid(data["dpid"])
            bandwidth = data["bandwidth"]
        except KeyError, message:
            return Response(status=400, body=str(message))
        self.visual_server.send_event_to_observers(\
                EventStreamBandwidthChange(stream_id, dpid, bandwidth))
        body = json.dumps({"stat": "succ"})
        return Response(content_type="application/json", body=body)


class StatsController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(StatsController, self).__init__(req, link, data, **config)
        self.visual_server = data["visual_server"]

    @route("stats", "/stats/flow/{dpid}", methods=["GET"],
           requirements={"dpid": DPID_PATTERN})
    def _flow_stat_handler(self, req, **kwargs):
        pass

    @route("stats", "/stats/priority_get/{dpid}", methods=["GET"],
           requirements={"dpid": DPID_PATTERN})
    def _priority_get_handler(self, req, **kwargs):
        pass

    @route("stats", "/stats/priority_mod", methods=["POST"])
    def _priority_mod_handler(self, req, **kwargs):
        pass


class StaticFileController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(StaticFileController, self).__init__(req, link, data, **config)
        path = "%s/../html/" % PATH
        self.static_app = DirectoryApp(path)

    @route("static", "/{filename:.*}")
    def _static_handler(self, req, **kwargs):
        if kwargs["filename"]:
            req.path_info = kwargs["filename"]
        return self.static_app(req)
