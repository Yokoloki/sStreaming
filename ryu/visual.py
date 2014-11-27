import os
import json
import logging
from webob import Response
from webob.static import DirectoryApp

from ryu.base import app_manager
from ryu.app.wsgi import ControllerBase, route
from ryu.app.wsgi import WebSocketRPCClient, websocket
from ryu.contrib.tinyrpc.exc import InvalidReplyError
from socket import error as SocketError
from ryu.topology import event
from ryu.controller.handler import set_ev_cls
from ryu.lib.dpid import DPID_PATTERN
from events import EventHostReg, EventHostRequest

PATH = os.path.dirname(__file__)


class VisualServer(app_manager.RyuApp):

    def __init__(self, *args, **kwargs):
        super(VisualServer, self).__init__(*args, **kwargs)
        self.rpc_clients = []
        self.logger = logging.basicConfig(format="VisualServer: %(message)s")
        self.logger.setLevel(logging.DEBUG)

    def set_wrapper(self, wrapper):
        self.wrapper = wrapper

    def reg_DPSet(self, dpset):
        self.dpset = dpset

    def reg_controllers(self, wsgi):
        wsgi.register(TopologyController, {"visual_server": self})
        wsgi.register(WebSocketTopologyController, {"visual_server": self})
        wsgi.register(StaticFileController)

    def get_switches(self):
        rep = self.send_request(event.EventSwitchRequest(None))
        return rep.switches

    def get_links(self):
        rep = self.send_request(event.EventLinkRequest(None))
        return rep.links

    def get_hosts(self):
        rep = self.send_request(EventHostRequest(None))
        return rep.hosts

    @set_ev_cls(event.EventSwitchEnter)
    def _event_switch_enter_handler(self, ev):
        msg = ev.switch.to_dict()
        del msg["ports"]
        self._rpc_broadcall("event_switch_enter", msg)

    @set_ev_cls(event.EventSwitchLeave)
    def _event_switch_leave_handler(self, ev):
        msg = ev.switch.to_dict()
        del msg["ports"]
        self._rpc_broadcall("event_switch_leave", msg)

    @set_ev_cls(event.EventLinkAdd)
    def _event_link_add_handler(self, ev):
        msg = ev.link.to_dict()
        self._rpc_broadcall("event_link_add", msg)

    @set_ev_cls(event.EventLinkDelete)
    def _event_link_delete_handler(self, ev):
        msg = ev.link.to_dict()
        self._rpc_broadcall("event_link_delete", msg)

    @set_ev_cls(EventHostReg)
    def _event_host_reg_handler(self, ev):
        msg = ev.host.to_dict()
        self._rpc_broadcall("event_host_reg", msg)

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
        switches_dict = [switch.to_dict() for switch in switches]
        [s.pop("ports") for s in switches_dict]
        body = json.dumps(switches_dict)
        return Response(content_type="application/json", body=body)

    @route("topology", "/topology/links", methods=["GET"])
    def _list_links(self, req, **kwargs):
        links = self.visual_server.get_links()
        body = json.dumps([link.to_dict() for link in links])
        return Response(content_type="application/json", body=body)

    @route("topology", "/topology/hosts", methods=["GET"])
    def _list_hosts(self, req, **kwargs):
        hosts = self.visual_server.get_hosts()
        body = json.dumps([host.to_dict() for host in hosts])
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
