from copy import deepcopy


class Shortest_Path_Heuristic(object):

    def __init__(self):
        super(shortest_path_heuristic, self).__init__()
        self.event_map = {"EventSwitchLeave": "_topology_changed_handler",
                          "EventLinkDelete": "_topology_changed_handler",
                          "EventStreamClientEnter": "_client_enter_handler",
                          "EventStreamClientLeave": "_client_leave_handler"}

    # Make sure paths && pathlens is up to date
    def cal(self, stream, link_outport, paths, pathlens, ev):
        ev_name = ev.__class__.__name__
        func_name = self.event_map.get(ev_name)
        if not func_name:
            print "%s not in event_map of Shortest_Path_Heuristic" % ev_name
            exit(1)
        new_flows = getattr(self, func_name)(stream, link_outport, paths, pathlens, ev)
        return new_flows

    def _topology_changed_handler(self, stream, link_outport, paths, pathlens, ev):
        new_flows = {}
        new_flows[stream["src"]["dpid"]] = {"prev": -1,
                                            "in_port": stream["src"]["in_port"],
                                            "out_ports": set()}
        tree = set(new_flows.keys())
        pending = deepcopy(stream["clients"])
        while len(pending) != 0:
            next_to_add = None
            branch = None
            dist = 2 ** 31
            for tree_node in tree:
                for to_add in pending:
                    if to_add in pathlens[tree_node] and \
                            pathlens[tree_node][to_add] < dist:
                                next_to_add = to_add
                                branch = tree_node
                                dist = pathlens[tree_node][to_add]
            if next_to_add is None:
                print "Error: cannot build multicast tree"
                return None
            pending.remove(next_to_add)
            for i in xrange(pathlens[branch][next_to_add]):
                node = paths[branch][next_to_add][i]
                if node not in new_flows:
                    prev_node = paths[branch][next_to_add][i-1]
                    in_port = link_outport[(node, prev_node)]
                    new_flows[node] = {"prev": prev_node,
                                       "in_port": in_port,
                                       "out_ports": set()}
                if i < pathlens[branch][next_to_add]:
                    next_node = paths[branch][next_to_add][i-1]
                    out_port = link_outport[(node, next_node)]
                    new_flows[node]["out_ports"].add(out_port)
                if node in stream["clients"]:
                    new_flows[node]["out_ports"].update(stream["clients"][node])
                tree.add(node)
        return new_flows

    def _client_enter_handler(self, stream, link_outport, paths, pathlens, ev):
        new_flows = deepcopy(stream["curr_flows"])
        tree = set(new_flows.keys())
        next_to_add = ev.dpid
        client_port = ev.out_port
        if next_to_add not in tree:
            branch = None
            dist = 2 ** 31
            for tree_node in tree:
                if next_to_add in pathlens[tree_node] and \
                        pathlens[tree_node][next_to_add] < dist:
                            branch = tree_node
                            dist = pathlens[tree_node][next_to_add]
            if branch is None:
                print "Error: cannot build multicast tree"
                return None
            for i in xrange(pathlens[branch][next_to_add]):
                node = paths[branch][next_to_add][i]
                if node not in new_flows:
                    prev_node = paths[branch][next_to_add][i-1]
                    in_port = link_outport[(node, prev_node)]
                    new_flows[node] = {"prev": prev_node,
                                       "in_port": in_port,
                                       "out_ports": set()}
                if i < pathlens[branch][next_to_add]:
                    next_node = paths[branch][next_to_add][i-1]
                    out_port = link_outport[(node, next_node)]
                    new_flows[node]["out_ports"].add(out_port)
        new_flows[next_to_add]["out_ports"].add(client_port)
        return new_flows

    def _client_leave_handler(self, stream, link_outport, paths, pathlens, ev):
        new_flows = deepcopy(stream["curr_flows"])
        curr_node = ev.dpid
        rm_port = ev.out_port
        out_ports = new_flows[curr_node]["out_ports"]
        out_ports.remove(rm_port)
        prev_node = new_flows[curr_node]["prev"]
        while len(out_ports) == 0 and prev_node != -1:
            del new_flows[curr_node]
            rm_port = link_outport[(prev_node, curr_node)]
            curr_node = prev_node
            out_ports = new_flows[curr_node]["out_ports"]
            out_ports.remove(rm_port)
            prev_node = new_flows[curr_node]["prev"]
        return new_flows
