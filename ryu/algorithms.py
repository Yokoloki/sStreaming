from copy import deepcopy


class Shortest_Path_Heuristic(object):

    def __init__(self):
        super(Shortest_Path_Heuristic, self).__init__()
        self.event_map = {"EventSwitchLeave": "_topology_changed_handler",
                          "EventLinkDelete": "_topology_changed_handler",
                          "EventStreamClientEnter": "_client_enter_handler",
                          "EventStreamClientLeave": "_client_leave_handler"}

    # Make sure paths && pathlens is up to date
    def cal(self, stream, link_outport, paths, pathlens, ev):
        ev_name = ev.__class__.__name__
        func_name = self.event_map.get(ev_name)
        if func_name is None:
            print "%s not in event_map of Shortest_Path_Heuristic" % ev_name
            exit(1)
        new_tree = getattr(self, func_name)(stream, link_outport, paths, pathlens, ev)
        return new_tree

    def _topology_changed_handler(self, stream, paths, pathlens, ev):
        new_tree = {}
        mod_nodes = set()
        new_tree[stream["src"]["dpid"]] = {"parent": -1,
                                           "children": set()}
        in_nodes = set(new_tree.keys())
        pending = stream["clients"].keys()
        while len(pending) != 0:
            next_to_add = None
            branch = None
            dist = 2 ** 31
            for node in in_nodes:
                for to_add in pending:
                    if to_add in pathlens[node] and \
                            pathlens[node][to_add] < dist:
                                next_to_add = to_add
                                branch = node
                                dist = pathlens[node][to_add]
            if next_to_add is None:
                print "Error: cannot build multicast tree"
                return None, None
            pending.remove(next_to_add)
            for i in xrange(pathlens[branch][next_to_add]):
                node = paths[branch][next_to_add][i]
                if node not in new_tree:
                    parent = paths[branch][next_to_add][i-1]
                    new_tree[node] = {"parent": parent,
                                      "children": set()}
                if i < pathlens[branch][next_to_add]-1:
                    child = paths[branch][next_to_add][i+1]
                    new_tree[node]["children"].add(child)
                in_nodes.add(node)
        # Diff between prev_tree and new_tree
        mod_nodes.update(stream["m_tree"].keys() ^ new_tree.keys())
        to_check = stream["m_tree"].keys() & new_tree.keys()
        for node in to_check:
            prev_stat = stream["m_tree"][node]
            curr_stat = new_tree[node]
            if prev_stat["parent"] != curr_stat["parent"] or \
                    prev_stat["children"] != curr_stat["children"]:
                        mod_nodes.add(node)
                        continue
        return new_tree, mod_nodes

    def _client_enter_handler(self, stream, paths, pathlens, ev):
        new_tree = deepcopy(stream["m_tree"])
        mod_nodes = set()
        tree = set(new_tree.keys())
        next_to_add = ev.dpid
        client_port = ev.out_port
        mod_nodes.add(next_to_add)
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
                return None, None
            for i in xrange(pathlens[branch][next_to_add]):
                node = paths[branch][next_to_add][i]
                if node not in new_tree:
                    parent = paths[branch][next_to_add][i-1]
                    new_tree[node] = {"parent": parent,
                                      "children": set()}
                if i < pathlens[branch][next_to_add]-1:
                    child = paths[branch][next_to_add][i+1]
                    new_tree[node]["children"].add(child)
                mod_nodes.add(node)
        return new_tree, mod_nodes

    def _client_leave_handler(self, stream, paths, pathlens, ev):
        new_tree = deepcopy(stream["m_tree"])
        mod_nodes = set()
        curr_node = ev.dpid
        rm_port = ev.out_port
        mod_nodes.add(curr_node)
        if len(stream["clients"][ev.dpid]) != 0:
            return mod_nodes
        children = new_tree[curr_node]["children"]
        parent = new_tree[curr_node]["parent"]
        while len(children) == 0 and parent != -1:
            del new_tree[curr_node]
            new_tree[parent]["children"].remove(curr_node)
            curr_node = parent
            children = new_tree[curr_node]["children"]
            parent = new_tree[curr_node]["parent"]
            mod_nodes.add(curr_node)
        return new_tree, mod_nodes
