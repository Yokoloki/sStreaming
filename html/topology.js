var CONF = {
    image: {
        width: 50,
        height: 40
    },
    force: {
        width: 960,
        height: 500,
        dist: 200,
        charge: -600
    }
};

var ws = new WebSocket("ws://" + location.host + "/topology/ws");
ws.onmessage = function(event) {
    var data = JSON.parse(event.data);

    var result = rpc[data.method](data.params);

    var ret = {"id": data.id, "jsonrpc": "2.0", "result": result};
    this.send(JSON.stringify(ret));
}

function trim_zero(obj) {
    return String(obj).replace(/^0+/, "");
}

function dpid_to_int(dpid) {
    return Number("0x" + dpid);
}

var elem = {
    force: d3.layout.force()
        .size([CONF.force.width, CONF.force.height])
        .charge(CONF.force.charge)
        .linkDistance(CONF.force.dist)
        .on("tick", _tick),
    svg: d3.select("body").append("svg")
        .attr("id", "topology")
        .attr("width", CONF.force.width)
        .attr("height", CONF.force.height),
    console: d3.select("body").append("div")
        .attr("id", "console")
        .attr("width", CONF.force.width)
};
function _tick() {
    elem.link.attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });

    elem.node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

    elem.port.attr("transform", function(d) {
        var p = topo.get_port_point(d);
        return "translate(" + p.x + "," + p.y + ")";
    });
}
elem.drag = elem.force.drag().on("dragstart", _dragstart);
function _dragstart(d) {
    /*
    var dpid = dpid_to_int(d.dpid)
    d3.json("/stats/flow/" + dpid, function(e, data) {
        flows = data[dpid];
        console.log(flows);
        elem.console.selectAll("ul").remove();
        li = elem.console.append("ul")
            .selectAll("li");
        li.data(flows).enter().append("li")
            .text(function (d) { return JSON.stringify(d, null, " "); });
    });
    */
    d3.select(this).classed("fixed", d.fixed = true);
}
elem.node = elem.svg.selectAll(".node");
elem.link = elem.svg.selectAll(".link");
elem.port = elem.svg.selectAll(".port");
elem.update = function () {
    this.force
        .nodes(topo.nodes)
        .links(topo.links)
        .start();

    this.link = this.link.data(topo.links);
    this.link.exit().remove();
    this.link.enter().append("line")
        .attr("class", "link")
    this.link.attr("style", function(d) {
            if(d.target.type == "host")
                link_pri = d.source.pri;
            else
                link_pri = Math.min(d.source.pri, d.target.pri);
            return "stroke: hsl(" + link_pri*15 +", 100%, 50%)";
        })

    this.node = this.node.data(topo.nodes);
    this.node.exit().remove();
    var nodeEnter = this.node.enter().append("g")
        .attr("class", "node")
        .on("dblclick", function(d) { d3.select(this).classed("fixed", d.fixed = false); })
        .call(this.drag);
    nodeEnter.append("image")
        .attr("xlink:href", function(d) {
            if(d.type == "switch"){
                return "./router.svg";
            }
            else if(d.type == "host"){
                return "./host.svg";
            }
        })
        .attr("x", -CONF.image.width/2)
        .attr("y", -CONF.image.height/2)
        .attr("width", CONF.image.width)
        .attr("height", CONF.image.height);
    nodeEnter.append("text")
        .attr("dx", -CONF.image.width/2)
        .attr("dy", CONF.image.height-10)
        .text(function(d) { return "dpid: " + trim_zero(d.dpid); });

    var ports = topo.get_ports();
    this.port.remove();
    this.port = this.svg.selectAll(".port").data(ports);
    var portEnter = this.port.enter().append("g")
        .attr("class", "port");
    portEnter.append("circle")
        .attr("r", 8);
    portEnter.append("text")
        .attr("dx", -3)
        .attr("dy", 3)
        .text(function(d) { return trim_zero(d.port_no); });
};

function is_valid_link(link) {
    return (link.src.dpid < link.dst.dpid);
}

var topo = {
    nodes: [],
    links: [],
    node_index: {}, // dpid -> index of nodes array
    initialize: function (data) {
        this.add_nodes("switch", data.switches);
        this.add_links(data.links);
        this.add_nodes("host", data.hosts);
    },
    add_nodes: function (type, nodes) {
        if(type == "switch"){
            for (var i = 0; i < nodes.length; i++) {
                console.log("add switch: " + JSON.stringify(nodes[i]));
                nodes[i].type = "switch";
                nodes[i].pri = 3;
                this.nodes.push(nodes[i]);
            }
        }
        else if(type == "host"){
            for (var i = 0; i < nodes.length; i++) {
                console.log("add host: " + JSON.stringify(nodes[i]));

                nodes[i].type = "host";
                var host_idx = this.nodes.length;
                var switch_idx = this.node_index[nodes[i].dpid];
                if(switch_idx == null) continue;
                var link = {
                    type: "s2h",
                    source: switch_idx,
                    target: host_idx,
                    port: {
                        src: {dpid: nodes[i].dpid, port_no: nodes[i].port},
                        dst: {mac: nodes[i].mac}
                    }
                }
                this.nodes.push(nodes[i]);
                this.links.push(link);
            } 
        }
        this.refresh_node_index();
    },
    add_links: function (links) {
        for (var i = 0; i < links.length; i++) {
            if (!is_valid_link(links[i])) continue;
            console.log("add link: " + JSON.stringify(links[i]));

            var src_dpid = links[i].src.dpid;
            var dst_dpid = links[i].dst.dpid;
            var src_index = this.node_index[src_dpid];
            var dst_index = this.node_index[dst_dpid];
            if(src_index == null || dst_index == null) continue;
            var link = {
                type: "s2s",
                source: src_index,
                target: dst_index,
                port: {
                    src: links[i].src,
                    dst: links[i].dst
                }
            }
            this.links.push(link);
        }
    },
    delete_switches: function (switches) {
        for (var i = 0; i < switches.length; i++) {
            console.log("delete switch: " + JSON.stringify(switches[i]));
            sw = switches[i];
            //Delete switch && hosts of that switch
            for (var i = 0; i < this.nodes.length; i++) {
                if(this.nodes[i].dpid == sw.dpid) {
                    this.nodes.splice(i--, 1);
                }
            }
            //Delete related links
            for (var i = 0; i < this.links.length; i++) {
                link = this.links[i];
                if(link.type == "s2h") {
                    if(link.port.src.dpid == sw.dpid) {
                        this.links.splice(i--, 1);
                    }
                }
                else if(link.type == "s2s") {
                    if(link.port.src.dpid == sw.dpid 
                            || link.port.dst.dpid == sw.dpid) {
                        this.links.splice(i--, 1);
                    }
                }
            }
        }
        this.refresh_node_index();
        this.refresh_links();
    },
    delete_links: function (links) {
        for (var i = 0; i < links.length; i++) {
            if (!is_valid_link(links[i])) continue;
            console.log("delete link: " + JSON.stringify(links[i]));

            link_index = this.get_link_index(links[i]);
            if (link_index != null){
                this.links.splice(link_index, 1);
            }
        }
    },
    update_switches_pri: function(switches) {
        for (var i = 0; i < switches.length; i++) {
            console.log("update switch priority: " + JSON.stringify(switches[i]));
            if(!switches[i].dpid in this.node_index){
                console.log("update error: "+switches[i].dpid+" not found");
                continue;
            }
            sw_idx = this.node_index[switches[i].dpid];
            this.nodes[sw_idx].pri = switches[i].pri;
        }
    },
    get_link_index: function (link) {
        for (var i = 0; i < this.links.length; i++) {
            if (link.src.dpid == this.links[i].port.src.dpid &&
                    link.src.port_no == this.links[i].port.src.port_no &&
                    link.dst.dpid == this.links[i].port.dst.dpid &&
                    link.dst.port_no == this.links[i].port.dst.port_no) {
                return i;
            }
        }
        return null;
    },
    get_ports: function () {
        var ports = [];
        var pushed = {};
        for (var i = 0; i < this.links.length; i++) {
            function _push(p, dir) {
                key = p.dpid + ":" + p.port_no;
                if (key in pushed) {
                    return 0;
                }

                pushed[key] = true;
                p.link_idx = i;
                p.link_dir = dir;
                return ports.push(p);
            }
            if (this.links[i].type == "s2s") {
                _push(this.links[i].port.src, "source");
                _push(this.links[i].port.dst, "target");
            }
        }
        return ports;
    },
    get_port_point: function (d) {
        var weight = 0.9;

        var link = this.links[d.link_idx];
        var x1 = link.source.x;
        var y1 = link.source.y;
        var x2 = link.target.x;
        var y2 = link.target.y;

        if (d.link_dir == "target") weight = 1.0 - weight;

        var x = x1 * weight + x2 * (1.0 - weight);
        var y = y1 * weight + y2 * (1.0 - weight);

        return {x: x, y: y};
    },
    refresh_node_index: function(){
        this.node_index = {};
        for (var i = 0; i < this.nodes.length; i++) {
            node = this.nodes[i];
            if(node.type == "switch") {
                this.node_index[node.dpid] = i;
            }
            else if (node.type == "host") {
                this.node_index[node.mac] = i;
            }
        }
    },
    refresh_links: function(){
        for (var i = 0; i < this.links.length; i++) {
            link = this.links[i];
            if(link.type == "s2h") {
                link.source = this.node_index[link.port.src.dpid];
                link.target = this.node_index[link.port.dst.mac];
            }
            else if(link.type == "s2s") {
                link.source = this.node_index[link.port.src.dpid];
                link.target = this.node_index[link.port.dst.dpid];
            }
        }
    }
}

var rpc = {
    event_switch_enter: function (params) {
        var switches = [];
        for(var i=0; i < params.length; i++){
            switches.push({
                "dpid":params[i].dpid
            });
        }
        topo.add_nodes("switch", switches);
        elem.update();
        return "";
    },
    event_switch_leave: function (params) {
        var switches = [];
        for(var i=0; i < params.length; i++){
            switches.push({
                "dpid":params[i].dpid
            });
        }
        topo.delete_switches(switches);
        elem.update();
        return "";
    },
    event_link_add: function (links) {
        topo.add_links(links);
        elem.update();
        return "";
    },
    event_link_delete: function (links) {
        topo.delete_links(links);
        elem.update();
        return "";
    },
    event_host_reg: function (params) {
        var hosts = [];
        for(var i=0; i < params.length; i++){
            hosts.push({
                "mac": params[i].mac, 
                "dpid": params[i].dpid, 
                "port": params[i].port
            });
        }
        topo.add_nodes("host", hosts);
        elem.update();
        return "";
    },
    event_switch_pri_changed: function(params) {
        var switches = [];
        for(var i=0; i < params.length; i++){
            switches.push({
                "dpid": params[i].dpid,
                "pri": params[i].pri
            });
        }
        topo.update_switches_pri(switches);
        elem.update();
        return "";
    }
}

function initialize_topology() {
    d3.json("/topology/switches", function(error, switches) {
        d3.json("/topology/links", function(error, links) {
            d3.json("/topology/hosts", function(error, hosts) {
                topo.initialize({switches: switches, links: links, hosts: hosts});
                elem.update();
            });
        });
    });
}

function main() {
    initialize_topology();
}

main();
