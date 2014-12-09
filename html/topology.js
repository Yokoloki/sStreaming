var CONF = {
    image: {
        width: 50,
        height: 40
    },
    force: {
        width: 1024,
        height: 650,
        dist: 200,
        charge: -900
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

var streams = {
    selected_id: -1,
    sourcing_table: {},
    ids: [],
    menu: d3.select("#menu").append("select")
            .attr("class", "form-control")
            .on("change", _menu_select_changed)
}
streams.update_table = function(host_mac, new_ids) {
    this.sourcing_table[host_mac] = new_ids;
    var id_set = {};
    for(mac in this.sourcing_table){
        this.sourcing_table[mac].forEach(function(stream_id){
            id_set[stream_id] = "";
        });
    }
    var ids = [];
    for(id in id_set){
        ids.push(id);
    }
    ids.sort();
    this.ids = ids;
    var idx = -1;
    if(this.selected_id != -1){
        idx = this.ids.indexOf(this.selected_id);
        if(idx == -1){
            this.selected_id = -1;
            elem.update();
        }
    }
    this.setup_menu(idx);
}

streams.setup_menu = function(idx){
    if(!idx) idx = -1;
    this.menu.selectAll("option").remove();
    this.menu.append("option").text("Topology");
    for(var i=0; i<this.ids.length; i++){
        this.menu.append("option").text("Stream"+this.ids[i]);
    }
    this.menu.property("selectedIndex", idx+1);
}

function _menu_select_changed() {
    var idx = streams.menu.property("selectedIndex");
    data = streams.menu.selectAll("option")[0][idx].text;
    if(data == "Topology"){
        streams.selected_id = -1;
        elem.update();
    }
    else{
        streams.selected_id = Number(data.substring(6));
        elem.update();
    }
}

var elem = {
    force: d3.layout.force()
             .size([CONF.force.width, CONF.force.height])
             .charge(CONF.force.charge)
             .linkDistance(CONF.force.dist)
             .on("tick", _tick),
    svg: d3.select("#graph").append("svg")
           .attr("id", "topology")
           .attr("width", CONF.force.width)
           .attr("height", CONF.force.height),
    console: d3.select("#info").append("div")
               .attr("id", "console")
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
    elem.dragging = d;
    d3.select(this).classed("fixed", d.fixed = true);
    update_console();
}

function update_console(){
    if(!elem.dragging) return;
    elem.console.selectAll("div").remove();
    info_div = elem.console.append("div")
                   .attr("class", "panel panel-info")
    info_div.append("div")
            .attr("class", "panel-heading")
            .text("Node Info");
    table = info_div.append("table")
                    .attr("class", "table");
    if(elem.dragging.type == "host"){
        _table_add_entry(table, "Type", "host");
        _table_add_entry(table, "MAC", elem.dragging.mac);
        _table_add_entry(table, "Sourcing", JSON.stringify(elem.dragging.sourcing));
        _table_add_entry(table, "Receving", JSON.stringify(elem.dragging.receving));
    }
    else{
        _table_add_entry(table, "Type", "switch");
        _table_add_entry(table, "DPID", elem.dragging.dpid);
        _table_add_entry(table, "Priority", JSON.stringify(elem.dragging.priority));
    }

    if(elem.dragging.type == "host"){
        src_div = elem.console.append("div").attr("class", "panel panel-info");
        src_div.append("div").attr("class", "panel-heading").text("Source for");
        src_list = src_div.append("ul").attr("class", "list-group")
                        .attr("align", "right");
        _list_add_entry(src_list, "input", {
            "class": "form-control",
            "type": "text",
            "id": "stream_id_input",
            "placeholder": "Stream_id"
        });
        btn = _list_add_entry(src_list, "button", {
            "type": "button",
            "class": "btn btn-default"
        }).text("Submit").on("click", _post_source_for_request);
        if(streams.ids.length > 0){
            rec_div = elem.console.append("div").attr("class", "panel panel-info");
            rec_div.append("div").attr("class", "panel-heading").text("Receive from");
            rec_ul = rec_div.append("ul").attr("class", "list-group")
                            .attr("align", "right");
            select = _list_add_entry(rec_ul, "select", {
                "class": "form-control",
                "id": "stream_id_select"
            });
            for(var i=0; i<streams.ids.length; i++){
                select.append("option").text("Stream"+streams.ids[i]);
            }
            _list_add_entry(rec_ul, "button", {
                "type": "button",
                "class": "btn btn-default"
            }).text("Submit").on("click", _post_receive_from_request);
        }
    }
    else{
        if(streams.ids.length > 0){
            pri_div = elem.console.append("div").attr("class", "panel panel-info");
            pri_div.append("div").attr("class", "panel-heading").text("Priority Setting");
            pri_ul = pri_div.append("ul").attr("class", "list-group")
                            .attr("align", "right");
            id_select = _list_add_entry(pri_ul, "select", {
                "class": "form-control",
                "id": "stream_id_select"
            })
            for(var i=0; i<streams.ids.length; i++){
                id_select.append("option").text("Stream"+streams.ids[i]);
            }
            pri_select = _list_add_entry(pri_ul, "select", {
                "class": "form-control",
                "id": "priority_select"
            });
            pri_select.append("option").text("Low");
            pri_select.append("option").text("Mid");
            pri_select.append("option").text("High");

            _list_add_entry(pri_ul, "button", {
                "type": "button",
                "class": "btn btn-default"
            }).text("Submit").on("click", _post_priority_change_request);
        }
    }

}

function _table_add_entry(table, key, value) {
    tr = table.append("tr");
    tr.append("td").text(key);
    tr.append("td").text(value);
}

function _list_add_entry(list, type, attrs) {
    entry = list.append("li").attr("class", "list-group-item").append(type);
    for(key in attrs){
        entry.attr(key, attrs[key]);
    }
    return entry;
}

function _post_source_for_request() {
    var data = {
        "mac": elem.dragging.mac,
        "dpid": elem.dragging.dpid,
        "port_no": elem.dragging.port_no
    };
    data.stream_id = Number(elem.console.select("#stream_id_input").property("value"));
    d3.json("/streaming/source_for")
      .post(JSON.stringify(data), function(error, data){
          if(error) return console.warn(error);
          if(data.stat == "succ"){
              return console.log("post source for requset succ");
          }
      });
}

function _post_receive_from_request() {
    var data = {
        "mac": elem.dragging.mac,
        "dpid": elem.dragging.dpid,
        "port_no": elem.dragging.port_no
    };
    idx = elem.console.select("#stream_id_select").property("selectedIndex");
    data.stream_id = streams.ids[idx];
    d3.json("/streaming/receive_from")
      .post(JSON.stringify(data), function(error, data){
          if(error) return console.warn(error);
          if(data.stat == "succ"){
              return console.log("post receive from requset succ");
          }
      });
}

function _post_priority_change_request() {
    var data = {
        "dpid": elem.dragging.dpid
    };
    stream_idx = elem.console.select("#stream_id_select").property("selectedIndex");
    data.stream_id = streams.ids[stream_idx];
    priority_idx = elem.console.select("#priority_select").property("selectedIndex");
    data.priority = 1 + priority_idx * 4;
    console.log(JSON.stringify(data));
    d3.json("/streaming/priority_change")
      .post(JSON.stringify(data), function(error, data){
          if(error) return console.warn(error);
          if(data.stat == "succ"){
              return console.log("post priority change requset succ");
          }
      });
}


elem.node = elem.svg.selectAll(".node");
elem.link = elem.svg.selectAll(".link");
elem.port = elem.svg.selectAll(".port");
elem.update = function () {
    this.force
        .nodes(topo.nodes)
        .links(topo.links)
        .start();

    this.link.remove();
    this.link = elem.svg.selectAll(".link").data(topo.links);
    this.link.enter().append("line")
        .attr("class", "link")
    this.link.attr("style", function(d) {
        sel = streams.selected_id;
        if(d.type == "s2s"){
            if(d.source.priority[sel] && d.target.priority[sel])
                link_pri = Math.min(d.source.priority[sel], d.target.priority[sel]);
            else
                return "stroke-dasharray: 9, 9; stroke: gray";
        }
        else{
            if(d.source.priority[sel] && 
                (d.target.sourcing.indexOf(sel)!=-1 ||
                 d.target.receving.indexOf(sel)!=-1))
                link_pri = d.source.priority[sel];
            else
                return "stroke-dasharray: 9, 9; stroke: gray";
        }
        return "stroke: hsl(" + link_pri*15 +", 100%, 50%)";
    })

    this.node = this.node.data(topo.nodes);
    this.node.exit().remove();
    var nodeEnter = this.node.enter().append("g")
        .attr("class", "node")
        .on("dblclick", function(d) { 
            d3.select(this).classed("fixed", d.fixed = false); 
        })
        .call(this.drag);
    nodeEnter.append("image")
        .attr("xlink:href", function(d) {
            if(d.type == "switch")
                return "./router.svg";
            else if(d.type == "host")
                return "./host.svg";
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
    update_console();
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
                if(!nodes[i].priority) nodes[i].priority = {};
                this.nodes.push(nodes[i]);
            }
        }
        else if(type == "host"){
            for (var i = 0; i < nodes.length; i++) {
                console.log("add host: " + JSON.stringify(nodes[i]));

                nodes[i].type = "host";
                if(!nodes[i].receving) nodes[i].receving = [];
                if(!nodes[i].sourcing) nodes[i].sourcing = [];
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
    update_switches: function(switches) {
        for (var i = 0; i < switches.length; i++) {
            console.log("update switch: " + JSON.stringify(switches[i]));
            if(!switches[i].dpid in this.node_index){
                console.log("update error: "+switches[i].dpid+" not found");
                continue;
            }
            idx = this.node_index[switches[i].dpid];
            this.nodes[idx].priority = switches[i].priority;
        }
    },
    update_hosts: function(hosts) {
        for (var i = 0; i < hosts.length; i++) {
            console.log("update host: " + JSON.stringify(hosts[i]));
            if(!hosts[i].mac in this.node_index){
                console.log("update error: "+hosts[i].mac+" not found");
                continue;
            }
            idx = this.node_index[hosts[i].mac];
            this.nodes[idx].sourcing = hosts[i].sourcing.sort();
            this.nodes[idx].receving = hosts[i].receving.sort();
            streams.update_table(hosts[i].mac, hosts[i].sourcing);
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
                "dpid":params[i].dpid,
                "priority": params[i].priority
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
                "port": params[i].port,
                "sourcing": params[i].sourcing,
                "receving": params[i].receving
            });
        }
        topo.add_nodes("host", hosts);
        elem.update();
        return "";
    },
    event_switch_stat_changed: function(params) {
        var switches = [];
        for(var i=0; i < params.length; i++){
            switches.push({
                "dpid": params[i].dpid,
                "priority": params[i].priority
            });
        }
        console.log(JSON.stringify(switches));
        topo.update_switches(switches);
        elem.update();
        return "";
    },
    event_host_stat_changed: function(params) {
        var hosts = [];
        for(var i=0; i < params.length; i++){
            hosts.push({
                "mac": params[i].mac,
                "dpid": params[i].dpid,
                "port": params[i].port,
                "sourcing": params[i].sourcing,
                "receving": params[i].receving
            })
        }
        console.log(JSON.stringify(hosts));
        topo.update_hosts(hosts);
        elem.update();
        return "";
    }
}

function initialize_topology() {
    d3.json("/topology/disc", function(error, stat) {
        d3.json("/topology/switches", function(error, switches) {
            d3.json("/topology/links", function(error, links) {
                d3.json("/topology/hosts", function(error, hosts) {
                    topo.initialize({switches: switches, links: links, hosts: hosts});
                    elem.update();
                    streams.setup_menu();
                });
            });
        });
    });
}

function main() {
    initialize_topology();
}

main();
