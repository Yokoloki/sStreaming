function init_control_menu(){
    d3.json("/topology/hosts", function(error, hosts) {
        cb_1 = d3.select("#cb_div_1").append("select")
                                     .attr("class", "form-control")
                                     .attr("id", "cb_1");
        cb_2 = d3.select("#cb_div_2").append("select")
                                     .attr("class", "form-control")
                                     .attr("id", "cb_2");
        btn = d3.select("#btn_div").append("button")
                                   .attr("type", "button")
                                   .attr("class", "btn btn-default")
                                   .text("Submit")
                                   .on("click", play_videos);
        for(var i=0; i<hosts.length; i++){
            cb_1.append("option").text("h"+Number("0x"+hosts[i].dpid));
            cb_2.append("option").text("h"+Number("0x"+hosts[i].dpid));
        }
    });
}

function play_videos(){
    h_1 = d3.select("#cb_1").property("value");
    h_2 = d3.select("#cb_2").property("value");
    flowplayer("video1", "flowplayer.swf", {
        plugins: {
            flashls: {
                url: "flashlsFlowPlayer.swf",
                hls_maxbufferlength: 30
            }
        },
        clip: {
            accelerated: true,
            url: "streaming/"+h_1+"/stream.m3u8",
            provider: "flashls",
            autoPlay: true,
            autoBuffering: true
        }
    }).ipad();
    flowplayer("video2", "flowplayer.swf", {
        plugins: {
            flashls: {
                url: "flashlsFlowPlayer.swf",
                hls_maxbufferlength: 30
            }
        },
        clip: {
            accelerated: true,
            url: "streaming/"+h_2+"/stream.m3u8",
            provider: "flashls",
            autoPlay: true,
            autoBuffering: true
        }
    }).ipad();
}

function main() {
    init_control_menu();
}

main();
