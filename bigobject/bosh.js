module.exports = function(RED) {
    function bosh(config) {
        RED.nodes.createNode(this,config);
//	this.boserver = config.boserver;
	this.server = RED.nodes.getNode(config.boserver);
        var node = this;
	
	var request = require('request');
	var res_str;
	var server_url;	
	node.status({fill:"red",shape:"dot",text:"disconnected"});

        if (node.server) {
		server_url = 'http://' + node.server.host
			+ ":" + node.server.port  + '/cmd'
        } else {
		msg={payload:"connect BigObject server failed."
			, error:-1, nodeid:node.id}
		node.send(msg);
        }

        this.on('input', function(msg) {
                var myJSONObject = {"Stmt":msg.stmt};
	        request({
	            url: server_url,
	            method: "POST",
	            json: true,   // <--Very important!!!
	            body: myJSONObject
	        }, function (error, response, body){
	        //    res_str = body.Content;
//			res_str = body
			if(error == null)
			{
				node.status({fill:"green",shape:"dot",text:"connected"});
                                if(body.Status=="0")
                                {
                                        res_str = body;
        //                      msg.payload=res_str;
                                        msg = {payload : res_str
                                                , error:0, nodeid:node.id};
                                }
                                else
                                {
                                        res_str = body.Err;
                                        msg = {payload : res_str
		                                , error: body.Status , nodeid:node.id};
				}
//				res_str = body;
	//			msg.payload=res_str;
//				msg = {payload : res_str
//					, error:0, nodeid:node.id};
				node.send(msg);
			}
			else
			{
				msg = {payload : "send statement error : " + error
					, error:-1, nodeid:node.id}
				node.send(msg);
			}


	        });

        });

    }
    RED.nodes.registerType("BigObject sh",bosh);
}
