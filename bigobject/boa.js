module.exports = function(RED) {
    function boa(config) {
        RED.nodes.createNode(this,config);
//	this.boserver = config.boserver;
	this.stmt = config.stmt;
        this.server = RED.nodes.getNode(config.boserver);
        var node = this;
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

	var request = require('request');
	var res_str
//	var server_url = 'http://' + node.boserver  + '/cmd'	
	var stmt_check=node.stmt.split(" ")[0].toLowerCase();

	this.on('input', function(msg) {
	if(stmt_check != "find" && stmt_check != "get"
		&& stmt_check != "apply" )
	{
		msg={payload:"unsupport statements, please check the supported statement."
			, error:-1, nodeid:node.id};
                node.send(msg);
	}
	else
	{

	        request({
	            url: server_url,
	            method: "POST",
	            json: true,
	            body: {"Stmt":node.stmt}
	        }, function (error, response, body){
			if(error == null)
			{
				node.status({fill:"green",shape:"dot",text:"connected"});
                                if(body.Status=="0")
                                {
                                        res_str = body.Content;
//                                        msg = {payload : res_str
//                                                , error:0, nodeid:node.id};
					msg.payload=res_str;
					msg.error=0;
					msg.nodeid=node.id;
                                }
                                else
                                {
                                        res_str = body.Err;
//                                        msg = {payload : res_str
//                                                , error: body.Status , nodeid:node.id};
					msg.payload=res_str;
					msg.error=body.Status;
					msg.nodeid=node.id;


				}
//				res_str = body.Content;
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
	}

	});
    }
    RED.nodes.registerType("BigObject Analytics",boa);
}
