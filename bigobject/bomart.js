module.exports = function(RED) {
    function bomart(config) {
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
	if(stmt_check != "build" && stmt_check != "create" )
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
				res_str = body;
				msg = {payload : res_str
					, error:0, nodeid:node.id};
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
    RED.nodes.registerType("BigObject Mart",bomart);
}