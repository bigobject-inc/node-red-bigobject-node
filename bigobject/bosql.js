module.exports = function(RED) {
    function bosql(config) {
        RED.nodes.createNode(this,config);
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
	if(stmt_check != "select" && stmt_check != "create"
		&& stmt_check != "insert" && stmt_check != "load"
		&& stmt_check != "update" && stmt_check != "trim"
		&& stmt_check != "drop" && stmt_check != "delete"
		&& stmt_check != "alter" && stmt_check != "show"
		&& stmt_check != "desc" && stmt_check != "set" )
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
		        //    res_str = body.Content;
			if(error == null)
			{
				node.status({fill:"green",shape:"dot",text:"connected"});
				/////////////////////////////////////////////////
				if (body.toString().indexOf("\n") != -1)
				{
					var res_t = body.split('\n');

					if(JSON.parse(res_t[0]).Status != "0")
					{
		                                res_str = JSON.parse(res_t[0]).Err;
						msg.payload=res_str;
						msg.error=body.Status;
						msg.nodeid=node.id;
					}
					else
					{
						res_str = '[';
						for(i = 0 ; i < res_t.length ; i++)
						{
							if(res_t[i] != '')
							{
								if(i > 0 )
								{
									 res_str += ',';
								}

								res_str += res_t[i];
							}
						}
						res_str += ']';
//						console.log(res_str);
						msg.payload=JSON.parse(res_str);
						msg.error=0;
						msg.nodeid=node.id;

					}
				}
				else
				{
//					var res_t=[];
//					res_t[0]=body;
					if(body.Status != "0")
					{
		                                res_str = body.Err;
						msg.payload=res_str;
						msg.error=body.Status;
						msg.nodeid=node.id;
					}
					else
					{
						res_str = '[';
						res_str += JSON.stringify(body);
						res_str += ']';
						msg.payload=JSON.parse(res_str);
//						console.log(res_str);
						msg.error=0;
						msg.nodeid=node.id;

					}
				}
				/////////////////////////////////////////////////
/*
				if(body.Status=="0")
				{
					res_str = body.Content;
//					msg = {payload : res_str
//						, error:0, nodeid:node.id};
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

*/
				node.send(msg);
			}
			else
			{
				msg = {payload : "send statement error : " + error}
				node.send(msg);
			}
	        });
	}

	});
    }
    RED.nodes.registerType("BigObject SQL",bosql);
}
