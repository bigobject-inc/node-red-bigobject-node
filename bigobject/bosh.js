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
