module.exports = function(RED) {
    function bocsvstream(config) {
        RED.nodes.createNode(this,config);
	this.server = RED.nodes.getNode(config.boserver);
	this.table = config.table;
	this.index = config.index;
	this.encodingconv = config.encodingconv
	this.sourceencoding = config.sourceencoding

        var node = this;
	
	var request = require('request');
	var csv = require('csv-string');
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
		if(msg.payload != "")
		{
			if(node.encodingconv == true)
			{
				var iconv = require('iconv-lite');
				tmp_msg = iconv.decode(new Buffer(msg.payload), node.sourceencoding);
				//console.log("1")
				msg.payload = ((iconv.encode(tmp_msg, 'utf8')).toString()).replace(/'/g, '\\\'').replace(/"/g, '\\"');
				//console.log(msg.payload);
			}
			var insert_data_str = "";
			var msg_array = csv.parse(msg.payload , ",");
			
//			console.log(msg_array);
			var array = [];
			if(node.index != "*" && node.index != "")
			{
				array = JSON.parse("[" + node.index + "]");
			}


			for(var i = 0; i < msg_array.length; i++) 
			{
				if(insert_data_str != "")
				{
					insert_data_str += ",";
				}
				insert_data_str += "(";
				var msg_array_t = msg_array[i];
//				console.log(msg_array_t);
				if(node.index != "*" && node.index != "")
				{
					for(var j = 0; j < array.length; j++)
					{
						if(j != 0){ insert_data_str += ",";}
						insert_data_str += "'" + msg_array_t[array[j]] + "'";
					}
				}
				else
				{
					for(var j = 0; j < msg_array_t.length; j++)
                                        {
		                                if(j != 0){ insert_data_str += ",";}
                		                insert_data_str += "'" + msg_array_t[j] + "'";
                                        }

				}
				insert_data_str += ")"
			}
			var insert_stmt = "insert into " + node.table 
                                + " values " + insert_data_str;
//			console.log(insert_stmt);
			var myJSONObject = {"Stmt":insert_stmt};
		        request({
		            url: server_url,
		            method: "POST",
		            json: true,   // <--Very important!!!
		            body: myJSONObject
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
    RED.nodes.registerType("BigObject CSV stream",bocsvstream);
}
