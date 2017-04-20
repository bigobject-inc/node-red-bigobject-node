module.exports = function(RED) {
    function bocsvstream(config) {
        RED.nodes.createNode(this,config);
	this.server = RED.nodes.getNode(config.boserver);
	this.table = config.table;
	this.index = config.index;
	this.encodingconv = config.encodingconv;
	this.sourceencoding = config.sourceencoding;
	this.skiprows = config.skiprows;
	this.replacequote = config.replacequote;
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
				// if encoding convertion is set, convert to utf-8
				var iconv = require('iconv-lite');
				tmp_msg = iconv.decode(new Buffer(msg.payload), node.sourceencoding);
				msg.payload = (iconv.encode(tmp_msg, 'utf8')).toString()
			}
			if(node.replacequote == true)
			{
				// if quote replacement is set, replace quotes 
				msg.payload = msg.payload.replace(/'/g, '\\\'').replace(/"/g, '\\"');
			}

			var insert_data_str = "";
			// use csv parser to parse payload
			var msg_array = csv.parse(msg.payload , ",");
			
			var array = [];
			if(node.index != "*" && node.index != "")
			{
				array = JSON.parse("[" + node.index + "]");
			}


			for(var i = 0; i < msg_array.length; i++) 
			{
				// if skip rows is set, bypass the rows
				if(node.skiprows != "" && node.skiprows > i)
				{
					continue;
				}

				// construct the insert statement 
				if(insert_data_str != "")
				{
					insert_data_str += ",";
				}
				insert_data_str += "(";
				var msg_array_t = msg_array[i];

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
			var table_name ="";
			// if msg.table is assigned, use the name
			if (msg.table != undefined)			
			{
				table_name = msg.table;
			}
			else
			{
				table_name = node.table;
			}

			var insert_stmt = "insert into " + table_name 
                                + " values " + insert_data_str;

			// run the insert statment
			var myJSONObject = {"Stmt":insert_stmt};
		        request({
		            url: server_url,
		            method: "POST",
		            json: true,
		            body: myJSONObject
		        }, function (error, response, body){
				if(error == null)
				{
					node.status({fill:"green",shape:"dot",text:"connected"});
					res_str = body;

					msg.payload=res_str;
					msg.error=0;
					msg.nodeid=node.id;
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
