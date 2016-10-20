module.exports = function(RED) {
    function BOServerNode(n) {
        RED.nodes.createNode(this,n);
        this.host = n.host;
        this.port = n.port;
    }
    RED.nodes.registerType("boserver",BOServerNode);
}
