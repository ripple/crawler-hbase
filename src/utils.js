var moment = require('moment');
var _ = require('lodash');

module.exports = {
    keyToStart: function(key) {
      return moment(parseInt(key.split('_')[0])).format('YYYY-MM-DDTHH:mm:ss.msZ');
    },

    keyToEnd: function(key) {
      return moment(parseInt(key.split('_')[1])).format('YYYY-MM-DDTHH:mm:ss.msZ');
    },

    getNodesKey: function(crawlKey, pubKey) {
      return pubKey + '+' + crawlKey;
    },

    getCrawlNodeStatsKey: function(crawlKey, pubKey) {
      return crawlKey + '+' + pubKey;
    },

    getConnectionKey: function(crawlKey, fromPubKey, toPubKey) {
      return crawlKey + '+' + fromPubKey + '+' + toPubKey;
    },

    getTargetByConnectionKey: function(conKey) {
      return conKey.split('+')[2];
    },

    getSourceByConnectionKey: function(conKey) {
      return conKey.split('+')[1];
    },
    
    /**
     * @param  {Object} connections - each key is <from,to> string
     * returns {
     *   ingoings : <a mapping where each node is mapped to an array of its ingoing connections>
     *   outgoings : <a mapping where each node is mapped to an array of its outgoing connections>
     * }
     */
    getInAndOutGoingPeers: function(connections) {
      var ingoings = {};
      var outgoings = {};
      _.each(connections, function(val, to_from) {
        var to = to_from.split(',')[0];
        var from = to_from.split(',')[1];
        outgoings[from] = outgoings[from] || [];
        outgoings[from].push(to);
        ingoings[to] = ingoings[to] || [];
        ingoings[to].push(from);
      });
      return {ingoings: ingoings, outgoings: outgoings};
    }

}