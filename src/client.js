var Promise = require('bluebird');
var moment = require('moment');
var _ = require('lodash');
var utils = require('./utils');
var hbase;

function normalizeData(rows) {
  function normOne(r) {
    return _.mapValues(r, function(rProp) {
      return rProp.toString("UTF-8");
    })
  }
  if (rows instanceof Array)  return _.map(rows, normOne);
  else return normOne(rows);
}

function CrawlHbaseClient(dbUrl) {
  hbase = require('./database').initHbase(dbUrl);
}

CrawlHbaseClient.prototype.storeRawCrawl = function(crawl) {
  return new Promise(function(resolve, reject) {
    var key = moment(crawl.start).valueOf() + '_' + moment(crawl.end).valueOf();
    var cols = {
      'rc:entry_ipp':  crawl.entry,
      'rc:data':       JSON.stringify(crawl.data),
      'rc:exceptions': JSON.stringify(crawl.errors)
    };
    hbase
    .putRow('raw_crawls', key, cols)
    .then(function() {
      resolve(key);
    })
    .catch(reject);
  });
};

/**
 * the generic get function used by almost all the other specific gets
 * @param  {string} startKey - scan start 
 * @param  {string} endKey  - scan end   
 * @param  {number} limit - limit results    
 * @param  {bool} descending - order DESC
 * @param  {string} tableName  - table to use
 * @param  {string} filterString - filter for scanner
 * @return {Array}            
 */
CrawlHbaseClient.prototype.getRows = function(startKey, endKey, limit, descending, tableName, filterString) {
  tableName = tableName || 'raw_crawls';
  return new Promise(function(resolve, reject) {
    var options = {
        table: tableName,
        startRow: startKey,
        stopRow: endKey
      };
    if (descending) options.descending = true;
    if (limit) options.limit = limit;
    if (filterString) options.filterString = filterString;
    hbase.getScan(options, function(err, resp) {
      if (err) return reject(err);
      return resolve(normalizeData(resp));
    });
  });
};

CrawlHbaseClient.prototype.getLatestRow = function() {
  var self = this;
  return new Promise(function(resolve, reject) {
      self.getRows('0', '9', 1, true)
      .then(function(rows) {
        resolve(rows[0]);
      })
      .catch(reject);
    });
};

CrawlHbaseClient.prototype.getRowByKey = function(key) {
  var self = this;
  return new Promise(function(resolve, reject) {
      self.getRows(key, key)
      .then(function(rows) {
        if (rows.length) {
          resolve(rows[0]);
        } else {
          reject('no rows with given key found');
        }
      })
      .catch(reject);
    });
};

CrawlHbaseClient.prototype.storeProcessedCrawl = function(newProcessedCrawl, oldProcessedCrawl) {
  var self = this;
  return new Promise(function(resolve, reject) {
    var crawlKey = newProcessedCrawl.crawl.id;
    var changedNodes = self.buildChangedNodes(newProcessedCrawl.rippleds, oldProcessedCrawl && oldProcessedCrawl.rippleds);
    var nodeStats = self.buildNodeStats(newProcessedCrawl, oldProcessedCrawl);
    Promise.all([
      self.storeCrawlInfo(newProcessedCrawl.crawl, crawlKey),
      self.storeChangedNodes(changedNodes, crawlKey),
      self.storeCrawlNodeStats(nodeStats, crawlKey),
      self.storeConnections(newProcessedCrawl.connections, crawlKey),
    ])
    .then(function(retArray) {
      resolve(crawlKey);
    })
    .catch(reject);
  });
};

CrawlHbaseClient.prototype.storeCrawlInfo = function(crawl, crawlKey) {
  var cols = {
    'c:entry': crawl.entry || '',
  };
  return hbase.putRow('crawls', crawlKey, cols);
};

/**
 * get processed crawl info by crawlKey, or the latest crawl if the crawlKey is not specified
 * @param  {string} crawlKey
 * @return {Object}
 */
CrawlHbaseClient.prototype.getCrawlInfo = function(crawlKey) {  
  var self = this;
  return new Promise(function(resolve, reject) {
    crawlKey = crawlKey || '9';
      self.getRows('0', crawlKey, 1, true, 'crawls')
      .then(function(rows) {
        if (rows.length) {
          resolve(rows[0]);
        } else {
          reject('no crawls with given key found');
        }
      })
      .catch(reject);
    });  
};

CrawlHbaseClient.prototype.buildNodeStats = function(newCrawl, oldCrawl) {
  var np = utils.getInAndOutGoingPeers(newCrawl.connections);  
  var op = utils.getInAndOutGoingPeers(oldCrawl && oldCrawl.connections);

  var nodeStats = _.mapValues(newCrawl.rippleds, function(n, pubKey) {
    var ret = {};
    ret.exceptions = n.errors;
    ret.uptime = n.uptime;
    //TODO request_time
    ret.in_count = n.in;
    ret.out_count = n.out;
    ret.ipp = n.ipp;
    ret.version = n.version;
    ret.pubkey = pubKey;


    var addedInPeers = _.filter(np.ingoings[pubKey], function(inPeer) {
      return !op.ingoings[pubKey] || op.ingoings[pubKey].indexOf(inPeer) !== -1;
    });

    var addedOutPeers = _.filter(np.outgoings[pubKey], function(outPeer) {
      return !op.outgoings[pubKey] || op.outgoings[pubKey].indexOf(outPeer) !== -1;
    });

    var droppedInPeers = _.filter(op.ingoings[pubKey], function(inPeer) {
      return !np.ingoings[pubKey] || np.ingoings[pubKey].indexOf(inPeer) !== -1;
    });

    var droppedOutPeers = _.filter(op.outgoings[pubKey], function(outPeer) {
      return !np.outgoings[pubKey] || np.outgoings[pubKey].indexOf(outPeer) !== -1;
    });
    ret.in_add_count = addedInPeers.length;
    ret.out_add_count = addedOutPeers.length;
    ret.in_drop_count = droppedInPeers.length;
    ret.out_drop_count = droppedOutPeers.length;    
    return ret;
  });

  return nodeStats;
};

/**
 * returns the nodes that either just appeared or have changed since last crawl
 * @param  {Object} newCrawl
 * @param  {Object} oldCrawl
 * @return {Object}         
 */
CrawlHbaseClient.prototype.buildChangedNodes = function(newNodes, oldNodes) {
  var changedNodes = _.pick(newNodes, function(nn, pubKey) {
    var on = oldNodes && oldNodes[pubKey];
    return (!on || on.ipp !== nn.ipp || on.version !== nn.version);
  });
  return changedNodes;
};

CrawlHbaseClient.prototype.storeChangedNodes = function(nodes, crawlKey) {
  var rows = _.object(_.map(nodes, function(n, pubKey) {
    var key = utils.getNodesKey(crawlKey, pubKey);
    var cols = {
      'n:ipp': n.ipp || 'not_present',
      'n:version': n.version || 'not_present',
    };
    return [key, cols];
  }));
  return hbase.putRows('nodes', rows);
};

CrawlHbaseClient.prototype.getNodeHistory = function(pubKey) {
  var self = this;
  var startKey = utils.getNodesKey('0', pubKey);
  var stopKey = utils.getNodesKey('9', pubKey);
  return self.getRows(startKey, stopKey, false, false, 'nodes');
};

CrawlHbaseClient.prototype.storeCrawlNodeStats = function(nodes, crawlKey) {
  var rows = _.object(_.map(nodes, function(n, pubKey) {
    var key = utils.getCrawlNodeStatsKey(crawlKey, pubKey);
    var cols = {
      's:ipp': n.ipp,
      's:version': n.version,
      's:uptime': n.uptime,
      's:request_time': n.request_time,
      's:exceptions': n.exceptions,
      's:in_count': n.in_count,
      's:out_count': n.out_count,
      's:in_add_count': n.in_add_count,
      's:in_drop_count': n.in_drop_count,
      's:out_add_count': n.out_add_count,
      's:out_drop_count': n.out_drop_count,
      's:pubkey': n.pubkey,
    };
    return [key, cols];
  }));
  return hbase.putRows('crawl_node_stats', rows);
};

CrawlHbaseClient.prototype.getCrawlNodeStats = function(crawlKey) {
  var self = this;
  var startKey = utils.getCrawlNodeStatsKey(crawlKey, '0');
  var stopKey = utils.getCrawlNodeStatsKey(crawlKey, 'z');
  return self.getRows(startKey, stopKey, false, false, 'crawl_node_stats');
};


CrawlHbaseClient.prototype.storeConnections = function(connections, crawlKey) {
  var rows = _.object(_.map(connections, function(val, from_to) {
    var from = from_to.split(',')[0];
    var to = from_to.split(',')[1];
    var key = utils.getConnectionKey(crawlKey, from, to);
    var cols = {
      'cn:to': to
    };
    return [key, cols];
  }));
  return hbase.putRows('connections', rows);
};

CrawlHbaseClient.prototype.getConnections = function(crawlKey, pubKey, type) {
  var self = this;
  var startKey, stopKey;
  var fs;
  if(type === 'in') {
    //going to use column filter for this case
    fs = hbase.buildSingleColumnValueFilters([{family:'cn', qualifier: 'to', comparator: "=", value: pubKey}]);
    startKey = utils.getConnectionKey(crawlKey, '0', '0');
    stopKey = utils.getConnectionKey(crawlKey, 'z', 'z');
  } else {
    startKey = utils.getConnectionKey(crawlKey, pubKey, '0');
    stopKey = utils.getConnectionKey(crawlKey, pubKey, 'z');    
  }
  return self.getRows(startKey, stopKey, false, false, 'connections', fs);
};

module.exports = CrawlHbaseClient;