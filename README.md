# crawler-hbase
a library to interact with the crawler tables stored in hbase.
crawler hbase exports two modules: class called Client which constructs an hbase client and a module Utils which is an object containing helper functions.

## Class Client
```javascript
var HbaseClient = require("crawler-hbase").Client;
var client = new HbaseClient("0.0.0.0:9090");
```

### CrawlHbaseClient(dbUrl)
Constructs the client using the provided hbase dbUrl. It is assumed that there is Hbase-thrift running on the provided dbUrl.


###storeRawCrawl(crawl)
Stores a raw crawl into table raw_crawls.

###getRows(startKey, endKey, limit, descending, tableName, filterString)
The generic get function used by almost all the other specific gets

###getLatestRawCrawl()
Returns the latest raw crawl.

###getRawCrawlByKey(key)
Gets a raw crawl by key.

###storeProcessedCrawl(newProcessedCrawl, oldProcessedCrawl)
Stores newProcessedCrawl. oldProcessedCrawl is used to calculate the changes that happened between the two crawls.

###storeCrawlInfo(crawl, crawlKey)
Store crawl info.

###getCrawlInfo(crawlKey)
Get crawl info.

###buildNodeStats(newCrawl, oldCrawl)
Constructs the array of nodes to be stored in crawl_node_stats table.

###buildChangedNodes(newNodes, oldNodes)
Constructs the array of changed nodes to be stored in nodes table.

###storeChangedNodes(nodes, crawlKey)
Store changed nodes

###getNodeHistory(pubKey)
Get the array of all different versions tha given node appeared in crawls.

###storeCrawlNodeStats(nodes, crawlKey)
Store stats about the given nodes in the given crawl

###getCrawlNodeStats(crawlKey)
Get stats about the given nodes in the given crawl

###storeConnections(connections, crawlKey)
Store links between nodes.

###getConnections(crawlKey, pubKey, type)
Get links between nodes.


## Utils
provides helper methods to work with hbase tables' keys which have a lot of hidden information in them.

###keyToStart(key)
Get crawl start time from crawl's key

###keyToEnd(key)
Get crawl end time from crawl's key

###getNodesKey(crawlKey, pubKey)
Get key for nodes table using node's public key and crawl key.

###getCrawlNodeStatsKey(crawlKey, pubKey)
Get key for crawl_node_stats table using node's public key and crawl key.

###getConnectionKey(crawlKey, fromPubKey, toPubKey)
Get key for connections table using crawl key and peers' public keys.

###getInAndOutGoingPeers(connections)
connections is an object representing all connections in one crawl. Each key is a <from,to> string.
The function returns in and outgoing links in the following format:
```javascript
{
	ingoings : <a mapping where each node is mapped to an array of its ingoing connections>
	outgoings : <a mapping where each node is mapped to an array of its outgoing connections>
}
```