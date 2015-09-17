# crawler-hbase
a library to interact with the crawler tables stored in hbase.
crawler hbase exports two modules: class called Client which constructs an hbase client and a module Utils which is an object containing helper functions.

## Class Client
```
var HbaseClient = require("crawler-hbase").Client;
var client = new HbaseClient("0.0.0.0:9090");
```

### CrawlHbaseClient(dbUrl)
Constructs the client using the provided hbase dbUrl. It is assumed that there is Hbase-thrift running on the provided dbUrl.


###storeRawCrawl(crawl)
TODO Describtion

###getRows(startKey, endKey, limit, descending, tableName, filterString)
TODO Describtion

###getLatestRawCrawl()
TODO Describtion

###getRawCrawlByKey(key)
TODO Describtion

###storeProcessedCrawl(newProcessedCrawl, oldProcessedCrawl)
TODO Describtion

###storeCrawlInfo(crawl, crawlKey)
TODO Describtion

###getCrawlInfo(crawlKey) {
TODO Describtion

###buildNodeStats(newCrawl, oldCrawl)
TODO Describtion

###buildChangedNodes(newNodes, oldNodes)
TODO Describtion

###storeChangedNodes(nodes, crawlKey)
TODO Describtion

###getNodeHistory(pubKey)
TODO Describtion

###storeCrawlNodeStats(nodes, crawlKey)
TODO Describtion

###getCrawlNodeStats(crawlKey)
TODO Describtion

###storeConnections(connections, crawlKey)
TODO Describtion

###getConnections(crawlKey, pubKey, type)
TODO Describtion
