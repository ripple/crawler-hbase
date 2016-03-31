'use strict';
var assert = require('chai').assert;
var expect = require('chai').expect;
var Client = require('../index.js').Client;
var fs = require('fs');
var path = require('path');
var _ = require('lodash');

var oldPC = require('./data/processedCrawl1.json');
var newPC = require('./data/processedCrawl2.json');
var rawCrawl = require('./data/rawCrawl2.json');

function expectToBeValidRow(row) {
    expect(row).to.be.an('object');

    expect(row).to.have.property('rowkey');
    expect(row.rowkey).to.be.a('string');

    expect(row).to.have.property('entry_ipp');
    expect(row.entry_ipp).to.be.a('string');

    expect(row).to.have.property('data');
    expect(row.data).to.be.a('string');

    expect(row).to.have.property('exceptions');
    expect(row.exceptions).to.be.a('string');
}

describe('setup tables', function() {
  it ('should initialize tables', function(done) {
    this.timeout(30000);
    var dbUrl = process.env.HBASE_URL;
    var client = new Client(dbUrl);
    client.initTables(true)
    .then(function() {
      done();
    })
    .catch(done)
  });
});

describe('hbaseHelper', function() {
  this.timeout(6000);
  var dbUrl = process.env.HBASE_URL;
  describe('#init', function() {
    it('Shouldn\'t throw an error when given valid dbUrl', function(done) {
      var client = new Client(dbUrl);
      done();
    });
  });

  describe('#storeRawCrawl', function() {
    it('Should store a raw crawl', function(done) {
      var client = new Client(dbUrl);

      client.storeRawCrawl(rawCrawl)
      .then(function(crawlKey) {
        expect(crawlKey).to.be.a('string');
        done();
      })
      .catch(done);
    });
  });

  describe('#storeProcessedCrawl', function() {
    it('Should store a processed crawl and return the key', function(done) {
      var client = new Client(dbUrl);

      client.storeProcessedCrawl(newPC, oldPC)
      .then(function(crawlKey) {
        expect(crawlKey).to.be.a('string');
        done();
      })
      .catch(done);
    });
  });

  describe('#getLatestRawCrawl', function() {
    it('Should return an object with valid properties', function(done) {
      var client = new Client(dbUrl);
      client.getLatestRawCrawl()
      .then(function(row) {
        expectToBeValidRow(row);
        done();
      })
      .catch(done)
    });
  });

  describe('#getRawCrawlByKey', function() {
    it('Should return an object with valid properties', function(done) {
      var client = new Client(dbUrl);
      client.getLatestRawCrawl()
      .then(function(row) {
        var key = row.rowkey;
        return client.getRawCrawlByKey(key);
      })
      .then(function(row) {
        expectToBeValidRow(row);
        done();
      })
      .catch(done)
    });
  });

  describe('#buildChangedNodes', function() {
    it('Should return a valid changed nodes object', function() {
      var client = new Client(dbUrl);
      var changedNodes = client.buildChangedNodes(newPC && newPC.rippleds, oldPC && oldPC.rippleds);
      expect(changedNodes).to.be.an('object');
    });
  });

  describe('#buildNodeStats', function() {
    it('Should return a valid node stats object', function() {
      var client = new Client(dbUrl);

      var nodeStats = client.buildNodeStats(newPC, oldPC);
      expect(nodeStats).to.be.an('object');
      _.each(nodeStats, function (ns) {
        expect(ns).to.have.property('in_add_count');
        expect(ns.in_add_count).to.be.a('Number');

        expect(ns).to.have.property('in_drop_count');
        expect(ns.in_drop_count).to.be.a('Number');

        expect(ns).to.have.property('out_add_count');
        expect(ns.out_add_count).to.be.a('Number');

        expect(ns).to.have.property('out_drop_count');
        expect(ns.out_drop_count).to.be.a('Number');

        expect(ns).to.have.property('in_count');
        expect(ns.in_count).to.be.a('Number');

        expect(ns).to.have.property('out_count');
        expect(ns.out_count).to.be.a('Number');

        expect(ns).to.have.property('pubkey');
        expect(ns.pubkey).to.be.a('string');

        expect(ns.exceptions || '').to.be.a('string');

        expect(ns.ipp || '').to.be.a('string');

        expect(ns.version || '').to.be.a('string');

        expect(ns.uptime || 0).to.be.a('Number')
      });
    });
  });

  describe('#getCrawlInfo', function() {
    it('Should get info of the latest processed crawl', function(done) {
      var client = new Client(dbUrl);
      client.getCrawlInfo()
      .then(function(crawlInfo) {
        expect(crawlInfo).to.have.property('entry');
        expect(crawlInfo.entry).to.be.a('string');
        done();
      })
      .catch(done);
    });
  });

  describe('#getCrawlNodeStats', function() {
    it('Should get info of the latest processed crawl', function(done) {
      var client = new Client(dbUrl);
      client.getCrawlInfo()
      .then(function(crawlInfo) {
        var crawlKey = crawlInfo.rowkey;
        client.getCrawlNodeStats(crawlKey)
        .then(function(nodeStats) {
          expect(nodeStats).to.be.an('Array');
          expect(nodeStats.length).to.be.above(0);
          _.each(nodeStats, function (ns) {
            expect(ns).to.have.property('in_add_count');
            expect(parseInt(ns.in_add_count, 10)).to.be.a('Number');

            expect(ns).to.have.property('in_drop_count');
            expect(parseInt(ns.in_drop_count, 10)).to.be.a('Number');

            expect(ns).to.have.property('out_add_count');
            expect(parseInt(ns.out_add_count, 10)).to.be.a('Number');

            expect(ns).to.have.property('out_drop_count');
            expect(parseInt(ns.out_drop_count, 10)).to.be.a('Number');

            expect(ns).to.have.property('in_count');
            expect(parseInt(ns.in_count, 10)).to.be.a('Number');

            expect(ns).to.have.property('out_count');
            expect(parseInt(ns.out_count, 10)).to.be.a('Number');

            expect(ns).to.have.property('request_time');
            expect(parseInt(ns.request_time, 10)).to.be.a('Number');

            expect(ns).to.have.property('pubkey');
            expect(ns.pubkey).to.be.a('string');

            expect(ns.exceptions || '').to.be.a('string');

            expect(ns.ipp || '').to.be.a('string');

            expect(ns.version || '').to.be.a('string');

            expect(parseInt(ns.uptime, 10) || 0).to.be.a('Number')
          });
          done();
        })
      })
      .catch(done);
    });
  });

  describe('#getNodeHistory', function() {
    it('Should get history of the provided node', function(done) {
      var client = new Client(dbUrl);
      var pubkey = 'n9MjZdu3oBsE1YbE9sQjE2oaBPFnPevPn8ouDznRjdSZu3Zpiep6';
      client.getNodeHistory(pubkey)
      .then(function(nodeHistory) {
        expect(nodeHistory).to.be.an('Array');
        expect(nodeHistory.length).to.be.above(0);
        _.each(nodeHistory, function(nh) {
          expect(nh).to.have.property('ipp');
          expect(nh.ipp).to.be.a('string');

          expect(nh).to.have.property('version');
          expect(nh.version).to.be.a('string');
        });
        done();
      })
      .catch(done);
    });
  });

  describe('#getNodeState', function() {
    it('Should get current state of the provided node', function(done) {
      var client = new Client(dbUrl);
      var pubkey = 'n9MjZdu3oBsE1YbE9sQjE2oaBPFnPevPn8ouDznRjdSZu3Zpiep6';
      client.getNodeState(pubkey)
      .then(function(state) {
        expect(state.node_pubkey).to.be.a('string');
        expect(state.ipp).to.be.a('string');
        expect(state.version).to.be.a('string');
        expect(state.last_updated).to.be.a('string');
        done();
      })
      .catch(done);
    });
  });


  describe('#getConnections', function() {
    it('Should get valid OUTgoing connections for the latest crawl', function(done) {
      var client = new Client(dbUrl);
      client.getCrawlInfo()
      .then(function(crawlInfo) {
        var crawlKey = crawlInfo.rowkey;
        client.getCrawlNodeStats(crawlKey)
        .then(function(nodeStats) {
          var pubKey = nodeStats[0].pubkey;
          client.getConnections(crawlKey, pubKey, 'out')
          .then(function(outgoingConections) {
            expect(outgoingConections).to.be.an('Array');
            _.each(outgoingConections, function(oc) {
              expect(oc).to.have.property('to');
              expect(oc.to).to.be.a('string');

              expect(oc).to.have.property('rowkey');
              expect(oc.rowkey).to.be.a('string');
            })
            done();
          })
        })
      })
      .catch(done);
    });

    it('Should get valid INgoing connections for the latest crawl', function(done) {
      var client = new Client(dbUrl);
      client.getCrawlInfo()
      .then(function(crawlInfo) {
        var crawlKey = crawlInfo.rowkey;
        client.getCrawlNodeStats(crawlKey)
        .then(function(nodeStats) {
          var pubKey = nodeStats[0].pubkey;
          client.getConnections(crawlKey, pubKey, 'in')
          .then(function(ingoingConections) {
            expect(ingoingConections).to.be.an('Array');
            _.each(ingoingConections, function(oc) {
              expect(oc).to.have.property('to');
              expect(oc.to).to.be.a('string');

              expect(oc).to.have.property('rowkey');
              expect(oc.rowkey).to.be.a('string');
            })
            done();
          })
        })
      })
      .catch(done);
    });
  });

describe('#getAllConnections', function() {
    it('Should get valid connections for the latest crawl', function(done) {
      var client = new Client(dbUrl);
      client.getCrawlInfo()
      .then(function(crawlInfo) {
        var crawlKey = crawlInfo.rowkey;
        client.getCrawlNodeStats(crawlKey)
        .then(function(nodeStats) {
          var pubKey = nodeStats[0].pubkey;
          client.getAllConnections(crawlKey)
          .then(function(allConections) {
            expect(allConections).to.be.an('Array');
            expect(allConections.length).to.be.above(0);
            _.each(allConections, function(oc) {
              expect(oc).to.have.property('to');
              expect(oc.to).to.be.a('string');

              expect(oc).to.have.property('rowkey');
              expect(oc.rowkey).to.be.a('string');
            })
            done();
          })
        })
      })
      .catch(done);
    });
  });

});
