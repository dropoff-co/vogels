'use strict';

const Scan     = require('./scan');
const async    = require('async');
const NodeUtil = require('util');
const utils    = require('./utils');
const Readable = require('stream').Readable;
const _        = require('lodash');

const ParallelScan = module.exports = function (table, serializer, totalSegments) {
  Scan.call(this, table, serializer);

  this.totalSegments = totalSegments;
};

NodeUtil.inherits(ParallelScan, Scan);

ParallelScan.prototype.exec = function (callback) {
  const self = this;

  let streamMode = false;
  const combinedStream = new Readable({objectMode: true});

  if(!callback) {
    streamMode = true;
    callback = function (err) {
      if(err) {
        combinedStream.emit('error', err);
      }
    };
  }

  const scanFuncs = [];
  _.times(self.totalSegments, function(segment) {
    let scn = new Scan(self.table, self.serializer);
    scn.request = _.cloneDeep(self.request);

    scn = scn.segments(segment, self.totalSegments).loadAll();

    const scanFunc = function (callback) {
      if(streamMode) {
        const stream = scn.exec();

        stream.on('error', callback);

        stream.on('readable', function () {
          var data = stream.read();
          if(data) {
            combinedStream.push(data);
          }
        });

        stream.on('end', callback);

      } else {
        return scn.exec(callback);
      }
    };

    scanFuncs.push(scanFunc);
  });

  let started = false;
  const startScans = function () {
    if(started) {
      return;
    }

    started = true;

    async.parallel(scanFuncs, function (err, responses) {
      if(err) {
        return callback(err);
      }

      combinedStream.push(null);
      return callback(null, utils.mergeResults(responses, self.table.tableName()));
    });
  };

  if(streamMode) {
    combinedStream._read = startScans;
  } else {
    startScans();
  }

  return combinedStream;
};
