'use strict';

const _           = require('lodash');
const async       = require('async');

const internals = {};

const NO_ERROR = void(0);
const DONE = 'DONE';

internals.buildInitialGetItemsRequest = function (tableName, keys, options) {
  const request = {};

  request[tableName] = _.merge({}, {Keys : keys}, options);

  return { RequestItems : request };
};

internals.serializeKeys = function (keys, table, serializer) {
  return keys.map(function (key) {
    return serializer.buildKey(key, null, table.schema);
  });
};

internals.mergeResponses = function (tableName, responses) {
  const base = {
    Responses : {},
    ConsumedCapacity : []
  };

  base.Responses[tableName] = [];

  return responses.reduce(function (memo, resp) {
    if(resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
};

internals.paginatedRequest = function (request, table, callback) {
  const responses = [];

  const doFunc = function (callback) {

    table.runBatchGetItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = resp.UnprocessedKeys;
      responses.push(resp);

      return callback();
    });
  };

  const testFunc = function () {
    return request !== null && !_.isEmpty(request);
  };

  const resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.tableName(), responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.buckets = function (keys) {
  const buckets = [];

  while( keys.length ) {
    buckets.push( keys.splice(0, 100) );
  }

  return buckets;
};

internals.initialBatchGetItems = function (keys, table, serializer, options, callback) {
  const serializedKeys = internals.serializeKeys(keys, table, serializer);
  const schema = table.schema;
  const request = internals.buildInitialGetItemsRequest(table.tableName(), serializedKeys, options);

  const dynamoItems = [];
  const items = [];

  const paginateRequest = function(cb) {
    internals.paginatedRequest(request, table, function (err, data) {
      if (err) {
        cb(err);
      } else {
        _.forEach(data.Responses[table.tableName()], function(di) {
          dynamoItems.push(di);
        });
        cb(NO_ERROR, DONE);
      }
    });
  };

  const deserializeItem = function(i, cb) {
    serializer.deserializeItem(i, schema, function(error, data) {
      if (error) {
        cb(error);
      } else {
        items.push(table.initItem(data));
        cb();
      }
    });
  };

  const deserializeItems = function(cb) {
    async.eachLimit(dynamoItems, 10, async.ensureAsync(deserializeItem), function(error) {
      if (error) {
        cb(error);
      } else {
        cb(NO_ERROR, DONE);
      }
    });
  };

  const seriesHandler = function(error, results) {
    if (error) {
      callback(error);
    } else if (results) {
      callback(null, items);
    }
  };

  async.series({
    paginate: paginateRequest,
    deserialize: deserializeItems
  }, seriesHandler);
  // internals.paginatedRequest(request, table, function (err, data) {
  //   if(err) {
  //     return callback(err);
  //   }
  //
  //   var dynamoItems = data.Responses[table.tableName()];
  //
  //   var items = _.map(dynamoItems, function(i) {
  //     return table.initItem(serializer.deserializeItem(i, schema));
  //   });
  //
  //   return callback(null, items);
  // });
};

internals.getItems = function (table, serializer) {

  return function (keys, options, callback) {

    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(keys)), function (key, callback) {
      internals.initialBatchGetItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };

};

module.exports = function (table, serializer) {

  return {
    getItems : internals.getItems(table, serializer)
  };

};
