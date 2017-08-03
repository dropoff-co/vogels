'use strict';

const _            = require('lodash');
const Item         = require('./item');
const Query        = require('./query');
const Scan         = require('./scan');
const EventEmitter = require('events').EventEmitter;
const async        = require('async');
const utils        = require('./utils');
const ParallelScan = require('./parallelScan');
const expressions  = require('./expressions');

const NO_ERROR = void(0);
const DONE = 'DONE';

const internals = {};

const Table = module.exports = function (name, schema, serializer, docClient, logger) {
  this.config = {name : name};
  this.schema = schema;
  this.serializer = serializer;
  this.docClient = docClient;
  this.log = logger;

  this._before = new EventEmitter();
  this.before = this._before.on.bind(this._before);

  this._after= new EventEmitter();
  this.after = this._after.on.bind(this._after);
};

Table.prototype.initItem = function (attrs) {
  const self = this;

  if(self.itemFactory) {
    return new self.itemFactory(attrs);
  } else {
    return new Item(attrs, self);
  }
};

Table.prototype.tableName = function () {
  if(this.schema.tableName) {
    if(_.isFunction(this.schema.tableName)) {
      return this.schema.tableName.call(this);
    } else {
      return this.schema.tableName;
    }
  } else {
    return this.config.name;
  }
};

Table.prototype.sendRequest = function (method, params, callback) {
  const self = this;

  let driver;
  if (_.isFunction(self.docClient[method])) {
    driver = self.docClient;
  } else if (_.isFunction(self.docClient.service[method])) {
    driver = self.docClient.service;
  }

  const startTime = Date.now();

  self.log.info({params : params}, 'vogels %s request', method.toUpperCase());
  driver[method].call(driver, params, function (err, data) {
    const elapsed = Date.now() - startTime;

    if (err) {
      self.log.warn({err : err}, 'vogels %s error', method.toUpperCase());
      return callback(err);
    } else {
      self.log.info({data : data}, 'vogels %s response - %sms', method.toUpperCase(), elapsed);
      return callback(null, data);
    }
  });
};

Table.prototype.get = function (hashKey, rangeKey, options, callback) {
  const self = this;

  if (_.isPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  let params = {
    TableName : self.tableName(),
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  params = _.merge({}, params, options);

  self.sendRequest('get', params, function (err, data) {
    if(err) {
      return callback(err);
    }

    if (data.Item) {
      self.serializer.deserializeItem(data.Item, self.schema, function(error, d) {
        if (error) {
          callback(error);
        } else {
          callback(null, self.initItem(d));
        }
      });
    } else {
      callback(null, null);
    }
  });
};

internals.callBeforeHooks = function (table, name, startFun, callback) {
  const listeners = table._before.listeners(name);

  return async.waterfall([startFun].concat(listeners), callback);
};

Table.prototype.create = function (item, options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  if (_.isArray(item)) {
    async.map(item, function (data, callback) {
      return internals.createItem(self, data, options, callback);
    }, callback);
  } else {
    return internals.createItem(self, item, options, callback);
  }
};

internals.createItem = function (table, item, options, callback) {
  const self = table;

  const start = function (callback) {
    const data = self.schema.applyDefaults(item);

    const paramName = _.isString(self.schema.createdAt) ? self.schema.createdAt : 'createdAt';

    if(self.schema.timestamps && self.schema.createdAt !== false && !_.has(data, paramName)) {
      data[paramName] = new Date().toISOString();
    }

    return callback(null, data);
  };

  internals.callBeforeHooks(self, 'create', start, function (err, data) {
    if(err) {
      return callback(err);
    }

    const result = self.schema.validate(data);

    if(result.error) {
      return callback(result.error);
    }

    const attrs = utils.omitNulls(data);

    let params = {
      TableName : self.tableName(),
      Item : void(0)
    };

    // console.log('++++++++++++Serialize Item attrs ' + JSON.stringify(attrs));
    // console.log('++++++++++++Serialize Item schema ' + JSON.stringify(self.schema));
    //
    // self.serializer.serializeItem(self.schema, attrs, function(error, Item) {
    //   console.log('++++++++++++++Serialize Item Callback');
    //   if (error) {
    //     callback(error);
    //     return;
    //   }
    //
    //   params.Item = Item;
    //
    //   if (options.expected) {
    //     internals.addConditionExpression(params, options.expected);
    //     options = _.omit(options, 'expected');
    //   }
    //
    //   if (options.overwrite === false) {
    //     const expected = _.chain([self.schema.hashKey, self.schema.rangeKey]).compact().reduce(function (result, key) {
    //       _.set(result, key + '.<>', _.get(params.Item, key));
    //       return result;
    //     }, {}).value();
    //
    //     internals.addConditionExpression(params, expected);
    //   }
    //
    //   options = _.omit(options, 'overwrite'); // remove overwrite flag regardless if true or false
    //
    //   params = _.merge({}, params, options);
    //
    //   self.sendRequest('put', params, function (err) {
    //     if(err) {
    //       return callback(err);
    //     }
    //
    //     const item = self.initItem(attrs);
    //     self._after.emit('create', item);
    //
    //     return callback(null, item);
    //   });
    // });

    let Item = void(0);

    const doSerialize = function(cb) {
      self.serializer.serializeItem(self.schema, attrs, function(error, i) {
        if (error) {
          cb(error);
        } else {
          Item = i;
          params.Item = Item;
          cb(NO_ERROR, i);
        }
      });
    };

    const processOptions = function(cb) {
      if (options.expected) {
        internals.addConditionExpression(params, options.expected);
        options = _.omit(options, 'expected');
      }

      if (options.overwrite === false) {
        const expected = _.chain([self.schema.hashKey, self.schema.rangeKey]).compact().reduce(function (result, key) {
          _.set(result, key + '.<>', _.get(params.Item, key));
          return result;
        }, {}).value();

        internals.addConditionExpression(params, expected);
      }

      options = _.omit(options, 'overwrite'); // remove overwrite flag regardless if true or false

      params = _.merge({}, params, options);

      cb(NO_ERROR, DONE);
    };

    const doSend = function(cb) {
      self.sendRequest('put', params, function (error) {
        if(error) {
          cb(error);
        } else {
          const item = self.initItem(attrs);
          self._after.emit('create', item);
          cb(null, item);
        }
      });
    };

    const seriesHandler = function(error, results) {
      if (error) {
        callback(error);
      } else {
        callback(NO_ERROR, results.send);
      }
    };

    async.series({
      serialize: doSerialize,
      options: processOptions,
      send: doSend
    }, seriesHandler);
  });
};

internals.updateExpressions = function (schema, data, options, callback) {
  expressions.serializeUpdateExpression(schema, data, function(error, exp) {
    if(options.UpdateExpression) {
      const parsed = expressions.parse(options.UpdateExpression);

      exp.expressions = _.reduce(parsed, function (result, val, key) {
        if(!_.isEmpty(val)) {
          result[key] = result[key].concat(val);
        }

        return result;
      }, exp.expressions);
    }

    if(_.isPlainObject(options.ExpressionAttributeValues)) {
      exp.values = _.merge({}, exp.values, options.ExpressionAttributeValues);
    }

    if(_.isPlainObject(options.ExpressionAttributeNames)) {
      exp.attributeNames = _.merge({}, exp.attributeNames, options.ExpressionAttributeNames);
    }

    const result = _.merge({}, {
      ExpressionAttributeValues : exp.values,
      ExpressionAttributeNames : exp.attributeNames,
      UpdateExpression : expressions.stringify(exp.expressions),
    });

    callback(NO_ERROR, result);
  });
};

Table.prototype.update = function (item, options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  const start = function (callback) {
    const paramName = _.isString(self.schema.updatedAt) ? self.schema.updatedAt : 'updatedAt';

    if(self.schema.timestamps && self.schema.updatedAt !== false && !_.has(item, paramName)) {
      item[paramName] = new Date().toISOString();
    }

    return callback(null, item);
  };

  internals.callBeforeHooks(self, 'update', start, function (err, data) {
    if(err) {
      return callback(err);
    }

    const hashKey = data[self.schema.hashKey];
    const rangeKey = data[self.schema.rangeKey] || null;

    let params = {
      TableName : self.tableName(),
      Key : self.serializer.buildKey(hashKey, rangeKey, self.schema),
      ReturnValues : 'ALL_NEW'
    };

    internals.updateExpressions(self.schema, data, options, function(error, exp) {
      if(exp.UpdateExpression) {
        params.UpdateExpression = exp.UpdateExpression;
        delete options.UpdateExpression;
      }

      if(exp.ExpressionAttributeValues) {
        params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
        delete options.ExpressionAttributeValues;
      }

      if(exp.ExpressionAttributeNames) {
        params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
        delete options.ExpressionAttributeNames;
      }

      if (options.expected) {
        internals.addConditionExpression(params, options.expected);
        delete options.expected;
      }

      params = _.chain({}).merge(params, options).omitBy(_.isEmpty).value();

      self.sendRequest('update', params, function (err, data) {
        if(err) {
          return callback(err);
        }

        if (data.Attributes) {
          self.serializer.deserializeItem(data.Attributes, self.schema, function(error, d) {
            if (error) {
              callback(error);
            } else {
              const result = self.initItem(d);
              self._after.emit('update', result);
              return callback(null, result);
            }
          });
        } else {
          self._after.emit('update', null);
          return callback(null, null);
        }
      });
    });
  });
};

internals.addConditionExpression = function (params, expectedConditions) {
  _.each(expectedConditions, function (val, key) {
    let operator;
    let expectedValue = null;

    const existingValueKeys = _.keys(params.ExpressionAttributeValues);

    if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === true) {
      operator = 'attribute_exists';
    } else if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === false) {
      operator = 'attribute_not_exists';
    } else if (_.isObject(val) && _.has(val, '<>')) {
      operator = '<>';
      expectedValue = _.get(val, '<>');
    } else {
      operator = '=';
      expectedValue = val;
    }

    const condition = expressions.buildFilterExpression(key, operator, existingValueKeys, expectedValue, null);
    params.ExpressionAttributeNames  = _.merge({}, condition.attributeNames, params.ExpressionAttributeNames);
    params.ExpressionAttributeValues = _.merge({}, condition.attributeValues, params.ExpressionAttributeValues);

    if (_.isString(params.ConditionExpression )) {
      params.ConditionExpression = params.ConditionExpression + ' AND (' + condition.statement + ')';
    } else {
      params.ConditionExpression = '(' + condition.statement + ')';
    }
  });
};

Table.prototype.destroy = function (hashKey, rangeKey, options, callback) {
  const self = this;

  if (_.isPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (_.isPlainObject(rangeKey) && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  if (_.isPlainObject(hashKey)) {
    rangeKey = hashKey[self.schema.rangeKey] || null;
    hashKey = hashKey[self.schema.hashKey];
  }

  let params = {
    TableName : self.tableName(),
    Key : self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  if (options.expected) {
    internals.addConditionExpression(params, options.expected);

    delete options.expected;
  }

  params = _.merge({}, params, options);

  self.sendRequest('delete', params, function (err, data) {
    if(err) {
      return callback(err);
    }

    if(data.Attributes) {
      self.serializer.deserializeItem(data.Attributes, self.schema, function(error, d) {
        if (error) {
          callback(error);
        } else {
          const item = self.initItem(d);
          self._after.emit('destroy', item);
          callback(null, item);
        }
      });
    } else {
      self._after.emit('destroy', null);
      return callback(null, null);
    }
  });
};

Table.prototype.query = function (hashKey) {
  const self = this;

  return new Query(hashKey, self, self.serializer);
};

Table.prototype.scan = function () {
  const self = this;

  return new Scan(self, self.serializer);
};

Table.prototype.parallelScan= function (totalSegments) {
  const self = this;

  return new ParallelScan(self, self.serializer, totalSegments);
};


internals.deserializeItems = function (table, callback) {
  return function (err, data) {
    if(err) {
      return callback(err);
    }

    const result = {};

    const processItem = function(i, cb) {
      table.serializer.deserializeItem(i, table.schema, function(error, data) {
        if (error) {
          cb(error);
        } else {
          if (!result.Items) {
            result.Items = [];
          }
          result.Items.push(table.initItem(data));
          cb();
        }
      });
    };

    const processItems = function(cb) {
      if (data.Items) {
        async.eachSeries(data.Items || [], async.ensureAsync(processItem), function(error) {
          if (error) {
            cb(error);
          } else {
            delete data.Items;
            cb(NO_ERROR, DONE);
          }
        });
      } else {
        cb(NO_ERROR, DONE);
      }
    };

    const processLastEvaluatedKey = function(cb) {
      if(data.LastEvaluatedKey) {
        result.LastEvaluatedKey = data.LastEvaluatedKey;

        delete data.LastEvaluatedKey;
      }

      cb(NO_ERROR, DONE);
    };

    async.series({
      items: processItems,
      lek: processLastEvaluatedKey
    }, function(error, results) {
      if (error) {
        callback(error);
      } else if (results) {
        callback(null, _.merge({}, data, result));
      }
    });
  };

};

Table.prototype.runQuery = function(params, callback) {
  const self = this;

  self.sendRequest('query', params, internals.deserializeItems(self, callback));
};

Table.prototype.runScan = function(params, callback) {
  const self = this;

  self.sendRequest('scan', params, internals.deserializeItems(self, callback));
};

Table.prototype.runBatchGetItems = function (params, callback) {

  const self = this;
  self.sendRequest('batchGet', params, callback);
};

internals.attributeDefinition = function (schema, key) {
  let type = schema._modelDatatypes[key];

  if(type === 'DATE' ||
     (type && type.indexOf('ENCRYPTED:') === 0)) {
    type = 'S';
  }

  return {
    AttributeName : key,
    AttributeType : type
  };
};

internals.keySchema = function (hashKey, rangeKey) {
  const result = [{
    AttributeName : hashKey,
    KeyType : 'HASH'
  }];

  if(rangeKey) {
    result.push({
      AttributeName : rangeKey,
      KeyType : 'RANGE'
    });
  }

  return result;
};

internals.secondaryIndex = function (schema, params) {
  const projection = params.projection || { ProjectionType : 'ALL' };

  return {
    IndexName : params.name,
    KeySchema : internals.keySchema(schema.hashKey, params.rangeKey),
    Projection : projection
  };
};

internals.globalIndex = function (indexName, params) {
  const projection = params.projection || { ProjectionType : 'ALL' };

  return {
    IndexName : indexName,
    KeySchema : internals.keySchema(params.hashKey, params.rangeKey),
    Projection : projection,
    ProvisionedThroughput: {
      ReadCapacityUnits: params.readCapacity || 1,
      WriteCapacityUnits: params.writeCapacity || 1
    }
  };
};

Table.prototype.createTable = function (options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  const attributeDefinitions = [];

  attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.hashKey));

  if(self.schema.rangeKey) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.rangeKey));
  }

  const localSecondaryIndexes = [];

  _.forEach(self.schema.secondaryIndexes, function (params) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    localSecondaryIndexes.push(internals.secondaryIndex(self.schema, params));
  });

  const globalSecondaryIndexes = [];

  _.forEach(self.schema.globalIndexes, function (params, indexName) {

    if(!_.find(attributeDefinitions, { 'AttributeName': params.hashKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.hashKey));
    }

    if(params.rangeKey && !_.find(attributeDefinitions, { 'AttributeName': params.rangeKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    }

    globalSecondaryIndexes.push(internals.globalIndex(indexName, params));
  });

  const keySchema = internals.keySchema(self.schema.hashKey, self.schema.rangeKey);

  const params = {
    AttributeDefinitions : attributeDefinitions,
    TableName : self.tableName(),
    KeySchema : keySchema,
    ProvisionedThroughput : {
      ReadCapacityUnits : options.readCapacity || 1,
      WriteCapacityUnits : options.writeCapacity || 1
    }
  };

  if(localSecondaryIndexes.length >= 1) {
    params.LocalSecondaryIndexes = localSecondaryIndexes;
  }

  if(globalSecondaryIndexes.length >= 1) {
    params.GlobalSecondaryIndexes = globalSecondaryIndexes;
  }

  self.sendRequest('createTable', params, callback);
};

Table.prototype.describeTable = function (callback) {

  const params = {
    TableName : this.tableName(),
  };

  this.sendRequest('describeTable', params, callback);
};

Table.prototype.deleteTable = function (callback) {
  callback = callback || _.noop;

  const params = {
    TableName : this.tableName(),
  };

  this.sendRequest('deleteTable', params, callback);
};

Table.prototype.updateTable = function (throughput, callback) {
  const self = this;
  if (typeof throughput === 'function' && !callback) {
    callback = throughput;
    throughput = {};
  }

  callback = callback || _.noop;
  throughput = throughput || {};

  async.parallel([
    async.apply(internals.syncIndexes, self),
    async.apply(internals.updateTableCapacity, self, throughput),
  ], callback);
};

internals.updateTableCapacity = function (table, throughput, callback) {
  const params = {};

  if (_.has(throughput, 'readCapacity') || _.has(throughput, 'writeCapacity')) {
    params.ProvisionedThroughput = {};

    if (_.has(throughput, 'readCapacity')) {
      params.ProvisionedThroughput.ReadCapacityUnits = throughput.readCapacity;
    }

    if (_.has(throughput, 'writeCapacity')) {
      params.ProvisionedThroughput.WriteCapacityUnits = throughput.writeCapacity;
    }
  }

  if (!_.isEmpty(params)) {
    params.TableName = table.tableName();
    table.sendRequest('updateTable', params, callback);
  } else {
    return callback();
  }
};

internals.syncIndexes = function (table, callback) {
  callback = callback || _.noop;

  table.describeTable(function (err, data) {
    if (err) {
      return callback(err);
    }

    const missing = _.values(internals.findMissingGlobalIndexes(table, data));
    if (_.isEmpty(missing)) {
      return callback();
    }

    // UpdateTable only allows one new index per UpdateTable call
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.html#GSI.OnlineOps.Creating
    const maxIndexCreationsAtaTime = 5;
    async.mapLimit(missing, maxIndexCreationsAtaTime, function (params, callback) {
      const attributeDefinitions = [];

      if(!_.find(attributeDefinitions, { 'AttributeName': params.hashKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.hashKey));
      }

      if(params.rangeKey && !_.find(attributeDefinitions, { 'AttributeName': params.rangeKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.rangeKey));
      }

      const currentWriteThroughput = data.Table.ProvisionedThroughput.WriteCapacityUnits;
      const newIndexWriteThroughput = _.ceil(currentWriteThroughput * 1.5);
      params.writeCapacity = params.writeCapacity || newIndexWriteThroughput;

      table.log.info('adding index %s to table %s', params.name, table.tableName());

      const updateParams = {
        TableName : table.tableName(),
        AttributeDefinitions : attributeDefinitions,
        GlobalSecondaryIndexUpdates : [{Create : internals.globalIndex(params.name, params)}]
      };

      table.sendRequest('updateTable', updateParams, callback);
    }, callback);
  });
};

internals.findMissingGlobalIndexes = function (table, data) {
  if(_.isNull(data) || _.isUndefined(data)) {
    // table does not exist
    return table.schema.globalIndexes;
  } else {
    const indexData = _.get(data, 'Table.GlobalSecondaryIndexes');
    const existingIndexNames = _.map(indexData, 'IndexName');

    return _.reduce(table.schema.globalIndexes, function (result, idx, indexName) {
      if (!_.includes(existingIndexNames, idx.name)) {
        result[indexName] = idx;
      }

      return result;
    }, {});
  }
};
