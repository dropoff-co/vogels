'use strict';

const _           = require('lodash');
const async       = require('async');
const AWS         = require('aws-sdk');
const utils       = require('./utils');
const encryption = require('./encryption');

const serializer = module.exports;

const internals = {};

const NO_ERROR = void(0);
const DONE = 'DONE';

internals.docClient = new AWS.DynamoDB.DocumentClient();

internals.createSet = function(value) {
  if(_.isArray(value) ) {
    return internals.docClient.createSet(value);
  } else {
    return internals.docClient.createSet([value]);
  }
};

const serialize = internals.serialize = {

  binary: function (value) {
    if(_.isString(value)) {
      return utils.strToBin(value);
    }

    return value;
  },

  date : function (value) {
    if(_.isDate(value)) {
      return value.toISOString();
    } else {
      return new Date(value).toISOString();
    }
  },

  boolean : function (value) {
    return !!(value && value !== 'false');
  },

  stringSet : function (value) {
    return internals.createSet(value, 'S');
  },

  numberSet : function (value) {
    return internals.createSet(value, 'N');
  },

  binarySet : function (value) {
    let bins = value;
    if(!_.isArray(value)) {
      bins = [value];
    }

    const vals = _.map(bins, serialize.binary);
    return internals.createSet(vals, 'B');
  }
};

internals.deserializeAttribute = function (value, key, datatypes, options, callback) {
  if(_.isObject(value) && _.isFunction(value.detectType) && _.isArray(value.values)) {
    // value is a Set object from document client
    return callback(void(0), value.values);
  } else if (value && key && datatypes && datatypes[key] === 'DATE') {
    return callback(void(0), new Date(value));
  } else if (value && key && datatypes && datatypes[key] && datatypes[key].indexOf('ENCRYPTED:') === 0) {
    if (options && options.SkipDecrypt === true) {
      callback(NO_ERROR, value);
    } else {
      encryption.decrypt(datatypes[key].substring(10), value, function(error, data) {
        if (error) {
          callback(error);
        } else {
          callback(NO_ERROR, data);
        }
      });
      // internals.kms.decrypt({
      //   CiphertextBlob: new Buffer(value, 'base64')
      // }, function (error, data) {
      //   if (error) {
      //     callback(NO_ERROR, value);
      //   } else {
      //     if (data && data.Plaintext) {
      //       callback(NO_ERROR, data.Plaintext.toString('utf8'));
      //     } else {
      //       callback(NO_ERROR, value);
      //     }
      //   }
      // });
    }
  } else {
    return callback(void(0), value);
  }
};

internals.serializeAttribute = serializer.serializeAttribute = function (value, type, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = void(0);
  }

  options = options || {};

  if(!type) { // if type is unknown, possibly because its an dynamic key return given value
    if (callback) {
      return callback(void(0), value);
    }
    return value;
  }

  if(_.isNull(value)) {
    if (callback) {
      return callback(void(0), null);
    }
    return null;
  }

  options = options || {};

  let toReturn = value;

  if (type.indexOf('ENCRYPTED:') === 0) {
    encryption.encrypt(type.substring(10), toReturn, function(error, data) {
      if (error) {
        callback(error);
      } else {
        callback(NO_ERROR, data);
      }
    });
    // if (internals.kms && callback) {
    //   const KeyId = type.split(':')[1];
    //
    //   const params = {
    //     KeyId: KeyId,
    //     Plaintext: toReturn
    //   };
    //
    //   internals.kms.encrypt(params, function(error, data) {
    //     if (error) {
    //       callback(error);
    //     } else {
    //       if (data && data.CiphertextBlob) {
    //         toReturn = data.CiphertextBlob.toString('base64');
    //       }
    //       return callback(void(0), toReturn);
    //     }
    //   });
    //
    // } else {
    //   if (callback) {
    //     return callback(void(0), toReturn);
    //   }
    //
    //   return toReturn;
    // }
  } else {
    if (type === 'DATE') {
      toReturn = serialize.date(value);
    } else if (type === 'BOOL') {
      toReturn = serialize.boolean(value);
    } else if (type === 'B') {
      toReturn = serialize.binary(value);
    } else if (type === 'NS') {
      toReturn = serialize.numberSet(value);
    } else if (type === 'SS') {
      toReturn = serialize.stringSet(value);
    } else if (type === 'BS') {
      toReturn = serialize.binarySet(value);
    }

    if (callback) {
      return callback(void(0), toReturn);
    }

    return toReturn;
  }
};

serializer.buildKey = function (hashKey, rangeKey, schema/*, callback*/) {
  const obj = {};

  if(_.isPlainObject(hashKey)) {
    obj[schema.hashKey] = hashKey[schema.hashKey];

    if(schema.rangeKey && !_.isNull(hashKey[schema.rangeKey]) && !_.isUndefined(hashKey[schema.rangeKey])) {
      obj[schema.rangeKey] = hashKey[schema.rangeKey];
    }
    _.each(schema.globalIndexes, function (keys) {
      if(_.has(hashKey, keys.hashKey)){
        obj[keys.hashKey] = hashKey[keys.hashKey];
      }

      if(_.has(hashKey, keys.rangeKey)){
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });

    _.each(schema.secondaryIndexes, function (keys) {
      if(_.has(hashKey, keys.rangeKey)){
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });

  } else {
    obj[schema.hashKey] = hashKey;

    if(schema.rangeKey && !_.isNull(rangeKey) && !_.isUndefined(rangeKey)) {
      obj[schema.rangeKey] = rangeKey;
    }
  }

  return serializer.serializeItem(schema, obj);
};

serializer.serializeItem = function (schema, item, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = void(0);
  }

  options = options || {};

  const serializeAsync = function(item, datatypes, callback) {
    datatypes = datatypes || {};

    if(!item) {
      callback(void(0), null);
      return;
    }

    const result = {};

    const pairHandler = function(val, key, cb) {
      if(options.expected && _.isObject(val) && _.isBoolean(val.Exists)) {
        result[key] = val;
        cb();
      } else if(_.isPlainObject(val)) {
        serializeAsync(val, datatypes[key], function(error, data) {
          if (error) {
            cb(error);
          } else {
            result[key] = data;
            cb();
          }
        });
      } else {
        internals.serializeAttribute(val, datatypes[key], options, function(error, attr) {
          if (error) {
            cb(error);
          } else {
            if(!_.isNull(attr) || options.returnNulls) {
              if(options.expected) {
                result[key] = {'Value' : attr};
              } else {
                result[key] = attr;
              }
            }
            cb();
          }
        });
      }
    };

    async.eachOfLimit(item, 10, async.ensureAsync(pairHandler), function(error) {
      if (error) {
        callback(error);
      } else {
        callback(void(0), result);
      }
    });
  };

  const serialize = function (item, datatypes) {
    datatypes = datatypes || {};

    if(!item) {
      return null;
    }

    return _.reduce(item, function (result, val, key) {
      if(options.expected && _.isObject(val) && _.isBoolean(val.Exists)) {
        result[key] = val;
        return result;
      }

      if(_.isPlainObject(val)) {
        result[key] = serialize(val, datatypes[key]);
        return result;
      }

      const attr = internals.serializeAttribute(val, datatypes[key], options);

      if(!_.isNull(attr) || options.returnNulls) {
        if(options.expected) {
          result[key] = {'Value' : attr};
        } else {
          result[key] = attr;
        }
      }

      return result;
    }, {});
  };

  if (callback) {
    return serializeAsync(item, schema._modelDatatypes, callback);
  } else {
    return serialize(item, schema._modelDatatypes);
  }
};

serializer.serializeItemForUpdate = function (schema, action, item) {
  const datatypes = schema._modelDatatypes;

  const data = utils.omitPrimaryKeys(schema, item);

  return _.reduce(data, function (result, value, key) {
    if(_.isNull(value)) {
      result[key] = {Action : 'DELETE'};
    } else if (_.isPlainObject(value) && value.$add) {
      result[key] = {Action : 'ADD', Value: internals.serializeAttribute(value.$add, datatypes[key])};
    } else if (_.isPlainObject(value) && value.$del) {
      result[key] = {Action : 'DELETE', Value: internals.serializeAttribute(value.$del, datatypes[key])};
    } else {
      result[key] =  {Action : action, Value: internals.serializeAttribute(value, datatypes[key])};
    }

    return result;
  }, {});
};

serializer.setEncryptionPlugin = function(plugin) {
  encryption.setEncryptionPlugin(plugin);
};
//
// serializer.setKMS = function(kms) {
//   internals.kms = kms;
// };

serializer.deserializeItem = function (item, schema, options, callback) {
  if (typeof schema === 'function') {
    options = {};
    callback = schema;
    schema = void(0);
  }

  if (typeof options === 'function') {
    callback = options;
    options = {};
  }

  if(_.isNull(item)) {
    callback(null, null);
    return;
  }

  const formatter = function (data, key, callback) {
    let mapped = void(0);

    const processArrayItem = function(value, cb) {
      let result = void(0);

      if(_.isPlainObject(value) || _.isArray(value)) {
        formatter(value, null, function(error, data) {
          if (error) {
            cb(error);
          } else {
            mapped.push(data);
            cb();
          }
        });
      } else {
        internals.deserializeAttribute(value, key, schema ? schema._modelDatatypes : void(0), options, function(error, data) {
          if (error) {
            cb(error);
          } else {
            mapped.push(data);
            cb();
          }
        });
      }
      return result;
    };

    const processArrayItems = function(cb) {
      if (_.isArray(data)) {
        mapped = [];

        async.eachLimit(data, 10, async.ensureAsync(processArrayItem), function(error) {
          if (error) {
            cb(error);
          } else {
            cb(NO_ERROR, DONE);
          }
        });
      } else {
        cb(NO_ERROR, DONE);
      }
    };

    const processMapItem = function(value, key, cb) {
      if(_.isPlainObject(value) || _.isArray(value)) {
        formatter(value, null, function(error, data) {
          if (error) {
            cb(error);
          } else {
            mapped[key] = data;
            cb();
          }
        });
      } else {
        internals.deserializeAttribute(value, key, schema ? schema._modelDatatypes : void(0), options, function(error, data) {
          if (error) {
            cb(error);
          } else {
            mapped[key] = data;
            cb();
          }
        });
      }
    };

    const processMapItems = function(cb) {
      if (!_.isArray(data)) {
        mapped = {};
        async.mapValuesLimit(data, 10, async.ensureAsync(processMapItem), function(error) {
          if (error) {
            cb(error);
          } else {
            cb(NO_ERROR, DONE);
          }
        });
      } else {
        cb(NO_ERROR, DONE);
      }
    };

    const seriesHandler = function(error, results) {
      if (error) {
        callback(error);
      } else if (results) {
        callback(NO_ERROR, mapped);
      }
    };

    async.series({
      array:processArrayItems,
      map: processMapItems
    }, seriesHandler);
  };

  formatter(item, void(0), function(error, data) {
    if (error) {
      callback(error);
    } else {
      callback(NO_ERROR, data);
    }
  });
};
