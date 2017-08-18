'use strict';

const Joi       = require('joi');
const uuid      = require('uuid');
const _         = require('lodash');

const JoiAKS = Joi.extend((joi) => ({
  base: joi.string(),
  name: 'string',
  language: {
    encrypted: 'when the value is present vogels will encrypt during serialization and decrypt during deserialization'
  },
  rules: [
    {
      name: 'encrypted',
      params : {
        aksCMK : Joi.string().default('my:aks:default:cmk')
      },
      setup(params) {
        this._flags.encrypted = params.aksCMK;
      }
    }
  ]
}));

const v4 = function() {
  return uuid.v4();
};

const v1 = function() {
  return uuid.v1();
};

const internals =  {};

internals.secondaryIndexSchema = Joi.object().keys({
  hashKey : Joi.string().when('type', { is: 'local', then: Joi.ref('$hashKey'), otherwise : Joi.required()}),
  rangeKey: Joi.string().when('type', { is: 'local', then: Joi.required(), otherwise: Joi.optional() }),
  type : Joi.string().valid('local', 'global').required(),
  name : Joi.string().required(),
  projection : Joi.object(),
  readCapacity : Joi.number().when('type', { is: 'global', then: Joi.optional(), otherwise : Joi.forbidden()}),
  writeCapacity : Joi.number().when('type', { is: 'global', then: Joi.optional(), otherwise : Joi.forbidden()})
});

internals.configSchema = Joi.object().keys({
  hashKey   : Joi.string().required(),
  rangeKey  : Joi.string(),
  tableName : Joi.alternatives().try(Joi.string(), Joi.func()),
  indexes   : Joi.array().items(internals.secondaryIndexSchema),
  schema    : Joi.object(),
  timestamps : Joi.boolean().default(false),
  createdAt  : Joi.alternatives().try(Joi.string(), Joi.boolean()),
  updatedAt  : Joi.alternatives().try(Joi.string(), Joi.boolean())
}).required();

internals.wireType = function (key, flags) {
  if (key === 'string') {
    if (flags && flags.encrypted) {
      return 'ENCRYPTED:' + flags.encrypted;
    } else {
      return 'S';
    }
  } else if (key === 'date') {
    return 'DATE';
  } else if (key === 'number') {
    return 'N';
  } else if (key === 'boolean') {
    return 'BOOL';
  } else if (key === 'binary') {
    return 'B';
  } else if (key === 'array') {
    return 'L';
  }
  return null;
};

internals.findDynamoTypeMetadata = function (data) {
  const meta = _.find(data.meta, function (data) {
    return _.isString(data.dynamoType);
  });

  if(meta) {
    return meta.dynamoType;
  } else {
    return internals.wireType(data.type, data.flags);
  }
};

internals.parseDynamoTypes = function (data) {
  if(_.isPlainObject(data) && data.type === 'object' && _.isPlainObject(data.children)) {
    return internals.parseDynamoTypes(data.children);
  }

  return _.reduce(data, function(result, val, key) {
    if(val.type === 'object' && _.isPlainObject(val.children)) {
      result[key] = internals.parseDynamoTypes(val.children);
    } else {
      result[key] = internals.findDynamoTypeMetadata(val);
    }

    return result;
  }, {});
};

const Schema = module.exports = function (config) {
  this.secondaryIndexes = {};
  this.globalIndexes = {};

  const context = {hashKey : config.hashKey};

  const self = this;
  Joi.validate(config, internals.configSchema, { context: context }, function (err, data) {
    if(err) {
      const msg = 'Invalid table schema, check your config ';
      throw new Error(msg + err.annotate());
    }

    self.hashKey    = data.hashKey;
    self.rangeKey   = data.rangeKey;
    self.tableName  = data.tableName;
    self.timestamps = data.timestamps;
    self.createdAt  = data.createdAt;
    self.updatedAt  = data.updatedAt;

    if(data.indexes) {
      self.globalIndexes    = _.chain(data.indexes).filter({ type: 'global' }).keyBy('name').value();
      self.secondaryIndexes = _.chain(data.indexes).filter({ type: 'local' }).keyBy('name').value();
    }

    if(data.schema) {
      self._modelSchema    = _.isPlainObject(data.schema) ? Joi.object().keys(data.schema) : data.schema;
    } else {
      self._modelSchema = Joi.object();
    }

    if(self.timestamps) {
      const valids = {};
      let createdAtParamName = 'createdAt';
      let updatedAtParamName = 'updatedAt';

      if(self.createdAt) {
        if(_.isString(self.createdAt)) {
          createdAtParamName = self.createdAt;
        }
      }

      if(self.updatedAt) {
        if(_.isString(self.updatedAt)) {
          updatedAtParamName = self.updatedAt;
        }
      }

      if(self.createdAt !== false) {
        valids[createdAtParamName] = Joi.date();
      }

      if(self.updatedAt !== false) {
        valids[updatedAtParamName] = Joi.date();
      }

      self._modelSchema = self._modelSchema.keys(valids);
    }

    self._modelDatatypes = internals.parseDynamoTypes(self._modelSchema.describe());
  });
};

Schema.types = {};

Schema.types.encrypted = function(key) {
  return JoiAKS.string().encrypted(key);
};

Schema.types.stringSet = function () {
  return Joi.array().items(Joi.string()).meta({dynamoType : 'SS'});
};

Schema.types.numberSet = function () {
  return Joi.array().items(Joi.number()).meta({dynamoType : 'NS'});
};

Schema.types.binarySet = function () {
  return Joi.array().items(Joi.binary(), Joi.string()).meta({dynamoType : 'BS'});
};

Schema.types.uuid = function () {
  return Joi.string().guid().default(v4, 'a v4 uuid');
};

Schema.types.timeUUID = function () {
  return Joi.string().guid().default(v1, 'a v1 uuid');
};

Schema.prototype.validate = function (params, options) {
  options = options || {};

  return Joi.validate(params, this._modelSchema, options);
};

internals.invokeDefaultFunctions = function (data) {
  return _.mapValues(data, function (val) {
    if(_.isFunction(val)) {
      return val.call(null);
    } else if (_.isPlainObject(val)) {
      return internals.invokeDefaultFunctions(val);
    } else {
      return val;
    }
  });
};

Schema.prototype.applyDefaults = function (data) {
  const self = this;

  let result = this.validate(data, {abortEarly : false});

  if (result.error &&
      result.error.isJoi === true &&
      result.error.name === 'ValidationError') {
    let retry = false;

    if (result.error.details && result.error.details.length > 0) {
      _.forEach(result.error.details, function(d) {
        if (d.type === 'any.required' && d.path) {
          const pathTokens = d.path.split('.');
          let target = data;

          while (pathTokens.length > 1) {
            const token = pathTokens.shift();
            target = target[token];
          }

          const pathSchema = Joi.reach(self._modelSchema, d.path);
          if (pathSchema) {
            if (pathSchema._flags && pathSchema._flags.default !== void(0) && pathSchema._flags.default !== null) {
              if (typeof pathSchema._flags.default === 'function') {
                target[pathTokens[0]] = pathSchema._flags.default();
                result = self.validate(data, {abortEarly : false});
              } else {
                target[pathTokens[0]] = pathSchema._flags.default;
                result = self.validate(data, {abortEarly : false});
              }
            }
          }
        }
      });
    }

    if (retry) {
      result = this.validate(data, {abortEarly : false});
    }
  }

  return internals.invokeDefaultFunctions(result.value);
};
