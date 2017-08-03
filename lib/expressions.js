'use strict';

const _           = require('lodash');
const async       = require('async');
const utils       = require('./utils');
const serializer  = require('./serializer');

const internals = {};
const NO_ERROR = void(0);

internals.actionWords = ['SET', 'ADD', 'REMOVE', 'DELETE'];

internals.regexMap = _.reduce(internals.actionWords, function (result, key) {
  result[key] = new RegExp(key + '\\s*(.+?)\\s*(SET|ADD|REMOVE|DELETE|$)');
  return result;
}, {});

// explanation http://stackoverflow.com/questions/3428618/regex-to-find-commas-that-arent-inside-and
internals.splitOperandsRegex = new RegExp(/\s*(?![^(]*\)),\s*/);

internals.match = function (actionWord, str) {
  const match = internals.regexMap[actionWord].exec(str);

  if(match && match.length >= 2) {
    return match[1].split(internals.splitOperandsRegex);
  } else {
    return null;
  }
};

exports.parse = function (str) {
  return _.reduce(internals.actionWords, function (result, actionWord) {
    result[actionWord] = internals.match(actionWord, str);
    return result;
  }, {});
};

exports.serializeUpdateExpression = function (schema, item, callback) {
  const datatypes = schema._modelDatatypes;

  const data = utils.omitPrimaryKeys(schema, item);

  const memo = {
    expressions : {},
    attributeNames : {},
    values : {},
  };

  memo.expressions = _.reduce(internals.actionWords, function (result, key) {
    result[key] = [];

    return result;
  }, {});

  const processDataPair = function(value, key, cb) {
    const valueKey = ':' + key;
    const nameKey = '#' + key;
    if(_.isNull(value) || (_.isString(value) && _.isEmpty(value)) ) {
      memo.expressions.REMOVE.push(nameKey);
      memo.attributeNames[nameKey] = key;
      cb();
    } else if (_.isPlainObject(value) && value.$add) {
      serializer.serializeAttribute(value.$add, datatypes[key], function(error, data) {
        if (error) {
          cb(error);
        } else {
          memo.expressions.ADD.push(nameKey + ' ' + valueKey);
          memo.values[valueKey] = data;
          memo.attributeNames[nameKey] = key;
          cb();
        }
      });
    } else if (_.isPlainObject(value) && value.$del) {
      serializer.serializeAttribute(value.$del, datatypes[key], function(error, data) {
        if (error) {
          cb(error);
        } else {
          memo.expressions.DELETE.push(nameKey + ' ' + valueKey);
          memo.values[valueKey] = data;
          memo.attributeNames[nameKey] = key;
          cb();
        }
      });
    } else {
      serializer.serializeAttribute(value, datatypes[key], function(error, data) {
        if (error) {
          cb(error);
        } else {
          memo.expressions.SET.push(nameKey + ' = ' + valueKey);
          memo.values[valueKey] = data;
          memo.attributeNames[nameKey] = key;
          cb();
        }
      });
    }
  };

  async.mapValuesSeries(data, async.ensureAsync(processDataPair), function(error) {
    if (error) {
      callback(error);
    } else {
      callback(NO_ERROR, memo);
    }
  });
  //
  // var result = _.reduce(data, function (result, value, key) {
  //   var valueKey = ':' + key;
  //   var nameKey = '#' + key;
  //
  //   if(_.isNull(value) || (_.isString(value) && _.isEmpty(value)) ) {
  //     result.expressions.REMOVE.push(nameKey);
  //     result.attributeNames[nameKey] = key;
  //   } else if (_.isPlainObject(value) && value.$add) {
  //     result.expressions.ADD.push(nameKey + ' ' + valueKey);
  //     result.values[valueKey] = serializer.serializeAttribute(value.$add, datatypes[key]);
  //     result.attributeNames[nameKey] = key;
  //   } else if (_.isPlainObject(value) && value.$del) {
  //     result.expressions.DELETE.push(nameKey + ' ' + valueKey);
  //     result.values[valueKey] = serializer.serializeAttribute(value.$del, datatypes[key]);
  //     result.attributeNames[nameKey] = key;
  //   } else {
  //     result.expressions.SET.push(nameKey + ' = ' + valueKey);
  //     result.values[valueKey] = serializer.serializeAttribute(value, datatypes[key]);
  //     result.attributeNames[nameKey] = key;
  //   }
  //
  //   return result;
  // }, memo);
  //
  // return result;
};

exports.stringify = function (expressions) {
  return _.reduce(expressions, function (result, value, key) {
    if(!_.isEmpty(value)) {
      if(_.isArray(value)) {
        result.push(key + ' ' + value.join(', '));
      } else {
        result.push(key + ' ' + value);
      }
    }

    return result;
  }, []).join(' ');
};

internals.formatAttributeValue = function (val) {
  if(_.isDate(val)) {
    return val.toISOString();
  }

  return val;
};

internals.functionOperators = [
  'attribute_exists',
  'attribute_not_exists',
  'attribute_type',
  'begins_with',
  'contains',
  'NOT contains',
  'size'
];

internals.isFunctionOperator = function (operator) {
  return _.includes(internals.functionOperators, operator);
};

internals.uniqAttributeValueName = function(key, existingValueNames) {
  let potentialName = ':' + key;
  let idx = 1;

  while(_.includes(existingValueNames, potentialName)) {
    idx++;
    potentialName = ':' + key + '_' + idx;
  }

  return potentialName;
};

exports.buildFilterExpression = function (key, operator, existingValueNames, val1, val2 ) {
  // IN filter expression is unlike all the others where val1 is an array of values
  if (operator === 'IN') {
    return internals.buildInFilterExpression(key, existingValueNames, val1);
  }

  let v1 = internals.formatAttributeValue(val1);
  let v2 = internals.formatAttributeValue(val2);

  if (operator === 'attribute_exists' && v1 === false) {
    operator = 'attribute_not_exists';
    v1 = null;
  } else if (operator === 'attribute_exists' && v1 === true) {
    v1 = null;
  }

  const path = '#' + key;
  const v1ValueName = internals.uniqAttributeValueName(key, existingValueNames);
  const v2ValueName = internals.uniqAttributeValueName(key, [v1ValueName].concat(existingValueNames));

  let statement = '';

  if (internals.isFunctionOperator(operator)) {
    if (!_.isNull(v1) && !_.isUndefined(v1)) {
      statement = operator + '(' + path + ', ' + v1ValueName + ')';
    } else {
      statement = operator + '(' + path + ')';
    }
  } else if (operator === 'BETWEEN') {
    statement = path + ' BETWEEN ' + v1ValueName + ' AND ' + v2ValueName;
  } else {
    statement = [path, operator, v1ValueName].join(' ');
  }

  const attributeValues = {};

  if (!_.isNull(v1) && !_.isUndefined(v1)) {
    attributeValues[v1ValueName] = v1;
  }

  if (!_.isNull(v2) && !_.isUndefined(v2)) {
    attributeValues[v2ValueName] = v2;
  }

  const attributeNames = {};
  attributeNames[path] = key;

  return {
    attributeNames : attributeNames,
    statement : statement,
    attributeValues : attributeValues
  };
};

internals.buildInFilterExpression = function (key, existingValueNames, values) {
  const path = '#' + key;

  const attributeNames = {};
  attributeNames[path] = key;

  const attributeValues = _.reduce(values, function(result, val) {
    const existing = _.keys(result).concat(existingValueNames);
    const p = internals.uniqAttributeValueName(key, existing);
    result[p] = internals.formatAttributeValue(val);
    return result;
  }, {});

  return {
    attributeNames : attributeNames,
    statement : path + ' IN (' + _.keys(attributeValues) + ')',
    attributeValues : attributeValues
  };
};
