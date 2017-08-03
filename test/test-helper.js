'use strict';

const sinon  = require('sinon');
const AWS    = require('aws-sdk');
const Table  = require('../lib/table');
const _      = require('lodash');
const bunyan = require('bunyan');

module.exports.mockKMS = function() {
  const kmsMock = new AWS.KMS({
    region: 'us-west-2',
    apiVersion: '2014-11-01'
  });

  kmsMock.encrypt = function(params, callback) {
    const Plaintext = params.Plaintext;

    const CiphertextBlob = new Buffer(Plaintext, 'utf8');
    callback(void(0), { CiphertextBlob });
  };

  kmsMock.decrypt = function(params, callback) {
    const CiphertextBlob = params.CiphertextBlob;
    const Plaintext = CiphertextBlob.toString('utf8');
    callback(void(0), { Plaintext });
  };

  return kmsMock;
};

module.exports.mockDynamoDB = function () {
  const opts = {
    endpoint : 'http://127.0.0.1:8000',
    apiVersion: '2012-08-10',
    region: 'us-west-2',
    accessKeyId: 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
    secretAccessKey: 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'
  };
  const db = new AWS.DynamoDB(opts);

  db.scan          = sinon.stub();
  db.putItem       = sinon.stub();
  db.deleteItem    = sinon.stub();
  db.query         = sinon.stub();
  db.getItem       = sinon.stub();
  db.updateItem    = sinon.stub();
  db.createTable   = sinon.stub();
  db.describeTable = sinon.stub();
  db.updateTable   = sinon.stub();
  db.deleteTable   = sinon.stub();
  db.batchGetItem  = sinon.stub();
  db.batchWriteItem = sinon.stub();

  return db;
};

module.exports.realDynamoDB = function () {
  const opts = {
    endpoint : 'http://127.0.0.1:8000',
    apiVersion: '2012-08-10',
    region: 'us-west-2',
    accessKeyId: 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
    secretAccessKey: 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'
  };
  return new AWS.DynamoDB(opts);
};

module.exports.mockDocClient = function () {
  const client = new AWS.DynamoDB.DocumentClient({service : exports.mockDynamoDB()});

  const operations= [
    'batchGet',
    'batchWrite',
    'put',
    'get',
    'delete',
    'update',
    'scan',
    'query'
  ];

  _.each(operations, function (op) {
    client[op] = sinon.stub();
  });

  client.service.scan          = sinon.stub();
  client.service.putItem       = sinon.stub();
  client.service.deleteItem    = sinon.stub();
  client.service.query         = sinon.stub();
  client.service.getItem       = sinon.stub();
  client.service.updateItem    = sinon.stub();
  client.service.createTable   = sinon.stub();
  client.service.describeTable = sinon.stub();
  client.service.updateTable   = sinon.stub();
  client.service.deleteTable   = sinon.stub();
  client.service.batchGetItem  = sinon.stub();
  client.service.batchWriteItem = sinon.stub();

  return client;
};

module.exports.mockSerializer = function () {
  return {
    buildKey               : sinon.stub(),
    deserializeItem        : sinon.stub(),
    serializeItem          : sinon.stub(),
    serializeItemForUpdate : sinon.stub()
  };
};

module.exports.mockTable = function () {
  return sinon.createStubInstance(Table);
};

module.exports.fakeUUID = function () {
  return {
    v1: sinon.stub(),
    v4: sinon.stub()
  };
};

module.exports.randomName = function (prefix) {
  return prefix + '_' + Date.now() + '.' + _.random(1000);
};

module.exports.testLogger = function() {
  return bunyan.createLogger({
    name: 'vogels-tests',
    serializers : {err: bunyan.stdSerializers.err},
    level : bunyan.FATAL
  });
};
