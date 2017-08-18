'use strict';

const Item   = require('../lib/item');
const Table  = require('../lib/table');
const Schema = require('../lib/schema');
const chai   = require('chai');
const expect = chai.expect;
const helper = require('./test-helper');
const serializer = require('../lib/serializer');
const Joi    = require('joi');

/* global describe,it,beforeEach */

chai.should();

describe('item', function() {
  let table;

  beforeEach(function () {
    const config = {
      hashKey: 'num',
      schema : {
        num : Joi.number(),
        name : Joi.string()
      }
    };

    const schema = new Schema(config);

    table = new Table('mockTable', schema, serializer, helper.mockDocClient(), helper.testLogger());
  });

  it('JSON.stringify should only serialize attrs', function() {
    const attrs = {num: 1, name: 'foo'};
    const item = new Item(attrs, table);
    const stringified = JSON.stringify(item);

    stringified.should.equal(JSON.stringify(attrs));
  });

  describe('#save', function () {

    it('should return error', function (done) {
      table.docClient.put.yields(new Error('fail'));

      const attrs = {num: 1, name: 'foo'};
      const item = new Item(attrs, table);

      item.save(function (err, data) {
        expect(err).to.exist;
        expect(data).to.not.exist;

        return done();
      });

    });

  });

  describe('#update', function () {
    it('should return item', function (done) {
      table.docClient.update.yields(null, {Attributes : {num : 1, name : 'foo'}});

      const attrs = {num: 1, name: 'foo'};
      const item = new Item(attrs, table);

      item.update(function (err, data) {
        expect(err).to.not.exist;
        expect(data.get()).to.eql({ num : 1, name : 'foo'});

        return done();
      });
    });


    it('should return error', function (done) {
      table.docClient.update.yields(new Error('fail'));

      const attrs = {num: 1, name: 'foo'};
      const item = new Item(attrs, table);

      item.update(function (err, data) {
        expect(err).to.exist;
        expect(data).to.not.exist;

        return done();
      });

    });

    it('should return null', function (done) {
      table.docClient.update.yields(null, {});

      const attrs = {num: 1, name: 'foo'};
      const item = new Item(attrs, table);

      item.update(function (err, data) {
        expect(err).to.not.exist;
        expect(data).to.not.exist;

        return done();
      });
    });

  });
});
