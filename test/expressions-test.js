'use strict';

const expressions = require('../lib/expressions');
const chai        = require('chai');
const expect      = chai.expect;
const Schema      = require('../lib/schema');
const Joi         = require('joi');

/* global describe,it,beforeEach */

chai.should();

describe('expressions', function () {

  describe('#parse', function () {

    it('should parse single SET action', function () {
      const out = expressions.parse('SET foo = :x');

      expect(out).to.eql({
        SET : ['foo = :x'],
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse multiple SET actions', function () {
      const out = expressions.parse('SET num = num + :n,Price = if_not_exists(Price, 100), #pr.FiveStar = list_append(#pr.FiveStar, :r)');

      expect(out).to.eql({
        SET : ['num = num + :n', 'Price = if_not_exists(Price, 100)', '#pr.FiveStar = list_append(#pr.FiveStar, :r)'],
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse ADD action', function () {
      const out = expressions.parse('ADD num :y');

      expect(out).to.eql({
        SET : null,
        ADD : ['num :y'],
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse REMOVE action', function () {
      const out = expressions.parse('REMOVE Title, RelatedItems[2], Pictures.RearView');

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : ['Title', 'RelatedItems[2]', 'Pictures.RearView'],
        DELETE : null
      });
    });


    it('should parse DELETE action', function () {
      const out = expressions.parse('DELETE color :c');

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : ['color :c']
      });
    });

    it('should parse ADD and SET actions', function () {
      const out = expressions.parse('ADD num :y SET name = :n');

      expect(out).to.eql({
        SET : ['name = :n'],
        ADD : ['num :y'],
        REMOVE : null,
        DELETE : null
      });
    });

    it('should parse multiple actions', function () {
      const out = expressions.parse('SET list[0] = :val1 REMOVE #m.nestedField1, #m.nestedField2 ADD aNumber :val2, anotherNumber :val3 DELETE aSet :val4');

      expect(out).to.eql({
        SET : ['list[0] = :val1'],
        ADD : ['aNumber :val2', 'anotherNumber :val3'],
        REMOVE : ['#m.nestedField1', '#m.nestedField2'],
        DELETE : ['aSet :val4']
      });
    });

    it('should return null actions when given null', function () {
      const out = expressions.parse(null);

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });

    it('should return null actions when given empty string', function () {
      const out = expressions.parse('');

      expect(out).to.eql({
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : null
      });
    });
  });

  describe('#serializeUpdateExpression', function () {
    let schema;

    beforeEach(function () {
      const config = {
        hashKey: 'id',
        schema : {
          id    : Joi.string(),
          email : Joi.string(),
          age   : Joi.number(),
          names : Schema.types.stringSet()
        }
      };

      schema = new Schema(config);
    });

    it('should return single SET action', function (next) {
      const updates = {
        id : 'foobar',
        email : 'test@test.com',
      };

      expressions.serializeUpdateExpression(schema, updates, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : ['#email = :email'],
          ADD    : [],
          REMOVE : [],
          DELETE : [],
        });

        expect(result.values).to.eql({ ':email' : 'test@test.com' });
        expect(result.attributeNames).to.eql({ '#email' : 'email' });
        next();
      });
    });

    it('should return multiple SET actions', function (next) {
      const updates = {
        id : 'foobar',
        email : 'test@test.com',
        age : 33,
        name : 'Steve'
      };

      expressions.serializeUpdateExpression(schema, updates, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : ['#email = :email', '#age = :age', '#name = :name'],
          ADD    : [],
          REMOVE : [],
          DELETE : [],
        });

        expect(result.values).to.eql({
          ':email' : 'test@test.com',
          ':age'   : 33,
          ':name'  : 'Steve'
        });

        expect(result.attributeNames).to.eql({
          '#email' : 'email',
          '#age'   : 'age',
          '#name'  : 'name',
        });

        next();
      });

    });

    it('should return SET and ADD actions', function (next) {
      const updates ={
        id : 'foobar',
        email : 'test@test.com',
        age : {$add : 1}
      };

      expressions.serializeUpdateExpression(schema, updates, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : ['#email = :email'],
          ADD    : ['#age :age'],
          REMOVE : [],
          DELETE : [],
        });

        expect(result.values).to.eql({
          ':email' : 'test@test.com',
          ':age'   : 1
        });

        expect(result.attributeNames).to.eql({
          '#email' : 'email',
          '#age'   : 'age',
        });

        next();
      });
    });

    it('should return single DELETE action', function (next) {
      const updates = {
        id : 'foobar',
        names : { $del : 'tester'},
      };

      expressions.serializeUpdateExpression(schema, updates, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : [],
          ADD    : [],
          REMOVE : [],
          DELETE : ['#names :names'],
        });

        const stringSet = result.values[':names'];

        expect(result.values).to.have.keys([':names']);
        expect(result.values[':names'].type).eql('String');
        expect(stringSet.values).to.eql([ 'tester']);
        expect(stringSet.type).to.eql( 'String');

        expect(result.attributeNames).to.eql({
          '#names' : 'names'
        });

        next();
      });

    });

    it('should return single REMOVE action', function (next) {
      const updates = {
        id : 'foobar',
        email : null,
      };

      expressions.serializeUpdateExpression(schema, updates, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : [],
          ADD    : [],
          REMOVE : ['#email'],
          DELETE : [],
        });

        expect(result.values).to.eql({});

        expect(result.attributeNames).to.eql({
          '#email' : 'email'
        });


        next();
      });

    });

    it('should return single REMOVE action when value is set to empty string', function (next) {
      const updates = {
        id : 'foobar',
        email : '',
      };

      expressions.serializeUpdateExpression(schema, updates, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : [],
          ADD    : [],
          REMOVE : ['#email'],
          DELETE : [],
        });

        expect(result.values).to.eql({});

        expect(result.attributeNames).to.eql({
          '#email' : 'email'
        });

        next();
      });

    });

    it('should return empty actions when passed empty object', function (next) {
      expressions.serializeUpdateExpression(schema, {}, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : [],
          ADD    : [],
          REMOVE : [],
          DELETE : [],
        });

        expect(result.values).to.eql({});
        expect(result.attributeNames).to.eql({});

        next();
      });
    });

    it('should return empty actions when passed null', function (next) {
      expressions.serializeUpdateExpression(schema, null, function(error, result) {
        expect(result.expressions).to.eql({
          SET    : [],
          ADD    : [],
          REMOVE : [],
          DELETE : [],
        });

        expect(result.values).to.eql({});
        expect(result.attributeNames).to.eql({});

        next();
      });

    });

  });

  describe('#stringify', function () {

    it('should return single SET action', function () {
      const params = {
        SET : ['#email = :email']
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email');
    });

    it('should return single SET action when param is a string', function () {
      const params = {
        SET : '#email = :email'
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email');
    });

    it('should return single SET action when other actions are null', function () {
      const params = {
        SET    : ['#email = :email'],
        ADD    : null,
        REMOVE : null,
        DELETE : null
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email');
    });

    it('should return multiple SET actions', function () {
      const params = {
        SET : ['#email = :email', '#age = :n', '#name = :name']
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email, #age = :n, #name = :name');
    });

    it('should return SET and ADD actions', function () {
      const params = {
        SET : ['#email = :email'],
        ADD : ['#age :n', '#foo :bar']
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email ADD #age :n, #foo :bar');
    });

    it('should return stringified ALL actions', function () {
      const params = {
        SET : ['#email = :email'],
        ADD : ['#age :n', '#foo :bar'],
        REMOVE : ['#title', '#picture', '#settings'],
        DELETE : ['#color :c']
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('SET #email = :email ADD #age :n, #foo :bar REMOVE #title, #picture, #settings DELETE #color :c');
    });

    it('should return empty string when passed empty actions', function () {
      const params = {
        SET : [],
        ADD : [],
        REMOVE : [],
        DELETE : []
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('');
    });

    it('should return empty string when passed null actions', function () {
      const params = {
        SET : null,
        ADD : null,
        REMOVE : null,
        DELETE : null
      };

      const out = expressions.stringify(params);
      expect(out).to.eql('');
    });

    it('should return empty string when passed empty object', function () {
      const out = expressions.stringify({});

      expect(out).to.eql('');
    });

    it('should return empty string when passed null', function () {
      const out = expressions.stringify(null);

      expect(out).to.eql('');
    });

    it('should result from stringifying a parsed string should equal original string', function () {
      const exp = 'SET #email = :email ADD #age :n, #foo :bar REMOVE #title, #picture, #settings DELETE #color :c';
      const parsed = expressions.parse(exp);

      expect(expressions.stringify(parsed)).to.eql(exp);
    });

  });

});
