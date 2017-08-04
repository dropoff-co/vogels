'use strict';
/**
 * Created by alwoss on 8/4/17.
 */
const NO_ERROR  = void(0);

let plugin      = void(0);

module.exports.setEncryptionPlugin = function(p) {
  if (p && typeof p.encrypt === 'function' && typeof p.decrypt === 'function') {
    plugin = p;
    return;
  }

  if (!p) {
    plugin = void(0);
    return;
  }

  if (typeof p.encrypt !== 'function') {
    throw new Error('Expecting a function called "encrypt" on your plugin');
  }

  if (typeof p.decrypt !== 'function') {
    throw new Error('Expecting a function called "decrypt" on your plugin');
  }
};

module.exports.encrypt = function(key, value, callback) {
  if (!key) {
    return callback(new Error('Expecting a key'));
  }

  if (!value) {
    return callback(new Error('Expecting a value'));
  }

  if (!plugin) {
    return callback(NO_ERROR, value);
  }

  plugin.encrypt(key, value, function(error, data) {
    if (error) {
      callback(error);
    } else if (!data || typeof data !== 'string') {
      callback(new Error('Expecting data parameter of callback to be a string'));
    } else {
      callback(NO_ERROR, data);
    }
  });
};

module.exports.decrypt = function(key, value, callback) {
  if (!key) {
    return callback(new Error('Expecting a key'));
  }

  if (!value) {
    return callback(new Error('Expecting a value'));
  }

  if (!plugin) {
    return callback(NO_ERROR, value);
  }

  plugin.decrypt(key, value, function(error, data) {
    if (error) {
      callback(error);
    } else if (!data || typeof data !== 'string') {
      callback(new Error('Expecting data parameter of callback to be a string'));
    } else {
      callback(NO_ERROR, data);
    }
  });
};
