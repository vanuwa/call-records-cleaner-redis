/**
 * Created by ikebal on 01.09.16.
 */
const Promise = require('bluebird');
const logger = require('../logger');
const redis = require('redis');
const util = require('util');

class Storage {

  /**
   * Data source constructor
   * @param {object} options Object contains info about connection and storage name
   */
  constructor (options) {
    this._client = redis.createClient(options.port, options.host);
    this.setUp(this._client);
    this.key_prefix = options.key_prefix;
  }

  destruct () {
    if (this._client) {
      this._client.quit();
    }
  }

  setUp (client = this._client) {
    client.on('error', (error) => {
      logger.warn('[ redis_data_source ][ on error ]', error);

      throw error;
    });
  }

  exists (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);

      this._client.exists(hash_key, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ EXISTS ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ EXISTS ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ EXISTS ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(!!result);
        }
      });
    });
  }

  /**
   * Insert value at the tail of the list stored at key. If key does not exist, it is created as empty list before performing the push operation.
   * @param {string} key List identifier
   * @param {object} value Value that would be inserted
   * @param {object} options Additional options like list key prefix
   * @return {Promise} thenable object
   */
  rpush (key, value, options = {}) {
    return new Promise((resolve, reject) => {
      // logger.debug(`[ REDIS ][ rpush ] { ${key}: ${util.inspect(value)} }`);

      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const hash_data = Storage.prepareString(value);

      this._client.rpush(hash_key, hash_data, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ RPUSH ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ RPUSH ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ RPUSH ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }

      });
    });
  }

  /**
   * Insert value at the head of the list stored at key. If key does not exist, it is created as empty list before performing the push operation.
   * @param {string} key List identifier
   * @param {object} value Value that would be inserted
   * @param {object} options Additional options like list key prefix
   * @return {Promise} thenable object
   */
  lpush (key, value, options = {}) {
    return new Promise((resolve, reject) => {
      logger.debug(`[ REDIS ][ lpush ] { ${key}: ${util.inspect(value)} }`);

      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const hash_data = Storage.prepareString(value);

      this._client.lpush(hash_key, hash_data, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ LPUSH ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LPUSH ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LPUSH ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }

      });
    });
  }

  /**
   * It blocks the connection when there are no elements to pop from any of the given lists. An element is popped from the tail of the list that is non-empty.
   * If none of the specified keys exist, BLPOP blocks the connection until another client performs an *PUSH operation against one of the keys.
   * @param {string} key List identifier
   * @param {object} options Additional options like list key prefix
   * @return {Promise} thenable object
   */
  brpop (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const timeout = options.timeout || 0;

      this._client.brpop(hash_key, timeout, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ BRPOP ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ BRPOP ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ BRPOP ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * It blocks the connection when there are no elements to pop from any of the given lists. An element is popped from the head of the first list that is non-empty.
   * If none of the specified keys exist, BLPOP blocks the connection until another client performs an *PUSH operation against one of the keys.
   * @param {string} key List identifier
   * @param {object} options Additional options like list key prefix
   * @return {Promise} thenable object
   */
  blpop (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const timeout = options.timeout || 1;

      this._client.blpop(hash_key, timeout, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ BLPOP ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ BLPOP ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ BLPOP ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }

  lpop (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);

      this._client.lpop(hash_key, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ LPOP ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LPOP ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LPOP ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }

  llen (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);

      this._client.llen(hash_key, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ LLEN ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LLEN ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LLEN ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }

  lrange (key, start, stop, options = {}) {
    return new Promise((resolve, reject) => {
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);

      this._client.lrange(hash_key, start, stop, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ LRANGE ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LRANGE ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ LRANGE ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Set value in a storage by key
   * @param {string} key Hash key
   * @param {object} value Value that would be stored
   * @param {object} options Additional options like hash key prefix
   * @returns {Promise} thenable object
   */
  save (key, value, options = {}) {
    return new Promise((resolve, reject) => {
      // logger.debug(`[ REDIS ][ save ] { ${key}: ${util.inspect(value)} }`);
      if (typeof key === 'undefined') {
        return reject('Key is undefined');
      }

      if (!this._client) {
        return reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const hash_data = Storage.prepareString(value);

      this._client.set(hash_key, hash_data, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ SET ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ SET ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ SET ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }

      });
    });
  }

  /**
   * Set value in a storage by key if not exists
   * @param {string} key Hash key
   * @param {object} value Value that would be stored
   * @param {object} options Additional options like hash key prefix
   * @returns {Promise} thenable object
   */
  savenx (key, value, options = {}) {
    return new Promise((resolve, reject) => {
      // logger.debug(`[ REDIS ][ savenx ] { ${key}: ${util.inspect(value)} }`);
      if (typeof key === 'undefined') {
        reject('Key is undefined');
      }

      if (!this._client) {
        reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const hash_data = Storage.prepareString(value);

      this._client.setnx(hash_key, hash_data, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ SETNX ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ SETNX ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ SETNX ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }

      });
    });
  }

  /**
   * Set key to hold the string value and set key to timeout after a given number of seconds
   * @param {string} key Hash key
   * @param {number} timeout Seconds to expire
   * @param {*} value Value that would be stored
   * @param {object} options Additional options like hash key prefix
   * @returns {Promise} thenable object
   */
  saveex (key, timeout, value, options = {}) {
    return new Promise((resolve, reject) => {
      // logger.debug(`[ REDIS ][ saveex ] { ${key}: ${util.inspect(value)} }`);
      if (typeof key === 'undefined') {
        reject('Key is undefined');
      }

      if (!this._client) {
        reject('There is no connection.');
      }

      const hash_key = this.hashKey(key, options.key_prefix);
      const hash_data = Storage.prepareString(value);

      this._client.setex(hash_key, timeout, hash_data, (error, result) => {
        if (error) {
          logger.debug(`[ REDIS ][ SETEX ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ SETEX ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
          logger.debug(`[ REDIS ][ SETEX ][ TYPEOF RESULT ] ${typeof result}\r\n`);

          reject(error);
        } else {
          resolve(result);
        }

      });
    });
  }

  /**
   * Get value from a storage by key
   * @param {string} key Hash key
   * @param {object} options Additional options like hash key prefix
   * @returns {Promise} thenable object
   */
  read (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (this._client) {
        const hash_key = this.hashKey(key, options.key_prefix);

        this._client.get(hash_key, (error, result) => {
          if (error) {
            logger.debug(`[ REDIS ][ GET ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
            logger.debug(`[ REDIS ][ GET ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
            logger.debug(`[ REDIS ][ GET ][ TYPEOF RESULT ] ${typeof result}\r\n`);

            reject(error);
          } else {
            resolve(result);
          }
        });
      } else {
        reject('There is no connection.');
      }
    });
  }

  /**
   * Remove value from a storage by key
   * @param {string} key Hash key
   * @param {object} options Additional options like hash key prefix
   * @returns {Promise} thenable object
   */
  remove (key, options = {}) {
    return new Promise((resolve, reject) => {
      if (this._client) {
        const hash_key = this.hashKey(key, options.key_prefix);

        this._client.del(hash_key, (error, result) => {
          if (error) {
            logger.debug(`[ REDIS ][ DEL ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
            logger.debug(`[ REDIS ][ DEL ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
            logger.debug(`[ REDIS ][ DEL ][ TYPEOF RESULT ] ${typeof result}\r\n`);

            reject(error);
          } else {
            resolve(result);
          }
        });
      } else {
        reject('There is no connection.');
      }
    });
  }

  /**
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted
   * @param {string} key Hash key
   * @param {number} timeout Seconds to expire
   * @param {object} options Additional options like hash key prefix
   * @returns {Promise} thenable object
   */
  expire (key, timeout, options = {}) {
    return new Promise((resolve, reject) => {
      if (this._client) {
        const hash_key = this.hashKey(key, options.key_prefix);

        this._client.expire(hash_key, timeout, (error, result) => {
          if (error) {
            logger.debug(`[ REDIS ][ EXPIRE ][ ERROR ] ${util.inspect(error, null, 10)}\r\n`);
            logger.debug(`[ REDIS ][ EXPIRE ][ RESULT ] ${util.inspect(result, null, 10)}\r\n`);
            logger.debug(`[ REDIS ][ EXPIRE ][ TYPEOF RESULT ] ${typeof result}\r\n`);

            reject(error);
          } else {
            resolve(result);
          }
        });
      } else {
        reject('There is no connection.');
      }
    });
  }

  hashKey (key, prefix = '') {
    return `${prefix || this.key_prefix || ''}${key}`;
  }

  static prepareString (data) {
    return typeof data === 'string' ? data : JSON.stringify(data);
  }

  static prepareHash (data) {
    if (Array.isArray(data)) {
      return { value: JSON.stringify(data) };
    }

    if (data !== null && typeof data === 'object') {
      const obj = {};

      Object.keys(data).forEach((key) => {
        const value = data[key];

        if (typeof value !== 'string') {
          obj[key] = JSON.stringify(value);
          return;
        }

        obj[key] = value;
      });

      return obj;
    }

    return data;
  }

  static parseHash (data) {

    if (data !== null && typeof data === 'object') {
      const obj = {};

      Object.keys(data).forEach((key) => {
        const value = data[key];

        if (typeof value === 'string') {
          logger.debug('[ parse HASH ] Value: ', value);
          obj[key] = JSON.parse(`${value}}`);
          return;
        }

        obj[key] = value;
      });

      return obj;
    }

    return data;
  }
}

module.exports = Storage;
