'use strict';

const _ = require('lodash');
const redis = require('redis');
const uuid = require('uuid/v4');

const DEFAULT_TTL = 1000*10;
const CONFIG_SCHEMA = {
  "properties": {
    "redis": {
      "description": "Redis connection info. (https://www.npmjs.com/package/redis#rediscreateclient)",
      "type": "object"
    },
    "reply_channel": {"type": "string"},
    "default_ttl": {
      "type": "integer",
      "default": DEFAULT_TTL
    },
    "names": {
      "description": "Map of request names to Redis channels",
      "type": "object",
      "patternProperties": {
        ".*": {"type": "string"}
      }
    },
    "patterns": {
      "description": "Map of request patterns to Redis channels",
      "type": "object",
      "patternProperties": {
        ".*": {"type": "string"}
      }
    }
  }
};

class Module {
  constructor(config, courier, service) {
    this.config = config;
    this.courier = courier;
    this.service = service;
    this._requests = {};

    this.config.default_ttl = this.config.default_ttl || DEFAULT_TTL;
  }

  initialize() {
    // set up connections/subscriptions
    this.sub = redis.createClient(this.config.redis);
    this.pub = redis.createClient(this.config.redis);

    this.sub.subscribe(this.config.reply_channel);
    this.sub.on('message', (channel, message) => this.handleReply(message));
  }

  terminate() {
    this.sub.quit();
    this.pub.quit();
  }

  handleReply(msg) {
    msg = this.deserializeReply(msg);
    const req = this._requests[msg.reqid];
    if(req) {
      delete this._requests[msg.reqid];
      if(req.tout) clearTimeout(req.tout);
      if(msg.err) req.cb(errorize(msg.err));
      else req.cb(null, msg.data);
    }
  }

  deserializeReply(msg) {
    msg = _.isString(msg) ? JSON.parse(msg) : msg;
    let idx = 0;
    return {
      reqid: msg.shift(),
      err: msg.shift(),
      data: msg.shift()
    };
  }

  onTimeout(reqid) {
    const req = this._requests[reqid];
    if(req) {
      delete this._requests[reqid];
      const err = this.createTimeoutError(reqid);
      req.cb(err);
    }
  }

  createTimeoutError(reqid) {
    return new Error('Request timed out');
  }

  // `data` is expected to be JSON.stringify-able
  handleRequest(channel, ctx, name, data, cb) {
    const reqid = ctx.id || uuid();
    const now = Date.now();
    const ttl = ctx.exp ? ctx.exp-now : this.config.default_ttl

    this._requests[reqid] = {
      t: now,
      ttl,
      cb,
      tout: setTimeout(this.onTimeout.bind(this), ttl, reqid)
    };

    const msg = this.serializeRequest(
      this.config.reply_channel,
      reqid,
      ttl,
      name,
      ctx,
      data
    );

    this.pub.publish(channel, msg);
  }

  serializeRequest(reply_channel, reqid, ttl, name, ctx, data) {
    return JSON.stringify([
      reply_channel,  // return route
      reqid,          // request id
      name,           // request name
      Date.now()+ttl, // expiry
      ctx.data || {}, // context info
      data || {}      // request data
    ]);
  }

  registerHandlers() {
    _.each(this.config.names, (v, k) => {
      this.courier.reply(k, (ctx, data, cb) => this.handleRequest(v, ctx, k, data, cb));
    });
    _.each(this.config.patterns, (v, k) => {
      this.courier.replyPattern(k, (ctx, name, data, cb) => this.handleRequest(v, ctx, name, data, cb));
    });
  }
}
Module.CONFIG_SCHEMA = CONFIG_SCHEMA;

module.exports = function(config, courier, service) {
  return new Module(config, courier, service);
}





function errorize(obj) {
  const err = new Error();
  _.extend(err, obj);
  return err;
}
