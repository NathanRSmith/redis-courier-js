'use strict';

const _ = require('lodash');
const redis = require('redis');
const {Context} = require('lib-courier-js');

const DEFAULT_TTL = 1000*10;
const CONFIG_SCHEMA = {
  "properties": {
    "redis": {
      "description": "Redis connection info. (https://www.npmjs.com/package/redis#rediscreateclient)",
      "type": "object"
    },
    "subscriptions": {
      "description": "Map of channels to request name prefix",
      "type": "object",
      "patternProperties": {
        ".*": {"type": "string"}
      }
    }
  }
};

class Module {
  constructor(config={}, courier, service) {
    this.config = config;
    this.courier = courier;
    this.service = service;
    console.error(config)
  }

  initialize() {
    // set up connections/subscriptions
    this.sub = redis.createClient(this.config.redis);
    this.pub = redis.createClient(this.config.redis);

    if(_.size(this.config.subscriptions)) this.sub.subscribe(_.keys(this.config.subscriptions));
    this.sub.on('message', this.handleRequest.bind(this));
  }

  terminate() {
    this.sub.quit();
    this.pub.quit();
  }

  handleRequest(channel, msg) {
    msg = this.deserializeRequest(msg);
    const ctx = this.createContext(msg);
    const prefix = this.config.subscriptions[channel] || '';

    this.courier.request(ctx, prefix+msg.name, msg.data)
      .tapCatch(err => ctx.logger.error(err))
      .then(
        res => this.pub.publish(msg.reply_channel, this.serializeReply(msg, null, res)),
        err => this.pub.publish(msg.reply_channel, this.serializeReply(msg, err, null))
      )
      // TODO: log?
      .catch(err => {});
  }

  serializeReply(msg, err=null, data) {
    if(err) {
      // console.err(err.stack)
      if(err.toJSON) err = err.toJSON();
      else err = _.pick(err, 'name', 'message');
    }

    const res = [msg.reqid, err];
    if(!_.isUndefined(data)) res.push(data);

    return JSON.stringify(res);
  }

  deserializeRequest(msg) {
    msg = _.isString(msg) ? JSON.parse(msg) : msg;
    let idx = 0;
    return {
      reply_channel: msg[idx++],
      reqid: msg[idx++],
      name: msg[idx++],
      exp: msg[idx++],
      ctx_data: msg[idx++],
      data: msg[idx++]
    };
  }

  createContext(msg) {
    return new Context(msg.ctx_data, {
      id: msg.reqid,
      logger: this.service.logger,
      scope_logger: true,
      exp: msg.exp,
      logger_id_field: 'req_id'
    });
  }
}
Module.CONFIG_SCHEMA = CONFIG_SCHEMA;

module.exports = function(config, courier, service) {
  return new Module(config, courier, service);
}
