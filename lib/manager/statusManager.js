var utils = require('../util/utils');
var Redis = require('ioredis');

var DEFAULT_PREFIX = 'POMELO:STATUS';

var StatusManager = function(app, opts) {
  this.app = app;
  this.opts = opts || {};
  this.prefix = opts.prefix || DEFAULT_PREFIX;
  this.host = opts.host;
  this.port = opts.port;
  this.redis = null;
};

module.exports = StatusManager;

StatusManager.prototype.start = function(cb) {
  this.redis = new Redis({
    host: this.host,
    port: this.port,
    password: this.opts.auth_pass,
    keyPrefix: this.prefix,
    dropBufferSupport: true,
  })

  this.redis.on("error", function (err) {
      console.error("[status-plugin][redis]" + err.stack);
  });
  this.redis.once('ready', cb);
};

StatusManager.prototype.stop = function(force, cb) {
  this.clean(() => {
    if (this.redis) {
      this.redis.end();
      this.redis = null;
    }
    utils.invokeCallback(cb);
  })
};

StatusManager.prototype.clean = function(cb) {
  var cmds = [];
  var self = this;

  const stream = this.redis.sscanStream(genSidKey(this), { count: 20 })
  stream.on('data', (resultKeys) => {
    for (var i = 0; i < resultKeys.length; i++) {
      cmds.push(['srem', genUidKey(self, resultKeys[i]), self.app.serverId]);
    }
  })

  stream.on('end', () => {
    cmds.push(['del', genSidKey(self)]);
    execMultiCommands(self.redis, cmds, cb);
  })
};

StatusManager.prototype.add = function(uid, sid ,cb) {
  var cmds = [];
  cmds.push(['sadd', genSidKey(this), uid]);
  cmds.push(['sadd', genUidKey(this, uid), sid]);
  execMultiCommands(this.redis, cmds, cb);
};

StatusManager.prototype.leave = function(uid, sid, cb) {
  var cmds = [];
  cmds.push(['srem', genSidKey(this), uid]);
  cmds.push(['srem', genUidKey(this, uid), sid]);
  execMultiCommands(this.redis, cmds, cb);
};

StatusManager.prototype.getSidsByUid = function(uid, cb) {
  var self = this;

  this.redis.smembers(genUidKey(this, uid), function(err, list) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    var commands = [];
    var sids = [];
    for(var i=0; i<list.length; i++) {
      var sid = list[i];
      var server = self.app.getServerById(sid);
      if (!server) {
        commands.push(['srem', genOtherSidKey(self, sid), uid]);
        commands.push(['srem', genUidKey(self, uid), sid]);
      } else {
        sids.push(sid);
      }
    }

    execMultiCommands(self.redis, commands, function (err) {
      utils.invokeCallback(cb, err, sids);
    });
  });
};

StatusManager.prototype.getSidsByUids = function(uids, cb) {
  var cmds = [];
  for (var i=0; i<uids.length; i++) {
    cmds.push(['exists', genUidKey(this, uids[i])]);
  }
  execMultiCommands(this.redis, cmds, function(err, list) {
    utils.invokeCallback(cb, err, list);
  });
};

var execMultiCommands = function(redis, cmds, cb) {
  if(!cmds.length) {
    utils.invokeCallback(cb);
    return;
  }
  redis.multi(cmds).exec(function(err, replies) {
    const list = replies.map((reply) => reply[1])
    utils.invokeCallback(cb, err, list);
  });
};

var genUidKey = function(self, uid) {
  return 'UID:' + uid;
};

var genSidKey = function(self) {
  return 'SID:' + self.app.serverId;
};

var genOtherSidKey = function(self, sid) {
  return 'SID:' + sid;
};