/*
  Aerostat Beam Engine - Redis-backed highly-scale-able and cloud-fit media beam engine.
  Copyright (C) 2019  Streampunk Media Ltd.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <https://www.gnu.org/licenses/>.

  https://www.streampunk.media/ mailto:furnace@streampunk.media
  14 Ormiscaig, Aultbea, Achnasheen, IV22 2JJ  U.K.
*/

/* Basic idea - pattern match a job spec like /^.*\.(jpg|jpeg)$/, set up a job,
  wait for it to complete and return the result. May need a timeout.
*/

const config = require('../config.js');
const Queue = require('bull');
const redisio = require('./redisio.js');
const Boom = require('boom');

const queues = {};
Object.keys(config.rules).forEach(k => {
  let rule = config.rules[k];
  if (rule.pathPattern) {
    rule.pathRE = new RegExp(rule.pathPattern);
  }
  if (rule.statusCode && !Array.isArray(rule.statusCode)) {
    rule.statusCode = [ rule.statusCode ];
  }
  if (rule.method && !Array.isArray(rule.method)) {
    rule.method = [ rule.method ];
  }
  if (!queues[rule.queue]) {
    queues[rule.queue] = new Queue(rule.queue, { redis: {
      port: config.redis.port, host: config.redis.host }});
  }
});
const [ preRules, postRules ] = // eslint-disable-line no-unused-vars
  [ Object.values(config.rules).filter(rule => rule.pathRE && !rule.postRoute),
    Object.values(config.rules).filter(rule => rule.pathRE && rule.postRoute) ];

async function jobCatcher (ctx, next) {

  for ( let x = 0 ; x < preRules.length ; x++ ) {
    let rule = preRules[x];
    if (rule.method.indexOf(ctx.request.method) < 0) {
      continue;
    }
    let match = rule.pathRE.exec(ctx.path);
    if (match) {
      let bullock = await queues[rule.queue].add({
        rule: rule,
        path: ctx.path,
        headers: ctx.request.headers,
        method: ctx.request.method
      });
      let result = await bullock.finished();
      // console.log(result);

      if (result.success && result.body.blobKey) {
        try {
          // Worker has returned a key to blob data in Redis
          result.type = result.body.blobType;
          result.body = await redisio.retrieveBlob(result.body.blobKey);
        } catch (err) {
          throw Boom.internal(`Failed to retrieve binary element ${result.body.blobKey}: ${err.message}`);
        }
      }

      if (result.body) { ctx.body = result.body; }
      if (result.status) { ctx.status = result.status; }
      if (result.type) { ctx.type = result.type; }

      if (result.success) {
        return;
      }
    }
  }

  try {
    await next();
  } finally {
    for ( let x = 0 ; x < postRules.length ; x++ ) {
      const rule = postRules[x];
      if (rule.statusCode.indexOf(ctx.status) < 0 || rule.method.indexOf(ctx.request.method) < 0) {
        continue;
      }
      const match = rule.pathRE.exec(ctx.path);
      if (match && queues[rule.queue]) {
        const bullock = await queues[rule.queue].add({
          rule: rule,
          path: ctx.path,
          headers: ctx.request.headers,
          method: ctx.request.method,
          body: (ctx.request.type === 'application/json') ? ctx.request.body : null,
        });

        const result = await bullock.finished();
        //console.log(result);
      }
    }
  }
}

async function closeQueues() {
  await Promise.all(Object.keys(queues).map(q => queues[q].close()));
}

module.exports = {
  jobCatcher,
  closeQueues
};
