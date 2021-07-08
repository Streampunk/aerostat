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

const config = require('../../config.js');
const { redisio, queue } = require('../../index.js');

const queues = config.app.queues.split(',');
const consumers = queues.map( x => queue(x) );
const workers = {};

Object.keys(config.rules).forEach(k => {
  const rule = config.rules[k];
  if (rule.queue && queues.includes(rule.queue) && rule.function) {
    workers[rule.function] = require(`./${rule.function}.js`);
  }
});

consumers.forEach(consumer => {
  consumer.process(async job => {
    const worker = workers[job.data.rule.function];
    if (worker) {
      return await worker(job);
    } else {
      throw new Error('file worker job failed!');
    }
  });
});

process.on('SIGINT', async () => {
  await redisio.close()
    .catch(console.error);
  process.exit();
});
