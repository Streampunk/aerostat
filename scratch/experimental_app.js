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

const Koa = require('koa');
const Router = require('koa-router');
const Redis = require('ioredis');
const Bull = require('bull');
const producer = new Bull('my-first-queue');
const beamcoder = require('bindings')('beamcoder');
const render = require('koa-ejs');
const path = require('path');

const app = new Koa();
const router = new Router();
const redis = new Redis();

render(app, {
  root: path.join(__dirname, 'views'),
  layout: 'template',
  viewExt: 'html',
  cache: false,
  debug: false
});

app.use(async (ctx, next) => {
  await redis.set('date', new Date());
  await redis.set('ip', ctx.ip);
  return next();
});

/* app.use(async (ctx) => {
  const users = [{ name: 'Dead Horse' }, { name: 'Jack' }, { name: 'Tom' }];
  await ctx.render('content', {
    users
  });
}); */

const timer = t => new Promise(f => { // eslint-disable-line no-unused-vars
  setTimeout(f, t);
});

router.get('/fred', async ctx => {
  let job = await producer.add(
    { foo: 'bar', date: (new Date()).toString() }
  );
  console.log('Initial state is ', await job.getState());
  let result = await job.finished();
  ctx.body = 'Done IN! ' + beamcoder.version + JSON.stringify(result);
});



router.get('/ginger.html', async (ctx) => {
  const users = [{ name: 'Dead Rabbit' }, { name: 'Jack' }, { name: 'Tom' }];
  await ctx.render('content', {
    users,
    ip: await redis.get('ip'),
    date: await redis.get('date')
  });
});

app
  .use(router.routes())
  .use(router.allowedMethods());

app.listen(3000);

app.on('error', (err) => {
  console.log(err.stack);
});
