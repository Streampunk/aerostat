/*
  Aerostat Beam Engine - Redis-backed highly-scale-able and cloud-fit media beam engine.
  Copyright (C) 2019 Streampunk Media Ltd.

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

const test = require('tape');
const redisio = require('../lib/redisio.js');
const beamcoder = require('beamcoder');
const testUtil = require('./testUtil.js');
const config = require('../config.js');

const beforeTest = async () => {
  let redis = await redisio.redisPool.use();
  let result = await redis.flushdb();
  redisio.redisPool.recycle(redis);
  return result === 'OK';
};

test.onFinish(() => {
  redisio.close();
});

const checkEmpty = t => {
  t.ok(redisio.redisPool.pool.every(x => x !== redisio.redisPool.EMPTY_SLOT),
    'pool has returned to empty state.');
};

test('Packet store and retrieve', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  let pkt = beamcoder.packet({
    pts: 42,
    dts: 43,
    data: Buffer.alloc(16383 + beamcoder.AV_INPUT_BUFFER_PADDING_SIZE),
    size: 16383,
    stream_index: 3,
    flags: { KEY: true, TRUSTED: true},
    side_data: { replaygain: Buffer.from('Zen time?') },
    duration: 44,
    pos: 45
  });
  t.deepEqual(await redisio.storeMedia('test_url', pkt), [ 'OK-crt', 'OK-crt' ],
    'redis reports store of packet and data OK.');
  let redis = await redisio.redisPool.use();
  t.ok(await redis.exists(`${config.redis.prepend}:test_url:stream_3:packet_42`),
    'packet key created.');
  t.ok(await redis.exists(`${config.redis.prepend}:test_url:stream_3:packet_42:data`),
    'data key created.');
  t.ok(await redis.ttl(`${config.redis.prepend}:test_url:stream_3:packet_42:data`) > 0,
    'data TTL is set.');
  t.deepEqual(
    await redis.zrange(`${config.redis.prepend}:test_url:stream_3:index`,
      0, -1, 'WITHSCORES'), // finds all packets
    [ `${config.redis.prepend}:test_url:stream_3:packet_42`, '42' ],
    'stores expected score and key into index.');

  let rpkt = await redisio.retrievePacket('test_url', 3, 42);
  t.ok(rpkt, 'roundtrip packet is truthy.');
  t.deepEqual(rpkt, pkt, 'roundtrip packet is the same.');

  t.equal(await redis.del(`${config.redis.prepend}:test_url:stream_3:packet_42:data`),
    1, 'deleted the data.');
  rpkt = await redisio.retrievePacket('test_url', 3, 42);
  t.ok(rpkt, 'roundtrip packet without data is truthy.');
  t.equal(rpkt.size, 16383, 'data size remains the same.');
  t.notOk(rpkt.data, 'data is now null.');
  pkt.data = null;
  t.deepEqual(rpkt.toJSON(), pkt.toJSON(),
    'data-stripped packet and original have the same base.');

  redisio.redisPool.recycle(redis);
  await redisio.close();
  t.equal(redisio.redisPool.size(), 0, 'redis pool is reset.');
  checkEmpty(t);
  t.end();
});

const stripAlloc = ({ alloc, ...other }) => ({ ...other }); // eslint-disable-line no-unused-vars
const stripBufSizes = ({ buf_sizes, ...other }) => ({ ...other });  // eslint-disable-line no-unused-vars

test('Frame store and retrieve', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  let frm = beamcoder.frame({
    pts: 42,
    width: 1920,
    height: 1080,
    format: 'yuv422p'
  }).alloc();
  t.deepEqual(await redisio.storeMedia('test_url', frm, 2),
    [ 'OK-crt', 'OK-crt', 'OK-crt', 'OK-crt' ],
    'redis reports store of frame and data OK.');
  // console.log(redisio.redisPool.pool[0].options);
  let redis = await redisio.redisPool.use();
  t.ok(await redis.exists(`${config.redis.prepend}:test_url:stream_2:frame_42`),
    'frame key created.');
  t.ok(await redis.exists(`${config.redis.prepend}:test_url:stream_2:frame_42:data_0`),
    'data_0 key created.');
  t.ok(await redis.exists(`${config.redis.prepend}:test_url:stream_2:frame_42:data_1`),
    'data_1 key created.');
  t.ok(await redis.exists(`${config.redis.prepend}:test_url:stream_2:frame_42:data_2`),
    'data_2 key created.');
  t.ok(await redis.ttl(`${config.redis.prepend}:test_url:stream_2:frame_42:data_0`) > 0,
    'data_0 TTL is set.');
  t.ok(await redis.ttl(`${config.redis.prepend}:test_url:stream_2:frame_42:data_1`) > 0,
    'data_1 TTL is set.');
  t.ok(await redis.ttl(`${config.redis.prepend}:test_url:stream_2:frame_42:data_2`) > 0,
    'data_2 TTL is set.');
  t.deepEqual(
    await redis.zrange(`${config.redis.prepend}:test_url:stream_2:index`,
      0, -1, 'WITHSCORES'), // finds all packets
    [ `${config.redis.prepend}:test_url:stream_2:frame_42`, '42' ],
    'stores expected score and key into index.');

  let rfrm = await redisio.retrieveFrame('test_url', 2, 42);
  t.ok(rfrm, 'roundtrip frame is truthy.');
  frm.buf_sizes = frm.data.map(x => x.length);
  t.deepEqual(stripAlloc(rfrm), stripAlloc(frm), 'roundtrip frame is the same.');

  t.equal(await redis.del(`${config.redis.prepend}:test_url:stream_2:frame_42:data_0`),
    1, 'deleted data_0.');
  t.equal(await redis.del(`${config.redis.prepend}:test_url:stream_2:frame_42:data_1`),
    1, 'deleted data_1.');
  t.equal(await redis.del(`${config.redis.prepend}:test_url:stream_2:frame_42:data_2`),
    1, 'deleted data_2.');
  rfrm = await redisio.retrieveFrame('test_url', 2, 42);
  t.ok(rfrm, 'roundtrip frame without data is truthy.');
  t.ok(Array.isArray(frm.data), 'data is now an empty array ...');
  t.equal(rfrm.data.length, 0, '... of length zero.');
  frm.data = null;
  t.deepEqual(rfrm.toJSON(), frm.toJSON(),
    'data-stripped frame and original have the same base.');

  await redisio.redisPool.recycle(redis);
  await redisio.close();
  t.equal(redisio.redisPool.size(), 0, 'redis pool is reset.');
  checkEmpty(t);
  t.end();
});

test('Retrieve media', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  let pkt = beamcoder.packet({
    pts: 42,
    dts: 43,
    data: Buffer.alloc(16383),
    stream_index: 3,
    flags: { KEY: true, TRUSTED: true},
    side_data: { replaygain: Buffer.from('Zen time?') },
    duration: 44,
    pos: 45
  });
  let frm = beamcoder.frame({
    pts: 43,
    width: 1920,
    height: 1080,
    format: 'yuv422p'
  }).alloc();
  t.deepEqual(await redisio.storeMedia('test_url', pkt), [ 'OK-crt', 'OK-crt' ],
    'redis reports store of packet and data OK.');
  t.deepEqual(await redisio.storeMedia('test_url', frm, 3),
    [ 'OK-crt', 'OK-crt', 'OK-crt', 'OK-crt' ],
    'redis reports store of frame and data OK.');

  t.comment('### searching from 42 upwards');
  let mix = await redisio.retrieveMedia('test_url', 3, 42);
  t.ok(Array.isArray(mix), 'result is an array ...');
  t.equal(mix.length, 2, '... of length two ...');
  t.deepEqual(mix.map(x => x.type), [ 'Packet', 'Frame' ],
    '... containing a packet and a frame.');

  t.comment('### searching from 42 to 42');
  mix = await redisio.retrieveMedia('test_url', 3, 42, 42);
  t.ok(Array.isArray(mix), 'result is an array ...');
  t.equal(mix.length, 1, '... of length one ...');
  t.deepEqual(mix.map(x => x.type), [ 'Packet' ],
    '... containing a packet.');

  t.comment('### searching from 42 limit 0 1');
  mix = await redisio.retrieveMedia('test_url', 3, 42, 0, 1);
  t.ok(Array.isArray(mix), 'result is an array ...');
  t.equal(mix.length, 1, '... of length one ...');
  t.deepEqual(mix.map(x => x.type), [ 'Packet' ],
    '... containing a packet.');

  t.comment('### searching from 42 limit 1 1');
  mix = await redisio.retrieveMedia('test_url', 3, 42, 1, 1);
  t.ok(Array.isArray(mix), 'result is an array ...');
  t.equal(mix.length, 1, '... of length one ...');
  t.deepEqual(mix.map(x => x.type), [ 'Frame' ],
    '... containing a frame.');


  t.comment('### searching from 100 expecting empty');
  try {
    await redisio.retrieveMedia('test_url', 3, 100);
    t.fail('retrieve media did not throw.');
  } catch (err) {
    console.log(err.message);
    t.ok(err.message.indexOf('Unable to find') >= 0, 'not found throws exception.');
  }

  await redisio.close();
  t.equal(redisio.redisPool.size(), 0, 'redis pool is reset.');

  checkEmpty(t);
  t.end();
});

test('Format store and retrieve', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  let fmt = testUtil.fmt;
  t.ok(fmt, 'format is truthy.');
  t.equal(fmt.streams.length, 2, 'format has two streams.');

  t.deepEqual(await redisio.storeFormat(fmt), ['OK','OK','OK'],
    'redis reports format stored OK.');

  let redis = await redisio.redisPool.use();
  t.ok(await redis.exists(`${config.redis.prepend}:${fmt.url}`),
    'format key exists.');
  t.ok(await redis.exists(`${config.redis.prepend}:${fmt.url}:stream_0`),
    'stream 0 key exists.');
  t.ok(await redis.exists(`${config.redis.prepend}:${fmt.url}:stream_1`),
    'stream 1 key exists.');
  t.ok(await redis.exists(`${config.redis.prepend}:index`),
    'index has been created.');

  let rfmt = await redisio.retrieveFormat(`${fmt.url}`);
  t.ok(rfmt, 'retrieved foramt is truthy.');
  t.deepEqual(rfmt.toJSON(), fmt.toJSON(), 'format roundtrips OK.');

  let listing = await redisio.listContent();
  t.ok(Array.isArray(listing), 'listing is an array ...');
  t.equal(listing.length, 1, '... of length 1.');
  t.deepEqual(listing, [ fmt.url ], 'listing is as expected.');

  t.comment('### checking empty list behaviour');
  listing = await redisio.listContent(1);
  t.ok(Array.isArray(listing), 'listing is an array ...');
  t.equal(listing.length, 0, '... of length 0.');

  t.comment('### checking single stream retrieve');
  let rstr = await redisio.retrieveStream(fmt.url, 0);
  t.ok(rstr, 'retrieved stream is truthy.');
  t.deepEqual(rstr.toJSON(), testUtil.stream0, 'stream roundtrips as expected.');

  redisio.redisPool.recycle(redis);
  await redisio.close();
  t.equal(redisio.redisPool.size(), 0, 'redis pool is reset.');
  t.end();
});

test('Retrieve non-existant', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  try {
    await redisio.retrieveFormat('wibble');
    t.fail('Retrieve an unknown element did not fail.');
  } catch (err) {
    t.ok(err.message.indexOf('Unable') >= 0, 'retrieve throws when missing.');
  }
  await redisio.close();
  t.equal(redisio.redisPool.size(), 0, 'redis pool is reset.');
  t.end();
});

test('Store and retrieve blob', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  try {
    let testBuffer = Buffer.from('wibble wobble jelly');
    let key = await redisio.storeBlob(testBuffer);
    t.ok(key.startsWith('beamengine:blob:'), 'key starts as expected.');
    t.ok(!isNaN(parseInt(key.slice(16))), 'key includes random number.');
    let result = await redisio.retrieveBlob(key);
    t.ok(Buffer.isBuffer(result), 'result is a buffer.');
    t.equal(Buffer.compare(result, testBuffer), 0, 'roundtrip buffer as expected.');
  } catch (err) {
    t.fail(err);
  }

  checkEmpty(t);
  t.end();
});

test('Equivalent relationships', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  t.deepEqual(await redisio.queryEquivalent('fmtA'), [],
    'equivalent is empty array for name when no format stored.');
  let fmtA = testUtil.fmt;
  fmtA.url = 'fmtA';
  t.deepEqual(await redisio.storeFormat(fmtA), ['OK','OK','OK'],
    'redis reports format A stored OK.');
  t.deepEqual(await redisio.queryEquivalent(fmtA), [],
    'equivalent is empty array for fmt when none specified.');
  t.deepEqual(await redisio.queryEquivalent(fmtA.url), [],
    'equivalent is empty array for fmt.url when none specified.');

  try {
    await redisio.createEquivalent(fmtA.url, fmtA);
    t.fail('Should have thrown when trying to create equivalent to itself.');
  } catch (err) {
    t.equal(err.message, 'Source cannot be equivalent to source.', 'expected error on equal sources.');
  }

  try {
    await redisio.createEquivalent(fmtA.url, 'wibble');
    t.fail('Should have thrown when trying to create equivalent to "wibble" as target.');
  } catch (err) {
    t.equal(err.message, 'Target \'wibble\' for a new equivalent relationship does not exist.',
      'non-existant target error message as expected.');
  }

  try {
    await redisio.createEquivalent('wibble', fmtA.url);
    t.fail('Should have thrown when trying to create equivalent to "wibble" as source.');
  } catch (err) {
    t.equal(err.message, 'Source \'wibble\' for a new equivalent relationship does not exist.',
      'non-existant source error message as expected.');
  }

  try {
    await redisio.createEquivalent('wibble', 'wobble');
    t.fail('Should have thrown when trying to create equivalent "wibble" to "wobble".');
  } catch (err) {
    t.equal(err.message, 'Both source \'wibble\' and target \'wobble\' for a new equivalent relationship do not exist.',
      'non-existant source and target error message as expected.');
  }

  let fmtB = testUtil.fmt;
  fmtB.url = 'fmtB';
  t.deepEqual(await redisio.storeFormat(fmtB), ['OK','OK','OK'],
    'redis reports format B stored OK.');
  t.deepEqual(await redisio.createEquivalent(fmtA, fmtB), [ 1, 1 ],
    'relationship created between fmt A and fmt B.');

  try {
    t.deepEqual(await redisio.queryEquivalent(fmtA), [ 'fmtB' ], 'query A gives B.');
    t.deepEqual(await redisio.queryEquivalent(fmtB), [ 'fmtA' ], 'query B gives A.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  let fmtC = testUtil.fmt;
  fmtC.url = 'fmtC';
  t.deepEqual(await redisio.storeFormat(fmtC), ['OK','OK','OK'],
    'redis reports format C stored OK.');
  t.deepEqual(await redisio.createEquivalent(fmtB, fmtC), [ 1, 1 ],
    'relationship created between fmt B and fmt C.');

  try {
    t.deepEqual((await redisio.queryEquivalent(fmtA, 3)).sort(), [ 'fmtB', 'fmtC' ],
      'depth 3 query of A gives B and C.');
    t.deepEqual((await redisio.queryEquivalent(fmtB, 3)).sort(), [ 'fmtA', 'fmtC' ],
      'depth 3 query of B gives A and C.');
    t.deepEqual((await redisio.queryEquivalent(fmtC, 10)).sort(), [ 'fmtA', 'fmtB' ],
      'depth 10 query of C gives A and B.');
    t.deepEqual((await redisio.queryEquivalent(fmtA, 1)).sort(), [ 'fmtB' ],
      'depth 1 query of A gives B.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  // Creaete a loop
  t.deepEqual(await redisio.createEquivalent(fmtC, fmtA), [ 1, 1 ],
    'loop relationship created between fmt C and fmt A.');

  try {
    t.deepEqual((await redisio.queryEquivalent(fmtA, 3)).sort(), [ 'fmtB', 'fmtC' ],
      'with loop, depth 3 query of A gives B and C.');
    t.deepEqual((await redisio.queryEquivalent(fmtB, 3)).sort(), [ 'fmtA', 'fmtC' ],
      'with loop, depth 3 query of B gives A and C.');
    t.deepEqual((await redisio.queryEquivalent(fmtC, 10)).sort(), [ 'fmtA', 'fmtB' ],
      'with loop, depth 10 query of C gives A and B.');
    t.deepEqual((await redisio.queryEquivalent(fmtA, 1)).sort(), [ 'fmtB', 'fmtC' ],
      'with loop, depth 1 query of A gives B and C.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  // Delete a loop
  t.deepEqual(await redisio.deleteEquivalent(fmtA, fmtC), [1, 1],
    'loop relationship removed between fmt A and fmt C.');
  t.deepEqual(await redisio.deleteEquivalent(fmtA, fmtC), [0, 0],
    'loop relationship second removal _fails_ as expected.');

  try {
    t.deepEqual((await redisio.queryEquivalent(fmtA, 1)).sort(), [ 'fmtB' ],
      'loop removed, depth 1 query of A gives just B.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  t.deepEqual(await redisio.createEquivalent(fmtA, fmtB), [0, 0],
    'remake relationship has expected result.');

  checkEmpty(t);
  t.end();
});

test('Rendition relationships', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  t.deepEqual(await redisio.queryRendition('fmtA'), { sources: [] },
    'rendition is empty array for name when no fmt stored.');
  let fmtA = testUtil.fmt;
  fmtA.url = 'fmtA';
  t.deepEqual(await redisio.storeFormat(fmtA), ['OK','OK','OK'],
    'redis reports format A stored OK.');
  t.deepEqual(await redisio.queryRendition(fmtA), { sources: [] },
    'rendition is empty array for fmt when none specified.');
  t.deepEqual(await redisio.queryRendition(fmtA.url), { sources: [] },
    'rendition is empty array for fmt.url when none specified.');

  try {
    await redisio.createRendition(fmtA.url, fmtA);
    t.fail('Should have thrown when trying to create rendition to itself.');
  } catch (err) {
    t.equal(err.message, 'Source cannot be a rendition of itself.', 'expected error on equal sources.');
  }

  try {
    await redisio.createRendition(fmtA.url, 'wibble');
    t.fail('Should have thrown when trying to create rendition to "wibble" as target.');
  } catch (err) {
    t.equal(err.message, 'Target \'wibble\' for a new rendition relationship does not exist.',
      'non-existant target error message as expected.');
  }

  try {
    await redisio.createRendition('wibble', fmtA.url);
    t.fail('Should have thrown when trying to create rendition to "wibble" as source.');
  } catch (err) {
    t.equal(err.message, 'Source \'wibble\' for a new rendition relationship does not exist.',
      'non-existant source error message as expected.');
  }

  try {
    await redisio.createRendition('wibble', 'wobble');
    t.fail('Should have thrown when trying to create rendition "wibble" to "wobble".');
  } catch (err) {
    t.equal(err.message, 'Both source \'wibble\' and target \'wobble\' for a new rendition relationship do not exist.',
      'non-existant source and target error message as expected.');
  }

  let fmtB = testUtil.fmt;
  fmtB.url = 'fmtB';
  t.deepEqual(await redisio.storeFormat(fmtB), ['OK','OK','OK'],
    'redis reports format B stored OK.');
  t.deepEqual(await redisio.createRendition(fmtA, fmtB), [ 1, 'OK' ],
    'relationship created between fmt A and fmt B.');

  try {
    t.deepEqual(await redisio.queryRendition(fmtA), { sources: [] }, 'query A gives empty.');
    t.deepEqual(await redisio.queryRendition(fmtB), { sources: [ 'fmtA' ] },
      'query B gives A.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  t.deepEqual(await redisio.createRendition(fmtA, fmtB, { video: 'video' }),
    [ 0, 'OK', 'OK' ], 'adding source map is OK.');
  try {
    t.deepEqual(await redisio.queryRendition(fmtB),
      { sources: [ 'fmtA' ], stream_map: { video: 'video' }},
      'query B gives A.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  try {
    await redisio.createRendition(fmtB, fmtA);
    t.fail('Successully created a relationship loop ... which is bad.');
  } catch (err) {
    t.equal(err.message, 'Target \'fmtA\' cannot be a rendition of itself.');
  }

  let fmtC = testUtil.fmt;
  fmtC.url = 'fmtC';
  t.deepEqual(await redisio.storeFormat(fmtC), ['OK','OK','OK'],
    'redis reports format C stored OK.');
  t.deepEqual(await redisio.createRendition(fmtB, fmtC), [ 1, 'OK' ],
    'relationship created between fmt B and fmt C.');

  try {
    t.deepEqual(await redisio.queryRendition(fmtA, 3),
      { sources: [] },
      'depth 3 query of A gives empty (original source).');
    t.deepEqual(await redisio.queryRendition(fmtB, 3),
      { sources: [ 'fmtA' ], stream_map: { video: 'video' } },
      'depth 3 query of B gives A.');
    t.deepEqual(await redisio.queryRendition(fmtC, false, 10),
      { sources: [ 'fmtB', 'fmtA' ] },
      'depth 10 query of C gives B then A.');
    t.deepEqual(await redisio.queryRendition(fmtC, 1),
      { sources: [ 'fmtB' ] },
      'depth 1 query of C gives B.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  try {
    await redisio.createRendition(fmtC, fmtA);
    t.fail('Successully created a relationship loop depth 2 ... which is bad.');
  } catch (err) {
    t.equal(err.message, 'Target \'fmtA\' cannot be a rendition of itself.');
  }

  try {
    t.deepEqual(await redisio.queryRendition(fmtA, true, 3),
      { sources: [], dependencies: [ 'fmtB' ] },
      'depth 3 query of A gives empty (original source) with reverse.');
    t.deepEqual(await redisio.queryRendition(fmtB, true, 3),
      { sources: [ 'fmtA' ], stream_map: { video: 'video' }, dependencies: [ 'fmtC' ] },
      'depth 3 query of B gives A with reverse.');
    t.deepEqual(await redisio.queryRendition(fmtC, true, 10),
      { sources: [ 'fmtB', 'fmtA' ], dependencies: [] },
      'depth 10 query of C gives B then A with reverse.');
    t.deepEqual(await redisio.queryRendition(fmtB, true, 1),
      { sources: [ 'fmtA' ], stream_map: { video: 'video' }, dependencies: [ 'fmtC' ] },
      'depth 1 query of C gives B with reverse.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  try {
    await redisio.createRendition(fmtB, fmtC, { subtitle: 'video' });
    t.fail('Reference to stream that does not exist succeeded.');
  } catch (err) {
    t.equal(err.message,
      'Stream map for target \'fmtC\' references stream \'subtitle\' that is not present.',
      'missing target stream spotted.');
  }

  try {
    await redisio.createRendition(fmtB, fmtC, { video: 'subtitle' });
    t.fail('Reference to stream that does not exist succeeded.');
  } catch (err) {
    t.equal(err.message,
      'Stream map for source \'fmtB\' references stream \'subtitle\' that is not present.',
      'missing target stream spotted.');
  }

  try {
    await redisio.createRendition(fmtB, fmtC, { stream_2: 'audio' });
    t.fail('Reference to stream that does not exist succeeded.');
  } catch (err) {
    t.equal(err.message,
      'Stream map for target \'fmtC\' references stream \'stream_2\' that is not present.',
      'missing target stream that exceeds index.');
  }

  try {
    t.deepEqual(await redisio.createRendition(fmtB, fmtC, { stream_1: 'audio[3]' }),
      [ 0, 'OK', 'OK' ], 'created stream map with channel reference.');
  } catch (err) {
    t.fail(`Should have created stream map with channel reference but failed: ${err.message}`);
  }

  try {
    t.deepEqual(await redisio.createRendition(fmtB, fmtC, { stream_1: ['audio[3]', 'audio_0' ] }),
      [ 0, 'OK', 'OK' ], 'created stream map with arrays and channel reference.');
  } catch (err) {
    t.fail(`Should have created stream map with channel reference but failed: ${err.message}`);
  }

  try {
    t.deepEqual(await redisio.queryRendition(fmtC, 3),
      { sources: [ 'fmtB', 'fmtA' ], stream_map: { stream_1: [ 'audio[3]', 'audio_0' ] }},
      'depth 3 query of A gives empty (original source) with expected stream map.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  try {
    await redisio.createRendition(fmtB, fmtC, { stream_1: ['audio[3]', 'subtitle' ] });
    t.fail('Reference to stream that does not exist succeeded.');
  } catch (err) {
    t.equal(err.message,
      'Stream map for source \'fmtB\' references stream \'subtitle\' that is not present.',
      'missing target stream in array.');
  }

  t.deepEqual(await redisio.deleteRendition(fmtB, fmtC), [ 1, 1, 1 ],
    'recorded rendition C of B deleted.');

  try {
    t.deepEqual(await redisio.queryRendition(fmtC, 3),
      { sources: [ ] },
      'rendition relationship from C to B is no more.');
    t.deepEqual(await redisio.queryRendition(fmtB, true, 3),
      { sources: [ 'fmtA' ], stream_map: { video: 'video' }, dependencies: [] },
      'and reverse relationship is also gone.');
  } catch (err) {
    t.fail(`Failed to query equivalent relationships: ${err.message}`);
  }

  t.deepEqual(await redisio.deleteRendition(fmtB, fmtC), [ 0, 0, 0 ],
    'second delete changes nothing.');

  t.deepEqual(await redisio.deleteRendition(fmtA, fmtB), [ 1, 1, 1 ],
    'reset to nout OK.');

  let redis = await redisio.redisPool.use();
  t.deepEqual(await redis.keys('*rendition*'), [], 'nothing about renditions left.');
  redisio.redisPool.recycle(redis);

  checkEmpty(t);
  t.end();
});

test('Tranformation relationships', async t => {
  t.ok(await beforeTest(), 'test database flushed OK.');
  let fmtA = testUtil.fmt;
  fmtA.url = 'fmtA';
  t.deepEqual(await redisio.storeFormat(fmtA), ['OK','OK','OK'],
    'redis reports format A stored OK.');

  let fmtB = testUtil.fmt;
  fmtB.url = 'fmtB';
  t.deepEqual(await redisio.storeFormat(fmtB), ['OK','OK','OK'],
    'redis reports format B stored OK.');

  let fmtC = testUtil.fmt;
  fmtC.url = 'fmtC';
  t.deepEqual(await redisio.storeFormat(fmtC), ['OK','OK','OK'],
    'redis reports format C stored OK.');

  t.deepEqual(await redisio.createTransformation(fmtA, [fmtB, fmtC]), [2, 4],
    'result of creating one to many transformation is as expected.');

  try {
    let transResult = { sources: [ 'fmtA' ], targets: [ 'fmtB', 'fmtC' ], recipe: ''};
    t.deepEqual(await redisio.queryTransformation(fmtB), transResult,
      'query of fmt B is as expected.');
    t.deepEqual(await redisio.queryTransformation(fmtC), transResult,
      'query of fmt C is as expected.');
  } catch (err) {
    t.fail('Failed to query transformation forward.');
  }

  try {
    t.deepEqual(await redisio.queryTransformation(fmtA, true),
      { source: 'fmtA', transformations: [ [ 'fmtB', 'fmtC' ] ]},
      'reverse query of fmt A is as epected.');
  }  catch (err) {
    t.fail('Failed to query transformation in reverse.');
  }

  t.deepEqual(await redisio.deleteTransformation(fmtB), [ 2, 5 ],
    'delete of relationship completes OK.');

  let fmtD = testUtil.fmt;
  fmtD.url = 'fmtD';
  t.deepEqual(await redisio.storeFormat(fmtD), ['OK','OK','OK'],
    'redis reports format D stored OK.');

  let recipeObj = { recipe: 'for', the: 'people' };
  t.deepEqual(
    await redisio.createTransformation(
      [fmtA, fmtB], [fmtC, fmtD],
      JSON.stringify(recipeObj)),
    [3, 5],
    'result of creating many-to-many transformation is as expected.');

  let fmtE = testUtil.fmt;
  fmtE.url = 'fmtE';
  t.deepEqual(await redisio.storeFormat(fmtE), ['OK','OK','OK'],
    'redis reports format E stored OK.');

  t.deepEqual(await redisio.createTransformation(fmtA, fmtE, 'recipe'), [2, 3],
    'result of creating one-to-one transformation is as expected.');

  try {
    t.deepEqual(await redisio.queryTransformation('fmtA'),
      { sources: [] },
      'query fmt A - not a target.');
    t.deepEqual(await redisio.queryTransformation('fmtB'),
      { sources: [] },
      'query fmt B - not a target.');
    // Order of sources should be preserved
    t.deepEqual(await redisio.queryTransformation('fmtC', false),
      { sources: [ 'fmtA', 'fmtB' ], targets: [ 'fmtC', 'fmtD' ], recipe: recipeObj },
      'query fmt C - relationship recorded as expected.');
    t.deepEqual(await redisio.queryTransformation('fmtD'),
      { sources: [ 'fmtA', 'fmtB' ], targets: [ 'fmtC', 'fmtD' ], recipe: recipeObj },
      'query fmt D - relationship redorded as expected.');
    t.deepEqual(await redisio.queryTransformation('fmtE'),
      { sources: [ 'fmtA' ], targets: [ 'fmtE' ], recipe: 'recipe' },
      'query fmt E - relationship recorded as expected.');
  } catch (err) {
    t.fail('Failed to query transformation relationships forward.');
  }

  try {
    t.deepEqual(await redisio.queryTransformation('fmtA', true)
      .then(({ source, transformations }) =>
        ({ source, transformations: transformations.sort() })), /* eslint-disable indent */
      { source: 'fmtA', transformations: [ [ 'fmtC', 'fmtD' ], [ 'fmtE' ]] }, /* eslint-disable indent */
      'query fmt A in reverse - relationship recorded as expected.'); /* eslint-disable indent */
    t.deepEqual(await redisio.queryTransformation('fmtB', true),
      { source: 'fmtB', transformations: [ [ 'fmtC', 'fmtD' ]] },
      'query fmt B in reverse - relationship recorded as expected.');
    t.deepEqual(await redisio.queryTransformation('fmtC', true),
      { sources: [] },
      'query fmt C in reverse - relationship recorded as expected.');
    t.deepEqual(await redisio.queryTransformation('fmtD', true),
      { sources: [] },
      'query fmt D in reverse - relationship recorded as expected.');
    t.deepEqual(await redisio.queryTransformation('fmtE', true),
      { sources: [] },
      'query fmt E in reverse - relationship recorded as expected.');
  } catch (err) {
    t.fail(`Failed to query transformation relationships in reverse: ${err.message}`);
  }

  try {
    await redisio.deleteTransformation(['fmtC']);
    t.fail('Should have thrown exception becauase target missing.');
  } catch (err) {
    t.equal(err.message,
      'Could not identify a transformation identifieed by all given targets: \'[fmtC]\'.',
      'missing target causes delete failure.');
  }

  try {
    await redisio.deleteTransformation(['fmtC', 'fmtE']);
    t.fail('Should have thrown exception becauase target missing.');
  } catch (err) {
    t.equal(err.message,
      'Could not identify a transformation identifieed by all given targets: \'[fmtC,fmtE]\'.',
      'missing target causes delete failure.');
  }

  try {
    await redisio.deleteTransformation(['fmtC', 'fmtD', 'fmtE']);
    t.fail('Should have thrown exception becauase target missing.');
  } catch (err) {
    t.equal(err.message,
      'Could not identify a transformation identifieed by all given targets: \'[fmtC,fmtD,fmtE]\'.',
      'missing target causes delete failure.');
  }

  t.deepEqual(await redisio.deleteTransformation(['fmtC', 'fmtD']), [3, 5],
    'delete many-to-many transformation as expected.');

  try {
    t.deepEqual(await redisio.queryTransformation('fmtC'),
      { sources: [] },
      'query fmt C - no longer a target.');
    t.deepEqual(await redisio.queryTransformation('fmtD'),
      { sources: [] },
      'query fmt D - no longer a target.');
    t.deepEqual(await redisio.queryTransformation('fmtE'),
      { sources: [ 'fmtA' ], targets: [ 'fmtE' ], recipe: 'recipe' },
      'query fmt E - relationship still recorded as expected.');
  } catch (err) {
    t.fail('Failed to query transformation relationships forward.');
  }

  try {
    await redisio.createTransformation(['fmtC'], [fmtA, 'fmtC']);
    t.fail('Should have detected source in targets.');
  } catch (err) {
    t.equal(err.message,
      'Source \'fmtC\' is also listed as a transformation target.',
      'detected source in targets.');
  }

  try {
    await redisio.createTransformation('fmtE', fmtA);
    t.fail('Should not be able to create a loop from fmt A to fmt A via fmt E.');
  } catch (err) {
    t.equal(err.message, 'Loop detected in new transformation for element fmtA.',
      'cannot create a loop.');
  }

  try {
    await redisio.createTransformation(['fmtA', 'fmtZ'], fmtE);
    t.fail('Should have detected that source Z does not exist.');
  } catch (err) {
    t.equal(err.message,
      'A source of \'[fmtA,fmtZ]\' for a new transformation relationship does not exist.',
      'source does not exist was detected.');
  }

  try {
    await redisio.createTransformation(['fmtA', 'fmtB'], ['fmtZ', 'fmtC']);
    t.fail('Should have detected that target Z does not exist.');
  } catch (err) {
    t.equal(err.message,
      'A target of \'[fmtZ,fmtC]\' for a new transformation relationship does not exist.',
      'target does not exist was detected.');
  }

  try {
    await redisio.createTransformation(['wibble', 'wobble'], ['jelly', 'on', 'plate']);
    t.fail('Should have detected that all sources and targets do not exist.');
  } catch (err) {
    t.equal(err.message,
      'Both a source of \'[wibble,wobble]\' and a target of \'[jelly,on,plate]\' for a new transformation relationship do not exist.',
      'sources and targets do not exist detected.');
  }

  try {
    await redisio.createTransformation('fmtC', 'fmtE');
    t.fail('Should have detected that fmt E is already a target.');
  } catch (err) {
    t.equal(err.message,
      'Target \'fmtE\' is already recorded as the target of another transformation.',
      'detected that specified target is already a target.');
  }

  try {
    await redisio.createTransformation('fmtC', [ 'fmtA', fmtA ]);
    t.fail('Should have detected as repeated target.');
  } catch (err) {
    t.equal(err.message,
      'Targets must be unique.',
      'detected a repeated target.');
  }

  try {
    t.deepEqual(await redisio.createTransformation(['fmtA', fmtA], 'fmtC'), [2, 3],
      'successfully created relationship with repeated source.');
    t.deepEqual(await redisio.queryTransformation('fmtC'),
      { sources: [ 'fmtA', 'fmtA' ], targets: [ 'fmtC' ], recipe: '' },
      'should be able to retrieve relationship with repeated sources.');
  } catch (err) {
    t.fail('Should have been able to create a relationship with a repeated source.');
  }

  checkEmpty(t);
  t.end();
});
