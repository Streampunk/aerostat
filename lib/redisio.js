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

const Redis = require('ioredis');
const mappings = require('./mappings.js');
const config = require('../config.js');
const uuid = require('uuid');
const beamcoder = require('beamcoder');
const mediaSpec = require('./mediaSpec.js');

/*
   Basic idea ...

   format stored at 'beamcoder:<url>' - TODO how to deal with multiple formats
   list of formats at beamcoder:content
   streams for format stored at 'beamcoder:<url>:stream_<index>'
     (streams embed codec parameters and contain nb_streams counter)
   packets for stream stored at 'beamcoder:<url>:stream_<index>:packet_<pts>'
   frames for stream stored at 'beamcoder:<url>:stream_<index>:frame_<pts>'
   sorted index sorted set is stored at 'beamcoder:<url>:stream_<index>:index'

   If <pts> is not available, a counter will be used.
   If <url> is empty, a UUID will be generated and stored as a urn.
   'beamcoder' is taken from config file
   Index maps pts to keys of frames or packets
*/

const EMPTY_SLOT = Object.freeze(Object.create(null));

const zip = (...rows) => [...rows[0]].map((_,c) => rows.map(row => row[c]));

// TODO check this does not get too large
const streamTable = {};

const redisPool = {
  pool: [],
  nextFree: null,
  queue: [],
  allConnections: [],
  EMPTY_SLOT: EMPTY_SLOT,
  use: async function() {
    if (this.nextFree == null || this.nextFree == this.pool.length) {
      if (!this.grow( this.pool.length || 5 )) {
        let wait = new Promise((fulfil) => {
          this.queue.push(fulfil);
        });
        return wait;
      }
    }
    let obj = this.pool[this.nextFree];
    this.pool[this.nextFree++] = EMPTY_SLOT;
    return obj;
  },
  recycle: function (obj) {
    // Don't place closing connections back into the pool
    if (obj.manuallyClosing || (obj.status && obj.status === 'end')) return;
    if (this.nextFree === null || this.nextFree === -1) {
      this.pool[this.pool.length] = obj;
    } else {
      this.pool[--this.nextFree] = obj;
    }
    if (this.queue.length > 0) {
      this.queue.shift()(this.use());
    }
  },
  grow: function (count = this.pool.length) {
    if (count > 0 && this.nextFree == null) {
      this.nextFree = 0;
    }

    if ((this.pool.length + count) > config.redis.pool) {
      return false;
    }

    if (count > 0) {
      let curLen = this.pool.length;
      this.pool.length += count;
      for ( let x = curLen ; x < this.pool.length ; x++ ) {
        // console.log(x, this.testing, config.redis.db, config.testing.db);
        this.pool[x] = new Redis({
          port: config.redis.port,
          host: config.redis.host,
          db: config.redis.db,
          password: config.redis.password
        });
        this.allConnections.push(this.pool[x]);
      }
    }
    return true;
  },
  size: function () {
    return this.pool.length;
  },
  close: async function () {
    console.log(this.pool.map(x => x === EMPTY_SLOT));
    let connsLength = this.allConnections.length;
    let result = Promise.all(
      this.allConnections.map(x => x.quit()));
    result = result.then(x => {
      this.allConnections = this.allConnections.slice(connsLength); // in case more conns added
      return x;
    });
    this.queue.forEach(x => x(null));
    this.queue = [];
    this.pool = [];
    this.nextFree = null;
    let closeTimeout = null;
    return Promise.race([
      result.then(x => { clearTimeout(closeTimeout); return x; }),
      new Promise((f, r) => {
        closeTimeout = setTimeout(() => {
          console.log('Timeout fired. Forcing disconnect.');
          this.allConnections.slice(0, connsLength).forEach(x => { x.disconnect(); });
          this.allConnections = this.allConnections.slice(connsLength);
          r(`Failed to close Redis connections within ${config.redis.closeTimeout}ms. Forced diconnect.`);
        }, config.redis.closeTimeout);
      })
    ]);
  }
};

['pool', 'nextFree', 'queue', 'grow', 'allConnections', 'EMPTY_SLOT'].forEach(p => {
  Object.defineProperty(redisPool, p, {
    enumerable: false
  });
});

async function buildStreamTable(redis, url) {
  let names = Array.isArray(redis) ?
    await redis[0].hgetall(`${config.redis.prepend}:${url}:streams`) :
    await redis.hgetall(`${config.redis.prepend}:${url}:streams`);
  if ((!names) || (Object.keys(names).length === 0)) {
    return; // Go quietly ... let other reasons be found
  }
  for ( let name in names ) {
    streamTable[`${config.redis.prepend}:${url}:${name}`] =
      `${config.redis.prepend}:${url}:${names[name]}`;
  }
}

const makeStreamBase = (url, streamk) => {
  let streamBase = typeof streamk === 'number' ?
    `${config.redis.prepend}:${typeof url === 'string' ? url : url.url}:stream_${streamk}` :
    `${config.redis.prepend}:${typeof url === 'string' ? url : url.url}:${streamk}`;
  streamBase = streamTable[streamBase] ? streamTable[streamBase] : streamBase;
  return streamBase;
};

function makeStreamNames(fmt) {
  let streams = fmt.streams;
  let counters = { unknown: 0, video: 0, audio: 0,
    data: 0, subtitle: 0, attchment: 0 };
  let result = {};
  for ( let x = 0 ; x < streams.length ; x++ ) {
    let mediaType = counters[streams[x].codecpar.codec_type] >= 0 ?
      streams[x].codecpar.codec_type : 'unknown';
    let streamName = `stream_${x}`;
    if (counters[mediaType] == 0) {
      result[mediaType] = streamName;
    }
    result[`${mediaType}_${counters[mediaType]++}`] = streamName;
    result[`stream${x}`] = streamName;
    result[streamName] = streamName;
  }
  // TODO match behaviour to av_find_stream_index
  if (counters.video > 0) result.default = result.video;
  else if (counters.audio > 0) result.default = result.audio;
  if (Object.keys(result).length === 0) { result.default = 'none'; }
  return result;
}

async function listContent(start = 0, limit = 10) {
  let redis = await redisPool.use();
  let list = await redis.lrange(`${config.redis.prepend}:index`, start,
    (start < 0 && start + limit < limit) ? -1 : start + limit - 1);
  redisPool.recycle(redis);
  return list.map(x => x.slice(config.redis.prepend.length + 1));
}

async function storeFormat(fmt, overwrite = true) {
  let redis = await Promise.all(Array.from(
    new Array(fmt.streams.length > 0 ? fmt.streams.length + 1 : 2),
    () => redisPool.use()));
  if ((!fmt.url) || (fmt.url && fmt.url.length === 0)) {
    fmt.url = `urn:uuid:${uuid.v4()}`;
  }
  let key = `${config.redis.prepend}:${fmt.url}`;
  if (!overwrite) {
    let exists = await redis[0].exists(key);
    if (exists) {
      redis.forEach(x => redisPool.recycle(x));
      throw new Error(`A format with key '${key}' already exists.`);
    }
  }
  let delWork = [ redis[0].del(key) ];
  for ( let x = 1 ; x < redis.length ; x++ ) {
    delWork.push(redis[x].del(`${key}:stream_${x-1}`));
  }
  await Promise.all(delWork);
  let setWork = [ redis[0].hmset(key, mappings.formatToRedis(fmt.toJSON())) ];
  for ( let x = 1 ; x <= fmt.streams.length ; x++ ) {
    setWork.push(
      redis[x].hmset(`${key}:stream_${x-1}`,
        mappings.streamToRedis(fmt.streams[x-1].toJSON())));
  }
  let result = await Promise.all(setWork);
  await Promise.all([
    redis[0].rpush(`${config.redis.prepend}:index`, key),
    redis[1].hmset(`${key}:streams`, makeStreamNames(fmt)) ]);

  redis.forEach(x => redisPool.recycle(x));
  return result;
}

// TODO wot if that packet has no pts?

async function storePacket(fmt, packet) {
  let redis = await Promise.all([redisPool.use(), redisPool.use()]);
  let idxk = `${config.redis.prepend}:${typeof fmt === 'string' ? fmt : fmt.url}` +
    `:stream_${packet.stream_index}:index`;
  let key = `${config.redis.prepend}:${typeof fmt === 'string' ? fmt : fmt.url}` +
    `:stream_${packet.stream_index}:packet_${packet.pts}`;
  let datak = `${key}:data`;
  let [ delResult ] = await Promise.all([redis[0].del(key), redis[1].del(datak)]);
  let result1 = await Promise.all([
    redis[0].hmset(key, mappings.packetToRedis(packet.toJSON())),
    (packet.data && packet.data.length > 0) ? redis[1].setBuffer(datak, packet.data) : Promise.resolve('OK') ]);
  let result2 = await Promise.all([ // TODO role this up
    redis[0].zadd(idxk, packet.pts, key),
    (packet.data && packet.data.length > 0) ? redis[1].pexpire(datak, config.redis.packetTTL) : Promise.resolve(1) ]);
  if (delResult == 1 && result2[0] === 0) { result2[0] = 1; }
  redis.forEach(x => redisPool.recycle(x));
  return zip(result1, result2).map(x =>
    (x[0].toString() === 'OK' || x[0] === 1)
    && (x[1].toString() === 'OK' || x[1] === 1) ? `OK-${delResult ? 'ovw' : 'crt'}` : null );
}

const streamMatch = /_(\d+)$/;

async function storePacketData(fmt, stream_index, pts, data) {
  let redis = await Promise.all([redisPool.use(), redisPool.use()]);
  let streamBase = makeStreamBase(fmt, stream_index);
  let streamDigits = streamMatch.exec(streamBase);
  if (!streamDigits) {
    await buildStreamTable(redis, fmt);
    streamBase = makeStreamBase(fmt, stream_index);
    streamDigits = streamMatch.exec(streamBase);
  }
  let key = `${streamBase}:packet_${pts}`;
  let datak = `${key}:data`;

  let [ pktExists, dataLength ] = await Promise.all([
    redis[0].exists(key), redis[1].strlen(datak) ]);
  if (pktExists === 0) {
    redis.forEach(x => redisPool.recycle(x));
    throw new Error(`Packet metadata for key '${key}' does not exist.`);
  }
  let result = await redis[0].setBuffer(datak, data);
  redis.forEach(x => redisPool.recycle(x));
  return (result !== null) ? `${result}-${dataLength > 0 ? 'ovw' : 'crt'}` : null;
}

async function storeFrame(fmt, frame, stream_index) {
  let redis = await Promise.all(
    Array.from(new Array(frame.data.length + 1), () => redisPool.use()));
  if (stream_index !== 0 && !stream_index) {
    stream_index = frame.stream_index;
    if (!stream_index) {
      throw new Error('Cannot determine stream_index for frame.');
    }
  }

  let streamBase = makeStreamBase(fmt, stream_index);
  let streamDigits = streamMatch.exec(streamBase);
  if (!streamDigits) {
    await buildStreamTable(redis, fmt);
    streamBase = makeStreamBase(fmt, stream_index);
    streamDigits = streamMatch.exec(streamBase);
  }
  let idxk = `${streamBase}:index`;
  let key = `${streamBase}:frame_${frame.pts}`;
  let datak_base = `${key}:data`;
  if (streamDigits) { frame.stream_index = +streamDigits[1]; }

  let delWork = [ redis[0].del(key) ];
  for ( let x = 1 ; x < redis.length ; x++ ) {
    delWork.push(redis[x].del(`${datak_base}_${x-1}`));
  }
  let [ delResult ] = await Promise.all(delWork);

  let setWork = [ redis[0].hmset(key, mappings.frameToRedis(frame.toJSON())) ];
  for ( let x = 1 ; x < redis.length ; x++ ) {
    setWork.push(redis[x].setBuffer(`${datak_base}_${x-1}`, frame.data[x-1]));
  }
  let result1 = (await Promise.all(setWork))
    .map(x => Buffer.isBuffer(x) ? x.toString() : x);
  let indexWork = [ redis[0].zadd(idxk, frame.pts, key) ];
  for ( let x = 1 ; x < redis.length ; x++ ) {
    indexWork.push(redis[x].pexpire(`${datak_base}_${x-1}`, config.redis.frameTTL));
  }
  let result2 = await Promise.all(indexWork);
  if (delResult === 1 && result2[0] === 0) { result2[0] = 1; }
  redis.forEach(x => redisPool.recycle(x));
  return zip(result1, result2).map(x => x[0] === 'OK' && x[1] === 1 ?
    (delResult ? 'OK-ovw' : 'OK-crt') : null );
}

async function storeFrameData(fmt, stream_index, pts, data, idx = -1) {
  if (!Array.isArray(data)) { data = [ data ]; }
  let redis = await Promise.all(
    Array.from(new Array(data.length + 1), () => redisPool.use()));

  let streamBase = makeStreamBase(fmt, stream_index);
  let streamDigits = streamMatch.exec(streamBase);
  if (!streamDigits) {
    await buildStreamTable(redis, fmt);
    streamBase = makeStreamBase(fmt, stream_index);
    streamDigits = streamMatch.exec(streamBase);
  }
  let key = `${streamBase}:frame_${pts}`;
  let datak_base = `${key}:data`;

  let existsWork = [ redis[0].exists(key) ];
  if (idx === -1) {
    for ( let x = 1 ; x < redis.length ; x++ ) {
      existsWork.push(redis[x].strlen(`${datak_base}_${x - 1}`));
    }
  } else {
    existsWork.push(redis[1].strlen(`${datak_base}_${idx}`));
  }
  let [ frameExists, ...dataLengths ] = await Promise.all(existsWork);
  if (frameExists === 0) {
    redis.forEach(x => redisPool.recycle(x));
    throw new Error(`Frame metadata for key '${key}' does not exist.`);
  }
  let setWork = [];
  if (idx === -1) {
    for ( let x = 0 ; x < data.length ; x++) {
      setWork.push(redis[x].setBuffer(`${datak_base}_${x}`, data[x]));
    }
  } else {
    setWork.push(redis[0].setBuffer(`${datak_base}_${idx}`, data[0]));
  }
  let results = await Promise.all(setWork);
  redis.forEach(x => redisPool.recycle(x));
  return zip(results, dataLengths).map(x => {
    if (x[0].toString() !== 'OK') return null;
    return x[1] > 0 ? 'OK-ovw' : 'OK-crt';
  });
}

async function storeMedia(fmt, element, stream_index) {
  switch (element.type) {
  case 'Packet':
    return storePacket(fmt, element);
  case 'Frame':
    return storeFrame(fmt, element, stream_index);
  default:
    throw new Error('Cannot store media element unless of type Packet or Frame.');
  }
}

async function retrieveFormat(url) {
  let redisZero = await redisPool.use();
  let key = `${config.redis.prepend}:${url}`;
  let fmtb = await redisZero.hgetallBuffer(key);
  if ((!fmtb) || (Object.keys(fmtb).length === 0)) {
    redisPool.recycle(redisZero);
    throw new Error(`Unable to retrieve a format with key '${key}'.`);
  }
  let fmto = mappings.formatFromRedis(fmtb);
  let result = beamcoder.format(fmto);
  if (fmto.nb_streams > 0) {
    let redisProms = [ redisZero ];
    for ( let x = 1 ; x < fmto.nb_streams ; x++ ) {
      redisProms.push(redisPool.use());
    }
    let redis = await Promise.all(redisProms);
    let getWork = [];
    for ( let x = 0 ; x < redis.length ; x++ ) {
      getWork.push(redis[x].hgetallBuffer(`${key}:stream_${x}`));
    }
    let strs = await Promise.all(getWork);
    if (strs.some(x => x === null)) {
      redis.forEach(x => redisPool.recycle(x));
      throw new Error(`Cannot retrieve at least one of the streams '${strs.map(x => x ? '.' : 'X')}'.`);
    }
    for ( let x = 0 ; x < strs.length ; x++ ) {
      result.newStream(mappings.streamFromRedis(strs[x]));
    }
    redis.forEach(x => redisPool.recycle(x));
  } else {
    redisPool.recycle(redisZero);
  }
  return result;
}

async function getWithStreamAlias(redis, url, streamk, mediak = '') {
  let streamBase = makeStreamBase(url, streamk);
  let key = mediak.length > 0 ? `${streamBase}:${mediak}` : streamBase;
  let method = mediak && mediak.slice(-7).indexOf('data') >= 0 ? 'getBuffer': 'hgetallBuffer';
  //  console.log(method, Array.isArray(redis) ? redis[0][method] : redis[method]);
  let result = Array.isArray(redis) ?
    await redis[0][method](key) : await redis[method](key);
  if ((!result) || (Object.keys(result).length === 0)) {
    await buildStreamTable(redis, url);
    streamBase = streamTable[streamBase] ? streamTable[streamBase] : streamBase;
    key = mediak.length > 0 ? `${streamBase}:${mediak}` : streamBase;
    method = mediak && mediak.slice(-7).indexOf('data') >= 0 ? 'getBuffer': 'hgetallBuffer';
    result = Array.isArray(redis) ?
      await redis[0][method](key) : await redis[method](key);
  }
  return result;
}

async function retrieveStream(url, stream_index) {
  let redis = await redisPool.use();
  let strb = await getWithStreamAlias(redis, url, stream_index);
  redisPool.recycle(redis);
  if ((!strb) || (Object.keys(strb).length === 0)) {
    throw new Error(
      `Unable to retrieve a stream with key '${config.redis.prepend}:${url}:${typeof stream_index === 'number' ? 'stream_' : ''}${stream_index}'.`);
  }
  let stro = mappings.streamFromRedis(strb);
  let fmt = beamcoder.format();
  let result = fmt.newStream(stro);
  result.__index(stro.index); // Fix up stream index with secret backdoor
  return result;
}

const timeBaseCache = {};

async function retrieveMedia(fmt, stream_index, pts_start,
  pts_end = Number.MAX_SAFE_INTEGER, offset = 0, limit = 10,
  flags = mediaSpec.DEFAULT, metadataOnly = false) {
  if (arguments.length == 5) {
    [pts_end, offset, limit] = [Number.MAX_SAFE_INTEGER, pts_end, offset];
  }
  let redis = [];
  let mediaElements = [];
  let tbb = null;
  let basek = makeStreamBase(fmt, stream_index);
  let idxk = `${basek}:index`;
  switch (flags) {
  case mediaSpec.DEFAULT:
    redis.push(await redisPool.use());
    mediaElements =
      await redis[0].zrangebyscore(idxk, pts_start, pts_end, 'limit', offset, limit);
    if (mediaElements.length === 0) { // Possibly because no stream index?
      await buildStreamTable(redis, fmt, stream_index);
      basek = streamTable[basek] ? streamTable[basek] : basek;
      idxk = `${basek}:index`;
      mediaElements =
        await redis[0].zrangebyscore(idxk, pts_start, pts_end, 'limit', offset, limit);
    }
    break;
  case mediaSpec.DEFAULT | mediaSpec.FUZZY:
    redis = await Promise.all([ redisPool.use(), redisPool.use() ]);
    mediaElements = await Promise.all([
      redis[0].zrangebyscore(idxk, pts_start, '+inf', 'withscores', 'limit', 0, 1),
      redis[1].zrevrangebyscore(idxk, pts_start, '-inf', 'withscores', 'limit', 0, 1)
    ]);
    if (mediaElements.length === 0 ||
      (mediaElements[0].length === 0 && mediaElements[1] === 0)) {
      mediaElements = [];
      break;
    }
    if ( // Find the closest timestamp - for fuzzy matching
      (mediaElements[0].length > 0 ? Math.abs(pts_start - (+mediaElements[0][1])) : Number.MAX_SAFE_INTEGER) <
      (mediaElements[1].length > 0 ? Math.abs(pts_start - (+mediaElements[1][1])) : Number.MAX_SAFE_INTEGER)) {
      tbb = +mediaElements[0][1];
      mediaElements = [ mediaElements[0][0] ];
    } else {
      tbb = +mediaElements[1][1];
      mediaElements = [ mediaElements[1][0] ];
    }
    if (pts_end > pts_start) {
      mediaElements =
        await redis[0].zrangebyscore(idxk, tbb, pts_end, 'limit', offset, limit);
    }
    break;
  case mediaSpec.INDEX:
    redis.push(await redisPool.use());
    if (pts_start !== pts_end) {
      pts_start = pts_start + offset;
      pts_end = (pts_end - pts_start > limit) || (pts_end === -1) ? pts_start + limit - 1: pts_end;
    }
    mediaElements =
      await redis[0].zrange(idxk, pts_start, `${pts_end}`);
    if (mediaElements.length === 0) { // Possibly because no stream index?
      await buildStreamTable(redis, fmt, stream_index);
      basek = streamTable[basek] ? streamTable[basek] : basek;
      idxk = `${basek}:index`;
      mediaElements =
        await redis[0].zrange(idxk, pts_start, `${pts_end}`);
    }
    break;
  case mediaSpec.REALTIME: // TODO need to store a stream
    redis = await Promise.all([ redisPool.use(), redisPool.use() ]);
    if (timeBaseCache[basek]) {
      tbb = timeBaseCache[basek];
    } else {
      tbb = await Promise.all([
        redis[0].hget(basek, 'time_base_num'),
        redis[1].hget(basek, 'time_base_den') ]);
      if ((!tbb[0]) || (!tbb[1])) {
        await buildStreamTable(redis, fmt, stream_index);
        basek = streamTable[basek] ? streamTable[basek] : basek;
        idxk = `${basek}:index`;
        tbb = await Promise.all([
          redis[0].hget(basek, 'time_base_num'),
          redis[1].hget(basek, 'time_base_den') ]);
        if ((!tbb[0]) || (!tbb[1])) break;
      }
      tbb = tbb.map(x => parseInt(x.toString()));
      timeBaseCache[basek] = tbb;
    }
    pts_start = ((pts_start - 0.005 ) * tbb[1]) / tbb[0]; // nudge 1/200th left
    pts_end = ((pts_end + 0.005) * tbb[1]) / tbb[0]; // nudge 1/200th right
    mediaElements =
      await redis[0].zrangebyscore(idxk, pts_start, pts_end, 'limit', offset, limit);
    break;
  case mediaSpec.DURATION:
    redis.push(await redisPool.use());
    mediaElements = pts_end >= 0 ?
      await redis[0].zrangebyscore(idxk, pts_start, '+inf', 'limit', 0, pts_end + 1) :
      (await redis[0].zrevrangebyscore(idxk, pts_start, '-inf', 'limit', 0, -pts_end));
    if (mediaElements.length === 0) { // Possibly because no stream index?
      await buildStreamTable(redis, fmt, stream_index);
      basek = streamTable[basek] ? streamTable[basek] : basek;
      idxk = `${basek}:index`;
      mediaElements = pts_end >= 0 ?
        await redis[0].zrangebyscore(idxk, pts_start, '+inf', 'limit', 0, pts_end) :
        await redis[0].zrevrangebyscore(idxk, pts_start, '-inf', 'limit', 0, -pts_end);
    }
    if (mediaElements.length > 0) {
      mediaElements = mediaElements.slice(-1);
    }
    break;
  case mediaSpec.DURATION | mediaSpec.FUZZY:
  default:
    break;
  }
  redis.forEach(x => redisPool.recycle(x));
  if (mediaElements.length === 0) {
    throw new Error('Unable to find requested media elements.');
  }
  return metadataOnly ?
    await Promise.all(mediaElements.map(k =>
      k.indexOf('packet') >= 0 ? retrievePacketMetadata(k) : retrieveFrameMetadata(k) )) :
    await Promise.all(mediaElements.map(k =>
      k.indexOf('packet') >= 0 ? retrievePacket(k) : retrieveFrame(k) ));
}

async function retrievePacket(fmtOrKey, stream_index = 0, pts = 0) {
  let redis = await Promise.all([ redisPool.use(), redisPool.use() ]);
  let dbreq = fmtOrKey.indexOf('packet_') < 0 ?
    await Promise.all([
      getWithStreamAlias(redis[0], fmtOrKey, stream_index, `packet_${pts}`),
      redis[1].getBuffer(`${makeStreamBase(fmtOrKey, stream_index)}:packet_${pts}:data`) ]) :
    await Promise.all([
      redis[0].hgetallBuffer(fmtOrKey),
      redis[1].getBuffer(`${fmtOrKey}:data`) ]);
  redisPool.recycle(redis[0]);
  redisPool.recycle(redis[1]);
  if ((!dbreq[0]) || (Object.keys(dbreq[0]).length === 0)) {
    throw new Error(`Packet in stream '${stream_index}' with timestamp '${pts}' not found.`);
  }
  let pkt = beamcoder.packet(mappings.packetFromRedis(dbreq[0]));
  if (dbreq[1]) {
    pkt.data = dbreq[1];
  }
  return pkt;
}

async function retrievePacketMetadata(fmtOrKey, stream_index = 0, pts = 0) {
  let redis = await redisPool.use();
  let pktb = fmtOrKey.indexOf('packet') < 0 ?
    await getWithStreamAlias(redis, fmtOrKey, stream_index, `packet_${pts}`) :
    await redis.hgetallBuffer(fmtOrKey);
  redisPool.recycle(redis);
  if ((!pktb) || (Object.keys(pktb).length === 0)) {
    throw new Error(`Packet in stream '${stream_index}' with timestamp '${pts}' is not found.`);
  }
  let pkt = beamcoder.packet(mappings.packetFromRedis(pktb));
  return pkt;
}

async function retrievePacketData(fmtOrKey, stream_index = 0, pts = 0) {
  let redis = await redisPool.use();
  let pktData = fmtOrKey.indexOf('packet_') < 0 ?
    await getWithStreamAlias(redis, fmtOrKey, stream_index, `packet_${pts}:data`) :
    await redis.hgetallBuffer(fmtOrKey);
  redisPool.recycle(redis);
  if (!Buffer.isBuffer(pktData)) {
    throw new Error(`Packet data in stream '${stream_index}' with timestamp '${pts}' is not found.`);
  }
  return pktData;
}

async function retrieveFrame(fmtOrKey, stream_index = 0, pts = 0) {
  let redisZero = await redisPool.use();
  let frameb = fmtOrKey.indexOf('frame_') < 0 ?
    await getWithStreamAlias(redisZero, fmtOrKey, stream_index, `frame_${pts}`) :
    await redisZero.hgetallBuffer(fmtOrKey);
  if ((!frameb) || (Object.keys(frameb).length === 0)) {
    redisPool.recycle(redisZero);
    throw new Error(`Frame in stream '${stream_index}' with timestamp '${pts}' is not found.`);
  }
  let frm = beamcoder.frame(mappings.frameFromRedis(frameb));
  let bufSizes = frm.buf_sizes;
  if (frm.buf_sizes.length > 0) {
    let redis = [ redisZero ];
    for ( let x = 1 ; x < frm.buf_sizes.length ; x++ ) {
      redis.push(await redisPool.use());
    }
    let key = `${makeStreamBase(fmtOrKey, stream_index)}:frame_${pts}`;
    let getData = [];
    for ( let x = 0 ; x < redis.length ; x++ ) {
      getData.push(redis[x].getBuffer(`${key}:data_${x}`));
    }
    let data = await Promise.all(getData);
    redis.forEach(x => redisPool.recycle(x));
    if (data.every(x => x !== null)) {
      frm.data = data;
      frm.buf_sizes = data.map(x => x.length);
    } else if (data.every(x => x === null)) {
      frm.data = [];
      frm.buf_sizes = bufSizes;
    } else { // TODO this will probably cauise an f-up at some point
      throw new Error(`Unable to retrieve data for some of the planes for key '${key}':` +
        `${data.map(x => x ? 'o' : 'X')}`);
    } // otherwise no data was stored / retrieved
  } else {
    redisPool.recycle(redisZero);
  }
  frm.stream_index = stream_index;
  return frm;
}

async function retrieveFrameMetadata(fmtOrKey, stream_index = 0, pts = 0) {
  let redis = await redisPool.use();
  let frameb =  fmtOrKey.indexOf('frame') < 0 ?
    await getWithStreamAlias(redis, fmtOrKey, stream_index, `frame_${pts}`) :
    await redis.hgetallBuffer(fmtOrKey);
  redisPool.recycle(redis);
  if ((!frameb) || (Object.keys(frameb).length === 0)) {
    throw new Error(`Frame in stream '${stream_index}' with timestamp '${pts}' is not found.`);
  }
  let frm = beamcoder.frame(mappings.frameFromRedis(frameb));
  frm.stream_index = stream_index;
  return frm;
}

const bufSizeCache = {};

async function retrieveFrameData(fmtOrKey, stream_index = 0, pts = 0, idx = '') {
  let redisZero = await redisPool.use();
  if (idx.length === 0) { // Get all data and concatenate
    let bufSizesKey = `${fmtOrKey}:${stream_index}`;
    let bufSizes = bufSizeCache[bufSizesKey];
    if (!bufSizes) {
      let frameb =  fmtOrKey.indexOf('frame_') < 0 ?
        await getWithStreamAlias(redisZero, fmtOrKey, stream_index, `frame_${pts}`) :
        await redis.hgetallBuffer(fmtOrKey);
      bufSizeCache[bufSizesKey] = JSON.parse(frameb.buf_sizes);
      bufSizes = bufSizeCache[bufSizesKey];
    }
    let redis = [ redisZero ];
    for ( let x = 1 ; x < bufSizes.length ; x++ ) {
      redis.push(await redisPool.use());
    }
    let dataWork = [];
    for ( let x = 0 ; x < bufSizes.length ; x++ ) {
      dataWork.push(getWithStreamAlias(redis[x], fmtOrKey,
        stream_index, `frame_${pts}:data_${x}`));
    }
    // let start = process.hrtime();
    let result = await Promise.all(dataWork);
    // console.log('Retrieve time:', process.hrtime(start));
    redis.forEach(x => redisPool.recycle(x));
    if (result.some(x => x === null)) {
      throw new Error(`Frame data in stream '${stream_index}' with timestamp '${pts}' is not found, pattern [${result.map(x => x === null ? 'X' : 'o')}].`);
    }
    return result;
  }
  let frameData = fmtOrKey.indexOf('frame_') < 0 ?
    await getWithStreamAlias(redisZero, fmtOrKey, stream_index,
      `frame_${pts}:data_${idx}`) :
    await redisZero.hgetallBuffer(fmtOrKey);
  redisPool.recycle(redisZero);
  if (!frameData) {
    throw new Error(`Frame data in stream '${stream_index}' with timestamp '${pts}' and index '${idx}' is not found.`);
  }
  return frameData;
}

async function storeBlob (data, customKey = '') {
  let redis = await redisPool.use();
  let key = `${config.redis.prepend}:blob:`;
  if (customKey && typeof customKey === 'string') {
    key += customKey;
  } else {
    key += `${Math.trunc(Math.random() * Number.MAX_SAFE_INTEGER)}`;
  }
  let result = await redis.setBuffer(key, data, 'PX', config.redis.ephemeralTTL);
  redisPool.recycle(redis);
  if (result != 'OK') {
    throw new Error('Failed to store ephemeral blob data.');
  }
  return key;
}

async function retrieveBlob (key) {
  let redis = await redisPool.use();
  if (typeof key === 'number') {
    key = `${config.redis.prepend}:blob:${key}`;
  }
  if (!key.startsWith(`${config.redis.prepend}:blob:`)) {
    key = `${config.redis.prepend}:blob:${key}`;
  }
  let result = await redis.getBuffer(key);
  redisPool.recycle(redis);
  if (!Buffer.isBuffer(result)) {
    throw new Error(`Failed to retrieve blob with key '${key}'.`);
  }
  return result;
}

async function checkBlob (key) {
  let redis = await redisPool.use();
  if (typeof key === 'number') {
    key = `${config.redis.prepend}:blob:${key}`;
  }
  if (!key.startsWith(`${config.redis.prepend}:blob:`)) {
    key = `${config.redis.prepend}:blob:${key}`;
  }
  let result = await redis.pexpire(key, config.redis.ephemeralTTL);
  redisPool.recycle(redis);
  return (result === 1) ? key : null;
}

async function createEquivalent (source, target) {
  if (source.url && typeof source.url === 'string') {
    source = source.url;
  }
  if (target.url && typeof target.url === 'string') {
    target = target.url;
  }

  if (source === target) {
    throw new Error('Source cannot be equivalent to source.');
  }
  let [ sourceRedis, targetRedis ] = await Promise.all([redisPool.use(), redisPool.use()]);
  const recycle = () => { redisPool.recycle(sourceRedis); redisPool.recycle(targetRedis); };
  let [ sourceExists, targetExists ] = await Promise.all([
    sourceRedis.exists(`${config.redis.prepend}:${source}`),
    targetRedis.exists(`${config.redis.prepend}:${target}`)
  ]);
  if (sourceExists < 1 && targetExists < 1) {
    recycle();
    throw new Error(`Both source '${source}' and target '${target}' for a new equivalent relationship do not exist.`);
  }
  if (sourceExists < 1) {
    recycle();
    throw new Error(`Source '${source}' for a new equivalent relationship does not exist.`);
  }
  if (targetExists < 1) {
    recycle();
    throw new Error(`Target '${target}' for a new equivalent relationship does not exist.`);
  }

  let sourceSet = `${config.redis.prepend}:equivalent:${source}`;
  let targetSet = `${config.redis.prepend}:equivalent:${target}`;
  let result = await Promise.all([
    sourceRedis.sadd(sourceSet, target),
    targetRedis.sadd(targetSet, source) ]);
  recycle();
  return result;
}

async function deleteEquivalent (source, target) {
  if (source.url && typeof source.url === 'string') {
    source = source.url;
  }
  if (target.url && typeof target.url === 'string') {
    target = target.url;
  }
  let [ sourceRedis, targetRedis ] = await Promise.all([redisPool.use(), redisPool.use()]);
  const recycle = () => { redisPool.recycle(sourceRedis); redisPool.recycle(targetRedis); };
  let sourceSet = `${config.redis.prepend}:equivalent:${source}`;
  let targetSet = `${config.redis.prepend}:equivalent:${target}`;
  let result = await Promise.all([
    sourceRedis.srem(sourceSet, target),
    targetRedis.srem(targetSet, source) ]);
  recycle();
  return result;
}

async function getMemberNames(source, depth, parents) {
  parents.push(source);
  let redis = await redisPool.use();
  let sourceSet = `${config.redis.prepend}:equivalent:${source}`;
  let members = await redis.smembers(sourceSet);
  redisPool.recycle(redis);
  if (depth > 1) {
    let subNames = await Promise.all(members
      .filter(x => parents.indexOf(x) < 0)
      .map(x => getMemberNames(x, depth - 1, parents)));
    let combined = members.concat(subNames.reduce((l, r) => l.concat(r), []));
    let mems = combined
      .filter((e, pos) => combined.indexOf(e) === pos) // Ensure list is unique
      .filter(x => x !== source);
    console.log('>>>', source, combined, mems);
    return mems;
  } else {
    return members.filter(x => x !== source);
  }
}

async function queryEquivalent (source, depth = 1) {
  if (source.url && typeof source.url === 'string') {
    source = source.url;
  }
  let equivSetNames = await getMemberNames(source, depth, []);
  return equivSetNames;
}

const addSourceStreams = streams => {
  let sourceStreams = [];
  for ( let stream of streams ) {
    if (Array.isArray(stream)) {
      sourceStreams = sourceStreams.concat(addSourceStreams(stream));
    } else {
      if (typeof stream === 'string') {
        let firstSq = stream.indexOf('[');
        if (firstSq >= 0) stream = stream.slice(0, firstSq);
        sourceStreams.push(stream);
        continue;
      }
      if (typeof stream === 'number') {
        sourceStreams.push(`stream_${stream}`);
        continue;
      }
    }
  }
  return sourceStreams;
};

async function checkStreamMap (source, target, map) {
  if (!map || Object.keys(map).length === 0) return;
  if (!streamTable[`${config.redis.prepend}:${target}:stream_0`]) {
    let redis = await redisPool.use();
    await buildStreamTable(redis, target);
    redisPool.recycle(redis);
  }
  let targetStreams = Object.keys(map);
  let alreadyMapped = {};
  targetStreams.forEach(k => {
    let mapping = streamTable[`${config.redis.prepend}:${target}:${k}`];
    if (!mapping) {
      throw new Error(`Stream map for target '${target}' references stream '${k}' that is not present.`);
    }
    if (alreadyMapped[mapping]) {
      throw new Error(`Stream alias '${k}' has already been mapped to '${mapping}'. Stream mappings must be unique.`);
    }
    alreadyMapped[mapping] = true;
  });
  if (!streamTable[`${config.redis.prepend}:${source}:stream_0`]) {
    let redis = await redisPool.use();
    await buildStreamTable(redis, source);
    redisPool.recycle(redis);
  }
  let sourceStreams = addSourceStreams(Object.values(map));
  sourceStreams.forEach(k => {
    let mapping = streamTable[`${config.redis.prepend}:${source}:${k}`];
    if (!mapping) {
      throw new Error(`Stream map for source '${source}' references stream '${k}' that is not present.`);
    }
    // Source streams can be mapped multiple times
  });
  // TODO check channel count for audio streams?
}

async function createRendition (source, target, streamMap = {}) {
  if (source.url && typeof source.url === 'string') {
    source = source.url;
  }
  if (target.url && typeof target.url === 'string') {
    target = target.url;
  }

  if (source === target) {
    throw new Error('Source cannot be a rendition of itself.');
  }
  let [ sourceRedis, targetRedis ] = await Promise.all([redisPool.use(), redisPool.use()]);
  const recycle = () => { redisPool.recycle(sourceRedis); redisPool.recycle(targetRedis); };
  let [ sourceExists, targetExists ] = await Promise.all([
    sourceRedis.exists(`${config.redis.prepend}:${source}`),
    targetRedis.exists(`${config.redis.prepend}:${target}`)
  ]);
  if (sourceExists < 1 && targetExists < 1) {
    recycle();
    throw new Error(`Both source '${source}' and target '${target}' for a new rendition relationship do not exist.`);
  }
  if (sourceExists < 1) {
    recycle();
    throw new Error(`Source '${source}' for a new rendition relationship does not exist.`);
  }
  if (targetExists < 1) {
    recycle();
    throw new Error(`Target '${target}' for a new rendition relationship does not exist.`);
  }

  // Check stream_map
  try {
    await checkStreamMap(source, target, streamMap);
  } catch (err) {
    recycle();
    throw err;
  }

  if (await targetRedis.exists(`${config.redis.prepend}:trans_target:${target}`)) {
    recycle();
    throw new Error(`Target ${target} is already recorded as the target of a  transformation.`);
  }

  let sourceSet = `${config.redis.prepend}:rendition_deps:${source}`;
  let targetKey = `${config.redis.prepend}:rendition:${target}`;

  let cycle = source;
  do {
    cycle = await sourceRedis.get(`${config.redis.prepend}:rendition:${cycle}`);
    if (cycle === target) {
      recycle();
      throw new Error(`Target '${target}' cannot be a rendition of itself.`);
    }
  } while (cycle);
  let result = await Promise.all([
    sourceRedis.sadd(sourceSet, target),
    targetRedis.set(targetKey, source) ]);

  if (streamMap && Object.keys(streamMap).length > 0) {
    result.push(await sourceRedis.hmset(`${config.redis.prepend}:rendition_map:${target}`, streamMap));
  }
  recycle();

  return result;
}

async function deleteRendition (source, target) {
  if (source.url && typeof source.url === 'string') {
    source = source.url;
  }
  if (target.url && typeof target.url === 'string') {
    target = target.url;
  }
  let [ sourceRedis, targetRedis ] = await Promise.all(
    [redisPool.use(), redisPool.use()]);
  const recycle = () => {
    redisPool.recycle(sourceRedis);
    redisPool.recycle(targetRedis);
  };
  let sourceSet = `${config.redis.prepend}:rendition_deps:${source}`;
  let targetKey = `${config.redis.prepend}:rendition:${target}`;
  let map_key = `${config.redis.prepend}:rendition_map:${target}`;
  let result = await Promise.all([
    sourceRedis.srem(sourceSet, target),
    targetRedis.del(targetKey) ]);
  if (result[1] === 1) { // Only delete stream map if target deleted
    result.push(await targetRedis.del(map_key));
  } else {
    result.push(0);
  }
  recycle();
  return result;
}

async function getSubRenditions(target, depth, parents) {
  parents.push(target);
  let redis = await redisPool.use();
  let targetKey = `${config.redis.prepend}:rendition:${target}`;
  let source = await redis.get(targetKey);
  redisPool.recycle(redis);
  if (source && depth > 1) {
    let subNames = await getSubRenditions(source, depth - 1, parents);
    let combined = [ source ].concat(subNames);
    let mems = combined
      .filter((e, pos) => combined.indexOf(e) === pos) // Ensure list is unique
      .filter(x => x !== target);
    return mems;
  } else {
    return source && source !== target ? [ source ] : [];
  }
}

async function queryRendition (target, reverse = false, depth = 1) {
  if (typeof reverse === 'number') {
    depth = reverse;
    reverse = false;
  }
  if (target.url && typeof target.url === 'string') {
    target = target.url;
  }
  let subs = await getSubRenditions(target, depth, []);
  let redis = await redisPool.use();

  let streamMap = await redis.hgetall(`${config.redis.prepend}:rendition_map:${target}`);
  Object.keys(streamMap).forEach(k => {
    streamMap[k] = streamMap[k].split(/,/);
    streamMap[k] = streamMap[k].filter(x => x.length > 0);
    if (streamMap[k].length === 0) {
      streamMap[k] = 'default';
    } else if (streamMap[k].length === 1) {
      streamMap[k] = streamMap[k][0];
    }
  });

  let result = { sources: subs };
  if (Object.keys(streamMap).length > 0) {
    result.stream_map = streamMap;
  }

  if (reverse) {
    result.dependencies = await redis.smembers(`${config.redis.prepend}:rendition_deps:${target}`);
  }

  redisPool.recycle(redis);
  return result;
}

const allUnique = a => {
  let items = new Set();
  for ( let i of a ) {
    if (items.has(i)) return false;
    items.add(i);
  }
  return true;
};

async function createTransformation (source, target, recipe = '') {
  [ source, target ] = [
    Array.isArray(source) ? source : [ source ],
    Array.isArray(target) ? target : [ target ] ];
  source = source.map(s => s.url && typeof s.url === 'string' ? s.url : s);
  target = target.map(t => t.url && typeof t.url === 'string' ? t.url : t);

  for ( let s of source ) {
    if (target.indexOf(s) >= 0) {
      throw new Error(`Source '${s}' is also listed as a transformation target.`);
    }
  }
  // this replicates the above
  // for ( let t of target ) {
  //   if (source.indexOf(t) >= 0) {
  //     throw new Error(`Target '${t}' is also listed as a transformation source.`);
  //   }
  // }
  if (!allUnique(target)) {
    throw new Error('Targets must be unique.');
  }
  // TODO assume sources can be repeated?

  let [ sourceRedis, targetRedis ] = await Promise.all([ redisPool.use(), redisPool.use() ]);
  const recycle = () => { redisPool.recycle(sourceRedis); redisPool.recycle(targetRedis); };

  let [ sourceWork, targetWork ] = [ Promise.resolve(1), Promise.resolve(1) ];
  for ( let s of source ) {
    sourceWork = sourceWork.then(r => {
      if (r === 0) return 0;
      return sourceRedis.exists(`${config.redis.prepend}:${s}`);
    });
  }
  for ( let t of target ) {
    targetWork = targetWork.then(r => {
      if (r === 0) return 0;
      return targetRedis.exists(`${config.redis.prepend}:${t}`);
    });
  }
  let [ sourcesExist, targetsExist ] = await Promise.all([sourceWork, targetWork]);
  if (sourcesExist < 1 && targetsExist < 1) {
    recycle();
    throw new Error(`Both a source of '[${source}]' and a target of '[${target}]' for a new transformation relationship do not exist.`);
  }
  if (sourcesExist < 1) {
    recycle();
    throw new Error(`A source of '[${source}]' for a new transformation relationship does not exist.`);
  }
  if (targetsExist < 1) {
    recycle();
    throw new Error(`A target of '[${target}]' for a new transformation relationship does not exist.`);
  }

  let alreadySeen = new Set(target);

  const deeper = async a => {
    for ( let s of a ) {
      let trans_target = await sourceRedis.get(`${config.redis.prepend}:trans_target:${s}`);
      if (!trans_target) continue;
      let subs = await sourceRedis.smembers(`${config.redis.prepend}:transformation:${trans_target}`);
      for ( let x of subs ) {
        if (alreadySeen.has(x)) {
          recycle();
          throw new Error(`Loop detected in new transformation for element ${x}.`);
        }
      }
      for ( let x of subs ) {
        alreadySeen.add(x);
      }
      deeper(subs);
    }
  };
  await deeper(source);

  for ( let t of target ) {
    let [ targetTrans, targetRends ] = await Promise.all([
      sourceRedis.exists(`${config.redis.prepend}:trans_target:${t}`),
      targetRedis.exists(`${config.redis.prepend}:rendition:${t}`) ]);
    if (targetTrans) {
      recycle();
      throw new Error(`Target '${t}' is already recorded as the target of another transformation.`);
    }
    if (targetRends) {
      recycle();
      throw new Error(`Target '${t}' is already recorded as the target of a rendition.`);
    }
  }

  target = target.sort();
  let transTargets = target.map(t => `${config.redis.prepend}:trans_target:${t}`);
  let transName = target.reduce((l, r) => l + ',' + r);
  let transTarget = `${config.redis.prepend}:transformation:${transName}`;
  let transArgs = `${config.redis.prepend}:trans_args:${transName}`;
  let transRecipe = `${config.redis.prepend}:trans_recipe:${transName}`;

  let transRev = source.map(s => `${config.redis.prepend}:trans_rev:${s}`);

  [ sourceWork, targetWork ] = [ Promise.resolve(1), Promise.resolve(1) ];

  for ( let s of transRev ) {
    sourceWork = sourceWork.then(x => {
      return sourceRedis.sadd(s, transName).then(y => y > 0 ? x + 1 : x);
    });
  }
  sourceWork = sourceWork.then(x => {
    return sourceRedis.set(transArgs, source.toString()).then(y => y > 0 ? x + 1 : x);
  });

  for ( let t of transTargets ) {
    targetWork = targetWork.then(x => {
      return targetRedis.set(t, transName).then(y => y === 'OK' ? x + 1 : x);
    });
  }
  targetWork = targetWork.then(x => {
    return targetRedis.sadd(transTarget, source).then(y => y > 1 ? x + 1 : x);
  });
  targetWork = targetWork.then(x => {
    return targetRedis.set(transRecipe, recipe).then(y => y === 'OK' ? x + 1 : x);
  });

  let result = await Promise.all([sourceWork, targetWork]);

  // TODO check the result?
  recycle();

  return result;
}

async function deleteTransformation (target) {
  let transName, sources;
  let [ targetRedis, sourceRedis ] = await Promise.all([redisPool.use(), redisPool.use()]);
  const recycle = () => { redisPool.recycle(sourceRedis); redisPool.recycle(targetRedis); };
  if (Array.isArray(target)) {
    target = target.map(x => x.url ? x.url : x);
    transName = target.sort().reduce((x, y) => x + ',' + y);
    sources = await targetRedis.smembers(`${config.redis.prepend}:transformation:${transName}`);
    if (Object.keys(sources).length === 0) {
      recycle();
      throw new Error(`Could not identify a transformation identifieed by all given targets: '[${target}]'.`);
    }
  } else {
    target = target.url ? target.url : target;
    transName = await targetRedis.get(`${config.redis.prepend}:trans_target:${target}`);
    if (!transName) {
      recycle();
      throw new Error(`Could not find a transformation involving target '${target}'.`);
    }
    sources = await targetRedis.smembers(`${config.redis.prepend}:transformation:${transName}`);
  }

  let targets = transName.split(/,/).map(x => `${config.redis.prepend}:trans_target:${x}`);
  targets.push(`${config.redis.prepend}:transformation:${transName}`);
  targets.push(`${config.redis.prepend}:trans_args:${transName}`);
  targets.push(`${config.redis.prepend}:trans_recipe:${transName}`);
  let targetWork = targetRedis.del(targets);
  let sourceWork = Promise.resolve(1);
  for ( let s of sources ) {
    sourceWork = sourceWork.then(x => {
      return sourceRedis.srem(`${config.redis.prepend}:trans_rev:${s}`, transName).then(y => x + y);
    });
  }
  let result  = await Promise.all([sourceWork, targetWork]);
  recycle();
  return result;
}

async function queryTransformation (target, reverse = false) {
  target = target.url ? target.url : target;
  let [ targetRedis, sourceRedis ] = await Promise.all([redisPool.use(), redisPool.use()]);
  const recycle = () => { redisPool.recycle(sourceRedis); redisPool.recycle(targetRedis); };

  if (!reverse) {
    let transName = await targetRedis.get(`${config.redis.prepend}:trans_target:${target}`);
    if (!transName) {
      recycle();
      return { sources: [] };
    }
    let targets = transName.split(/,/);
    let [ sources, recipe ] = await Promise.all([
      sourceRedis.get(`${config.redis.prepend}:trans_args:${transName}`),
      targetRedis.get(`${config.redis.prepend}:trans_recipe:${transName}`) ]);
    recycle();
    sources = sources.split(/,/);

    try {
      recipe = typeof recipe === 'string' && recipe.length > 0 ? JSON.parse(recipe) : '';
    } catch (err) {
      // that's fine ... just passing thru
    }
    return { sources, targets, recipe };
  }

  let transNames = await targetRedis.smembers(`${config.redis.prepend}:trans_rev:${target}`);
  let transformations = transNames.map(x => x.split(/,/));
  recycle();
  if (Object.keys(transformations).length === 0) {
    return { sources: [] };
  }
  return { source: target, transformations };
}

function resetCaches() {
  for ( let prop in streamTable) delete streamTable[prop];
  for ( let prop in timeBaseCache) delete timeBaseCache[prop];
  for ( let prop in bufSizeCache) delete bufSizeCache[prop];
}

module.exports = {
  redisPool,
  listContent,
  storeFormat,
  storeMedia,
  storePacket,
  storePacketData,
  storeFrame,
  storeFrameData,
  retrieveFormat,
  retrieveStream,
  retrieveMedia,
  retrievePacket,
  retrieveFrame,
  retrievePacketMetadata,
  retrievePacketData,
  retrieveFrameMetadata,
  retrieveFrameData,
  storeBlob,
  retrieveBlob,
  checkBlob,
  resetCaches,
  createEquivalent,
  createRendition,
  createTransformation,
  deleteEquivalent,
  deleteRendition,
  deleteTransformation,
  queryEquivalent,
  queryRendition,
  queryTransformation,
  close: async () => { return redisPool.close(); },
  mediaFlags: {
    DEFAULT: mediaSpec.DEFAULT,
    FUZZY: mediaSpec.FUZZY,
    INDEX: mediaSpec.INDEX,
    REALTIME: mediaSpec.REALTIME,
    DURATION: mediaSpec.DURATION
  }
};

// TODO Delete operations.
