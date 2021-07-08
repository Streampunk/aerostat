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

// Do not put secrets into this configuration file. Use command line or environment variable.

module.exports = {
  // Configuration of connections to Redis
  redis: {
    // True if connecting to a redis cluster
    cluster: process.env.BEAM_TEST_REDIS_CLUSTER === 'true',
    // IP address of primary redis host
    host: process.env.BEAM_TEST_REDIS_HOST || '127.0.0.1',
    // Port number of primary redis host connection
    port: +process.env.BEAM_TEST_REDIS_PORT || 6379,
    // Password to authenticate with redis (optional)
    password: process.env.BEAM_TEST_REDIS_PASSWORD || null,
    // Redis database to be used for main cache
    db: +process.env.BEAM_TEST_REDIS_DB || 15,
    // Maximum pool size for Redis connection pool
    pool: +process.env.BEAM_TEST_REDIS_POOL || 20,
    // Value to prepend to beamengine database keys
    prepend: process.env.BEAM_TEST_REDIS_PREPEND || 'beamengine',
    // Time (ms) before expiry of packet data
    packetTTL: +process.env.BEAM_TEST_REDIS_PACKET_TTL || 120000,
    // Time (ms) before expiry of frame data
    frameTTL: +process.env.BEAM_TEST_REDIS_FRAME_TTL || 10000,
    // Time (ms) before expiry of ephemeral data blob
    ephemeralTTL: +process.env.BEAM_TEST_REDIS_EPHEMERAL_TTL || 10000,
    // Time (ms) before error when closing Redis pool
    closeTimeout: +process.env.BEAM_TEST_REDIS_CLOSE_TIMEOUT || 2000
  },
  // Configuration of the application server
  app: {
    // Local port on which to run the app server
    port: +process.env.BEAM_TEST_APP_PORT || 13131,
    // Comma separated list of the queue that the worker handles
    queues: process.env.BEAM_PROD_APP_QUEUES || 'media_workers',
  },
  // Rules that match path patterns and cause work to happen
  // - All rules can be overridden by a config files loaded on the command line
  // - Rules cannot be configured using environment variables
  rules: {
    // List each rule as a separate property
    jpeg: {
      // Match only HTTP methods of this type - string or array of strings
      method: 'GET',
      // Regular expression to match against the HTTP path - use a string
      pathPattern: '^/beams/.*\\.(jpg|jpeg)$',
      // Name of the queue to which jobs should be added
      queue: 'media_workers',
      // Name of the function to execute.
      function: 'makeJPEG',
      // Run the rule after the main beam router (true) or pre-router (false)
      postRoute: false
    },
    mp4: {
      method: 'GET',
      pathPattern: '^/beams/.*\\.(mp4)$',
      queue: 'media_workers',
      function: 'makeMP4',
      postRoute: false
    },
    fileStore: {
      method: ['PUT', 'POST'],
      pathPattern: '^/beams.*',
      statusCode: [200, 201],
      queue: 'file_workers',
      function: 'storeCopy',
      postRoute: true
    },
    dataMissing: {
      method: 'GET',
      pathPattern: '^.*(packet|frame)_(\\d+)(.raw.*|/data.*)$',
      statusCode: 404,
      queue: 'file_workers',
      function: 'retrieveCopy',
      postRoute: true
    }
  }
};
