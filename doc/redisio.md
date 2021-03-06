# Redis IO API

The RedisIO API provides programmatic access to manipulate beam engine media structures stored in redis, including _formats_, _streams_, _packets_ and _frames_. Redis IO uses a pool of redis connections that are recycled as they become available. Methods are asynchronous (`async`) and, as much as possible, the underlying work is done in parallel.

To use the API, include the `redisio` property of the `beamengine` module:

    const { redisio } = require('beamengine');

This will create a redis connection pool automatically with a maximum of `config.redis.pool` connections. Note that after using the redisio module, it is advisable to call asynchronous method `close`:

    await redisio.close();

Closing will return once all redis connections have safely quit, or after `config.redis.closeTimeout` milliseconds (defaults to 2000ms), whichever comes first.

Beam engine workers can access their source data or store results in four different ways:

1. Using the methods of this API.
2. Using HTTP or HTTPS content beam API requests.
3. Directly communicating with Redis (not recommended).
4. Via URLs and a third-party store (not ideal).

## API - content items

_Content items_ are described by their _format_ and the terms are used interchangably in this documentation.

### redisio.listContent([start, [limit]])

List the names of all content items from index `start` (default is `0`), returning no more than `limit` (default is `10`) items.

Returns an array of names.

### redisio.retrieveFormat(name)

Retrieve the format with the given `name`.

If a format with the given name exists, a beam coder format-type object is returned. Otherwise, an exception is thrown.

### redisio.storeFormat(format, [overwrite])

Store the given `format`, a format object that is either a beam coder [demuxer](https://github.com/Streampunk/beamcoder#demuxing) or [muxer](https://github.com/Streampunk/beamcoder#muxing). If the `format` contains a `url` property, this will be set as its name, otherwise a URI-representation of a [type 4 UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) will be generated.

Set `overwrite` to `false` to prevent overwriting an existing format. The default value is `true`.

On success, returns `[ 'OK', 'OK' ]` to indicate that both the index and the format were updated successfully.

### redisio.retrieveStream(name, stream_id)

From a format with the given `name`, retrieve details of a stream identified by `stream_id`. The stream identifier may be:

* an index number, either an integer number or prefixed with `stream_`, making e.g. `1` and `stream_1` equivalent identifiers;
* the kind of media stream - one of `video`, `audio`, `subtitle` (for captions), `data` or `attachment` - with an optional by-type index specifier e.g. `audio_0` for the first audio stream, `audio_1` for the second, and so on.
* a `default` reference - defers to FFmpeg libraries to determine the stream that is the default, normally the first video track.

Returns a beam coder stream-type object if the stream is found. Otherwise, throws an exception.

## API - media elements

Access to media elements, which are either [_packets_](https://github.com/Streampunk/beamcoder#creating-packets) or [_frames_](https://github.com/Streampunk/beamcoder#creating-frames). Some methods work with both media element metadata and payload, some just the metadata and others just the payload. Some methods a generic wrappers around both frames and packets and others a specific to type.

### redisio.retrieveMedia(name, stream_id, start, [end], [offset], [limit], [flags], [metadataOnly])

Search for and retrieve media elements matching a given query, with or without metadata payload. The search for media elements is carried out in the format with the given `name` within the stream identified by `stream_id`. [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`. The search starts at the given inclusive numerical `start` point, continues to the optional inclusive numerical `end` point (defaults to [maximum safe integer](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER)), with the type of query configured by the `flags`:

* `redisio.mediaFlags.DEFAULT` - `start` and `end` are media element timestamps measured in units of stream time base
* `redisio.mediaFlags.FUZZY` - `start` point is considered to be approximate - find the closest matching value;
* `redisio.mediaFlags.INDEX` - `start` and `end` points are 1-based index values based on the sequence of media elements in the stream.
* `redisio.mediaFlags.REALTIME` - `start` and `end` points are measured in seconds, requiring timestamps to be resolved to realtime using the stream's time base
* `redisio.mediaFlags.DURATION` - `start` time is a media element timestamp measured in units of stream time base and `end` is an index offset, e.g. `1` means he next frame after `start` and `-10` is the tenth frame before `start`.

The response is an array of media elements, either packets or frames. For large responses, the array can be _paged_ by starting at the optional `offset` (defaults to `0`) and limited to `limit` values (defaults to `10`). Set the  `metadataOnly` flag to true to return media elements without payloads included, otherwise set to `false` to include payloads (default `false`). If no media elements are found or an error occurs, an exception is thrown.

### redisio.retrievePacket(nameOrKey, [stream_id], [pts])

Retrieve metadata and payload data for a specific single packet, where `nameOrKey` is either a complete redis key or the name of a format. If the name of a format, a stream identifier (`stream_id`) and exact presentation time stamp (`pts`) must be provided. [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

If the packet is found and retrieval was a success, the result is a beam coder _packet_ with its data payload included. Otherwise, an exception is thrown.

### redisio.retrievePacketMetadata(nameOrKey, [stream_id], [pts])

Retrieve metadata for a specific single packet, where `nameOrKey` is either a complete redis key or the name of a format. If the name of a format, a stream identifier (`stream_id`) and exact presentation time stamp (`pts`) must be provided.  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

If the packet is found and retrieval was a success, the result is a beam coder _packet_ without its data payload included. Otherwise, an exception is thrown.

### redisio.retrievePacketData(nameOrKey, [stream_id], [pts])

Retrieve payload data for a specific single packet, where `nameOrKey` is either a complete redis key or the name of a format. If the name of a format, a stream identifier (`stream_id`) and exact presentation time stamp (`pts`) must be provided.  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

If the packet is found and retrieval was a success, the result is a [Node.js `Buffer`](https://nodejs.org/api/buffer.html) containing the data payload of the packet. Otherwise, an exception is thrown.

### redisio.retrieveFrame(nameOrKey, [stream_id], [pts])

Retrieve metadata and payload data for a specific single frame, where `nameOrKey` is either a complete redis key or the name of a format. If the name of a format, a stream identifier (`stream_id`) and exact presentation time stamp (`pts`) must be provided.  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

If the frame is found and retrieval was a success, the result is a beam coder _frame_ with all of its data payloads included. Otherwise, an exception is thrown.

### redisio.retrieveFrameMetadata(nameOrKey, [stream_id], [pts])

Retrieve metadata for a specific single frame, where `nameOrKey` is either a complete redis key or the name of a format. If the name of a format, a stream identifier (`stream_id`) and exact presentation time stamp (`pts`) must be provided.  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

If the frame is found and retrieval was a success, the result is a beam coder _frame_ witout any of its data payloads included.  Otherwise, an exception is thrown.

Array-valued property `.buf-sizes` contains the sizes of the related data payloads.

### redisio.retrieveFrameData(nameOrKey, [stream_id], [pts], [data_index])

Retrieve payload data for a specific single frame, where `nameOrKey` is either a complete redis key or the name of a format. If the name of a format, a stream identifier (`stream_id`) and exact presentation time stamp (`pts`) must be provided.  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`. Where provided, the `data_index` requests data for a specific data plane by zero-based numerical index, otherwise the data for all planes is retrieved and concatenated.

If requesting data for a single plane, the result is a [Node.js `Buffer`](https://nodejs.org/api/buffer.html) containing the data payload of the single data plane in the frame. Otherwise, an exception is thrown.

If requesting data for all the planes, the result is an array containing the `data` with [Node.js `Buffer`](https://nodejs.org/api/buffer.html) elements with the data for each plane. For example:

    [ Buffer <0a 1c 7c ... >, Buffer <9f 32 00 ... >, ... ]

If the frame does not exist or one of more of the data planes requested is not available, an exception is thrown.

### redisio.storeMedia(name, element, [stream_index])

Store a single element of media including both its metadata and payload. The media `element` will be either created or replaced for format with the given `name`.

If the media element is a beam coder _packet_, the stream will be identified from its `.stream_index` property. For beam coder _frames_, either the frame has an additional `.stream_index` property or the `stream_index` must be provided as an argument. In all cases, this must be a numerical stream index, not a name or a `default`.

__TODO__ - consider allowing the use of media kind stream identifiers?

Whether a _packet_ or _frame_, the presentation timestamp of the media element will be taken from the `pts` property.

The value returned is an array indicating success or failure of the storage operation, with the first element of the array referring to whether metadata storage was successful and subsequent element relating to data storage. Possible array values are:

* `'OK-crt'` - Successful storage, new value created.
* `'OK-ovw'` - Successful storage, existing value overwritten.
* `null` - Failure to store element.

So, for example, on storing a new packet, the following result is expected:

    [ 'OK-crt', 'OK-crt' ]

If the metadata storage succeeded but data storage failed:

    [ 'OK-crt', null ]

For frames, the number of elements in the array is the number of data planes plus one. Here is the result for a frame with format `yuv420p` that has been overwritten successfully:

    [ 'OK-ovw', 'OK-ovw', 'OK-ovw', 'OK-ovw' ]

### redisio.storePacket(name, packet)

As [`storeMedia`](#redisiostoremedianame-element-stream_index) except that the `element` is a `packet` and must be a beam coder _packet_. To store just metadata for a packet, set the `.data` property to `null`.

### redisio.storePacketData(name, stream_id, pts, data)

Store or replace the data payload `data` for a specific packet. The packet is part of the format with the given `name`, in the stream identified by `stream_id` and has the given presentation timestamp (`pts`).  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

On success, the response is `'OK-crt'` if the data was created or `'OK-ovw'` if the data was overwritten successfully. If the writing of data failed, `null` is returned. If no associated metadata is available for the packet, an exception is thrown.

### redisio.storeFrame(name, frame, [stream_index])

As [`storeMedia`](#redisiostoremedianame-element-stream_index) except that the `element` is a `frame` and must be a beam coder _frame_. To store just metadata for a frame, set the `.data` property to `null` or `[]`.

### redisio.storeFrameData(name, stream_id, pts, data, [data_index])

Store or replace some or all of the payload `data` for a specific frame. The frame is part of the format with the given `name`, in the stream identified by `stream_id` and has the given presentation timestamp (`pts`).  [As described previously](#redisioretrievestreamname-stream_id), the stream identifier may be an index, media type or `default`.

If a `data_index` is provided, the call replaces or creates data for just one of the planes and the `data` is a single [Node.js `Buffer`](https://nodejs.org/api/buffer.html). If no `data_index` is provided, the data is expected to be provided as an array of [Node.js `Buffer`s](https://nodejs.org/api/buffer.html), one for each plane.

The response is an array of values representing whether each of the data planes was successfully stored, with `'OK-crt'` if the data plane is created, `'OK-ovw'` if it is overwritten and `null` if an error occurred. If a data index was provided, the array will contain a single element.

## API - ephemeral data blobs

As a means to communicate a one-time result of type `Buffer` from a worker to its caller, including a caller that initiated a request, temporary _ephemeral blobs_ may be stored in redis. These blobs expire after `config.redis.ephemeralTTL` milliseconds (defaults to 10000ms).

### redisio.storeBlob(data)

Store the given `data` blob, a [Node.js `Buffer`](https://nodejs.org/api/buffer.html), into redis. Returns an auto-generated random `key` to use to retrieve the data and starts the time-to-live clock.

### redisio.retrieveBlob(key)

Retrieve the data blob with the given `key`. Returns a [Node.js `Buffer`](https://nodejs.org/api/buffer.html) containing the data or throws an exception if the buffer cannot be found, possibly because the value has expired in the cache.

## API - relationships

The [relationships model of a beam engine](../README.md#relationships) allows three different kinds of  association between items of contents to be recorded. In combination with rules, workers can use the relationships to maintain state or make elements just-in-time.

### redisio.createEquivalent(source, target)

Record an _equivalent relationship_ between a `source` content item and a `target`. The arguments are either a beam coder value of format type (`format`, `muxer` or `demuxer`) or the name of a content item.

Both `source` and `target` must exist already. Equivalent relationships are commutative and so a relationship is recorded both `source` to `target` and `target` to `source`. The result is an array of numbers that should be `[ 1, 1 ]` if the relationship is new and `[ 0, 0 ]` if the relationship is already recorded.

### redisio.queryEquivalent(source, [depth])

Query which content items are equivalent to the given `source`. The `source` can be a either a beam coder value of format type (`format`, `muxer` or `demuxer`) or the name of a content item.

If `A` is equivalent to `B` and `B` is equivalent to `C` then `A` is equivalent to `C`. The optional `depth` parameter that defaults to `1` determines how deep the search in the recorded relationships, so a query for `A` with depth `2` returns `B` and `C`, but with depth `1` returns just `B` as no direct relationship with `C` is recorded.

The result is an array containing the names of the content items that are equivalent, e.g.:

    [ 'B', 'C' ]

Queries are bidirectional and so a query for `B` of depth 1 returns `[ 'A', 'C' ]` as direct relationships are recorded between all three.

### redisio.deleteEquivalent(source, target)

Delete a direct recorded equivalent relationship between two content items, a `source` and a `target`, although the order is not actually important in this case.  The `source` and `target` can be a either a beam coder value of format type (`format`, `muxer` or `demuxer`) or the name of a content item.

A successful result is `[ 1, 1 ]` indicating that the `source` to `target` and `target` to `source` recorded relationships have been removed.

Note that if `A` is equivalent to `B` and `B` is equivalent to `C`, no direct relationship is recorded between `A` and `C` and so cannot be removed. Expect a result of `[ 0, 0 ]` in this case.

### redisio.createRendition(source, target, [stream_map])

Creates a rendition relationship to record that a `target` is made from its `source`.  The `source` and `target` can be a either a beam coder value of format type (`format`, `muxer` or `demuxer`) or the name of a content item. The formats of related items have codec parameters and time base parameters and it should be possible to determine a recipe for how the `target` is made from the `source` based on those parameters.

If no `stream_map` is specified, streams are assumed to map one-for-one by stream index. [See below](#stream-map) for details of specifying a stream map.

Both `source` and `target` must exist already or an exception is thrown. Rendition relationships are directional, as a `target` is rendered from its `source`. It is not possible to record a relationship from a `source` to a `target` and from the same `target` back to the same `source`. A `target` cannot have more than one `source` as that would be a form of transformation relationship. If a `stream_map` is provided then this is checked to see that all streams exist in the source and target and that target streams are specified uniquely, i.e. `audio`, `audio_0` and `stream_6` are aliases for the same stream.

The result is an array of numbers that should be `[ 1, "OK" ]` or `[ 1, "OK", "OK" ]` if the relationship is new and `[ 0, "OK" ]` or `[ 0, "OK", "OK" ]` if the relationship is already recorded. The third element of the array indicates

#### Stream map

The `stream_map` is a JSON object that describes how the target tracks are made from the source tracks, with a bit of special notation to help out. The stream name aliases previously defined can be used to specify the stream mappings. Any stream not described for the target does not exist in the target.

```json
{
  "video": "video",
  "audio_0": "audio_2",
  "audio_1": "audio_3",
  "audio_2": [ "audio_0", "audio_1" ],
  "audio_3": "audio_7[3]",
  "subtitle": "subtitle_2",
  "data": "data"
}
```

This example stream map can be read as:

* The target rendition has 7 streams related to the source `video`, `audio_0`, `audio_1`, `audio_2`, `audio_3`, `subtitle`, `data`. It may have more as specified in its format.
* `video` and `data` streams map one-to=one, transformed according to their codec parameters.
* The first pair of audio streams `audio_0` and `audio_1` in the target are renditions of the second pair of audio streams `audio_2` and `audio_3` in the source.
* The stream `audio_2` has more than one channel made by concatenating together channels from `audio_0` and `audio_1` in the source. So if the source streams are flagged as mono and the target track as stereo, the stereo stream has `audio_0` on the left channel and `audio_1` on the right channel.
* Single channel audio stream `audio_3` of the target rendition consists of the fourth (`[3]`) channel of the `audio_7` stream from the source.
* The single `subtitle`-type stream of the target is made from the third subtitle stream of the source.

More formally, the target streams are the property names of the object and their value specifies how they are made from source streams. Stream names can be by:

* stream index of the form `stream_0`, `stream_1`, `stream_2` etc.
* media type aliases `video`, `audio`, `subtitle`, `data` or `attachment`, with by-media-type index added `audio_0` for the first audio stream, `audio_2` for the second etc.
* `default` to identify the default stream, as determined by FFmpeg

The channels of multiple audio streams can be concatenated to make a multi-channel source track using an array. For example, if source audio streams `audio_6` has 2 channels and `audio_8` has 3 channels, `[ "audio_6", "audio_8" ]` creates a target stream with 5 audio channels, the first two from `audio_6` and the final three from `audio_8`.

A specific audio stream can be selected using array index notation with a zero-based channel index, for example `audio_1[1]` for the second audio channel from audio stream `audio_1`.

Channel concatenation and channel selection can be combined, with single channels from sources selected and then concatenated. For example, creating a 2-channel target stream:

    [ "audio_8[1]", "audio_6[0]" ]

### redisio.queryRendition(target, [reverse], [depth])

Query rendition relationships recorded between a `target` content items and either the content item it depends on (forward relationships) or content items that depend on / are rendered from it (`reverse` relationships).

The `depth` of the query, which defaults to `1`, determines how many levels of recorded rendition relationships are included in the response. For example, if `C` is a rendition of `B` and `B` is a rendition of `A`, then a query for target `A` returns `B` and `C`.

If `reverse` is set to `true` (the default is `false`) then the response includes every content item that is recorded as a rendition of the `target`, the _dependencies_. This is direct dependencies only, the `depth` value has no impact.

If the `target` does not exist or has no recorded rendition relationships, the following object is returned:

    { sources: [] }

The result of a successful query is an object containing the list of `sources` (array of source content item names) and any `stream_map` that was specified between the `target` and the direct `source`. If `reverse` is `true`, the `dependencies` property contains an array of content item names that are renditions of the `target`. For example:

```javascript
let result = queryRendition('A', true, 2);
```

An example `result` is:

```javascript
{
  sources: [ 'B', 'C' ],
  stream_map: {
    video: 'video',
    audio: [ 'audio_0', 'audio_1' ],
    subtitle: 'subtitle'
  },
  dependencies: [ 'X', 'Y', 'Z' ]
}
```

### redisio.deleteRendition(source, target)

Delete a direct rendition relationship recorded between a `target` and `source`.  The `source` and `target` can be a either a beam coder value of format type (`format`, `muxer` or `demuxer`) or the name of a content item. The order of `source` and `target` is important.

The result of a successful deletion is `[ 1, 1, 1 ]` indicating that the forward and reverse recording of the relationship have been removed along with the stream map. A result of `[ 0, 0, 0 ]` indicates that no such rendition relationship is currently recorded. A result `[ 1, 1, 0 ]` indicates that the rendition is no longer recorded and it did not have a stream map.

### redisio.createTransformation(source, target, [recipe, [bounds]])

A transformation relationship records that a `source` or sources are transformed to make a `target` or targets. The relationship may include the `recipe`, a specification of the filter graph for the relationship, and optional time `bounds` representing presentation time stamps between which the relationship is valid. As a simple example, consider a target content item that is made by scaling the video stream of its source.

The `source` and `target` parameters are either single values or arrays of values, either beam coder values of format type (`format`, `muxer` or `demuxer`) or content item names. The content items must already exist. The source and targets must meet the following rules:

* Targets must be unique within a single transformation relationship.
* It should not be possible to define a transformation loop from a target to itself at any depth.
* Any target cannot be recorded as the target of another rendition or transformation.

A successful result is an array containing integers, something like `[2, 3]` representing the number of source and target records created in Redis. A consequence of the rules is that transformation relationships cannot be overwritten, meaning that that the values returned should not be zero. To change a transformation relationship, first [delete it](#redisiodeletetransformationsource-target) and then recreate it.

A `recipe` describes how targets are made from sources and can be expressed in the same format as the object passed in to create a [beam coder filterer](https://github.com/Streampunk/beamcoder#filterer). The recipe is not processed in any way as it is just stored in redis and returned on query, so could be an FFmpeg filter (assumed) or some other recipe description. If the `recipe` is a Javascript object, it is serialized to JSON before storage. If the `recipe` is a string, it is stored as is. If no `recipe` is provided, this should be interpreted as meaning that a transformation relationship should be acknowledged as in existence but not explicitly recorded.

__TODO__ consider whether time bounded transformation relationships add value at this level?

### redisio.queryTransformation(target, [reverse])

Query any transformation relationship associated with the given `target`, or if the `reverse` flag is set then show any targets that this relationship is part of. The `target` parameter can be a beam coder value of format type (`format`, `muxer` or `demuxer`) or a content item's name.

Unlike for renditions or equivalent relationships, transformation queries do not support depth queries.

The result of a successful _forward_ query (`reverse` flag not set) is the complete details of the transformation relationship recorded for the target. For example, the Javascript object:

```javascript
{
  sources: [ 'C', 'D', 'E'],
  targets: [ 'A', 'B' ],
  recipe : {
    filterType: 'video',
    inputParams: {
      width: 1920,
      height: 1080,
      // More parameters
    },
    outputParams: {
      pixelFormat: 'yuv422'
    },
    filterSpec: 'scale=1280:720'
  }
}
```

Note that `sources` can be repeated. The array returned is similar to an array of arguments passed to a function. 

The `recipe` will be one of the following:

* An empty string (`""`) signifying that no recipe has been explicitly recorded
* If the recipe could be parsed as a JSON object, the JSON object parsed
* Otherwise a string describing the recipe

For a query where the `reverse` flag is set to `true` (default is `false`), the results is an array of sub-arrays of targets for the given source (as the `target` parameter). Each sub-array is a single transformation relationship that the source participates in. For example, in JSON format:

```json
{
  "source" : "C",
  "tranformations": [
    [ "A", "B" ],
    [ "X", "Y", "Z" ]
  ]
}
```

If the `target` is not found as the result of the query, an empty value is returned as follows, whatever `reverse` flag is set to:

```javascript
{ sources: [] }
```

### redisio.deleteTransformation(target)

A transformation relationship is uniquely identified by the content items that make up its targets. To delete a transformation, pass in the name or one or more of the `target`s. If a single target is passed in, the transformation relationship it participates in is found and deleted. If more than one target is passed in, this list must contain all the targets that participate in the relationship before it is deleted.

On success, the result of the delete is an array of two positive integers, e.g. `[3, 4]`. If a matching transformation relationship is not found then an exception is thrown.
