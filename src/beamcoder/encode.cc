/*
  Aerostat Beam Engine - Redis-backed highly-scale-able and cloud-fit media beam engine.
  Copyright (C) 2018  Streampunk Media Ltd.

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

#include "encode.h"

void encoderExecute(napi_env env, void* data) {
  encoderCarrier* c = (encoderCarrier*) data;
  const AVCodec* codec = nullptr;
  int ret;

  codec = avcodec_find_decoder_by_name(c->codecName);
  if (codec == nullptr) {
    c->status = BEAMCODER_ERROR_ALLOC_ENCODER;
    c->errorMsg = "Failed to find an encoder from it's name.";
    return;
  }
  c->encoder = avcodec_alloc_context3(codec);
  if (c->encoder == nullptr) {
    c->status = BEAMCODER_ERROR_ALLOC_ENCODER;
    c->errorMsg = "Problem allocating encoder context.";
    return;
  }
  if ((ret = avcodec_open2(c->encoder, codec, nullptr))) {
    c->status = BEAMCODER_ERROR_ALLOC_ENCODER;
    c->errorMsg = avErrorMsg("Problem allocating encoder: ", ret);
    return;
  }
};

void encoderComplete(napi_env env, napi_status asyncStatus, void* data) {
  encoderCarrier* c = (encoderCarrier*) data;
  napi_value result, value;

  if (asyncStatus != napi_ok) {
    c->status = asyncStatus;
    c->errorMsg = "Encoder allocator failed to complete.";
  }
  REJECT_STATUS;

  c->status = napi_create_object(env, &result);
  REJECT_STATUS;

  c->status = napi_create_string_utf8(env, "encoder", NAPI_AUTO_LENGTH, &value);
  REJECT_STATUS;
  c->status = napi_set_named_property(env, result, "type", value);
  REJECT_STATUS;

  c->status = napi_create_external(env, c->encoder, encoderFinalizer, nullptr, &value);
  REJECT_STATUS;
  c->encoder = nullptr;
  c->status = napi_set_named_property(env, result, "_encoder", value);
  REJECT_STATUS;

  napi_status status;
  status = napi_resolve_deferred(env, c->_deferred, result);
  FLOATING_STATUS;

  tidyCarrier(env, c);
};

napi_value encoder(napi_env env, napi_callback_info info) {
  napi_value resourceName, promise, value;
  napi_valuetype type;
  encoderCarrier* c = new encoderCarrier;
  bool isArray, hasName, hasID;
  AVCodecID id;

  c->status = napi_create_promise(env, &c->_deferred, &promise);
  REJECT_RETURN;

  size_t argc = 1;
  napi_value args[1];

  c->status = napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  REJECT_RETURN;

  if (argc != 1) {
    REJECT_ERROR_RETURN("Encoder requires a single options object.",
      BEAMCODER_INVALID_ARGS);
  }

  c->status = napi_typeof(env, args[0], &type);
  REJECT_RETURN;
  c->status = napi_is_array(env, args[0], &isArray);
  REJECT_RETURN;
  if ((type != napi_object) || (isArray == true)) {
    REJECT_ERROR_RETURN("Encoder must be configured with a single parameter, an options object.",
      BEAMCODER_INVALID_ARGS);
  }

  c->status = napi_has_named_property(env, args[0], "name", &hasName);
  REJECT_RETURN;
  c->status = napi_has_named_property(env, args[0], "codecID", &hasID);
  REJECT_RETURN;

  if (!(hasName || hasID)) {
    REJECT_ERROR_RETURN("Decoder must be identified with a 'codecID' or a 'name'.",
      BEAMCODER_INVALID_ARGS);
  }

  if (hasName) {
    c->status = napi_get_named_property(env, args[0], "name", &value);
    REJECT_RETURN;
    c->codecName = (char*) malloc(sizeof(char) * (c->codecNameLen + 1));
    c->status = napi_get_value_string_utf8(env, value, c->codecName,
      64, &c->codecNameLen);
    REJECT_RETURN;
  }
  else {
    c->status = napi_get_named_property(env, args[0], "codecID", &value);
    REJECT_RETURN;
    c->status = napi_get_value_int32(env, value, (int32_t*) &id);
    REJECT_RETURN;
    c->codecName = (char*) avcodec_get_name(id);
    c->codecNameLen = strlen(c->codecName);
  }

  c->status = napi_create_string_utf8(env, "Encoder", NAPI_AUTO_LENGTH, &resourceName);
  REJECT_RETURN;
  c->status = napi_create_async_work(env, nullptr, resourceName, encoderExecute,
    encoderComplete, c, &c->_request);
  REJECT_RETURN;
  c->status = napi_queue_async_work(env, c->_request);
  REJECT_RETURN;

  return promise;
};

void encoderFinalizer(napi_env env, void* data, void* hint) {
  AVCodecContext* encoder = (AVCodecContext*) data;
  avcodec_close(encoder);
  avcodec_free_context(&encoder);
};

void encodeExecute(napi_env env, void* data) {
  encodeCarrier* c = (encodeCarrier*) data;
  HR_TIME_POINT encodeStart = NOW;

  c->totalTime = microTime(encodeStart);
};

void encodeComplete(napi_env env, napi_status asyncStatus, void* data) {
  encodeCarrier* c = (encodeCarrier*) data;
  napi_value result, value;

  if (asyncStatus != napi_ok) {
    c->status = asyncStatus;
    c->errorMsg = "Encode operation failed to complete.";
  }
  REJECT_STATUS;

  c->status = napi_create_object(env, &result);
  REJECT_STATUS;

  c->status = napi_create_int64(env, c->totalTime, &value);
  REJECT_STATUS;
  c->status = napi_set_named_property(env, result, "totalTime", value);
  REJECT_STATUS;

  napi_status status;
  status = napi_resolve_deferred(env, c->_deferred, result);
  FLOATING_STATUS;

  tidyCarrier(env, c);
};

napi_value encode(napi_env env, napi_callback_info info) {
  napi_value resourceName, promise, value;
  napi_valuetype type;
  encodeCarrier* c = new encodeCarrier;
  bool isArray;

  c->status = napi_create_promise(env, &c->_deferred, &promise);
  REJECT_RETURN;

  c->status = napi_create_string_utf8(env, "Encode", NAPI_AUTO_LENGTH, &resourceName);
  REJECT_RETURN;
  c->status = napi_create_async_work(env, nullptr, resourceName, encodeExecute,
    encodeComplete, c, &c->_request);
  REJECT_RETURN;
  c->status = napi_queue_async_work(env, c->_request);
  REJECT_RETURN;

  return promise;
};