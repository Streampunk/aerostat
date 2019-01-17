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

#include "format.h"

napi_value getIFormatName(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env, iformat->name, NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatName(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env, oformat->name, NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatLongName(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env, iformat->long_name, NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatLongName(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env, oformat->long_name, NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatMimeType(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (iformat->mime_type != nullptr) ? iformat->mime_type : "", NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatMimeType(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (oformat->mime_type != nullptr) ? oformat->mime_type : "", NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatExtensions(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (iformat->extensions != nullptr) ? iformat->extensions : "", NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatExtensions(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (oformat->extensions != nullptr) ? oformat->extensions : "", NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_status getIOFormatFlags(napi_env env, int flags, napi_value* result, bool isInput) {
  napi_status status;
  napi_value value;

  printf("IsInput %i flags %i\n", isInput, flags);

  status = napi_create_object(env, &value);
  PASS_STATUS;
  status = beam_set_bool(env, value, "NOFILE", flags & AVFMT_NOFILE); // O I
  PASS_STATUS;
  status = beam_set_bool(env, value, "NEEDNUMBER", flags & AVFMT_NEEDNUMBER); // O I
  PASS_STATUS;
  if (isInput) {
    status = beam_set_bool(env, value, "SHOW_IDS", flags & AVFMT_SHOW_IDS); // I
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "GLOBALHEADER", flags & AVFMT_GLOBALHEADER); // O
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "NOTIMESTAMPS", flags & AVFMT_NOTIMESTAMPS); // O
    PASS_STATUS;
  }
  if (isInput) {
    status = beam_set_bool(env, value, "GENERIC_INDEX", flags & AVFMT_GENERIC_INDEX); // I
    PASS_STATUS;
  }
  if (isInput) {
    status = beam_set_bool(env, value, "TS_DISCONT", flags & AVFMT_TS_DISCONT); // I
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "VARIABLE_FPS", flags & AVFMT_VARIABLE_FPS); // O
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "NODIMENSIONS", flags & AVFMT_NODIMENSIONS); // O
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "NOSTREAMS", flags & AVFMT_NOSTREAMS); // O
    PASS_STATUS;
  }
  if (isInput) {
    status = beam_set_bool(env, value, "NOBINSEARCH", flags & AVFMT_NOBINSEARCH); // I
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "NODIMENSIONS", flags & AVFMT_NODIMENSIONS); // O
    PASS_STATUS;
  }
  if (isInput) {
    status = beam_set_bool(env, value, "NOGENSEARCH", flags & AVFMT_NOGENSEARCH); // I
    PASS_STATUS;
  }
  if (isInput) {
    status = beam_set_bool(env, value, "NO_BYTE_SEEK", flags & AVFMT_NO_BYTE_SEEK); // I
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "ALLOW_FLUSH", flags & AVFMT_ALLOW_FLUSH); // O
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "TS_NONSTRICT", flags & AVFMT_TS_NONSTRICT); // O
    PASS_STATUS;
  }
  if (!isInput) {
    status = beam_set_bool(env, value, "TS_NEGATIVE", flags & AVFMT_TS_NEGATIVE); // O
    PASS_STATUS;
  }
  if (isInput) {
    status = beam_set_bool(env, value, "SEEK_TO_PTS", flags & AVFMT_SEEK_TO_PTS); // I
    PASS_STATUS;
  }

  *result = value;
  return napi_ok;
}

napi_value getOFormatFlags(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = getIOFormatFlags(env, oformat->flags, &result, false);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatFlags(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = getIOFormatFlags(env, iformat->flags, &result, true);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatRawCodecID(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_int32(env, iformat->raw_codec_id, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatPrivDataSize(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_int32(env, oformat->priv_data_size, &result);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatPrivDataSize(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_int32(env, iformat->priv_data_size, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatPrivClass(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (oformat->priv_class != nullptr) ? oformat->priv_class->class_name : "",
    NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getIFormatPrivClass(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVInputFormat* iformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &iformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (iformat->priv_class != nullptr) ? iformat->priv_class->class_name : "",
    NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatAudioCodec(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (char*) avcodec_get_name(oformat->audio_codec),
    NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatVideoCodec(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (char*) avcodec_get_name(oformat->video_codec),
    NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value getOFormatSubtitleCodec(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result;
  AVOutputFormat* oformat;

  status = napi_get_cb_info(env, info, nullptr, nullptr, nullptr, (void**) &oformat);
  CHECK_STATUS;

  status = napi_create_string_utf8(env,
    (char*) avcodec_get_name(oformat->subtitle_codec),
    NAPI_AUTO_LENGTH, &result);
  CHECK_STATUS;

  return result;
}

napi_value muxers(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result, muxer;
  void* opaque = nullptr;
  const AVOutputFormat* oformat = nullptr;

  status = napi_create_object(env, &result);
  CHECK_STATUS;

  oformat = av_muxer_iterate(&opaque);
  while ( oformat != nullptr ) {
    status = fromAVOutputFormat(env, oformat, &muxer);
    CHECK_STATUS;
    status = napi_set_named_property(env, result, oformat->name, muxer);
    CHECK_STATUS;

    oformat = av_muxer_iterate(&opaque);
  }

  return result;
}

napi_value demuxers(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value result, demuxer;
  void* opaque = nullptr;
  const AVInputFormat* iformat = nullptr;

  status = napi_create_object(env, &result);
  CHECK_STATUS;

  iformat = av_demuxer_iterate(&opaque);
  while ( iformat != nullptr ) {
    status = fromAVInputFormat(env, iformat, &demuxer);
    CHECK_STATUS;
    status = napi_set_named_property(env, result, iformat->name, demuxer);
    CHECK_STATUS;

    iformat = av_demuxer_iterate(&opaque);
  }

  return result;
}

napi_status fromAVOutputFormat(napi_env env,
    const AVOutputFormat* oformat, napi_value* result) {
  napi_status status;
  napi_value jsOFormat, extOFormat, typeName;

  status = napi_create_object(env, &jsOFormat);
  PASS_STATUS;
  status = napi_create_string_utf8(env, "OutputFormat", NAPI_AUTO_LENGTH, &typeName);
  PASS_STATUS;
  status = napi_create_external(env, (void*) oformat, nullptr, nullptr, &extOFormat);
  PASS_STATUS;

  // TODO - codec_tag is a bit hard
  napi_property_descriptor desc[] = {
    { "type", nullptr, nullptr, nullptr, nullptr, typeName, napi_enumerable, nullptr },
    { "name", nullptr, nullptr, getOFormatName, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "long_name", nullptr, nullptr, getOFormatLongName, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "mime_type", nullptr, nullptr, getOFormatMimeType, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "extensions", nullptr, nullptr, getOFormatExtensions, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "flags", nullptr, nullptr, getOFormatFlags, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "priv_data_size", nullptr, nullptr, getOFormatPrivDataSize, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "priv_class", nullptr, nullptr, getOFormatPrivClass, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "audio_codec", nullptr, nullptr, getOFormatAudioCodec, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "video_codec", nullptr, nullptr, getOFormatVideoCodec, nullptr,
      nullptr, napi_enumerable, (void*) oformat }, // 10
    { "subtitle_codec", nullptr, nullptr, getOFormatSubtitleCodec, nullptr,
      nullptr, napi_enumerable, (void*) oformat },
    { "_oformat", nullptr, nullptr, nullptr, nullptr, extOFormat, napi_default, nullptr }
  };
  status = napi_define_properties(env, jsOFormat, 12, desc);
  PASS_STATUS;

  *result = jsOFormat;
  return napi_ok;
}

napi_status fromAVInputFormat(napi_env env,
    const AVInputFormat* iformat, napi_value* result) {
  napi_status status;
  napi_value jsIFormat, extIFormat, typeName;

  status = napi_create_object(env, &jsIFormat);
  PASS_STATUS;
  status = napi_create_string_utf8(env, "InputFormat", NAPI_AUTO_LENGTH, &typeName);
  PASS_STATUS;
  status = napi_create_external(env, (void*) iformat, nullptr, nullptr, &extIFormat);
  PASS_STATUS;

  // TODO - codec_tag is a bit hard
  napi_property_descriptor desc[] = {
    { "type", nullptr, nullptr, nullptr, nullptr, typeName, napi_enumerable, nullptr },
    { "name", nullptr, nullptr, getIFormatName, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "long_name", nullptr, nullptr, getIFormatLongName, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "mime_type", nullptr, nullptr, getIFormatMimeType, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "extensions", nullptr, nullptr, getIFormatExtensions, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "flags", nullptr, nullptr, getIFormatFlags, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "raw_codec_id", nullptr, nullptr, getIFormatRawCodecID, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "priv_data_size", nullptr, nullptr, getIFormatPrivDataSize, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "priv_class", nullptr, nullptr, getOFormatPrivClass, nullptr,
      nullptr, napi_enumerable, (void*) iformat },
    { "_iformat", nullptr, nullptr, nullptr, nullptr, extIFormat, napi_default, nullptr } // 10
  };
  status = napi_define_properties(env, jsIFormat, 10, desc);
  PASS_STATUS;

  *result = jsIFormat;
  return napi_ok;
}
