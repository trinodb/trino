/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.hive.formats.avro;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

import static io.trino.hive.formats.avro.SnappyAirCompressorCodec.SNAPPY_CODEC_FACTORY;
import static io.trino.hive.formats.avro.ZstdAirCompressorCodec.ZSTD_CODEC_FACTORY;
import static org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.file.CodecFactory.deflateCodec;
import static org.apache.avro.file.CodecFactory.nullCodec;

/**
 * Inner join between Trino plugin supported codec types and Avro spec supported codec types
 * Spec list: <a href="https://avro.apache.org/docs/1.11.1/specification/#required-codecs">required</a> and
 * <a href="https://avro.apache.org/docs/1.11.1/specification/#optional-codecs">optionals</a>
 */
public enum AvroCompressionKind
{
    NULL(DataFileConstants.NULL_CODEC, nullCodec()),
    DEFLATE(DataFileConstants.DEFLATE_CODEC, deflateCodec(DEFAULT_DEFLATE_LEVEL)),
    SNAPPY(DataFileConstants.SNAPPY_CODEC, SNAPPY_CODEC_FACTORY),
    ZSTANDARD(DataFileConstants.ZSTANDARD_CODEC, ZSTD_CODEC_FACTORY);

    private final String codecString;
    private final CodecFactory trinoCodecFactory;

    static {
        for (AvroCompressionKind kind : AvroCompressionKind.values()) {
            CodecFactory.addCodec(kind.codecString, kind.getTrinoCodecFactory());
        }
    }

    AvroCompressionKind(String codecString, CodecFactory trinoCodecFactory)
    {
        this.codecString = codecString;
        this.trinoCodecFactory = trinoCodecFactory;
    }

    @Override
    public String toString()
    {
        return codecString;
    }

    public CodecFactory getTrinoCodecFactory()
    {
        return trinoCodecFactory;
    }
}
