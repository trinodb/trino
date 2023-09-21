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

import static org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.file.CodecFactory.DEFAULT_ZSTANDARD_BUFFERPOOL;
import static org.apache.avro.file.CodecFactory.DEFAULT_ZSTANDARD_LEVEL;
import static org.apache.avro.file.CodecFactory.deflateCodec;
import static org.apache.avro.file.CodecFactory.nullCodec;
import static org.apache.avro.file.CodecFactory.snappyCodec;
import static org.apache.avro.file.CodecFactory.zstandardCodec;

/**
 * Inner join between Trino plugin supported codec types and Avro spec supported codec types
 * Spec list: <a href="https://avro.apache.org/docs/1.11.1/specification/#required-codecs">required</a> and
 * <a href="https://avro.apache.org/docs/1.11.1/specification/#optional-codecs">optionals</a>
 */
public enum AvroCompressionKind
{
    // Avro nulls out codec's if it can't find the proper library locally
    NULL(DataFileConstants.NULL_CODEC, nullCodec() != null),
    DEFLATE(DataFileConstants.DEFLATE_CODEC, deflateCodec(DEFAULT_DEFLATE_LEVEL) != null),
    SNAPPY(DataFileConstants.SNAPPY_CODEC, snappyCodec() != null),
    ZSTANDARD(DataFileConstants.ZSTANDARD_CODEC, zstandardCodec(DEFAULT_ZSTANDARD_LEVEL, DEFAULT_ZSTANDARD_BUFFERPOOL) != null);

    private final String codecString;
    private final boolean supportedLocally;

    AvroCompressionKind(String codecString, boolean supportedLocally)
    {
        this.codecString = codecString;
        this.supportedLocally = supportedLocally;
    }

    @Override
    public String toString()
    {
        return codecString;
    }

    public CodecFactory getCodecFactory()
    {
        return CodecFactory.fromString(codecString);
    }

    public boolean isSupportedLocally()
    {
        return supportedLocally;
    }
}
