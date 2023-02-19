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
package io.trino.plugin.hive;

import io.trino.orc.metadata.CompressionKind;
import org.apache.avro.file.DataFileConstants;
import org.apache.parquet.format.CompressionCodec;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public enum HiveCompressionCodec
{
    NONE(null, CompressionKind.NONE, CompressionCodec.UNCOMPRESSED, DataFileConstants.NULL_CODEC),
    SNAPPY(io.trino.hive.formats.compression.CompressionKind.SNAPPY, CompressionKind.SNAPPY, CompressionCodec.SNAPPY, DataFileConstants.SNAPPY_CODEC),
    LZ4(io.trino.hive.formats.compression.CompressionKind.LZ4, CompressionKind.LZ4, CompressionCodec.LZ4, null),
    ZSTD(io.trino.hive.formats.compression.CompressionKind.ZSTD, CompressionKind.ZSTD, CompressionCodec.ZSTD, DataFileConstants.ZSTANDARD_CODEC),
    // Using DEFLATE for GZIP for Avro for now so Avro files can be written in default configuration
    // TODO(https://github.com/trinodb/trino/issues/12580) change GZIP to be unsupported for Avro when we change Trino default compression to be storage format aware
    GZIP(io.trino.hive.formats.compression.CompressionKind.GZIP, CompressionKind.ZLIB, CompressionCodec.GZIP, DataFileConstants.DEFLATE_CODEC);

    private final Optional<io.trino.hive.formats.compression.CompressionKind> hiveCompressionKind;
    private final CompressionKind orcCompressionKind;
    private final CompressionCodec parquetCompressionCodec;

    private final Optional<String> avroCompressionCodec;

    HiveCompressionCodec(
            io.trino.hive.formats.compression.CompressionKind hiveCompressionKind,
            CompressionKind orcCompressionKind,
            CompressionCodec parquetCompressionCodec,
            String avroCompressionCodec)
    {
        this.hiveCompressionKind = Optional.ofNullable(hiveCompressionKind);
        this.orcCompressionKind = requireNonNull(orcCompressionKind, "orcCompressionKind is null");
        this.parquetCompressionCodec = requireNonNull(parquetCompressionCodec, "parquetCompressionCodec is null");
        this.avroCompressionCodec = Optional.ofNullable(avroCompressionCodec);
    }

    public Optional<io.trino.hive.formats.compression.CompressionKind> getHiveCompressionKind()
    {
        return hiveCompressionKind;
    }

    public CompressionKind getOrcCompressionKind()
    {
        return orcCompressionKind;
    }

    public CompressionCodec getParquetCompressionCodec()
    {
        return parquetCompressionCodec;
    }

    public Optional<String> getAvroCompressionCodec()
    {
        return avroCompressionCodec;
    }
}
