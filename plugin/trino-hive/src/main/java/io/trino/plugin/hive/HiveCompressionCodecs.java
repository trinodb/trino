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

import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

public final class HiveCompressionCodecs
{
    private HiveCompressionCodecs() {}

    public static HiveCompressionCodec selectCompressionCodec(ConnectorSession session, StorageFormat storageFormat)
    {
        HiveCompressionOption compressionOption = HiveSessionProperties.getCompressionCodec(session);
        return HiveStorageFormat.getHiveStorageFormat(storageFormat)
                .map(format -> selectCompressionCodec(compressionOption, format))
                .orElseGet(() -> selectCompressionCodecForUnknownStorageFormat(compressionOption));
    }

    public static HiveCompressionCodec selectCompressionCodec(ConnectorSession session, HiveStorageFormat storageFormat)
    {
        return selectCompressionCodec(HiveSessionProperties.getCompressionCodec(session), storageFormat);
    }

    public static HiveCompressionCodec selectCompressionCodec(HiveCompressionOption compressionOption, HiveStorageFormat storageFormat)
    {
        HiveCompressionCodec selectedCodec = selectCompressionCodec(compressionOption);

        // perform codec vs format validation
        if (storageFormat == HiveStorageFormat.AVRO && selectedCodec.getAvroCompressionCodec().isEmpty()) {
            throw new TrinoException(HiveErrorCode.HIVE_UNSUPPORTED_FORMAT, "Compression codec " + selectedCodec + " not supported for " + storageFormat);
        }

        return selectedCodec;
    }

    private static HiveCompressionCodec selectCompressionCodec(HiveCompressionOption compressionOption)
    {
        return switch (compressionOption) {
            case NONE -> HiveCompressionCodec.NONE;
            case SNAPPY -> HiveCompressionCodec.SNAPPY;
            case LZ4 -> HiveCompressionCodec.LZ4;
            case ZSTD -> HiveCompressionCodec.ZSTD;
            case GZIP -> HiveCompressionCodec.GZIP;
        };
    }

    private static HiveCompressionCodec selectCompressionCodecForUnknownStorageFormat(HiveCompressionOption compressionOption)
    {
        return switch (compressionOption) {
            case NONE -> HiveCompressionCodec.NONE;
            case SNAPPY -> HiveCompressionCodec.SNAPPY;
            case LZ4 -> HiveCompressionCodec.LZ4;
            case ZSTD -> HiveCompressionCodec.ZSTD;
            case GZIP -> HiveCompressionCodec.GZIP;
        };
    }
}
