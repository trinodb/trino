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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.trino.parquet.ParquetCompressionUtils;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;

public class Decompressor
{
    private final boolean isNativeZstdDecompressorEnabled;
    private final boolean isNativeSnappyDecompressorEnabled;

    public Decompressor(ParquetReaderOptions options)
    {
        this.isNativeZstdDecompressorEnabled = options.isNativeZstdDecompressorEnabled();
        this.isNativeSnappyDecompressorEnabled = options.isNativeSnappyDecompressorEnabled();
    }

    public Slice decompress(ParquetDataSourceId dataSourceId, CompressionCodec codec, Slice input, int uncompressedSize)
            throws IOException
    {
        return ParquetCompressionUtils.decompress(dataSourceId, codec, input, uncompressedSize, isNativeZstdDecompressorEnabled, isNativeSnappyDecompressorEnabled);
    }
}
