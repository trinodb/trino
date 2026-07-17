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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.reader.flat.NullsDecoders.createNullsDecoder;

public interface FlatDefinitionLevelDecoder
{
    void init(Slice input);

    /// Consume `length` definition levels and return the number of non-null values encountered.
    ///
    /// If the return value is less than `length`, the requested range in `values` contains the decoded validity bitmap.
    /// If the return value is equal to `length`, every requested position is valid, but the requested range in `values`
    /// is not guaranteed to be populated. The caller must materialize the range before using it as a validity bitmap.
    ///
    /// The requested range in `values` must initially contain only unset bits. The array uses the
    /// [io.trino.spi.block.Bitmap] encoding.
    int readNext(long[] values, int offset, int length);

    /**
     * Skip 'length' values and return the number of non-nulls encountered
     */
    int skip(int length);

    interface DefinitionLevelDecoderProvider
    {
        FlatDefinitionLevelDecoder create(int maxDefinitionLevel);
    }

    static FlatDefinitionLevelDecoder getFlatDefinitionLevelDecoder(int maxDefinitionLevel, boolean vectorizedDecodingEnabled)
    {
        checkArgument(maxDefinitionLevel >= 0 && maxDefinitionLevel <= 1, "Invalid max definition level: %s", maxDefinitionLevel);
        if (maxDefinitionLevel == 0) {
            return new ZeroDefinitionLevelDecoder();
        }
        return createNullsDecoder(vectorizedDecodingEnabled);
    }
}
