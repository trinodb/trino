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
package io.trino.hive.formats.encodings.text;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.encodings.ColumnEncoding;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

public interface TextColumnEncoding
        extends ColumnEncoding
{
    void encodeValueInto(Block block, int position, SliceOutput output)
            throws FileCorruptionException;

    void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException;
}
