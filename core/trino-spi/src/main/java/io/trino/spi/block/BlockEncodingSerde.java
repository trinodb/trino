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
package io.trino.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.type.Type;

public interface BlockEncodingSerde
{
    /**
     * Read a block encoding from the input.
     */
    Block readBlock(SliceInput input);

    /**
     * Write a blockEncoding to the output.
     */
    void writeBlock(SliceOutput output, Block block);

    /**
     * Estimate the size of the block when serialized to the on-the-wire representation.
     */
    default long estimatedWriteSize(Block block)
    {
        return block.getSizeInBytes();
    }

    /**
     * Reads a type from the input.
     */
    Type readType(SliceInput sliceInput);

    /**
     * Write a type to the output.
     */
    void writeType(SliceOutput sliceOutput, Type type);
}
