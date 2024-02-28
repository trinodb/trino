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
package io.trino.operator;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

public interface FlatHashStrategy
{
    boolean isAnyVariableWidth();

    int getTotalFlatFixedLength();

    int getTotalVariableWidth(Block[] blocks, int position);

    void readFlat(byte[] fixedChunk, int fixedOffset, byte[] variableChunk, BlockBuilder[] blockBuilders);

    void writeFlat(Block[] blocks, int position, byte[] fixedChunk, int fixedOffset, byte[] variableChunk, int variableOffset);

    boolean valueNotDistinctFrom(
            byte[] leftFixedChunk,
            int leftFixedOffset,
            byte[] leftVariableChunk,
            Block[] rightBlocks,
            int rightPosition);

    long hash(Block[] blocks, int position);

    long hash(byte[] fixedChunk, int fixedOffset, byte[] variableChunk);

    void hashBlocksBatched(Block[] blocks, long[] hashes, int offset, int length);
}
