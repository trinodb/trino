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
package io.trino.operator.unnest;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public class ReplicatedBlockBuilder
{
    private Block source;
    private int sourcePosition;

    public void resetInputBlock(Block source)
    {
        this.source = requireNonNull(source, "source is null");
        sourcePosition = 0;
    }

    public Block buildOutputBlock(int[] outputEntriesPerPosition, int offset, int inputBatchSize, int outputRowCount)
    {
        int[] ids = new int[outputRowCount];

        int fromPosition = 0;
        for (int i = 0; i < inputBatchSize; i++) {
            int toPosition = fromPosition + outputEntriesPerPosition[offset + i];
            Arrays.fill(ids, fromPosition, toPosition, sourcePosition++);
            fromPosition = toPosition;
        }

        return DictionaryBlock.create(outputRowCount, source, ids);
    }
}
