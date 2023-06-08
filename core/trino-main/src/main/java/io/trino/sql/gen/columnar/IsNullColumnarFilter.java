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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.trino.operator.project.InputChannels;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.sql.relational.InputReferenceExpression;

import java.util.List;

public final class IsNullColumnarFilter
        implements ColumnarFilter
{
    private final InputChannels inputChannels;

    public IsNullColumnarFilter(InputReferenceExpression inputReferenceExpression)
    {
        List<Integer> channels = ImmutableList.of(inputReferenceExpression.getField());
        this.inputChannels = new InputChannels(channels, channels);
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public int filterPositionsRange(int[] outputPositions, int offset, int size, Page page)
    {
        Block block = page.getBlock(0);
        int nullPositionsCount = 0;
        if (!block.mayHaveNull()) {
            return 0;
        }

        for (int position = offset; position < offset + size; position++) {
            outputPositions[nullPositionsCount] = position;
            nullPositionsCount += block.isNull(position) ? 1 : 0;
        }
        return nullPositionsCount;
    }

    @Override
    public int filterPositionsList(int[] outputPositions, int[] activePositions, int offset, int size, Page page)
    {
        Block block = page.getBlock(0);
        int nullPositionsCount = 0;
        if (!block.mayHaveNull()) {
            return 0;
        }

        for (int index = offset; index < offset + size; index++) {
            int position = activePositions[index];
            outputPositions[nullPositionsCount] = position;
            nullPositionsCount += block.isNull(position) ? 1 : 0;
        }
        return nullPositionsCount;
    }
}
