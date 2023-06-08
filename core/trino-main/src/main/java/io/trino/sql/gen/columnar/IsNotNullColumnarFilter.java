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

import static java.lang.System.arraycopy;

public final class IsNotNullColumnarFilter
        implements ColumnarFilter
{
    private final InputChannels inputChannels;

    public IsNotNullColumnarFilter(InputReferenceExpression inputReferenceExpression)
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
        int nonNullPositionsCount = 0;
        int position;
        if (block.mayHaveNull()) {
            for (position = offset; position < offset + size; position++) {
                outputPositions[nonNullPositionsCount] = position;
                nonNullPositionsCount += block.isNull(position) ? 0 : 1;
            }
        }
        else {
            for (position = offset; position < offset + size; position++) {
                outputPositions[nonNullPositionsCount++] = position;
            }
        }

        return nonNullPositionsCount;
    }

    @Override
    public int filterPositionsList(int[] outputPositions, int[] activePositions, int offset, int size, Page page)
    {
        Block block = page.getBlock(0);
        int nonNullPositionsCount = 0;
        if (block.mayHaveNull()) {
            for (int index = offset; index < offset + size; index++) {
                int position = activePositions[index];
                outputPositions[nonNullPositionsCount] = position;
                nonNullPositionsCount += block.isNull(position) ? 0 : 1;
            }
        }
        else {
            arraycopy(activePositions, offset, outputPositions, 0, size);
            nonNullPositionsCount = size;
        }

        return nonNullPositionsCount;
    }
}
