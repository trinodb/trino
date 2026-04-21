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

import io.trino.operator.project.InputChannels;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

public final class IsNotNullColumnarFilter
        implements ColumnarFilter
{
    private final InputChannels inputChannels;

    public static Class<? extends ColumnarFilter> createIsNotNullColumnarFilter(IsNull isNull)
    {
        checkArgument(isNull.value() != null, "isNull %s should have a value", isNull);
        if (!(isNull.value() instanceof Reference)) {
            throw new UnsupportedOperationException("NOT(IS_NULL) columnar evaluation is supported only for Reference");
        }
        return IsNotNullColumnarFilter.class;
    }

    public IsNotNullColumnarFilter(InputChannels inputChannels)
    {
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage page)
    {
        ValueBlock block = (ValueBlock) page.getBlock(0);
        int nonNullPositionsCount = 0;
        int position;
        if (block.mayHaveNull()) {
            Optional<ByteArrayBlock> isNullsBlock = block.getNulls();
            if (isNullsBlock.isPresent()) {
                byte[] isNull = isNullsBlock.get().getRawValues();
                int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                for (position = offset; position < offset + size; position++) {
                    outputPositions[nonNullPositionsCount] = position;
                    nonNullPositionsCount += isNull[isNullOffset + position] == 0 ? 1 : 0;
                }
                return nonNullPositionsCount;
            }
        }

        for (position = offset; position < offset + size; position++) {
            outputPositions[nonNullPositionsCount++] = position;
        }
        return nonNullPositionsCount;
    }

    @Override
    public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage page)
    {
        ValueBlock block = (ValueBlock) page.getBlock(0);
        if (block.mayHaveNull()) {
            Optional<ByteArrayBlock> isNullsBlock = block.getNulls();
            if (isNullsBlock.isPresent()) {
                int nonNullPositionsCount = 0;
                byte[] isNull = isNullsBlock.get().getRawValues();
                int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                for (int index = offset; index < offset + size; index++) {
                    int position = activePositions[index];
                    outputPositions[nonNullPositionsCount] = position;
                    nonNullPositionsCount += isNull[isNullOffset + position] == 0 ? 1 : 0;
                }
                return nonNullPositionsCount;
            }
        }

        arraycopy(activePositions, offset, outputPositions, 0, size);
        return size;
    }
}
