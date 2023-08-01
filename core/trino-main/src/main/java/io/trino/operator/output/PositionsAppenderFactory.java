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
package io.trino.operator.output;

import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import java.util.Optional;

import static io.trino.operator.output.PositionsAppenderUtil.MAX_ARRAY_SIZE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class PositionsAppenderFactory
{
    private final BlockTypeOperators blockTypeOperators;
    private static final int EXPECTED_VARIABLE_WIDTH_BYTES_PER_ENTRY = 32;

    public PositionsAppenderFactory(BlockTypeOperators blockTypeOperators)
    {
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    public UnnestingPositionsAppender create(Type type, int expectedPositions, long maxPageSizeInBytes)
    {
        Optional<BlockPositionIsDistinctFrom> distinctFromOperator = Optional.empty();
        if (type.isComparable()) {
            distinctFromOperator = Optional.of(blockTypeOperators.getDistinctFromOperator(type));
        }
        return new UnnestingPositionsAppender(createPrimitiveAppender(type, expectedPositions, maxPageSizeInBytes), distinctFromOperator);
    }

    private PositionsAppender createPrimitiveAppender(Type type, int expectedPositions, long maxPageSizeInBytes)
    {
        if (type.getValueBlockType() == RowBlock.class) {
            return RowPositionsAppender.createRowAppender(this, (RowType) type, expectedPositions, maxPageSizeInBytes);
        }
        if (type.getValueBlockType() == VariableWidthBlock.class) {
            // it is guaranteed Math.min will not overflow; safe to cast
            int expectedBytes = (int) min((long) expectedPositions * EXPECTED_VARIABLE_WIDTH_BYTES_PER_ENTRY, maxPageSizeInBytes);
            expectedBytes = min(expectedBytes, MAX_ARRAY_SIZE);
            return new TypedPositionsAppender(new VariableWidthBlockBuilder(null, expectedPositions, expectedBytes));
        }
        return new TypedPositionsAppender(type, expectedPositions);
    }
}
