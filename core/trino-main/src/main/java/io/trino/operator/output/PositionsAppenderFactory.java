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

import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PositionsAppenderFactory
{
    private final BlockTypeOperators blockTypeOperators;

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
        if (type.getValueBlockType() == ByteArrayBlock.class) {
            return new BytePositionsAppender(expectedPositions);
        }
        if (type.getValueBlockType() == ShortArrayBlock.class) {
            return new ShortPositionsAppender(expectedPositions);
        }
        if (type.getValueBlockType() == IntArrayBlock.class) {
            return new IntPositionsAppender(expectedPositions);
        }
        if (type.getValueBlockType() == LongArrayBlock.class) {
            return new LongPositionsAppender(expectedPositions);
        }
        if (type.getValueBlockType() == Fixed12Block.class) {
            return new Fixed12PositionsAppender(expectedPositions);
        }
        if (type.getValueBlockType() == Int128ArrayBlock.class) {
            return new Int128PositionsAppender(expectedPositions);
        }
        if (type.getValueBlockType() == VariableWidthBlock.class) {
            return new SlicePositionsAppender(expectedPositions, maxPageSizeInBytes);
        }
        if (type.getValueBlockType() == RowBlock.class) {
            return RowPositionsAppender.createRowAppender(this, (RowType) type, expectedPositions, maxPageSizeInBytes);
        }
        return new TypedPositionsAppender(type, expectedPositions);
    }
}
