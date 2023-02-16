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

import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VariableWidthType;
import io.trino.type.BlockTypeOperators;

import static java.util.Objects.requireNonNull;

public class PositionsAppenderFactory
{
    private final BlockTypeOperators blockTypeOperators;

    public PositionsAppenderFactory(BlockTypeOperators blockTypeOperators)
    {
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    public PositionsAppender create(Type type, int expectedPositions, long maxPageSizeInBytes)
    {
        if (!type.isComparable()) {
            return new UnnestingPositionsAppender(createPrimitiveAppender(type, expectedPositions, maxPageSizeInBytes));
        }

        return new UnnestingPositionsAppender(
                new RleAwarePositionsAppender(
                        blockTypeOperators.getDistinctFromOperator(type),
                        createPrimitiveAppender(type, expectedPositions, maxPageSizeInBytes)));
    }

    private PositionsAppender createPrimitiveAppender(Type type, int expectedPositions, long maxPageSizeInBytes)
    {
        if (type instanceof FixedWidthType) {
            switch (((FixedWidthType) type).getFixedSize()) {
                case Byte.BYTES:
                    return new BytePositionsAppender(expectedPositions);
                case Short.BYTES:
                    return new ShortPositionsAppender(expectedPositions);
                case Integer.BYTES:
                    return new IntPositionsAppender(expectedPositions);
                case Long.BYTES:
                    return new LongPositionsAppender(expectedPositions);
                case Int96ArrayBlock.INT96_BYTES:
                    return new Int96PositionsAppender(expectedPositions);
                case Int128ArrayBlock.INT128_BYTES:
                    return new Int128PositionsAppender(expectedPositions);
                default:
                    // size not supported directly, fallback to the generic appender
            }
        }
        else if (type instanceof VariableWidthType) {
            return new SlicePositionsAppender(expectedPositions, maxPageSizeInBytes);
        }
        else if (type instanceof RowType) {
            return RowPositionsAppender.createRowAppender(this, (RowType) type, expectedPositions, maxPageSizeInBytes);
        }

        return new TypedPositionsAppender(type, expectedPositions);
    }
}
