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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Building blocks of {@link PageWithPositionComparator#sortKeyPrefix}, which derives a {@code long} from the value
 * of the first sort channel, such that rows with different prefixes are ordered like their prefixes.
 */
public final class SortKeyPrefix
{
    private SortKeyPrefix() {}

    public enum Source
    {
        /**
         * The value is a {@code long} ordered by signed comparison
         */
        LONG,
        /**
         * The value is a slice ordered by unsigned comparison of its bytes
         */
        BYTES,
    }

    /**
     * Returns how a prefix is derived from values of the type, or empty when the type is not supported.
     */
    public static Optional<Source> prefixSource(Type type)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) || type.equals(DATE)
                || type instanceof TimeType
                || (type instanceof DecimalType decimalType && decimalType.isShort())
                || (type instanceof TimestampType timestampType && timestampType.isShort())) {
            return Optional.of(Source.LONG);
        }
        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            return Optional.of(Source.BYTES);
        }
        return Optional.empty();
    }

    /**
     * Prefix of a null, which is ordered before or after every other prefix. A value can have the same prefix,
     * and then the rows are compared with the comparator.
     */
    public static long nullPrefix(SortOrder sortOrder)
    {
        if (sortOrder.isNullsFirst()) {
            return Long.MIN_VALUE;
        }
        return Long.MAX_VALUE;
    }

    /**
     * Reverses the order of prefixes
     */
    public static long descending(long prefix)
    {
        return ~prefix;
    }

    /**
     * Returns the first eight bytes of a value, padded with zeros, as a {@code long} with the same signed order
     * as the unsigned order of the bytes.
     */
    public static long bytesPrefix(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        Slice slice = valueBlock.getRawSlice();
        int offset = valueBlock.getRawSliceOffset(valuePosition);
        int length = valueBlock.getSliceLength(valuePosition);

        long bytes;
        if (length >= Long.BYTES) {
            bytes = Long.reverseBytes(slice.getLong(offset));
        }
        else {
            bytes = 0;
            for (int index = 0; index < length; index++) {
                bytes |= (slice.getByte(offset + index) & 0xFFL) << (Long.SIZE - Byte.SIZE * (index + 1));
            }
        }
        return bytes ^ Long.MIN_VALUE;
    }
}
