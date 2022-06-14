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
package io.trino.plugin.iceberg;

import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.Type.Range;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public final class TrinoTypes
{
    private TrinoTypes() {}

    /**
     * Returns the maximum value that compares less than {@code value}.
     * <p>
     * The type of the value must match {@link Type#getJavaType}.
     *
     * @throws IllegalStateException if this type is not {@link Type#isOrderable() orderable}
     */
    public static Optional<Object> getPreviousValue(Type type, Object value)
    {
        if (!type.isOrderable()) {
            throw new IllegalArgumentException("Type is not orderable: " + type);
        }
        requireNonNull(value, "value is null");

        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            Range typeRange = type.getRange().orElseThrow();
            return getAdjacentValue((long) typeRange.getMin(), (long) typeRange.getMax(), (long) value, Direction.PREV);
        }

        if (type == DATE) {
            // TODO update the code here when type implements getRange
            verify(type.getRange().isEmpty(), "Type %s unexpectedly returned a range", type);
            return getAdjacentValue(Integer.MIN_VALUE, Integer.MAX_VALUE, (long) value, Direction.PREV);
        }

        if (type instanceof TimestampType) {
            // Iceberg supports only timestamp(6)
            checkArgument(((TimestampType) type).getPrecision() == 6, "Unexpected type: %s", type);
            // TODO update the code here when type implements getRange
            verify(type.getRange().isEmpty(), "Type %s unexpectedly returned a range", type);
            return getAdjacentValue(Long.MIN_VALUE, Long.MAX_VALUE, (long) value, Direction.PREV);
        }

        return Optional.empty();
    }

    /**
     * Returns the minimum value that compares greater than {@code value}.
     * <p>
     * The type of the value must match {@link Type#getJavaType}.
     *
     * @throws IllegalStateException if this type is not {@link Type#isOrderable() orderable}
     */
    public static Optional<Object> getNextValue(Type type, Object value)
    {
        if (!type.isOrderable()) {
            throw new IllegalArgumentException("Type is not orderable: " + type);
        }
        requireNonNull(value, "value is null");

        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            Range typeRange = type.getRange().orElseThrow();
            return getAdjacentValue((long) typeRange.getMin(), (long) typeRange.getMax(), (long) value, Direction.NEXT);
        }

        if (type == DATE) {
            // TODO update the code here when type implements getRange
            verify(type.getRange().isEmpty(), "Type %s unexpectedly returned a range", type);
            return getAdjacentValue(Integer.MIN_VALUE, Integer.MAX_VALUE, (long) value, Direction.NEXT);
        }

        if (type instanceof TimestampType) {
            // Iceberg supports only timestamp(6)
            checkArgument(((TimestampType) type).getPrecision() == 6, "Unexpected type: %s", type);
            // TODO update the code here when type implements getRange
            verify(type.getRange().isEmpty(), "Type %s unexpectedly returned a range", type);
            return getAdjacentValue(Long.MIN_VALUE, Long.MAX_VALUE, (long) value, Direction.NEXT);
        }

        return Optional.empty();
    }

    private static Optional<Object> getAdjacentValue(long min, long max, long value, Direction direction)
    {
        switch (direction) {
            case PREV:
                if (value == min) {
                    return Optional.empty();
                }
                return Optional.of(value - 1);

            case NEXT:
                if (value == max) {
                    return Optional.empty();
                }
                return Optional.of(value + 1);
        }
        throw new UnsupportedOperationException("Unsupported direction: " + direction);
    }

    private enum Direction
    {
        PREV,
        NEXT
    }
}
