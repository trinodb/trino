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
package io.trino.plugin.hive.coercions;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.VarcharType;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_TIMESTAMP_COERCION;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.DateType.DATE;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public final class DateCoercer
{
    private static final long START_OF_MODERN_ERA_DAYS = java.time.LocalDate.of(1900, 1, 1).toEpochDay();

    private DateCoercer() {}

    public static class VarcharToDateCoercer
            extends TypeCoercer<VarcharType, DateType>
    {
        public VarcharToDateCoercer(VarcharType fromType, DateType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            String value = fromType.getSlice(block, position).toStringUtf8();
            try {
                LocalDate localDate = ISO_LOCAL_DATE.parse(value, LocalDate::from);
                if (localDate.toEpochDay() < START_OF_MODERN_ERA_DAYS) {
                    throw new TrinoException(HIVE_INVALID_TIMESTAMP_COERCION, "Coercion on historical dates is not supported");
                }
                toType.writeLong(blockBuilder, localDate.toEpochDay());
            }
            catch (DateTimeParseException ignored) {
                throw new IllegalArgumentException("Invalid date value: " + value + " is not a valid date");
            }
        }
    }

    public static class DateToVarcharCoercer
            extends TypeCoercer<DateType, VarcharType>
    {
        public DateToVarcharCoercer(VarcharType toType)
        {
            super(DATE, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            int value = fromType.getInt(block, position);
            try {
                if (value < START_OF_MODERN_ERA_DAYS) {
                    throw new TrinoException(HIVE_INVALID_TIMESTAMP_COERCION, "Coercion on historical dates is not supported");
                }
                Slice converted = utf8Slice(ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(value)));
                if (!toType.isUnbounded() && countCodePoints(converted) > toType.getBoundedLength()) {
                    throw new TrinoException(INVALID_ARGUMENTS, format("Varchar representation of '%s' exceeds %s bounds", converted.toStringUtf8(), toType));
                }
                toType.writeSlice(blockBuilder, converted);
            }
            catch (DateTimeException ignored) {
                throw new IllegalArgumentException("Invalid date value: " + value + " is exceeding supported date range");
            }
        }
    }
}
