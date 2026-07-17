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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.NumberParser.parseLong;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public final class VarcharToIntegralNumericCoercers
{
    private VarcharToIntegralNumericCoercers() {}

    public static TypeCoercer<VarcharType, ? extends Type> createVarcharToIntegerNumberCoercer(VarcharType fromType, Type toType, boolean isOrcFile)
    {
        return isOrcFile ? new OrcVarcharToIntegralNumericCoercer<>(fromType, toType) : new VarcharToIntegralNumericCoercer<>(fromType, toType);
    }

    // For coercions logic for non ORC file format is derived from
    // https://github.com/apache/hive/blob/4df4d75bf1e16fe0af75aad0b4179c34c07fc975/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/primitive/PrimitiveObjectInspectorUtils.java#L683
    public static class VarcharToIntegralNumericCoercer<T extends Type>
            extends TypeCoercer<VarcharType, T>
    {
        public VarcharToIntegralNumericCoercer(VarcharType fromType, T toType)
        {
            super(fromType, toType);
            checkArgument(
                    toType.equals(TINYINT) || toType.equals(SMALLINT) || toType.equals(INTEGER) || toType.equals(BIGINT),
                    "Unsupported datatype for coercion from varchar to %s",
                    toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            try {
                Slice value = fromType.getSlice(block, position);
                long parsed = parseLong(value, 0, value.length());
                if (toType.equals(BIGINT)) {
                    toType.writeLong(blockBuilder, parsed);
                    return;
                }

                // Integer.parseInt rejects anything outside the int range
                if (parsed != (int) parsed) {
                    throw new NumberFormatException();
                }
                // Hive narrows to tinyint and smallint by truncating rather than by rejecting
                int intValue = (int) parsed;
                if (toType.equals(TINYINT)) {
                    toType.writeLong(blockBuilder, (byte) intValue);
                }
                else if (toType.equals(SMALLINT)) {
                    toType.writeLong(blockBuilder, (short) intValue);
                }
                else {
                    toType.writeLong(blockBuilder, intValue);
                }
            }
            catch (NumberFormatException e) {
                blockBuilder.appendNull();
            }
        }
    }

    // For ORC file format, if the numeric representation crosses the range of a given datatype, it is treated as null
    // https://github.com/apache/orc/blob/39368b5910032e0dbb257e5ebc98061e50a5e1e1/java/core/src/java/org/apache/orc/impl/ConvertTreeReaderFactory.java#L330
    public static class OrcVarcharToIntegralNumericCoercer<T extends Type>
            extends TypeCoercer<VarcharType, T>
    {
        private final long minValue;
        private final long maxValue;

        public OrcVarcharToIntegralNumericCoercer(VarcharType fromType, T toType)
        {
            super(fromType, toType);
            if (toType.equals(TINYINT)) {
                minValue = Byte.MIN_VALUE;
                maxValue = Byte.MAX_VALUE;
            }
            else if (toType.equals(SMALLINT)) {
                minValue = Short.MIN_VALUE;
                maxValue = Short.MAX_VALUE;
            }
            else if (toType.equals(INTEGER)) {
                minValue = Integer.MIN_VALUE;
                maxValue = Integer.MAX_VALUE;
            }
            else if (toType.equals(BIGINT)) {
                minValue = Long.MIN_VALUE;
                maxValue = Long.MAX_VALUE;
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Could not create Coercer from varchar to %s", toType));
            }
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            try {
                Slice slice = fromType.getSlice(block, position);
                long value = parseLong(slice, 0, slice.length());
                if (minValue <= value && value <= maxValue) {
                    toType.writeLong(blockBuilder, value);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            catch (NumberFormatException e) {
                blockBuilder.appendNull();
            }
        }
    }
}
