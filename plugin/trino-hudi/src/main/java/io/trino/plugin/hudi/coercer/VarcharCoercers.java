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
package io.trino.plugin.hudi.coercer;

import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.function.Function;

import static io.trino.spi.type.DecimalConversions.doubleToLongDecimal;
import static io.trino.spi.type.DecimalConversions.doubleToShortDecimal;
import static java.lang.Float.intBitsToFloat;
import static java.time.temporal.ChronoField.EPOCH_DAY;

public class VarcharCoercers
{
    private VarcharCoercers()
    {
    }

    public static Function<Block, Block> createVarcharToDecimalCoercer(VarcharType fromType, DecimalType toType)
    {
        if (toType.isShort()) {
            return new VarcharToShortDecimalCoercer(fromType, toType);
        }
        return new VarcharToLongDecimalCoercer(fromType, toType);
    }

    public static Function<Block, Block> createVarcharToDateCoercer(VarcharType fromType, DateType toType)
    {
        return new VarcharToDateCoercer(fromType, toType);
    }

    public static Function<Block, Block> createDoubleToVarcharCoercer(DoubleType fromType, VarcharType toType)
    {
        return new DoubleToVarcharCoercer(fromType, toType);
    }

    public static Function<Block, Block> createFloatToVarcharCoercer(RealType fromType, VarcharType toType)
    {
        return new FloatToVarcharCoercer(fromType, toType);
    }

    private static class VarcharToDateCoercer
            extends TypeCoercer<VarcharType, DateType>
    {
        public VarcharToDateCoercer(VarcharType fromType, DateType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    LocalDate.parse(fromType.getSlice(block, position).toStringUtf8()).getLong(EPOCH_DAY));
        }
    }

    private static class VarcharToShortDecimalCoercer
            extends TypeCoercer<VarcharType, DecimalType>
    {
        public VarcharToShortDecimalCoercer(VarcharType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    doubleToShortDecimal(
                            Double.parseDouble(
                                    fromType.getSlice(block, position).toStringUtf8()),
                            toType.getPrecision(),
                            toType.getScale()));
        }
    }

    private static class VarcharToLongDecimalCoercer
            extends TypeCoercer<VarcharType, DecimalType>
    {
        public VarcharToLongDecimalCoercer(VarcharType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeObject(blockBuilder,
                    doubleToLongDecimal(
                            Double.parseDouble(
                                    fromType.getSlice(block, position).toStringUtf8()),
                            toType.getPrecision(),
                            toType.getScale()));
        }
    }

    private static class DoubleToVarcharCoercer
            extends TypeCoercer<DoubleType, VarcharType>
    {
        protected DoubleToVarcharCoercer(DoubleType fromType, VarcharType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            Double value = fromType.getDouble(block, position);
            toType.writeString(blockBuilder, value.toString());
        }
    }

    private static class FloatToVarcharCoercer
            extends TypeCoercer<RealType, VarcharType>
    {
        protected FloatToVarcharCoercer(RealType fromType, VarcharType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            float value = intBitsToFloat((int) fromType.getLong(block, position));
            toType.writeString(blockBuilder, String.valueOf(value));
        }
    }
}
