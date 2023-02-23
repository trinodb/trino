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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.util.function.Function;

import static io.trino.spi.type.DecimalConversions.realToLongDecimal;
import static io.trino.spi.type.DecimalConversions.realToShortDecimal;
import static java.lang.Float.floatToIntBits;

public class IntegerCoercers
{
    private IntegerCoercers()
    {
    }

    public static Function<Block, Block> createIntegerToRealCoercer(Type fromType, RealType toType)
    {
        return new IntegerToRealCoercer(fromType, toType);
    }

    public static Function<Block, Block> createIntegerToDoubleCoercer(Type fromType, DoubleType toType)
    {
        return new IntegerToDoubleCoercer(fromType, toType);
    }

    public static Function<Block, Block> createIntegerToDecimalCoercer(Type fromType, DecimalType toType)
    {
        if (toType.isShort()) {
            return new IntegerToShortDecimalCoercer(fromType, toType);
        }
        return new IntegerToLongDecimalCoercer(fromType, toType);
    }

    public static class IntegerToRealCoercer<F extends Type>
            extends TypeCoercer<F, RealType>
    {
        public IntegerToRealCoercer(F fromType, RealType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder, floatToIntBits(fromType.getLong(block, position)));
        }
    }

    public static class IntegerToDoubleCoercer<F extends Type>
            extends TypeCoercer<F, DoubleType>
    {
        public IntegerToDoubleCoercer(F fromType, DoubleType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeDouble(blockBuilder, fromType.getLong(block, position));
        }
    }

    private static class IntegerToShortDecimalCoercer<F extends Type>
            extends TypeCoercer<F, DecimalType>
    {
        public IntegerToShortDecimalCoercer(F fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    realToShortDecimal(floatToIntBits(fromType.getLong(block, position)), toType.getPrecision(), toType.getScale()));
        }
    }

    private static class IntegerToLongDecimalCoercer<F extends Type>
            extends TypeCoercer<F, DecimalType>
    {
        public IntegerToLongDecimalCoercer(F fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeObject(blockBuilder,
                    realToLongDecimal(floatToIntBits(fromType.getLong(block, position)), toType.getPrecision(), toType.getScale()));
        }
    }
}
