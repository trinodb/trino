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
package io.trino.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlDecimal;

import java.math.BigDecimal;

import static io.trino.spi.type.Decimals.writeBigDecimal;

public class TestLongDecimalType
        extends AbstractTestType
{
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(30, 10);

    public TestLongDecimalType()
    {
        super(LONG_DECIMAL_TYPE, SqlDecimal.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = LONG_DECIMAL_TYPE.createBlockBuilder(null, 15);
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("32345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("32345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("42345678901234567890.1234567890"));
        return blockBuilder.build();
    }

    @Override
    protected Object getNonNullValue()
    {
        return Int128.ZERO;
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        BigDecimal decimal = toBigDecimal((Int128) value, 10);
        BigDecimal greaterDecimal = decimal.add(BigDecimal.ONE);
        return Decimals.valueOf(greaterDecimal);
    }

    private static BigDecimal toBigDecimal(Int128 value, int scale)
    {
        return new BigDecimal(value.toBigInteger(), scale);
    }
}
