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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLongDecimalType
        extends AbstractTestType
{
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(30, 10);

    // the unscaled bounds of DECIMAL(30, 10)
    private static final Int128 MIN_VALUE = Int128.valueOf("-999999999999999999999999999999");
    private static final Int128 MAX_VALUE = Int128.valueOf("999999999999999999999999999999");

    public TestLongDecimalType()
    {
        super(LONG_DECIMAL_TYPE, SqlDecimal.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = LONG_DECIMAL_TYPE.createFixedSizeBlockBuilder(15);
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
        return blockBuilder.buildValueBlock();
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

    @Test
    public void testRange()
    {
        for (int precision = MAX_SHORT_PRECISION + 1; precision <= MAX_PRECISION; precision++) {
            Type type = createDecimalType(precision, 1);
            Type.Range range = type.getRange().orElseThrow();
            BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
            assertThat(range.getMin()).isEqualTo(Int128.valueOf(max.negate()));
            assertThat(range.getMax()).isEqualTo(Int128.valueOf(max));
        }
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(MIN_VALUE))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(Int128.valueOf("-999999999999999999999999999998")))
                .isEqualTo(Optional.of(MIN_VALUE));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(Int128.valueOf("-123456789012345678901234567891")));

        assertThat(type.getPreviousValue(Int128.ZERO))
                .isEqualTo(Optional.of(Int128.valueOf(-1)));
        // the borrow reaches the high word
        assertThat(type.getPreviousValue(Int128.valueOf("18446744073709551616")))
                .isEqualTo(Optional.of(Int128.valueOf("18446744073709551615")));

        assertThat(type.getPreviousValue(MAX_VALUE))
                .isEqualTo(Optional.of(Int128.valueOf("999999999999999999999999999998")));
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(MIN_VALUE))
                .isEqualTo(Optional.of(Int128.valueOf("-999999999999999999999999999998")));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(Int128.valueOf("-123456789012345678901234567889")));

        assertThat(type.getNextValue(Int128.valueOf(-1)))
                .isEqualTo(Optional.of(Int128.ZERO));
        // the carry reaches the high word
        assertThat(type.getNextValue(Int128.valueOf("18446744073709551615")))
                .isEqualTo(Optional.of(Int128.valueOf("18446744073709551616")));

        assertThat(type.getNextValue(Int128.valueOf("999999999999999999999999999998")))
                .isEqualTo(Optional.of(MAX_VALUE));
        assertThat(type.getNextValue(MAX_VALUE))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testGetObjectValueRejectsOutOfRange()
    {
        for (int precision = MAX_SHORT_PRECISION + 1; precision <= MAX_PRECISION; precision++) {
            for (int scale : new int[] {0, precision / 2, precision}) {
                DecimalType type = createDecimalType(precision, scale);

                BigInteger tenToPrecision = BigInteger.TEN.pow(precision);
                BigInteger maxUnscaled = tenToPrecision.subtract(BigInteger.ONE);
                assertThat(type.getObjectValue(blockOf(type, Int128.valueOf(maxUnscaled)), 0))
                        .isEqualTo(new SqlDecimal(maxUnscaled, precision, scale));
                assertThat(type.getObjectValue(blockOf(type, Int128.valueOf(maxUnscaled.negate())), 0))
                        .isEqualTo(new SqlDecimal(maxUnscaled.negate(), precision, scale));

                assertThatThrownBy(() -> type.getObjectValue(blockOf(type, Int128.valueOf(tenToPrecision)), 0))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Value out of range for DECIMAL(%s, %s): %s", precision, scale, tenToPrecision);
                assertThatThrownBy(() -> type.getObjectValue(blockOf(type, Int128.valueOf(tenToPrecision.negate())), 0))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Value out of range for DECIMAL(%s, %s): %s", precision, scale, tenToPrecision.negate());
            }
        }
    }

    private static ValueBlock blockOf(DecimalType type, Int128 unscaledValue)
    {
        BlockBuilder blockBuilder = type.createFixedSizeBlockBuilder(1);
        type.writeObject(blockBuilder, unscaledValue);
        return blockBuilder.buildValueBlock();
    }
}
