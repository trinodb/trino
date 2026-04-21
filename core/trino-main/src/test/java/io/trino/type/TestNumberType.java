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

import com.google.common.math.IntMath;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.TrinoNumber;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;

import static io.trino.spi.type.NumberType.NUMBER;
import static java.math.RoundingMode.UP;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNumberType
        extends AbstractTestType
{
    public TestNumberType()
    {
        super(NUMBER, SqlNumber.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = NUMBER.createBlockBuilder(null, 15);
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("-12345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("-12345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("-12345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("22345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("22345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("22345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("22345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("22345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("32345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("32345678901234567890.1234567890")));
        NUMBER.writeObject(blockBuilder, TrinoNumber.from(new BigDecimal("42345678901234567890." + "1234567890".repeat(IntMath.divide(getNumberMaxDecimalPrecision(), 10, UP)))));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getNonNullValue()
    {
        return TrinoNumber.from(BigDecimal.ZERO);
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return switch (((TrinoNumber) value).toBigDecimal()) {
            case TrinoNumber.BigDecimalValue(BigDecimal bigDecimal) -> TrinoNumber.from(bigDecimal.add(BigDecimal.ONE));
            default -> throw new UnsupportedOperationException("Cannot calculate greater value for: " + value);
        };
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEmpty();
    }

    private static int getNumberMaxDecimalPrecision()
    {
        try {
            Field field = NumberType.class.getDeclaredField("MAX_DECIMAL_PRECISION");
            field.setAccessible(true);
            return (int) field.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
