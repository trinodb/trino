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
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static java.lang.Math.pow;
import static org.assertj.core.api.Assertions.assertThat;

public class TestShortDecimalType
        extends AbstractTestType
{
    private static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(4, 2);

    public TestShortDecimalType()
    {
        super(SHORT_DECIMAL_TYPE, SqlDecimal.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = SHORT_DECIMAL_TYPE.createFixedSizeBlockBuilder(15);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, -1234);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, -1234);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, -1234);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 3321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 3321);
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, 4321);
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((long) value) + 1;
    }

    @Test
    public void testRange()
    {
        for (int precision = 1; precision <= MAX_SHORT_PRECISION; precision++) {
            Type type = createDecimalType(precision, 1);
            Type.Range range = type.getRange().orElseThrow();
            long max = (long) pow(10, precision) - 1;
            assertThat(range.getMin()).isEqualTo(-max);
            assertThat(range.getMax()).isEqualTo(max);
        }
    }

    @Test
    public void testPreviousValue()
    {
        long minValue = -9999L;
        long maxValue = 9999L;

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(-1235L));

        assertThat(type.getPreviousValue(maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValue()
    {
        long minValue = -9999L;
        long maxValue = 9999L;

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(type.getNextValue(minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(-1233L));

        assertThat(type.getNextValue(maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }
}
