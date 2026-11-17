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
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.NumberType.NUMBER;
import static java.math.RoundingMode.UP;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNumberType
        extends AbstractTestType
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

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

    @Test
    public void testEqualOperatorConventionsConsistent()
            throws Throwable
    {
        List<TrinoNumber> values = List.of(
                TrinoNumber.from(BigDecimal.ZERO),
                TrinoNumber.from(new BigDecimal("1.5")),
                TrinoNumber.from(new BigDecimal("1.50")),
                TrinoNumber.from(new BigDecimal("-1.5")),
                TrinoNumber.from(new BigDecimal("12345678901234567890.1234567890")),
                TrinoNumber.from(new BigDecimal("1E+100")),
                TrinoNumber.from(new BigDecimal("1E-100")),
                TrinoNumber.from(new TrinoNumber.NotANumber()),
                TrinoNumber.from(new TrinoNumber.Infinity(false)),
                TrinoNumber.from(new TrinoNumber.Infinity(true)));
        List<TrinoNumber> otherValues = values.reversed();

        ValueBlock block = createBlock(values);
        ValueBlock otherBlock = createBlock(otherValues);
        FlatValues flat = FlatValues.write(block, 0, 0);
        FlatValues otherFlat = FlatValues.write(otherBlock, 3, 5);

        MethodHandle stackEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        MethodHandle blockBlockEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle flatFlatEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, FLAT, FLAT));
        MethodHandle flatBlockEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, FLAT, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle blockFlatEqual = TYPE_OPERATORS.getEqualOperator(NUMBER, simpleConvention(NULLABLE_RETURN, VALUE_BLOCK_POSITION_NOT_NULL, FLAT));
        MethodHandle stackIdentical = TYPE_OPERATORS.getIdenticalOperator(NUMBER, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
        MethodHandle blockBlockIdentical = TYPE_OPERATORS.getIdenticalOperator(NUMBER, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle flatFlatIdentical = TYPE_OPERATORS.getIdenticalOperator(NUMBER, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
        MethodHandle flatBlockIdentical = TYPE_OPERATORS.getIdenticalOperator(NUMBER, simpleConvention(FAIL_ON_NULL, FLAT, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle blockFlatIdentical = TYPE_OPERATORS.getIdenticalOperator(NUMBER, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, FLAT));

        for (int left = 0; left < values.size(); left++) {
            for (int right = 0; right < otherValues.size(); right++) {
                TrinoNumber.AsBigDecimal leftDecimal = values.get(left).toBigDecimal();
                TrinoNumber.AsBigDecimal rightDecimal = otherValues.get(right).toBigDecimal();
                boolean expectedIdentical = leftDecimal.equals(rightDecimal);
                boolean expected = expectedIdentical && !(leftDecimal instanceof TrinoNumber.NotANumber);
                String description = leftDecimal + " = " + rightDecimal;

                assertThat((Boolean) stackEqual.invoke(values.get(left), otherValues.get(right))).describedAs(description).isEqualTo(expected);
                assertThat((Boolean) blockBlockEqual.invokeExact(block, left, otherBlock, right)).describedAs(description).isEqualTo(expected);
                assertThat((Boolean) flatFlatEqual.invokeExact(flat.fixed(), flat.fixedOffset(left), flat.variable(), flat.variableOffset(left), otherFlat.fixed(), otherFlat.fixedOffset(right), otherFlat.variable(), otherFlat.variableOffset(right)))
                        .describedAs(description)
                        .isEqualTo(expected);
                assertThat((Boolean) flatBlockEqual.invokeExact(flat.fixed(), flat.fixedOffset(left), flat.variable(), flat.variableOffset(left), otherBlock, right)).describedAs(description).isEqualTo(expected);
                assertThat((Boolean) blockFlatEqual.invokeExact(block, left, otherFlat.fixed(), otherFlat.fixedOffset(right), otherFlat.variable(), otherFlat.variableOffset(right))).describedAs(description).isEqualTo(expected);

                assertThat((boolean) stackIdentical.invoke(values.get(left), otherValues.get(right))).describedAs(description).isEqualTo(expectedIdentical);
                assertThat((boolean) blockBlockIdentical.invokeExact(block, left, otherBlock, right)).describedAs(description).isEqualTo(expectedIdentical);
                assertThat((boolean) flatFlatIdentical.invokeExact(flat.fixed(), flat.fixedOffset(left), flat.variable(), flat.variableOffset(left), otherFlat.fixed(), otherFlat.fixedOffset(right), otherFlat.variable(), otherFlat.variableOffset(right)))
                        .describedAs(description)
                        .isEqualTo(expectedIdentical);
                assertThat((boolean) flatBlockIdentical.invokeExact(flat.fixed(), flat.fixedOffset(left), flat.variable(), flat.variableOffset(left), otherBlock, right)).describedAs(description).isEqualTo(expectedIdentical);
                assertThat((boolean) blockFlatIdentical.invokeExact(block, left, otherFlat.fixed(), otherFlat.fixedOffset(right), otherFlat.variable(), otherFlat.variableOffset(right))).describedAs(description).isEqualTo(expectedIdentical);
            }
        }
    }

    private static ValueBlock createBlock(List<TrinoNumber> values)
    {
        BlockBuilder blockBuilder = NUMBER.createBlockBuilder(null, values.size());
        values.forEach(value -> NUMBER.writeObject(blockBuilder, value));
        return blockBuilder.buildValueBlock();
    }

    private record FlatValues(byte[] fixed, int fixedStart, byte[] variable, int[] variableOffsets)
    {
        static FlatValues write(ValueBlock block, int fixedStart, int variableStart)
                throws Throwable
        {
            int positionCount = block.getPositionCount();
            int[] variableOffsets = new int[positionCount];
            int variableSize = variableStart;
            for (int i = 0; i < positionCount; i++) {
                variableOffsets[i] = variableSize;
                variableSize += NUMBER.getFlatVariableWidthSize(block, i);
            }
            FlatValues flat = new FlatValues(new byte[fixedStart + NUMBER.getFlatFixedSize() * positionCount], fixedStart, new byte[variableSize], variableOffsets);
            MethodHandle writeFlat = TYPE_OPERATORS.getReadValueOperator(NUMBER, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION));
            for (int i = 0; i < positionCount; i++) {
                writeFlat.invokeExact(block, i, flat.fixed(), flat.fixedOffset(i), flat.variable(), flat.variableOffset(i));
            }
            return flat;
        }

        int fixedOffset(int position)
        {
            return fixedStart + position * NUMBER.getFlatFixedSize();
        }

        int variableOffset(int position)
        {
            return variableOffsets[position];
        }
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
