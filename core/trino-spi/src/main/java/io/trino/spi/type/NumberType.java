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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.ScalarOperator;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TrinoNumber.AsBigDecimal.COMPARE_NAN_LAST;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * A high precision decimal type. The precise limits and rounding behavior are considered an
 * implementation detail and may evolve over time together with the unstable binary format.
 * <p>
 * Stack representation of this type is {@link TrinoNumber} which wraps a slice with the format documented below.
 * <b>Note:</b> the binary format is not stable and may change between releases. Only the Java API is considered stable.
 * <p>
 * <h2>Current unstable binary format</h2>
 * <pre>
 *    ┌───────────────────────┬─────────────────────────┐
 *    │  header (16-bit, LE)  │ unsigned magnitude (BE) │
 *    └───────────────────────┴─────────────────────────┘
 *     └─────────────────────┘
 *             2 bytes                0+ bytes
 * </pre>
 * <dl>
 *     <dt>header</dt>
 *     <dd>A 16 bit number stored in little-endian byte order.
 *         The most significant bit indicates whether the number is negative (1) or non-negative (0).
 *         The remaining 15 bits store the scale + {@value TrinoNumber#SCALE_BASE} as an unsigned integer.
 *      </dd>
 *     <dt>unsigned magnitude</dt>
 *     <dd>A big-endian minimal representation of the absolute value of the unscaled integer containing digits of the value.
 *         The representation is minimal, i.e. it does not have leading zero bytes.
 *         This guarantees that two numerically equal values have the same binary representation.
 *     </dd>
 * </dl>
 * <p>
 * A value stored like this represents the following number:
 * <pre>
 *     (-1)^sign * magnitude * 10^(-scale)
 * </pre>
 */
public class NumberType
        extends AbstractVariableWidthType
{
    public static final String NAME = "number";
    public static final NumberType NUMBER = new NumberType();

    // Precision loss prevents creation of values that would occupy very large amount of memory.
    // Max precision is implementation detail and subject to change.
    static final int MAX_DECIMAL_PRECISION = 200;

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(NumberType.class, lookup(), TrinoNumber.class);

    private NumberType()
    {
        super(new TypeSignature(NAME), TrinoNumber.class);
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlNumber(((TrinoNumber) getObject(block, position)).toBigDecimal());
    }

    @Override
    public Object getObject(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        Slice slice = valueBlock.getSlice(valuePosition);
        return new TrinoNumber(slice);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        TrinoNumber decimal = (TrinoNumber) value;
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(decimal.bytes());
    }

    @Override
    public boolean equals(Object other)
    {
        return other == NUMBER;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(TrinoNumber left, TrinoNumber right)
    {
        if (left.isNaN() || right.isNaN()) {
            // NaN is not equal to any value, including itself
            return false;
        }
        return left.bytes().equals(right.bytes());
    }

    // TODO EQUAL with block, position, block, position
    // TODO EQUAL with flat slice, block position

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(TrinoNumber value)
    {
        Slice slice = value.bytes();
        return XxHash64.hash(slice);
    }

    // TODO XX_HASH_64 with block, position
    // TODO XX_HASH_64 with flat slice

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(TrinoNumber left, TrinoNumber right)
    {
        return COMPARE_NAN_LAST.compare(left.toBigDecimal(), right.toBigDecimal());
    }

    // TODO COMPARISON_UNORDERED_LAST with block, position, block, position

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(TrinoNumber left, TrinoNumber right)
    {
        if (left.isNaN() || right.isNaN()) {
            // NaN is not less than any value, including itself
            return false;
        }
        return COMPARE_NAN_LAST.compare(left.toBigDecimal(), right.toBigDecimal()) < 0;
    }

    // TODO LESS_THAN with block, position, block, position

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(TrinoNumber left, TrinoNumber right)
    {
        if (left.isNaN() || right.isNaN()) {
            // NaN is not less than or equal to any value, including itself
            return false;
        }
        return COMPARE_NAN_LAST.compare(left.toBigDecimal(), right.toBigDecimal()) <= 0;
    }

    // TODO LESS_THAN_OR_EQUAL with block, position, block, position ?
}
