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

import io.airlift.slice.XxHash64;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarOperator;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public final class RealType
        extends AbstractIntType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(RealType.class, lookup(), long.class);

    public static final RealType REAL = new RealType();

    private RealType()
    {
        super(new TypeSignature(StandardTypes.REAL));
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return intBitsToFloat(block.getInt(position, 0));
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        int floatValue;
        try {
            floatValue = toIntExact(value);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value (%sb) is not a valid single-precision float", Long.toBinaryString(value)));
        }
        blockBuilder.writeInt(floatValue).closeEntry();
    }

    @Override
    public boolean equals(Object other)
    {
        return other == REAL;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public Optional<Range> getRange()
    {
        // The range for real is undefined because NaN is a special value that
        // is *not* in any reasonable definition of a range for this type.
        return Optional.empty();
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long left, long right)
    {
        return intBitsToFloat((int) left) == intBitsToFloat((int) right);
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        float realValue = intBitsToFloat((int) value);
        if (realValue == 0) {
            realValue = 0;
        }
        return AbstractLongType.hash(floatToIntBits(realValue));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        float realValue = intBitsToFloat((int) value);
        if (realValue == 0) {
            realValue = 0;
        }
        return XxHash64.hash(floatToIntBits(realValue));
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    private static boolean distinctFromOperator(long left, @IsNull boolean leftNull, long right, @IsNull boolean rightNull)
    {
        if (leftNull || rightNull) {
            return leftNull != rightNull;
        }

        float leftFloat = intBitsToFloat((int) left);
        float rightFloat = intBitsToFloat((int) right);
        if (Float.isNaN(leftFloat) && Float.isNaN(rightFloat)) {
            return false;
        }
        return leftFloat != rightFloat;
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonUnorderedLastOperator(long left, long right)
    {
        return Float.compare(intBitsToFloat((int) left), intBitsToFloat((int) right));
    }

    @ScalarOperator(COMPARISON_UNORDERED_FIRST)
    private static long comparisonUnorderedFirstOperator(long leftBits, long rightBits)
    {
        // Float compare puts NaN last, so we must handle NaNs manually
        float left = intBitsToFloat((int) leftBits);
        float right = intBitsToFloat((int) rightBits);
        if (Float.isNaN(left) && Float.isNaN(right)) {
            return 0;
        }
        if (Float.isNaN(left)) {
            return -1;
        }
        if (Float.isNaN(right)) {
            return 1;
        }
        return Float.compare(left, right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return intBitsToFloat((int) left) < intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return intBitsToFloat((int) left) <= intBitsToFloat((int) right);
    }
}
