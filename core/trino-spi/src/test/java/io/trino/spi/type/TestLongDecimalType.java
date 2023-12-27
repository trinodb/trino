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

import com.google.common.base.Throwables;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.lang.Math.signum;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestLongDecimalType
{
    private static final LongDecimalType TYPE = (LongDecimalType) LongDecimalType.createDecimalType(20, 10);
    private static final MethodHandle TYPE_COMPARISON = new TypeOperators().getComparisonUnorderedLastOperator(TYPE, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));

    @Test
    public void testCompareTo()
    {
        testCompare("0", "-1234567891.1234567890", 1);
        testCompare("1234567890.1234567890", "1234567890.1234567890", 0);
        testCompare("1234567890.1234567890", "1234567890.1234567891", -1);
        testCompare("1234567890.1234567890", "1234567890.1234567889", 1);
        testCompare("1234567890.1234567890", "1234567891.1234567890", -1);
        testCompare("1234567890.1234567890", "1234567889.1234567890", 1);
        testCompare("0", "1234567891.1234567890", -1);
        testCompare("1234567890.1234567890", "0", 1);
        testCompare("0", "0", 0);
        testCompare("-1234567890.1234567890", "-1234567890.1234567890", 0);
        testCompare("-1234567890.1234567890", "-1234567890.1234567891", 1);
        testCompare("-1234567890.1234567890", "-1234567890.1234567889", -1);
        testCompare("-1234567890.1234567890", "-1234567891.1234567890", 1);
        testCompare("-1234567890.1234567890", "-1234567889.1234567890", -1);
        testCompare("0", "-1234567891.1234567890", 1);
        testCompare("-1234567890.1234567890", "0", -1);
        testCompare("-1234567890.1234567890", "1234567890.1234567890", -1);
        testCompare("1234567890.1234567890", "-1234567890.1234567890", 1);
    }
    @Test
    public void testOverflowThrowsTrinoException() {
        String overflowDecimal = "99999999999999999999999999999999999999.0";
        try {
            Decimals.valueOf(new BigDecimal(overflowDecimal));
            Assertions.fail("%s should throw a TrinoException".formatted(overflowDecimal));
        } catch (ArithmeticException e) {
            Assertions.fail(("%s should throw a TrinoException and not an IllegalArgumentException because this " +
                    "shows user input error").formatted(overflowDecimal));
        } catch (TrinoException e) {
            if (!e.getErrorCode().equals(io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode())) {
                Assertions.fail("the error code should be a " +
                        "NUMERIC_VALUE_OUT_OF_RANGE, but was %s".formatted(e));
            }
        }


    }
    private void testCompare(String decimalA, String decimalB, int expected)
    {
        try {
            long actual = (long) TYPE_COMPARISON.invokeExact(decimalAsBlock(decimalA), 0, decimalAsBlock(decimalB), 0);
            assertThat((int) signum(actual))
                    .describedAs("bad comparison result for " + decimalA + ", " + decimalB)
                    .isEqualTo((int) signum(expected));
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private Block decimalAsBlock(String value)
    {
        Int128 decimal = Decimals.valueOf(new BigDecimal(value));
        BlockBuilder blockBuilder = new Int128ArrayBlockBuilder(null, 1);
        TYPE.writeObject(blockBuilder, decimal);
        return blockBuilder.build();
    }
}
