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

import com.google.common.primitives.Shorts;
import io.trino.spi.type.TrinoNumber.BigDecimalValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static java.math.RoundingMode.UNNECESSARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Percentage.withPercentage;

class TestTrinoNumber
{
    private static final int TEST_MAX_DECIMAL_PRECISION = 30;

    @Test
    void testHeaderRoundTrip()
    {
        for (var negated : new boolean[] {false, true}) {
            for (int scale = TrinoNumber.MIN_SCALE; scale <= TrinoNumber.MAX_SCALE; scale++) {
                short header = TrinoNumber.header(negated, scale);
                assertThat(TrinoNumber.scaleFromHeader(header)).isEqualTo(Shorts.checkedCast(scale));
                assertThat(TrinoNumber.negatedFromHeader(header)).isEqualTo(negated);
            }

            for (int badScale : new int[] {TrinoNumber.MIN_SCALE - 1, TrinoNumber.MAX_SCALE + 1, Integer.MIN_VALUE, Integer.MAX_VALUE}) {
                assertThatThrownBy(() -> TrinoNumber.header(negated, badScale))
                        .hasMessage("Scale out of range: " + badScale);
            }
        }
    }

    @Test
    void testBigDecimalRoundTrip()
    {
        assertRoundTrip(BigDecimal.ZERO);
        assertRoundTrip(BigDecimal.ONE);
        assertRoundTrip(BigDecimal.TWO);
        assertRoundTrip(BigDecimal.TEN, new BigDecimal("1e1"));

        assertRoundTrip(new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("0.000"), new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("0").setScale(3, UNNECESSARY), new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("0").setScale(-3, UNNECESSARY), new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("0.00001"));
        assertRoundTrip(new BigDecimal("1"));
        assertRoundTrip(new BigDecimal("10"), new BigDecimal("1e1"));
        assertRoundTrip(new BigDecimal("127"));
        assertRoundTrip(new BigDecimal("128"));
        assertRoundTrip(new BigDecimal("255"));
        assertRoundTrip(new BigDecimal("256"));
        assertRoundTrip(new BigDecimal("1000"), new BigDecimal("1e3"));
        assertRoundTrip(new BigDecimal("1000.000"), new BigDecimal("1e3"));
        assertRoundTrip(new BigDecimal("1000.0001"));

        assertRoundTrip(new BigDecimal("-0"));
        assertRoundTrip(new BigDecimal("-0.000"), new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("0").setScale(3, UNNECESSARY).negate(), new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("0").setScale(-3, UNNECESSARY).negate(), new BigDecimal("0"));
        assertRoundTrip(new BigDecimal("-0.00001"));
        assertRoundTrip(new BigDecimal("-1"));
        assertRoundTrip(new BigDecimal("-10"), new BigDecimal("-1e1"));
        assertRoundTrip(new BigDecimal("-127"));
        assertRoundTrip(new BigDecimal("-128"));
        assertRoundTrip(new BigDecimal("-255"));
        assertRoundTrip(new BigDecimal("-256"));
        assertRoundTrip(new BigDecimal("-1000"), new BigDecimal("-1e3"));
        assertRoundTrip(new BigDecimal("-1000.000"), new BigDecimal("-1e3"));
        assertRoundTrip(new BigDecimal("-1000.0001"));

        assertRoundTrip(new BigDecimal("20050910133100123"));
        assertRoundTrip(new BigDecimal("20050910133100123.000000"), new BigDecimal("20050910133100123"));
        assertRoundTrip(new BigDecimal("20050910.133100123"));
        assertRoundTrip(new BigDecimal("20050910.133100123000000"), new BigDecimal("20050910.133100123"));

        assertRoundTrip(new BigDecimal("3.1415926535897932384626433832"));
        assertRoundTrip(new BigDecimal("0.0000000000000000000000000000000000000000000314159265358979323846264338328"));

        // exceeding [test] max precision
        assertRoundTrip(
                new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307"),
                new BigDecimal("3.14159265358979323846264338328"));
        assertRoundTrip(
                new BigDecimal("0.00000000000000000000000000000000000000000003141592653589793238462643383279502884197169399375105820974944592307"),
                new BigDecimal("0.0000000000000000000000000000000000000000000314159265358979323846264338328"));
        assertRoundTrip(
                new BigDecimal("444555666777888999555666777888999555666777888999555666777888999"),
                new BigDecimal("444555666777888999555666777889000000000000000000000000000000000").stripTrailingZeros());
        assertRoundTrip(
                new BigDecimal("444555666777888999555666.777888999555666777888999555666777888999"),
                new BigDecimal("444555666777888999555666.777889000000000000000000000000000000000").stripTrailingZeros());
        assertRoundTrip(
                new BigDecimal("444555666777888999555666777888.999555666777888999555666777888999"),
                new BigDecimal("444555666777888999555666777889"));
        assertRoundTrip(
                new BigDecimal("4445556667778889995556667778889995556667.77888999555666777888999"),
                new BigDecimal("4445556667778889995556667778890000000000").stripTrailingZeros());
    }

    private void assertRoundTrip(BigDecimal jdkBigDecimal)
    {
        assertRoundTrip(jdkBigDecimal, jdkBigDecimal);
    }

    private void assertRoundTrip(BigDecimal inputJdkBigDecimal, BigDecimal resultingJdkBigDecimal)
    {
        if (inputJdkBigDecimal.stripTrailingZeros().precision() <= TEST_MAX_DECIMAL_PRECISION) {
            assertThat(resultingJdkBigDecimal)
                    .isEqualByComparingTo(inputJdkBigDecimal);
        }
        else {
            assertThat(resultingJdkBigDecimal)
                    .isCloseTo(inputJdkBigDecimal, withPercentage(0.000000000000000000000000001));
        }
        TrinoNumber converted = TrinoNumber.from(new BigDecimalValue(inputJdkBigDecimal), TEST_MAX_DECIMAL_PRECISION);
        assertThat(converted.toBigDecimal()).isEqualTo(new BigDecimalValue(resultingJdkBigDecimal));
    }
}
