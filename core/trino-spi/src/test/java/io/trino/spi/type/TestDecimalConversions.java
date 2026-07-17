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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static io.trino.spi.type.DecimalConversions.MAX_EXACT_DOUBLE;
import static io.trino.spi.type.DecimalConversions.MAX_EXACT_FLOAT;
import static io.trino.spi.type.DecimalConversions.longDecimalToDouble;
import static io.trino.spi.type.DecimalConversions.longDecimalToReal;
import static io.trino.spi.type.DecimalConversions.shortDecimalToDouble;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

class TestDecimalConversions
{
    @Test
    void testLongDecimalToDouble()
    {
        for (BigInteger unscaledValue : testValues()) {
            Int128 unscaledInt128 = Int128.valueOf(unscaledValue);
            for (int scale = 0; scale <= 38; scale++) {
                assertThat(longDecimalToDouble(unscaledInt128, scale))
                        .as("longDecimalToDouble(%s, %d)", unscaledInt128, scale)
                        .isEqualTo(new BigDecimal(unscaledValue, scale).doubleValue());
            }
        }
    }

    @Test
    void testShortDecimalToDouble()
    {
        BigInteger shortDecimalBound = BigInteger.TEN.pow(MAX_SHORT_PRECISION);
        for (BigInteger unscaledValue : testValues()) {
            if (unscaledValue.abs().compareTo(shortDecimalBound) >= 0) {
                // does not fit in a short decimal
                continue;
            }
            long unscaled = unscaledValue.longValueExact();
            for (int scale = 0; scale <= MAX_SHORT_PRECISION; scale++) {
                long tenToScale = BigInteger.TEN.pow(scale).longValueExact();
                assertThat(shortDecimalToDouble(unscaled, tenToScale))
                        .as("shortDecimalToDouble(%s, scale=%d)", unscaled, scale)
                        .isEqualTo(new BigDecimal(unscaledValue, scale).doubleValue());
            }
        }
    }

    @Test
    void testLongDecimalToReal()
    {
        for (BigInteger unscaledValue : testValues()) {
            Int128 unscaledInt128 = Int128.valueOf(unscaledValue);
            for (int scale = 0; scale <= 38; scale++) {
                assertThat(Float.intBitsToFloat(toIntExact(longDecimalToReal(unscaledInt128, scale))))
                        .as("longDecimalToReal(%s, %d)", unscaledInt128, scale)
                        .isEqualTo(new BigDecimal(unscaledValue, scale).floatValue());
            }
        }
    }

    private static List<BigInteger> testValues()
    {
        ImmutableList.Builder<BigInteger> values = ImmutableList.builder();

        values.add(BigInteger.ZERO);
        values.add(BigInteger.ONE);
        values.add(BigInteger.ONE.negate());
        values.add(BigInteger.TEN);
        values.add(BigInteger.TEN.negate());

        for (BigInteger cutoff : List.of(MAX_EXACT_FLOAT.toBigInteger(), MAX_EXACT_DOUBLE.toBigInteger())) {
            for (int delta = -4; delta <= 4; delta++) {
                values.add(cutoff.add(BigInteger.valueOf(delta)));
                values.add(cutoff.add(BigInteger.valueOf(delta)).negate());
            }
        }

        values.add(new BigInteger("12345678901234567890"));
        values.add(new BigInteger("12345678901234567890").negate());
        values.add(new BigInteger("12345678901234567890123456789012345678"));
        values.add(new BigInteger("12345678901234567890123456789012345678").negate());

        values.add(Int128.MAX_VALUE.toBigInteger());
        values.add(Int128.MAX_VALUE.toBigInteger().negate());
        values.add(Int128.MIN_VALUE.toBigInteger());

        return values.build();
    }
}
