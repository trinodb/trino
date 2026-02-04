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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

class TestBigdecimal
{
    @Test
    void testBigDecimalRoundTrip()
    {
        assertRoundTrip(BigDecimal.ZERO);
        assertRoundTrip(BigDecimal.ONE);
        assertRoundTrip(BigDecimal.TWO);
        assertRoundTrip(BigDecimal.TEN, new BigDecimal("1e1"));

        assertRoundTrip(new BigDecimal("0"));
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

        assertRoundTrip(new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307"));
    }

    private void assertRoundTrip(BigDecimal jdkBigDecimal)
    {
        assertRoundTrip(jdkBigDecimal, jdkBigDecimal);
    }

    private void assertRoundTrip(BigDecimal inputJdkBigDecimal, BigDecimal resultingJdkBigDecimal)
    {
        checkArgument(inputJdkBigDecimal.compareTo(resultingJdkBigDecimal) == 0);
        Bigdecimal converted = Bigdecimal.from(inputJdkBigDecimal);
        assertThat(converted.toBigDecimal()).isEqualTo(resultingJdkBigDecimal);
    }
}
