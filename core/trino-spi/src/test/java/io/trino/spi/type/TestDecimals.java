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

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.overflows;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDecimals
{
    @Test
    public void testParse()
    {
        assertParseResult("0", 0L, 1, 0);
        assertParseResult("0.", 0L, 1, 0);
        assertParseResult(".0", 0L, 1, 1);
        assertParseResult("+0", 0L, 1, 0);
        assertParseResult("-0", 0L, 1, 0);
        assertParseResult("000", 0L, 1, 0);
        assertParseResult("+000", 0L, 1, 0);
        assertParseResult("-000", 0L, 1, 0);
        assertParseResult("0000000000000000000000000000", 0L, 1, 0);
        assertParseResult("+0000000000000000000000000000", 0L, 1, 0);
        assertParseResult("-0000000000000000000000000000", 0L, 1, 0);
        assertParseResult("1.1", 11L, 2, 1);
        assertParseResult("1.", 1L, 1, 0);
        assertParseResult("+1.1", 11L, 2, 1);
        assertParseResult("+1.", 1L, 1, 0);
        assertParseResult("-1.1", -11L, 2, 1);
        assertParseResult("-1.", -1L, 1, 0);
        assertParseResult("0001.1", 11L, 2, 1);
        assertParseResult("+0001.1", 11L, 2, 1);
        assertParseResult("-0001.1", -11L, 2, 1);
        assertParseResult("0.1", 1L, 1, 1);
        assertParseResult(".1", 1L, 1, 1);
        assertParseResult("+0.1", 1L, 1, 1);
        assertParseResult("+.1", 1L, 1, 1);
        assertParseResult("-0.1", -1L, 1, 1);
        assertParseResult("-.1", -1L, 1, 1);
        assertParseResult(".1", 1L, 1, 1);
        assertParseResult("+.1", 1L, 1, 1);
        assertParseResult("-.1", -1L, 1, 1);
        assertParseResult("000.1", 1L, 1, 1);
        assertParseResult("+000.1", 1L, 1, 1);
        assertParseResult("-000.1", -1L, 1, 1);
        assertParseResult("12345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("+12345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("-12345678901234567", -12345678901234567L, 17, 0);
        assertParseResult("00012345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("+00012345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("-00012345678901234567", -12345678901234567L, 17, 0);
        assertParseResult("0.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("+0.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("-0.12345678901234567", -12345678901234567L, 17, 17);
        assertParseResult("000.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("+000.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("-000.12345678901234567", -12345678901234567L, 17, 17);
        assertParseResult("12345678901234567890.123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("+12345678901234567890.123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("-12345678901234567890.123456789012345678", Int128.valueOf("-12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("00012345678901234567890.123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("+00012345678901234567890.123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("-00012345678901234567890.123456789012345678", Int128.valueOf("-12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("0.12345678901234567890123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("+0.12345678901234567890123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("-0.12345678901234567890123456789012345678", Int128.valueOf("-12345678901234567890123456789012345678"), 38, 38);
        assertParseResult(".12345678901234567890123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("+.12345678901234567890123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("-.12345678901234567890123456789012345678", Int128.valueOf("-12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("0000.12345678901234567890123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("+0000.12345678901234567890123456789012345678", Int128.valueOf("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("-0000.12345678901234567890123456789012345678", Int128.valueOf("-12345678901234567890123456789012345678"), 38, 38);

        assertParseResult("0_1_2_3_4", 1234L, 4, 0);
        assertParseFailure("0_1_2_3_4_");
        assertParseFailure("_0_1_2_3_4");

        assertParseResult("0_1_2_3_4.5_6_7_8", 12345678L, 8, 4);
        assertParseFailure("_0_1_2_3_4.5_6_7_8");
        assertParseFailure("0_1_2_3_4_.5_6_7_8");
        assertParseFailure("0_1_2_3_4._5_6_7_8");
        assertParseFailure("0_1_2_3_4.5_6_7_8_");
    }

    @Test
    public void testRejectNoDigits()
    {
        assertParseFailure(".");
        assertParseFailure("+.");
        assertParseFailure("-.");
    }

    @Test
    public void testEncodeShortScaledValue()
    {
        assertThat(encodeShortScaledValue(new BigDecimal("2.00"), 2)).isEqualTo(200L);
        assertThat(encodeShortScaledValue(new BigDecimal("2.13"), 2)).isEqualTo(213L);
        assertThat(encodeShortScaledValue(new BigDecimal("172.60"), 2)).isEqualTo(17260L);
        assertThat(encodeShortScaledValue(new BigDecimal("2"), 2)).isEqualTo(200L);
        assertThat(encodeShortScaledValue(new BigDecimal("172.6"), 2)).isEqualTo(17260L);

        assertThat(encodeShortScaledValue(new BigDecimal("-2.00"), 2)).isEqualTo(-200L);
        assertThat(encodeShortScaledValue(new BigDecimal("-2.13"), 2)).isEqualTo(-213L);
        assertThat(encodeShortScaledValue(new BigDecimal("-2"), 2)).isEqualTo(-200L);
    }

    @Test
    public void testEncodeScaledValue()
    {
        assertThat(encodeScaledValue(new BigDecimal("2.00"), 2)).isEqualTo(Int128.valueOf(200));
        assertThat(encodeScaledValue(new BigDecimal("2.13"), 2)).isEqualTo(Int128.valueOf(213));
        assertThat(encodeScaledValue(new BigDecimal("172.60"), 2)).isEqualTo(Int128.valueOf(17260));
        assertThat(encodeScaledValue(new BigDecimal("2"), 2)).isEqualTo(Int128.valueOf(200));
        assertThat(encodeScaledValue(new BigDecimal("172.6"), 2)).isEqualTo(Int128.valueOf(17260));

        assertThat(encodeScaledValue(new BigDecimal("-2.00"), 2)).isEqualTo(Int128.valueOf(-200));
        assertThat(encodeScaledValue(new BigDecimal("-2.13"), 2)).isEqualTo(Int128.valueOf(-213));
        assertThat(encodeScaledValue(new BigDecimal("-2"), 2)).isEqualTo(Int128.valueOf(-200));
        assertThat(encodeScaledValue(new BigDecimal("-172.60"), 2)).isEqualTo(Int128.valueOf(-17260));
    }

    @Test
    public void testOverflows()
    {
        assertThat(overflows(Int128.valueOf("100"), 2)).isTrue();
        assertThat(overflows(Int128.valueOf("-100"), 2)).isTrue();
        assertThat(overflows(Int128.valueOf("99"), 2)).isFalse();
        assertThat(overflows(Int128.valueOf("-99"), 2)).isFalse();
    }

    private void assertParseResult(String value, Object expectedObject, int expectedPrecision, int expectedScale)
    {
        assertThat(Decimals.parse(value)).isEqualTo(new DecimalParseResult(
                expectedObject,
                createDecimalType(expectedPrecision, expectedScale)));
    }

    private void assertParseFailure(String text)
    {
        try {
            Decimals.parse(text);
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = format("Invalid DECIMAL value '%s'", text);
            assertThat(e.getMessage())
                    .withFailMessage(() -> format("Unexpected exception, exception with message '%s' was expected", expectedMessage))
                    .isEqualTo(expectedMessage);
            return;
        }
        throw new AssertionError("Parse failure was expected");
    }
}
