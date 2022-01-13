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

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Objects;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.overflows;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
    }

    @Test
    public void testParseIncludeLeadingZerosInPrecision()
    {
        assertParseResultIncludeLeadingZerosInPrecision("0", 0L, 1, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+0", 0L, 1, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-0", 0L, 1, 0);
        assertParseResultIncludeLeadingZerosInPrecision("00000000000000000", 0L, 17, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+00000000000000000", 0L, 17, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-00000000000000000", 0L, 17, 0);
        assertParseResultIncludeLeadingZerosInPrecision("1.1", 11L, 2, 1);
        assertParseResultIncludeLeadingZerosInPrecision("+1.1", 11L, 2, 1);
        assertParseResultIncludeLeadingZerosInPrecision("-1.1", -11L, 2, 1);
        assertParseResultIncludeLeadingZerosInPrecision("0001.1", 11L, 5, 1);
        assertParseResultIncludeLeadingZerosInPrecision("+0001.1", 11L, 5, 1);
        assertParseResultIncludeLeadingZerosInPrecision("-0001.1", -11L, 5, 1);
        assertParseResultIncludeLeadingZerosInPrecision("000", 0L, 3, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+000", 0L, 3, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-000", -0L, 3, 0);
        assertParseResultIncludeLeadingZerosInPrecision("000.1", 1L, 4, 1);
        assertParseResultIncludeLeadingZerosInPrecision("+000.1", 1L, 4, 1);
        assertParseResultIncludeLeadingZerosInPrecision("-000.1", -1L, 4, 1);
        assertParseResultIncludeLeadingZerosInPrecision("000000000000000000", 0L, 18, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+000000000000000000", 0L, 18, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-000000000000000000", 0L, 18, 0);
        assertParseResultIncludeLeadingZerosInPrecision("000000000000000000.123", Int128.valueOf("123"), 21, 3);
        assertParseResultIncludeLeadingZerosInPrecision("+000000000000000000.123", Int128.valueOf("123"), 21, 3);
        assertParseResultIncludeLeadingZerosInPrecision("-000000000000000000.123", Int128.valueOf("-123"), 21, 3);
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
        assertEquals(encodeShortScaledValue(new BigDecimal("2.00"), 2), 200L);
        assertEquals(encodeShortScaledValue(new BigDecimal("2.13"), 2), 213L);
        assertEquals(encodeShortScaledValue(new BigDecimal("172.60"), 2), 17260L);
        assertEquals(encodeShortScaledValue(new BigDecimal("2"), 2), 200L);
        assertEquals(encodeShortScaledValue(new BigDecimal("172.6"), 2), 17260L);

        assertEquals(encodeShortScaledValue(new BigDecimal("-2.00"), 2), -200L);
        assertEquals(encodeShortScaledValue(new BigDecimal("-2.13"), 2), -213L);
        assertEquals(encodeShortScaledValue(new BigDecimal("-2"), 2), -200L);
    }

    @Test
    public void testEncodeScaledValue()
    {
        assertEquals(encodeScaledValue(new BigDecimal("2.00"), 2), Int128.valueOf(200));
        assertEquals(encodeScaledValue(new BigDecimal("2.13"), 2), Int128.valueOf(213));
        assertEquals(encodeScaledValue(new BigDecimal("172.60"), 2), Int128.valueOf(17260));
        assertEquals(encodeScaledValue(new BigDecimal("2"), 2), Int128.valueOf(200));
        assertEquals(encodeScaledValue(new BigDecimal("172.6"), 2), Int128.valueOf(17260));

        assertEquals(encodeScaledValue(new BigDecimal("-2.00"), 2), Int128.valueOf(-200));
        assertEquals(encodeScaledValue(new BigDecimal("-2.13"), 2), Int128.valueOf(-213));
        assertEquals(encodeScaledValue(new BigDecimal("-2"), 2), Int128.valueOf(-200));
        assertEquals(encodeScaledValue(new BigDecimal("-172.60"), 2), Int128.valueOf(-17260));
    }

    @Test
    public void testOverflows()
    {
        assertTrue(overflows(Int128.valueOf("100"), 2));
        assertTrue(overflows(Int128.valueOf("-100"), 2));
        assertFalse(overflows(Int128.valueOf("99"), 2));
        assertFalse(overflows(Int128.valueOf("-99"), 2));
    }

    private void assertParseResult(String value, Object expectedObject, int expectedPrecision, int expectedScale)
    {
        assertEquals(Decimals.parse(value),
                new DecimalParseResult(
                        expectedObject,
                        createDecimalType(expectedPrecision, expectedScale)));
    }

    private void assertParseResultIncludeLeadingZerosInPrecision(String value, Object expectedObject, int expectedPrecision, int expectedScale)
    {
        assertEquals(Decimals.parseIncludeLeadingZerosInPrecision(value),
                new DecimalParseResult(
                        expectedObject,
                        createDecimalType(expectedPrecision, expectedScale)));
    }

    private void assertParseFailure(String text)
    {
        try {
            Decimals.parse(text);
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = format("Invalid decimal value '%s'", text);
            if (!Objects.equals(e.getMessage(), expectedMessage)) {
                fail(format("Unexpected exception, exception with message '%s' was expected", expectedMessage), e);
            }
            return;
        }
        fail("Parse failure was expected");
    }
}
