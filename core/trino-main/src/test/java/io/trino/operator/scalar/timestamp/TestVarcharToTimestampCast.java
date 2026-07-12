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
package io.trino.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.trino.spi.type.LongTimestamp;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cast reads {@code yyyy-MM-dd[ HH:mm[:ss[.fraction]]]} straight from the bytes, and falls back
 * to the general pattern for everything else. The pattern based path is unchanged, so it is the
 * oracle: for every value the two must agree, on the result and on the failure.
 */
public class TestVarcharToTimestampCast
{
    private static final String[] VALUES = {
            // the shape the byte parser handles
            "2020-01-01",
            "2020-1-1",
            "2020-01-01 12:34",
            "2020-01-01 12:34:56",
            "2020-01-01 1:2:3",
            "2020-01-01 12:34:56.1",
            "2020-01-01 12:34:56.123",
            "2020-01-01 12:34:56.123456",
            "2020-01-01 12:34:56.123456789",
            "2020-01-01 12:34:56.123456789012",
            // rounding of a fraction longer than the precision
            "2020-01-01 12:34:56.9999999",
            "2020-01-01 12:34:56.5",
            "1970-01-01 00:00:00",
            "1969-12-31 23:59:59.999999",
            "0001-01-01 00:00:00",
            "99999-01-01",
            "999999999-12-31",
            // leap day
            "2020-02-29 00:00:00",
            // a fraction long enough to overflow the pattern based path's Long.parseLong
            "2020-01-01 12:34:56.1234567890123456789012",

            // shapes the byte parser must hand back to the pattern
            "+2020-01-01",
            "-2020-01-01",
            "2020-01-01 12:34:56 UTC",
            "2020-01-01 12:34:56 America/New_York",
            "2020-01-01 12:34:56+05:00",
            "2020-01-01 12:34:56 +05:00",
            "2020-01-01 UTC",
            "2020-01-01 12:34:56   UTC",
            "2020-01-01  12:34:56",

            // invalid, both paths must fail the same way
            "",
            "not a timestamp",
            "2020-13-01",
            "2020-01-32",
            "2020-02-30",
            "2021-02-29",
            "2020-01-01 25:00:00",
            "2020-01-01 12:60:00",
            "2020-01-01 12:00:61",
            "2020-01-01T12:00:00",
            "202-01-01",
            "2020-01",
            "2020-01-01 12",
            "2020-01-01 12:",
            "2020-01-01 12:34:",
            "2020-01-01 12:34:56.",
            "2020-01-01-12:34:56",
            "abcd-01-01",
    };

    @Test
    public void testShortTimestampMatchesPattern()
    {
        for (int precision = 0; precision <= 6; precision++) {
            for (String value : VALUES) {
                assertShortMatchesPattern(precision, value);
            }
        }
    }

    @Test
    public void testLongTimestampMatchesPattern()
    {
        for (int precision = 7; precision <= 12; precision++) {
            for (String value : VALUES) {
                assertLongMatchesPattern(precision, value);
            }
        }
    }

    @Test
    public void testRandomValuesMatchPattern()
    {
        Random random = new Random(4711);
        for (int i = 0; i < 20_000; i++) {
            String value = randomTimestamp(random);
            assertShortMatchesPattern(random.nextInt(7), value);
            assertLongMatchesPattern(7 + random.nextInt(6), value);
        }
    }

    private static String randomTimestamp(Random random)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("%04d-%d-%d".formatted(random.nextInt(1, 10000), random.nextInt(0, 15), random.nextInt(0, 35)));
        if (random.nextBoolean()) {
            builder.append(' ');
            builder.append("%d:%d".formatted(random.nextInt(0, 26), random.nextInt(0, 62)));
            if (random.nextBoolean()) {
                builder.append(":%d".formatted(random.nextInt(0, 62)));
                if (random.nextBoolean()) {
                    builder.append('.');
                    int digits = 1 + random.nextInt(14);
                    for (int i = 0; i < digits; i++) {
                        builder.append((char) ('0' + random.nextInt(10)));
                    }
                }
            }
        }
        return builder.toString();
    }

    private static void assertShortMatchesPattern(int precision, String value)
    {
        Slice slice = utf8Slice(value);
        String context = "precision=%s value='%s'".formatted(precision, value);

        Long expected = null;
        RuntimeException expectedFailure = null;
        try {
            expected = VarcharToTimestampCast.castToShortTimestamp(precision, value);
        }
        catch (RuntimeException e) {
            expectedFailure = e;
        }

        if (expectedFailure != null) {
            RuntimeException failure = expectedFailure;
            assertThatThrownBy(() -> VarcharToTimestampCast.castToShortTimestamp(precision, slice))
                    .describedAs(context)
                    .isInstanceOf(failure.getClass())
                    .hasMessage(failure.getMessage());
        }
        else {
            assertThat(VarcharToTimestampCast.castToShortTimestamp(precision, slice))
                    .describedAs(context)
                    .isEqualTo(expected);
        }
    }

    private static void assertLongMatchesPattern(int precision, String value)
    {
        Slice slice = utf8Slice(value);
        String context = "precision=%s value='%s'".formatted(precision, value);

        LongTimestamp expected = null;
        RuntimeException expectedFailure = null;
        try {
            expected = VarcharToTimestampCast.castToLongTimestamp(precision, value);
        }
        catch (RuntimeException e) {
            expectedFailure = e;
        }

        if (expectedFailure != null) {
            RuntimeException failure = expectedFailure;
            assertThatThrownBy(() -> VarcharToTimestampCast.castToLongTimestamp(precision, slice))
                    .describedAs(context)
                    .isInstanceOf(failure.getClass())
                    .hasMessage(failure.getMessage());
        }
        else {
            assertThat(VarcharToTimestampCast.castToLongTimestamp(precision, slice))
                    .describedAs(context)
                    .isEqualTo(expected);
        }
    }
}
