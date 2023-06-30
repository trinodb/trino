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
package io.trino.operator.scalar.timestamptz;

import io.trino.Session;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;

import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.DateTimes.MILLISECONDS_PER_SECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTimestampWithTimeZone
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testLiterals()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2020, 5, 1, 12, 34, 56, 0, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(1))
                .isEqualTo(timestampWithTimeZone(1, 2020, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(2))
                .isEqualTo(timestampWithTimeZone(2, 2020, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(3))
                .isEqualTo(timestampWithTimeZone(3, 2020, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(4))
                .isEqualTo(timestampWithTimeZone(4, 2020, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(5))
                .isEqualTo(timestampWithTimeZone(5, 2020, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(6))
                .isEqualTo(timestampWithTimeZone(6, 2020, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(7))
                .isEqualTo(timestampWithTimeZone(7, 2020, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(8))
                .isEqualTo(timestampWithTimeZone(8, 2020, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(9))
                .isEqualTo(timestampWithTimeZone(9, 2020, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(10))
                .isEqualTo(timestampWithTimeZone(10, 2020, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(11))
                .isEqualTo(timestampWithTimeZone(11, 2020, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'"))
                .hasType(createTimestampWithTimeZoneType(12))
                .isEqualTo(timestampWithTimeZone(12, 2020, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("Asia/Kathmandu")));

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890123 Asia/Kathmandu'").evaluate())
                .hasMessage("line 1:12: TIMESTAMP WITH TIME ZONE precision must be in range [0, 12]: 13");

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP '2020-13-01 Asia/Kathmandu'").evaluate())
                .hasMessage("line 1:12: '2020-13-01 Asia/Kathmandu' is not a valid TIMESTAMP literal");

        // negative epoch
        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(0, 1500, 5, 1, 12, 34, 56, 0, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(1, 1500, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(2, 1500, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(3, 1500, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1234 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(4, 1500, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12345 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(5, 1500, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123456 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(6, 1500, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1234567 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(7, 1500, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12345678 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(8, 1500, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123456789 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(9, 1500, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1234567890 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(10, 1500, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12345678901 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(11, 1500, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123456789012 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(12, 1500, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("Asia/Kathmandu")));

        // 5-digit year
        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(0, 12001, 5, 1, 12, 34, 56, 0, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.1 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(1, 12001, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.12 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(2, 12001, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.123 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(3, 12001, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.1234 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(4, 12001, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.12345 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(5, 12001, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.123456 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(6, 12001, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.1234567 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(7, 12001, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.12345678 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(8, 12001, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.123456789 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(9, 12001, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.1234567890 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(10, 12001, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.12345678901 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(11, 12001, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '12001-05-01 12:34:56.123456789012 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(12, 12001, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("Asia/Kathmandu")));

        // 5-digit year with + sign
        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(0, 12001, 5, 1, 12, 34, 56, 0, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.1 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(1, 12001, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.12 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(2, 12001, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.123 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(3, 12001, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.1234 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(4, 12001, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.12345 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(5, 12001, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.123456 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(6, 12001, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.1234567 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(7, 12001, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.12345678 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(8, 12001, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.123456789 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(9, 12001, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.1234567890 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(10, 12001, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.12345678901 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(11, 12001, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '+12001-05-01 12:34:56.123456789012 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(12, 12001, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("Asia/Kathmandu")));

        // 5-digit year with - sign
        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(0, -12001, 5, 1, 12, 34, 56, 0, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.1 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(1, -12001, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.12 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(2, -12001, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.123 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(3, -12001, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.1234 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(4, -12001, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.12345 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(5, -12001, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.123456 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(6, -12001, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.1234567 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(7, -12001, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.12345678 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(8, -12001, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.123456789 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(9, -12001, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.1234567890 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(10, -12001, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.12345678901 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(11, -12001, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("Asia/Kathmandu")));

        assertThat(assertions.expression("TIMESTAMP '-12001-05-01 12:34:56.123456789012 Asia/Kathmandu'"))
                .isEqualTo(timestampWithTimeZone(12, -12001, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("Asia/Kathmandu")));

        // negative offset hour
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2020, 5, 1, 12, 34, 56, 0, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(1))
                .isEqualTo(timestampWithTimeZone(1, 2020, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(2))
                .isEqualTo(timestampWithTimeZone(2, 2020, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(3))
                .isEqualTo(timestampWithTimeZone(3, 2020, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(4))
                .isEqualTo(timestampWithTimeZone(4, 2020, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(5))
                .isEqualTo(timestampWithTimeZone(5, 2020, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(6))
                .isEqualTo(timestampWithTimeZone(6, 2020, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(7))
                .isEqualTo(timestampWithTimeZone(7, 2020, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(8))
                .isEqualTo(timestampWithTimeZone(8, 2020, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(9))
                .isEqualTo(timestampWithTimeZone(9, 2020, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(10))
                .isEqualTo(timestampWithTimeZone(10, 2020, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(11))
                .isEqualTo(timestampWithTimeZone(11, 2020, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("-08:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 -08:35'"))
                .hasType(createTimestampWithTimeZoneType(12))
                .isEqualTo(timestampWithTimeZone(12, 2020, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("-08:35")));

        // negative offset minute
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2020, 5, 1, 12, 34, 56, 0, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(1))
                .isEqualTo(timestampWithTimeZone(1, 2020, 5, 1, 12, 34, 56, 100_000_000_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(2))
                .isEqualTo(timestampWithTimeZone(2, 2020, 5, 1, 12, 34, 56, 120_000_000_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(3))
                .isEqualTo(timestampWithTimeZone(3, 2020, 5, 1, 12, 34, 56, 123_000_000_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(4))
                .isEqualTo(timestampWithTimeZone(4, 2020, 5, 1, 12, 34, 56, 123_400_000_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(5))
                .isEqualTo(timestampWithTimeZone(5, 2020, 5, 1, 12, 34, 56, 123_450_000_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(6))
                .isEqualTo(timestampWithTimeZone(6, 2020, 5, 1, 12, 34, 56, 123_456_000_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(7))
                .isEqualTo(timestampWithTimeZone(7, 2020, 5, 1, 12, 34, 56, 123_456_700_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(8))
                .isEqualTo(timestampWithTimeZone(8, 2020, 5, 1, 12, 34, 56, 123_456_780_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(9))
                .isEqualTo(timestampWithTimeZone(9, 2020, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(10))
                .isEqualTo(timestampWithTimeZone(10, 2020, 5, 1, 12, 34, 56, 123_456_789_000L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(11))
                .isEqualTo(timestampWithTimeZone(11, 2020, 5, 1, 12, 34, 56, 123_456_789_010L, getTimeZoneKey("-00:35")));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012 -00:35'"))
                .hasType(createTimestampWithTimeZoneType(12))
                .isEqualTo(timestampWithTimeZone(12, 2020, 5, 1, 12, 34, 56, 123_456_789_012L, getTimeZoneKey("-00:35")));

        // others
        assertThat(assertions.expression("TIMESTAMP '2001-01-02 +07:09'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 0, 0, 0, 0, getTimeZoneKey("+07:09")));

        assertThat(assertions.expression("TIMESTAMP '2001-1-2 3:4:5.321+07:09'"))
                .hasType(createTimestampWithTimeZoneType(3))
                .isEqualTo(timestampWithTimeZone(3, 2001, 1, 2, 3, 4, 5, 321_000_000_000L, getTimeZoneKey("+07:09")));

        assertThat(assertions.expression("TIMESTAMP '2001-1-2 3:4:5+07:09'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 3, 4, 5, 0, getTimeZoneKey("+07:09")));

        assertThat(assertions.expression("TIMESTAMP '2001-1-2 3:4+07:09'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 3, 4, 0, 0, getTimeZoneKey("+07:09")));

        assertThat(assertions.expression("TIMESTAMP '2001-1-2 +07:09'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 0, 0, 0, 0, getTimeZoneKey("+07:09")));

        assertThat(assertions.expression("TIMESTAMP '2001-01-02 03:04:05.321 Europe/Berlin'"))
                .hasType(createTimestampWithTimeZoneType(3))
                .isEqualTo(timestampWithTimeZone(3, 2001, 1, 2, 3, 4, 5, 321_000_000_000L, getTimeZoneKey("Europe/Berlin")));

        assertThat(assertions.expression("TIMESTAMP '2001-01-02 03:04:05 Europe/Berlin'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 3, 4, 5, 0, getTimeZoneKey("Europe/Berlin")));

        assertThat(assertions.expression("TIMESTAMP '2001-01-02 03:04 Europe/Berlin'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 3, 4, 0, 0, getTimeZoneKey("Europe/Berlin")));

        assertThat(assertions.expression("TIMESTAMP '2001-01-02 Europe/Berlin'"))
                .hasType(createTimestampWithTimeZoneType(0))
                .isEqualTo(timestampWithTimeZone(0, 2001, 1, 2, 0, 0, 0, 0, getTimeZoneKey("Europe/Berlin")));

        // Overflow
        assertTrinoExceptionThrownBy(() -> assertions.expression("TIMESTAMP '123001-01-02 03:04:05.321 Europe/Berlin'").evaluate())
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '123001-01-02 03:04:05.321 Europe/Berlin' is not a valid TIMESTAMP literal");

        assertTrinoExceptionThrownBy(() -> assertions.expression("TIMESTAMP '+123001-01-02 03:04:05.321 Europe/Berlin'").evaluate())
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '+123001-01-02 03:04:05.321 Europe/Berlin' is not a valid TIMESTAMP literal");

        assertTrinoExceptionThrownBy(() -> assertions.expression("TIMESTAMP '-123001-01-02 03:04:05.321 Europe/Berlin'").evaluate())
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '-123001-01-02 03:04:05.321 Europe/Berlin' is not a valid TIMESTAMP literal");

        // missing space after day
        assertTrinoExceptionThrownBy(() -> assertions.expression("TIMESTAMP '2020-13-01-12'").evaluate())
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '2020-13-01-12' is not a valid TIMESTAMP literal");
    }

    @Test
    public void testPlusInterval()
    {
        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'", "INTERVAL '3' hour"))
                .matches("TIMESTAMP '2001-01-22 06:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' hour", "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'"))
                .matches("TIMESTAMP '2001-01-22 06:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'", "INTERVAL '3' day"))
                .matches("TIMESTAMP '2001-01-25 03:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' day", "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'"))
                .matches("TIMESTAMP '2001-01-25 03:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'", "INTERVAL '3' month"))
                .matches("TIMESTAMP '2001-04-22 03:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' month", "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'"))
                .matches("TIMESTAMP '2001-04-22 03:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'", "INTERVAL '3' year"))
                .matches("TIMESTAMP '2004-01-22 03:04:05.321 +05:09'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' year", "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'"))
                .matches("TIMESTAMP '2004-01-22 03:04:05.321 +05:09'");
    }

    @Test
    public void testMinusInterval()
    {
        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'", "INTERVAL '3' day"))
                .matches("TIMESTAMP '2001-01-19 03:04:05.321 +05:09'");

        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2001-1-22 03:04:05.321 +05:09'", "INTERVAL '3' month"))
                .matches("TIMESTAMP '2000-10-22 03:04:05.321 +05:09'");
    }

    @Test
    public void testCurrentTimestamp()
    {
        // round down
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 111111111, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("current_timestamp(0)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(1)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(2)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(3)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(4)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(5)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(6)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(7)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(8)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(9)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(10)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111110 Pacific/Apia'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_timestamp(11)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111100 Pacific/Apia'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_timestamp(12)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111000 Pacific/Apia'"); // Java instant provides p = 9 precision

        // round up
        session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 555555555, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("current_timestamp(0)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(1)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(2)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(3)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(4)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(5)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(6)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(7)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(8)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(9)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555555 Pacific/Apia'");
        assertThat(assertions.expression("current_timestamp(10)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555555550 Pacific/Apia'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_timestamp(11)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555555500 Pacific/Apia'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_timestamp(12)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555555000 Pacific/Apia'"); // Java instant provides p = 9 precision
    }

    @Test
    public void testCastToDate()
    {
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' AS DATE)")).matches("DATE '2020-05-01'");
    }

    @Test
    public void testCastFromDate()
    {
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.0 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.00 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.0000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.00000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000000 Pacific/Apia'");
    }

    @Test
    public void testCastToTime()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.12'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.123'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456789123'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.10'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.100'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.120'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIME(12))")).matches("TIME '12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.1'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.11'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.11111111111'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(0))")).matches("TIME '12:34:57'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(1))")).matches("TIME '12:34:56.6'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(2))")).matches("TIME '12:34:56.56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(3))")).matches("TIME '12:34:56.556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(4))")).matches("TIME '12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(5))")).matches("TIME '12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(6))")).matches("TIME '12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(7))")).matches("TIME '12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(8))")).matches("TIME '12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(9))")).matches("TIME '12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(10))")).matches("TIME '12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(11))")).matches("TIME '12:34:56.55555555556'");

        // round up, wrap-around
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(0))")).matches("TIME '00:00:00'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(1))")).matches("TIME '00:00:00.0'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(2))")).matches("TIME '00:00:00.00'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(3))")).matches("TIME '00:00:00.000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(4))")).matches("TIME '00:00:00.0000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(5))")).matches("TIME '00:00:00.00000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(6))")).matches("TIME '00:00:00.000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(7))")).matches("TIME '00:00:00.0000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(8))")).matches("TIME '00:00:00.00000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(9))")).matches("TIME '00:00:00.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(9))")).matches("TIME '00:00:00.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(9))")).matches("TIME '00:00:00.000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(10))")).matches("TIME '00:00:00.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(10))")).matches("TIME '00:00:00.0000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(11))")).matches("TIME '00:00:00.00000000000'");

        // timestamp before and after daylight savings change
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45 America/Los_Angeles' AS TIME(0))")).matches("TIME '01:23:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1 America/Los_Angeles' AS TIME(1))")).matches("TIME '01:23:45.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12 America/Los_Angeles' AS TIME(2))")).matches("TIME '01:23:45.12'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123 America/Los_Angeles' AS TIME(3))")).matches("TIME '01:23:45.123'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234 America/Los_Angeles' AS TIME(4))")).matches("TIME '01:23:45.1234'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345 America/Los_Angeles' AS TIME(5))")).matches("TIME '01:23:45.12345'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456 America/Los_Angeles' AS TIME(6))")).matches("TIME '01:23:45.123456'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567 America/Los_Angeles' AS TIME(7))")).matches("TIME '01:23:45.1234567'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678 America/Los_Angeles' AS TIME(8))")).matches("TIME '01:23:45.12345678'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789 America/Los_Angeles' AS TIME(9))")).matches("TIME '01:23:45.123456789'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567891 America/Los_Angeles' AS TIME(10))")).matches("TIME '01:23:45.1234567891'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678912 America/Los_Angeles' AS TIME(11))")).matches("TIME '01:23:45.12345678912'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789123 America/Los_Angeles' AS TIME(12))")).matches("TIME '01:23:45.123456789123'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45 America/Los_Angeles' AS TIME(0))")).matches("TIME '03:23:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1 America/Los_Angeles' AS TIME(1))")).matches("TIME '03:23:45.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12 America/Los_Angeles' AS TIME(2))")).matches("TIME '03:23:45.12'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123 America/Los_Angeles' AS TIME(3))")).matches("TIME '03:23:45.123'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1234 America/Los_Angeles' AS TIME(4))")).matches("TIME '03:23:45.1234'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12345 America/Los_Angeles' AS TIME(5))")).matches("TIME '03:23:45.12345'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123456 America/Los_Angeles' AS TIME(6))")).matches("TIME '03:23:45.123456'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1234567 America/Los_Angeles' AS TIME(7))")).matches("TIME '03:23:45.1234567'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12345678 America/Los_Angeles' AS TIME(8))")).matches("TIME '03:23:45.12345678'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123456789 America/Los_Angeles' AS TIME(9))")).matches("TIME '03:23:45.123456789'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1234567891 America/Los_Angeles' AS TIME(10))")).matches("TIME '03:23:45.1234567891'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12345678912 America/Los_Angeles' AS TIME(11))")).matches("TIME '03:23:45.12345678912'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123456789123 America/Los_Angeles' AS TIME(12))")).matches("TIME '03:23:45.123456789123'");
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.12+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.123+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1234+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12345+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123456+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567891+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678912+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789123+05:45'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.0000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.00000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.0000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.00000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.000000000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.10+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.100+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.10000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.100000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.10000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.100000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.10000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.100000000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.120+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1200+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.120000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1200000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.120000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1200000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.120000000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1230+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12300+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1230000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12300000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1230000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12300000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123000000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12340+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123400+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12340000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123400000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12340000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123400000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123450+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234500+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123450000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234500000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123450000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234560+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345600+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234560000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345600000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345670+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456700+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345670000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456700000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456780+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567800+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456780000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567890+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678900+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678910+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789100+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789120+05:45'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111111+05:45'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.55555555556+05:45'");

        // round up, wrap-around
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '00:00:00.000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '00:00:00.000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(9) WITH TIME ZONE)")).matches("TIME '00:00:00.000000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000000+05:45'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(10) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000000+05:45'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999 Asia/Kathmandu' AS TIME(11) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000000+05:45'");

        // timestamp before and after daylight savings change
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45 America/Los_Angeles' AS TIME(0) WITH TIME ZONE)")).matches("TIME '01:23:45-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1 America/Los_Angeles' AS TIME(1) WITH TIME ZONE)")).matches("TIME '01:23:45.1-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12 America/Los_Angeles' AS TIME(2) WITH TIME ZONE)")).matches("TIME '01:23:45.12-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123 America/Los_Angeles' AS TIME(3) WITH TIME ZONE)")).matches("TIME '01:23:45.123-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234 America/Los_Angeles' AS TIME(4) WITH TIME ZONE)")).matches("TIME '01:23:45.1234-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345 America/Los_Angeles' AS TIME(5) WITH TIME ZONE)")).matches("TIME '01:23:45.12345-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456 America/Los_Angeles' AS TIME(6) WITH TIME ZONE)")).matches("TIME '01:23:45.123456-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567 America/Los_Angeles' AS TIME(7) WITH TIME ZONE)")).matches("TIME '01:23:45.1234567-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678 America/Los_Angeles' AS TIME(8) WITH TIME ZONE)")).matches("TIME '01:23:45.12345678-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789 America/Los_Angeles' AS TIME(9) WITH TIME ZONE)")).matches("TIME '01:23:45.123456789-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567891 America/Los_Angeles' AS TIME(10) WITH TIME ZONE)")).matches("TIME '01:23:45.1234567891-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678912 America/Los_Angeles' AS TIME(11) WITH TIME ZONE)")).matches("TIME '01:23:45.12345678912-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789123 America/Los_Angeles' AS TIME(12) WITH TIME ZONE)")).matches("TIME '01:23:45.123456789123-08:00'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45 America/Los_Angeles' AS TIME(0) WITH TIME ZONE)")).matches("TIME '03:23:45-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1 America/Los_Angeles' AS TIME(1) WITH TIME ZONE)")).matches("TIME '03:23:45.1-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12 America/Los_Angeles' AS TIME(2) WITH TIME ZONE)")).matches("TIME '03:23:45.12-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123 America/Los_Angeles' AS TIME(3) WITH TIME ZONE)")).matches("TIME '03:23:45.123-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1234 America/Los_Angeles' AS TIME(4) WITH TIME ZONE)")).matches("TIME '03:23:45.1234-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12345 America/Los_Angeles' AS TIME(5) WITH TIME ZONE)")).matches("TIME '03:23:45.12345-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123456 America/Los_Angeles' AS TIME(6) WITH TIME ZONE)")).matches("TIME '03:23:45.123456-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1234567 America/Los_Angeles' AS TIME(7) WITH TIME ZONE)")).matches("TIME '03:23:45.1234567-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12345678 America/Los_Angeles' AS TIME(8) WITH TIME ZONE)")).matches("TIME '03:23:45.12345678-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123456789 America/Los_Angeles' AS TIME(9) WITH TIME ZONE)")).matches("TIME '03:23:45.123456789-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.1234567891 America/Los_Angeles' AS TIME(10) WITH TIME ZONE)")).matches("TIME '03:23:45.1234567891-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.12345678912 America/Los_Angeles' AS TIME(11) WITH TIME ZONE)")).matches("TIME '03:23:45.12345678912-08:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 03:23:45.123456789123 America/Los_Angeles' AS TIME(12) WITH TIME ZONE)")).matches("TIME '03:23:45.123456789123-08:00'");

        // negative offset
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45 -08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '01:23:45-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1 -08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '01:23:45.1-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12 -08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '01:23:45.12-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123 -08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '01:23:45.123-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234 -08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '01:23:45.1234-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345 -08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '01:23:45.12345-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456 -08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '01:23:45.123456-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567 -08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '01:23:45.1234567-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678 -08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '01:23:45.12345678-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789 -08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '01:23:45.123456789-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567891 -08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '01:23:45.1234567891-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678912 -08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '01:23:45.12345678912-08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789123 -08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '01:23:45.123456789123-08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45 -00:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '01:23:45-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1 -00:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '01:23:45.1-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12 -00:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '01:23:45.12-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123 -00:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '01:23:45.123-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234 -00:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '01:23:45.1234-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345 -00:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '01:23:45.12345-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456 -00:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '01:23:45.123456-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567 -00:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '01:23:45.1234567-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678 -00:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '01:23:45.12345678-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789 -00:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '01:23:45.123456789-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.1234567891 -00:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '01:23:45.1234567891-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.12345678912 -00:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '01:23:45.12345678912-00:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-03-08 01:23:45.123456789123 -00:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '01:23:45.123456789123-00:35'");
    }

    @Test
    public void testCastToTimestamp()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.12'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.123'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9 UTC' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.9'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99 UTC' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.99'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999 UTC' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999 UTC' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999 UTC' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999 UTC' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999999 UTC' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999999 UTC' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999999 UTC' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999999999 UTC' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999999999 UTC' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999999999 UTC' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999999999'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.9'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.99'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999999 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999999 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999999 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999999999 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999999999 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999999999 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999999999'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.10'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.100'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.120'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.4 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.94 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.9'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.994 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.99'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9994 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99994 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999994 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999994 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999994 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999994 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999999994 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999999994 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.9999999999'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999999994 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.99999999999'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5 Asia/Kathmandu' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.95 Asia/Kathmandu' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:57.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.995 Asia/Kathmandu' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:57.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9995 Asia/Kathmandu' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:57.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99995 Asia/Kathmandu' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:57.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999995 Asia/Kathmandu' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:57.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999995 Asia/Kathmandu' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:57.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999995 Asia/Kathmandu' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:57.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999995 Asia/Kathmandu' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:57.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.9999999995 Asia/Kathmandu' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:57.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.99999999995 Asia/Kathmandu' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:57.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.999999999995 Asia/Kathmandu' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:57.00000000000'");
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1234 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12345 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123456 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1234567 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12345678 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123456789 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1234567890 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12345678901 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123456789012 Asia/Kathmandu");

        // negative epoch
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1234 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1234 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12345 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12345 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123456 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123456 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1234567 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1234567 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12345678 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12345678 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123456789 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123456789 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1234567890 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1234567890 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12345678901 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12345678901 Asia/Kathmandu");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123456789012 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123456789012 Asia/Kathmandu");

        // 5-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '12001-05-01 12:34:56 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("+12001-05-01 12:34:56 Asia/Kathmandu");

        // 5-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-12001-05-01 12:34:56 Asia/Kathmandu' AS VARCHAR)")).isEqualTo("-12001-05-01 12:34:56 Asia/Kathmandu");
    }

    @Test
    public void testCastFromVarchar()
    {
        // round down
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111111 Asia/Kathmandu'");

        // round up
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555555555555 Asia/Kathmandu'");

        // 5-digit year
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111111111111 Asia/Kathmandu'");

        // 5-digit year with + sign
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.1111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.11111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('+12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56.111111111111 Asia/Kathmandu'");

        // 5-digit year with - sign
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.11 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.1111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.11111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.1111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.11111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.1111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.11111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST('-12001-05-01 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56.111111111111 Asia/Kathmandu'");
    }

    @Test
    public void testLowerDigitsZeroed()
    {
        // round down
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.000000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111111000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111111111000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE) AS TIMESTAMP(6) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE) AS TIMESTAMP(6) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE) AS TIMESTAMP(6) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE) AS TIMESTAMP(3) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.000 Asia/Kathmandu'");

        // round up
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:57.000000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.556000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.555556000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(9) WITH TIME ZONE) AS TIMESTAMP(12) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.555555556000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE) AS TIMESTAMP(6) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:57.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE) AS TIMESTAMP(6) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.556000 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE) AS TIMESTAMP(6) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE) AS TIMESTAMP(3) WITH TIME ZONE)"))
                .matches("TIMESTAMP '2020-05-10 12:34:57.000 Asia/Kathmandu'");
    }

    @Test
    public void testRoundDown()
    {
        // test positive epoch (year 2020) and negative epoch (year 1500)

        // long to short with millisecond representation at the p = 0 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56 Asia/Kathmandu'");

        // long to short with millisecond representation not at the boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.1 Asia/Kathmandu'");

        // long to short with millisecond representation at the p = 3 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.111 Asia/Kathmandu'");

        // long to short with microsecond representation not at the bounday
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.1111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.1111 Asia/Kathmandu'");

        // long to short with microsecond representation at the p = 6 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.111111 Asia/Kathmandu'");

        // long to long not at the boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111111 Asia/Kathmandu'");

        // short with microsecond to short with millisecond representation at the p = 0 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56 Asia/Kathmandu'");

        // short with microsecond representation to short with millisecond representation not at the boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.1 Asia/Kathmandu'");

        // short with microsecond representation to short with millisecond representation at the p = 3 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.111 Asia/Kathmandu'");

        // short with microsecond representation to short with microsecond representation not at the bounday
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.1111 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.1111 Asia/Kathmandu'");
    }

    @Test
    public void testRoundUp()
    {
        // test positive epoch (year 2020) and negative epoch (year 1500)

        // long to short with millisecond representation at the p = 0 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:57 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:57 Asia/Kathmandu'");

        // long to short with millisecond representation not at the boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.6 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.6 Asia/Kathmandu'");

        // long to short with millisecond representation at the p = 3 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.556 Asia/Kathmandu'");

        // long to short with microsecond representation not at the bounday
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.5556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.5556 Asia/Kathmandu'");

        // long to short with microsecond representation at the p = 6 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.555556 Asia/Kathmandu'");

        // long to long not at the boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.5555555556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.5555555556 Asia/Kathmandu'");

        // short with microsecond to short with millisecond representation at the p = 0 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:57 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:57 Asia/Kathmandu'");

        // short with microsecond representation to short with millisecond representation not at the boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.6 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.6 Asia/Kathmandu'");

        // short with microsecond representation to short with millisecond representation at the p = 3 boundary
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.556 Asia/Kathmandu'");

        // short with microsecond representation to short with microsecond representation not at the bounday
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-10 12:34:56.5556 Asia/Kathmandu'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555 Asia/Kathmandu' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '1500-05-10 12:34:56.5556 Asia/Kathmandu'");

        // overflow to next second
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.5 UTC' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.95 UTC' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.995 UTC' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.9995 UTC' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.99995 UTC' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999995 UTC' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.9999995 UTC' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.99999995 UTC' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999995 UTC' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.9999999995 UTC' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.000000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.99999999995 UTC' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999995 UTC' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000000 UTC'");

        // overflow to next second -- maximal precision
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.000000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000000 UTC'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000000 UTC'");
    }

    @Test
    public void testToIso8601()
    {
        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu')"))
                .hasType(createVarcharType(28))
                .isEqualTo("2020-05-01T12:34:56+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu')"))
                .hasType(createVarcharType(30))
                .isEqualTo("2020-05-01T12:34:56.1+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu')"))
                .hasType(createVarcharType(31))
                .isEqualTo("2020-05-01T12:34:56.12+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu')"))
                .hasType(createVarcharType(32))
                .isEqualTo("2020-05-01T12:34:56.123+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu')"))
                .hasType(createVarcharType(33))
                .isEqualTo("2020-05-01T12:34:56.1234+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu')"))
                .hasType(createVarcharType(34))
                .isEqualTo("2020-05-01T12:34:56.12345+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu')"))
                .hasType(createVarcharType(35))
                .isEqualTo("2020-05-01T12:34:56.123456+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu')"))
                .hasType(createVarcharType(36))
                .isEqualTo("2020-05-01T12:34:56.1234567+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu')"))
                .hasType(createVarcharType(37))
                .isEqualTo("2020-05-01T12:34:56.12345678+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu')"))
                .hasType(createVarcharType(38))
                .isEqualTo("2020-05-01T12:34:56.123456789+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu')"))
                .hasType(createVarcharType(39))
                .isEqualTo("2020-05-01T12:34:56.1234567890+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu')"))
                .hasType(createVarcharType(40))
                .isEqualTo("2020-05-01T12:34:56.12345678901+05:45");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu')"))
                .hasType(createVarcharType(41))
                .isEqualTo("2020-05-01T12:34:56.123456789012+05:45");

        // Following test will verify all precisions for timestamps fall in the DST gap
        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(28))
                .isEqualTo("2020-11-01T01:00:00-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.1 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(30))
                .isEqualTo("2020-11-01T01:00:00.1-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.12 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(31))
                .isEqualTo("2020-11-01T01:00:00.12-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.00 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(31))
                .isEqualTo("2020-11-01T01:00:00.00-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.123 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(32))
                .isEqualTo("2020-11-01T01:00:00.123-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.1234 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(33))
                .isEqualTo("2020-11-01T01:00:00.1234-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.12345 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(34))
                .isEqualTo("2020-11-01T01:00:00.12345-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.123456 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(35))
                .isEqualTo("2020-11-01T01:00:00.123456-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.1234567 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(36))
                .isEqualTo("2020-11-01T01:00:00.1234567-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.12345678 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(37))
                .isEqualTo("2020-11-01T01:00:00.12345678-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.123456789 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(38))
                .isEqualTo("2020-11-01T01:00:00.123456789-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.1234567890 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(39))
                .isEqualTo("2020-11-01T01:00:00.1234567890-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.12345678901 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(40))
                .isEqualTo("2020-11-01T01:00:00.12345678901-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.123456789012 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(41))
                .isEqualTo("2020-11-01T01:00:00.123456789012-06:00");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-11-01 07:00:00.000000000000 UTC' AT TIME ZONE 'America/Chicago')"))
                .hasType(createVarcharType(41))
                .isEqualTo("2020-11-01T01:00:00.000000000000-06:00");

        // Zulu offset
        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56 +00:00')"))
                .hasType(createVarcharType(28))
                .isEqualTo("2020-05-01T12:34:56Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1 +00:00')"))
                .hasType(createVarcharType(30))
                .isEqualTo("2020-05-01T12:34:56.1Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12 +00:00')"))
                .hasType(createVarcharType(31))
                .isEqualTo("2020-05-01T12:34:56.12Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123 +00:00')"))
                .hasType(createVarcharType(32))
                .isEqualTo("2020-05-01T12:34:56.123Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234 +00:00')"))
                .hasType(createVarcharType(33))
                .isEqualTo("2020-05-01T12:34:56.1234Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345 +00:00')"))
                .hasType(createVarcharType(34))
                .isEqualTo("2020-05-01T12:34:56.12345Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456 +00:00')"))
                .hasType(createVarcharType(35))
                .isEqualTo("2020-05-01T12:34:56.123456Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234567 +00:00')"))
                .hasType(createVarcharType(36))
                .isEqualTo("2020-05-01T12:34:56.1234567Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345678 +00:00')"))
                .hasType(createVarcharType(37))
                .isEqualTo("2020-05-01T12:34:56.12345678Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456789 +00:00')"))
                .hasType(createVarcharType(38))
                .isEqualTo("2020-05-01T12:34:56.123456789Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234567890 +00:00')"))
                .hasType(createVarcharType(39))
                .isEqualTo("2020-05-01T12:34:56.1234567890Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345678901 +00:00')"))
                .hasType(createVarcharType(40))
                .isEqualTo("2020-05-01T12:34:56.12345678901Z");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456789012 +00:00')"))
                .hasType(createVarcharType(41))
                .isEqualTo("2020-05-01T12:34:56.123456789012Z");
    }

    @Test
    public void testFormat()
    {
        // round down
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.100+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.110+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111100+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111110+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111100+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111110+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111111+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1111111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111111+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11111111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111111+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.111111111+05:45[Asia/Kathmandu]");

        // round up
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.500+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.550+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555500+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555550+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555500+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555550+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555555+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5555555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555556+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55555555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555556+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu')")).isEqualTo("2020-05-10T12:34:56.555555556+05:45[Asia/Kathmandu]");

        // 5-digit year in the future
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.5 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.500+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.55 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.550+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.5555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555500+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.55555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555550+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.5555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555500+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.55555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555550+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.555555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555555+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.5555555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555556+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.55555555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555556+05:45[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '12001-05-10 12:34:56.555555555555 Asia/Kathmandu')")).isEqualTo("+12001-05-10T12:34:56.555555556+05:45[Asia/Kathmandu]");

        // 5-digit year in the past
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.5 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.500+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.55 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.550+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.5555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555500+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.55555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555550+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.5555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555500+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.55555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555550+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.555555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555555+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.5555555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555556+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.55555555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555556+05:41:16[Asia/Kathmandu]");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-12001-05-10 12:34:56.555555555555 Asia/Kathmandu')")).isEqualTo("-12001-05-10T12:34:56.555555556+05:41:16[Asia/Kathmandu]");
    }

    @Test
    public void testFormatDateTime()
    {
        // format_datetime supports up to millisecond precision

        // round down
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.000 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.100 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.110 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1111111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11111111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.111 +0545");

        // round up
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.000 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.500 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.550 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.555 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5555555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55555555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu', 'yyyy-MM-dd HH:mm:ss.SSS Z')")).isEqualTo("2020-05-10 12:34:56.556 +0545");
    }

    @Test
    public void testDateFormat()
    {
        // date_format supports up to millisecond precision

        // round down
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.000000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.100000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.110000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1111111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11111111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111111111111 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");

        // round up
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.000000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.500000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.550000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.555000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5555555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55555555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555555555555 Asia/Kathmandu', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
    }

    @Test
    public void testDateDiff()
    {
        // date_diff truncates the fractional part

        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '1000'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2 Asia/Kathmandu')")).matches("BIGINT '1100'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22 Asia/Kathmandu')")).matches("BIGINT '1110'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2222222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22222222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222222222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2222222222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22222222222 Asia/Kathmandu')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222222222222 Asia/Kathmandu')")).matches("BIGINT '1111'");

        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '1000'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9 Asia/Kathmandu')")).matches("BIGINT '1800'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99 Asia/Kathmandu')")).matches("BIGINT '1880'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9999999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99999999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999999999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9999999999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99999999999 Asia/Kathmandu')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999999999999 Asia/Kathmandu')")).matches("BIGINT '1888'");

        // coarser unit
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '1'");

        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2222222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22222222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222222222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.2222222222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.22222222222 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.222222222222 Asia/Kathmandu')")).matches("BIGINT '1'");

        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9999999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99999999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999999999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.9999999999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.99999999999 Asia/Kathmandu')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111111 Asia/Kathmandu', TIMESTAMP '2020-05-10 12:34:56.999999999999 Asia/Kathmandu')")).matches("BIGINT '1'");
    }

    @Test
    public void testDateAdd()
    {
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.10 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.10 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.100 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.101 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1010 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.10000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.10100 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.100000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.101000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1000000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1010000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.10000000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.10100000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.100000000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.101000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1000000000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1010000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.10000000000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.10100000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.100000000000 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.101000000000 Asia/Kathmandu'");

        assertThat(assertions.expression("date_add('millisecond', 1000, TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:57 Asia/Kathmandu'");

        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.11 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.112 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.1121 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.11211 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.112111 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.1121111 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.11211111 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.112111111 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1111111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.1121111111 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11111111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.11211111111 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111111111111 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.112111111111 Asia/Kathmandu'");

        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.5 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.55 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.556 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.5565 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.55655 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.556555 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.5565555 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.55655555 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.556555555 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5555555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.5565555555 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55555555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.55655555555 Asia/Kathmandu'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555555555555 Asia/Kathmandu')")).matches("TIMESTAMP '1500-05-10 12:34:56.556555555555 Asia/Kathmandu'");
    }

    @Test
    public void testGreatest()
    {
        assertThat(assertions.function("greatest", "TIMESTAMP '2002-01-02 03:04:05.321 +07:09'", "TIMESTAMP '2001-01-02 01:04:05.321 +02:09'", "TIMESTAMP '2000-01-02 01:04:05.321 +02:09'"))
                .matches("TIMESTAMP '2002-01-02 03:04:05.321 +07:09'");

        assertThat(assertions.function("greatest", "TIMESTAMP '2001-01-02 03:04:05.321 +07:09'", "TIMESTAMP '2001-01-02 04:04:05.321 +10:09'"))
                .matches("TIMESTAMP '2001-01-02 03:04:05.321 +07:09'");
    }

    @Test
    public void testLeast()
    {
        assertThat(assertions.function("least", "TIMESTAMP '2001-01-02 03:04:05.321 +07:09'", "TIMESTAMP '2001-01-02 01:04:05.321 +02:09'", "TIMESTAMP '2002-01-02 01:04:05.321 +02:09'"))
                .matches("TIMESTAMP '2001-01-02 03:04:05.321 +07:09'");

        assertThat(assertions.function("least", "TIMESTAMP '2001-01-02 03:04:05.321 +07:09'", "TIMESTAMP '2001-01-02 01:04:05.321 +02:09'"))
                .matches("TIMESTAMP '2001-01-02 03:04:05.321 +07:09'");
    }

    @Test
    public void testTimeZoneHour()
    {
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.1 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.12 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.123 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.1234 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.12345 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.123456 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.1234567 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.12345678 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.123456789 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.1234567890 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.12345678901 +07:09')")).isEqualTo(7L);
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.123456789012 +07:09')")).isEqualTo(7L);
    }

    @Test
    public void testTimeZoneMinute()
    {
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.1 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.12 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.123 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.1234 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.12345 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.123456 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.1234567 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.12345678 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.123456789 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.1234567890 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.12345678901 +07:09')")).isEqualTo(9L);
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.123456789012 +07:09')")).isEqualTo(9L);
    }

    @Test
    public void testLastDayOfMonth()
    {
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu')")).matches("DATE '2020-05-31'");
    }

    @Test
    public void testToUnixTime()
    {
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("1589093396e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("1589093396.1e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("1589093396.12e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("1589093396.123e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("1589093396.1234e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("1589093396.1234498e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("1589093396.123456e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("1589093396.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("1589093396.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("1589093396.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("1589093396.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("1589093396.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("1589093396.1234567e0");
    }

    @Test
    public void testOrdering()
    {
        // short timestamp
        assertThat(assertions.query("" +
                                    "SELECT value FROM (VALUES " +
                                    "TIMESTAMP '2020-05-10 01:00:00 America/New_York', " +
                                    "TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles', " +
                                    "TIMESTAMP '2020-05-10 02:00:00 America/New_York', " +
                                    "TIMESTAMP '2020-05-10 02:00:00 America/Los_Angeles', " +
                                    "TIMESTAMP '2020-05-10 03:00:00 America/New_York', " +
                                    "TIMESTAMP '2020-05-10 03:00:00 America/Los_Angeles' " +
                                    ") t(value)" +
                                    "ORDER BY value"))
                .ordered()
                .matches("" +
                         "SELECT value FROM (VALUES " +
                         "TIMESTAMP '2020-05-10 01:00:00 America/New_York', " +
                         "TIMESTAMP '2020-05-10 02:00:00 America/New_York', " +
                         "TIMESTAMP '2020-05-10 03:00:00 America/New_York', " +
                         "TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles', " +
                         "TIMESTAMP '2020-05-10 02:00:00 America/Los_Angeles', " +
                         "TIMESTAMP '2020-05-10 03:00:00 America/Los_Angeles' " +
                         ") t(value)");

        // long timestamp
        assertThat(assertions.query("" +
                                    "SELECT value FROM (VALUES " +
                                    "TIMESTAMP '2020-05-10 01:00:00.000000 America/New_York', " +
                                    "TIMESTAMP '2020-05-10 01:00:00.000000 America/Los_Angeles', " +
                                    "TIMESTAMP '2020-05-10 02:00:00.000000 America/New_York', " +
                                    "TIMESTAMP '2020-05-10 02:00:00.000000 America/Los_Angeles', " +
                                    "TIMESTAMP '2020-05-10 03:00:00.000000 America/New_York', " +
                                    "TIMESTAMP '2020-05-10 03:00:00.000000 America/Los_Angeles' " +
                                    ") t(value)" +
                                    "ORDER BY value"))
                .ordered()
                .matches("" +
                         "SELECT value FROM (VALUES " +
                         "TIMESTAMP '2020-05-10 01:00:00.000000 America/New_York', " +
                         "TIMESTAMP '2020-05-10 02:00:00.000000 America/New_York', " +
                         "TIMESTAMP '2020-05-10 03:00:00.000000 America/New_York', " +
                         "TIMESTAMP '2020-05-10 01:00:00.000000 America/Los_Angeles', " +
                         "TIMESTAMP '2020-05-10 02:00:00.000000 America/Los_Angeles', " +
                         "TIMESTAMP '2020-05-10 03:00:00.000000 America/Los_Angeles' " +
                         ") t(value)");
    }

    @Test
    public void testJoin()
    {
        // short timestamp
        assertThat(assertions.query("" +
                                    "SELECT count(*) FROM (VALUES TIMESTAMP '2020-05-10 04:00:00 America/New_York') t(v) " +
                                    "JOIN (VALUES TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles') u(v) USING (v)"))
                .matches("VALUES BIGINT '1'");

        // long timestamp
        assertThat(assertions.query("" +
                                    "SELECT count(*) FROM (VALUES TIMESTAMP '2020-05-10 04:00:00.000000 America/New_York') t(v) " +
                                    "JOIN (VALUES TIMESTAMP '2020-05-10 01:00:00.000000 America/Los_Angeles') u(v) USING (v)"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testCastInvalidTimestamp()
    {
        assertThatThrownBy(() -> assertions.expression("CAST('ABC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: ABC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-00 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-00 00:00:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-00-01 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-00-01 00:00:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 25:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 25:00:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:61:00 UTC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:61:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:00:61 UTC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:00:61 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:00:00 ABC' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:00:00 ABC");

        assertThatThrownBy(() -> assertions.expression("CAST('ABC' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: ABC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-00 00:00:00 UTC' AS TIMESTAMP(12) WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-00 00:00:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-00-01 00:00:00 UTC' AS TIMESTAMP(12) WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-00-01 00:00:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 25:00:00 UTC' AS TIMESTAMP(12) WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 25:00:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:61:00 UTC' AS TIMESTAMP(12) WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:61:00 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:00:61 UTC' AS TIMESTAMP(12) WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:00:61 UTC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:00:00 ABC' AS TIMESTAMP(12) WITH TIME ZONE)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:00:00 ABC");
    }

    private BiFunction<Session, QueryRunner, Object> timestampWithTimeZone(int precision, int year, int month, int day, int hour, int minute, int second, long picoOfSecond, TimeZoneKey timeZoneKey)
    {
        return (session, queryRunner) -> {
            ZonedDateTime base = ZonedDateTime.of(year, month, day, hour, minute, second, 0, timeZoneKey.getZoneId());

            long epochMillis = base.toEpochSecond() * MILLISECONDS_PER_SECOND + picoOfSecond / PICOSECONDS_PER_MILLISECOND;
            int picosOfMilli = (int) (picoOfSecond % PICOSECONDS_PER_MILLISECOND);

            return SqlTimestampWithTimeZone.newInstance(precision, epochMillis, picosOfMilli, timeZoneKey);
        };
    }
}
