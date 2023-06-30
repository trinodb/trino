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

import io.trino.Session;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;

import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.DateTimes.MICROSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTimestamp
{
    private static final TimeZoneKey SESSION_TIME_ZONE = DEFAULT_TIME_ZONE_KEY;

    protected QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(SESSION_TIME_ZONE)
                .build();
        assertions = new QueryAssertions(session);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testToUnixTime()
    {
        // to_unixtime is defined for timestamp(p) with time zone, so here we test to_unixtime + required implicit casts
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56')")).matches("1589067296e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("1589067296.1e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("1589067296.12e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("1589067296.123e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("1589067296.1234e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("1589067296.1234498e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("1589067296.123456e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("1589067296.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("1589067296.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("1589067296.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("1589067296.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("1589067296.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("1589067296.1234567e0");
    }

    @Test
    public void testLiterals()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56'"))
                .hasType(createTimestampType(0))
                .isEqualTo(timestamp(0, 2020, 5, 1, 12, 34, 56, 0));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1'"))
                .hasType(createTimestampType(1))
                .isEqualTo(timestamp(1, 2020, 5, 1, 12, 34, 56, 100_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12'"))
                .hasType(createTimestampType(2))
                .isEqualTo(timestamp(2, 2020, 5, 1, 12, 34, 56, 120_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123'"))
                .hasType(createTimestampType(3))
                .isEqualTo(timestamp(3, 2020, 5, 1, 12, 34, 56, 123_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234'"))
                .hasType(createTimestampType(4))
                .isEqualTo(timestamp(4, 2020, 5, 1, 12, 34, 56, 123_400_000_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345'"))
                .hasType(createTimestampType(5))
                .isEqualTo(timestamp(5, 2020, 5, 1, 12, 34, 56, 123_450_000_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456'"))
                .hasType(createTimestampType(6))
                .isEqualTo(timestamp(6, 2020, 5, 1, 12, 34, 56, 123_456_000_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567'"))
                .hasType(createTimestampType(7))
                .isEqualTo(timestamp(7, 2020, 5, 1, 12, 34, 56, 123_456_700_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678'"))
                .hasType(createTimestampType(8))
                .isEqualTo(timestamp(8, 2020, 5, 1, 12, 34, 56, 123_456_780_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789'"))
                .hasType(createTimestampType(9))
                .isEqualTo(timestamp(9, 2020, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890'"))
                .hasType(createTimestampType(10))
                .isEqualTo(timestamp(10, 2020, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678901'"))
                .hasType(createTimestampType(11))
                .isEqualTo(timestamp(11, 2020, 5, 1, 12, 34, 56, 123_456_789_010L));

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789012'"))
                .hasType(createTimestampType(12))
                .isEqualTo(timestamp(12, 2020, 5, 1, 12, 34, 56, 123_456_789_012L));

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890123'").evaluate())
                .hasMessage("line 1:12: TIMESTAMP precision must be in range [0, 12]: 13");

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP '2020-13-01'").evaluate())
                .hasMessage("line 1:12: '2020-13-01' is not a valid TIMESTAMP literal");

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP 'xxx'").evaluate())
                .hasMessage("line 1:12: 'xxx' is not a valid TIMESTAMP literal");

        // negative epoch
        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56'"))
                .isEqualTo(timestamp(0, 1500, 5, 1, 12, 34, 56, 0));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1'"))
                .isEqualTo(timestamp(1, 1500, 5, 1, 12, 34, 56, 100_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12'"))
                .isEqualTo(timestamp(2, 1500, 5, 1, 12, 34, 56, 120_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123'"))
                .isEqualTo(timestamp(3, 1500, 5, 1, 12, 34, 56, 123_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1234'"))
                .isEqualTo(timestamp(4, 1500, 5, 1, 12, 34, 56, 123_400_000_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12345'"))
                .isEqualTo(timestamp(5, 1500, 5, 1, 12, 34, 56, 123_450_000_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123456'"))
                .isEqualTo(timestamp(6, 1500, 5, 1, 12, 34, 56, 123_456_000_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1234567'"))
                .isEqualTo(timestamp(7, 1500, 5, 1, 12, 34, 56, 123_456_700_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12345678'"))
                .isEqualTo(timestamp(8, 1500, 5, 1, 12, 34, 56, 123_456_780_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123456789'"))
                .isEqualTo(timestamp(9, 1500, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.1234567890'"))
                .isEqualTo(timestamp(10, 1500, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.12345678901'"))
                .isEqualTo(timestamp(11, 1500, 5, 1, 12, 34, 56, 123_456_789_010L));

        assertThat(assertions.expression("TIMESTAMP '1500-05-01 12:34:56.123456789012'"))
                .isEqualTo(timestamp(12, 1500, 5, 1, 12, 34, 56, 123_456_789_012L));

        // 6-digit year
        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56'"))
                .isEqualTo(timestamp(0, 123001, 5, 1, 12, 34, 56, 0));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.1'"))
                .isEqualTo(timestamp(1, 123001, 5, 1, 12, 34, 56, 100_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.12'"))
                .isEqualTo(timestamp(2, 123001, 5, 1, 12, 34, 56, 120_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.123'"))
                .isEqualTo(timestamp(3, 123001, 5, 1, 12, 34, 56, 123_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.1234'"))
                .isEqualTo(timestamp(4, 123001, 5, 1, 12, 34, 56, 123_400_000_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.12345'"))
                .isEqualTo(timestamp(5, 123001, 5, 1, 12, 34, 56, 123_450_000_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.123456'"))
                .isEqualTo(timestamp(6, 123001, 5, 1, 12, 34, 56, 123_456_000_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.1234567'"))
                .isEqualTo(timestamp(7, 123001, 5, 1, 12, 34, 56, 123_456_700_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.12345678'"))
                .isEqualTo(timestamp(8, 123001, 5, 1, 12, 34, 56, 123_456_780_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.123456789'"))
                .isEqualTo(timestamp(9, 123001, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.1234567890'"))
                .isEqualTo(timestamp(10, 123001, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.12345678901'"))
                .isEqualTo(timestamp(11, 123001, 5, 1, 12, 34, 56, 123_456_789_010L));

        assertThat(assertions.expression("TIMESTAMP '123001-05-01 12:34:56.123456789012'"))
                .isEqualTo(timestamp(12, 123001, 5, 1, 12, 34, 56, 123_456_789_012L));

        // 6-digit year with + sign
        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56'"))
                .isEqualTo(timestamp(0, 123001, 5, 1, 12, 34, 56, 0));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.1'"))
                .isEqualTo(timestamp(1, 123001, 5, 1, 12, 34, 56, 100_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.12'"))
                .isEqualTo(timestamp(2, 123001, 5, 1, 12, 34, 56, 120_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.123'"))
                .isEqualTo(timestamp(3, 123001, 5, 1, 12, 34, 56, 123_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.1234'"))
                .isEqualTo(timestamp(4, 123001, 5, 1, 12, 34, 56, 123_400_000_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.12345'"))
                .isEqualTo(timestamp(5, 123001, 5, 1, 12, 34, 56, 123_450_000_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.123456'"))
                .isEqualTo(timestamp(6, 123001, 5, 1, 12, 34, 56, 123_456_000_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.1234567'"))
                .isEqualTo(timestamp(7, 123001, 5, 1, 12, 34, 56, 123_456_700_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.12345678'"))
                .isEqualTo(timestamp(8, 123001, 5, 1, 12, 34, 56, 123_456_780_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.123456789'"))
                .isEqualTo(timestamp(9, 123001, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.1234567890'"))
                .isEqualTo(timestamp(10, 123001, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.12345678901'"))
                .isEqualTo(timestamp(11, 123001, 5, 1, 12, 34, 56, 123_456_789_010L));

        assertThat(assertions.expression("TIMESTAMP '+123001-05-01 12:34:56.123456789012'"))
                .isEqualTo(timestamp(12, 123001, 5, 1, 12, 34, 56, 123_456_789_012L));

        // 6-digit year with - sign
        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56'"))
                .isEqualTo(timestamp(0, -123001, 5, 1, 12, 34, 56, 0));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.1'"))
                .isEqualTo(timestamp(1, -123001, 5, 1, 12, 34, 56, 100_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.12'"))
                .isEqualTo(timestamp(2, -123001, 5, 1, 12, 34, 56, 120_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.123'"))
                .isEqualTo(timestamp(3, -123001, 5, 1, 12, 34, 56, 123_000_000_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.1234'"))
                .isEqualTo(timestamp(4, -123001, 5, 1, 12, 34, 56, 123_400_000_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.12345'"))
                .isEqualTo(timestamp(5, -123001, 5, 1, 12, 34, 56, 123_450_000_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.123456'"))
                .isEqualTo(timestamp(6, -123001, 5, 1, 12, 34, 56, 123_456_000_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.1234567'"))
                .isEqualTo(timestamp(7, -123001, 5, 1, 12, 34, 56, 123_456_700_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.12345678'"))
                .isEqualTo(timestamp(8, -123001, 5, 1, 12, 34, 56, 123_456_780_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.123456789'"))
                .isEqualTo(timestamp(9, -123001, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.1234567890'"))
                .isEqualTo(timestamp(10, -123001, 5, 1, 12, 34, 56, 123_456_789_000L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.12345678901'"))
                .isEqualTo(timestamp(11, -123001, 5, 1, 12, 34, 56, 123_456_789_010L));

        assertThat(assertions.expression("TIMESTAMP '-123001-05-01 12:34:56.123456789012'"))
                .isEqualTo(timestamp(12, -123001, 5, 1, 12, 34, 56, 123_456_789_012L));
    }

    @Test
    public void testLocalTimestamp()
    {
        // round down
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 111111111, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("localtimestamp(0)", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("localtimestamp(1)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("localtimestamp(2)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("localtimestamp(3)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("localtimestamp(4)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("localtimestamp(5)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("localtimestamp(6)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("localtimestamp(7)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("localtimestamp(8)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("localtimestamp(9)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("localtimestamp(10)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111110'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("localtimestamp(11)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111100'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("localtimestamp(12)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111000'"); // Java instant provides p = 9 precision

        // round up
        session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 555555555, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("localtimestamp(0)", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("localtimestamp(1)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("localtimestamp(2)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("localtimestamp(3)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("localtimestamp(4)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("localtimestamp(5)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("localtimestamp(6)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("localtimestamp(7)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("localtimestamp(8)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("localtimestamp(9)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555555'");
        assertThat(assertions.expression("localtimestamp(10)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555555550'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("localtimestamp(11)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555555500'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("localtimestamp(12)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555555000'"); // Java instant provides p = 9 precision
    }

    @Test
    public void testCastToDate()
    {
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567890' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678901' AS DATE)")).matches("DATE '2020-05-01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789012' AS DATE)")).matches("DATE '2020-05-01'");
    }

    @Test
    public void testCastFromDate()
    {
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 00:00:00'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 00:00:00.0'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 00:00:00.00'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 00:00:00.000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 00:00:00.0000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 00:00:00.00000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 00:00:00.000000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000000'");
        assertThat(assertions.expression("CAST(DATE '2020-05-01' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000000'");
    }

    @Test
    public void testCastToTime()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(2))")).matches("TIME '12:34:56.12'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(3))")).matches("TIME '12:34:56.123'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(4))")).matches("TIME '12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(5))")).matches("TIME '12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(6))")).matches("TIME '12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(7))")).matches("TIME '12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(8))")).matches("TIME '12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(9))")).matches("TIME '12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIME(10))")).matches("TIME '12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912' AS TIME(11))")).matches("TIME '12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123' AS TIME(12))")).matches("TIME '12:34:56.123456789123'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(1))")).matches("TIME '12:34:56.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(2))")).matches("TIME '12:34:56.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(3))")).matches("TIME '12:34:56.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(4))")).matches("TIME '12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(5))")).matches("TIME '12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(6))")).matches("TIME '12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(7))")).matches("TIME '12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(8))")).matches("TIME '12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(9))")).matches("TIME '12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(10))")).matches("TIME '12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(11))")).matches("TIME '12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(12))")).matches("TIME '12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(2))")).matches("TIME '12:34:56.10'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(3))")).matches("TIME '12:34:56.100'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(4))")).matches("TIME '12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(5))")).matches("TIME '12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(6))")).matches("TIME '12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(7))")).matches("TIME '12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(8))")).matches("TIME '12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(9))")).matches("TIME '12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(10))")).matches("TIME '12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(11))")).matches("TIME '12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(12))")).matches("TIME '12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(3))")).matches("TIME '12:34:56.120'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(4))")).matches("TIME '12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(5))")).matches("TIME '12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(6))")).matches("TIME '12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(7))")).matches("TIME '12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(8))")).matches("TIME '12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(9))")).matches("TIME '12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(10))")).matches("TIME '12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(11))")).matches("TIME '12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(12))")).matches("TIME '12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(4))")).matches("TIME '12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(5))")).matches("TIME '12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(6))")).matches("TIME '12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(7))")).matches("TIME '12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(8))")).matches("TIME '12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(9))")).matches("TIME '12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(10))")).matches("TIME '12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(11))")).matches("TIME '12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(12))")).matches("TIME '12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(5))")).matches("TIME '12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(6))")).matches("TIME '12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(7))")).matches("TIME '12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(8))")).matches("TIME '12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(9))")).matches("TIME '12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(10))")).matches("TIME '12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(11))")).matches("TIME '12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(12))")).matches("TIME '12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(6))")).matches("TIME '12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(7))")).matches("TIME '12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(8))")).matches("TIME '12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(9))")).matches("TIME '12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(10))")).matches("TIME '12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(11))")).matches("TIME '12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(12))")).matches("TIME '12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(7))")).matches("TIME '12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(8))")).matches("TIME '12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(9))")).matches("TIME '12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(10))")).matches("TIME '12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(11))")).matches("TIME '12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(12))")).matches("TIME '12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(8))")).matches("TIME '12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(9))")).matches("TIME '12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(10))")).matches("TIME '12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(11))")).matches("TIME '12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(12))")).matches("TIME '12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(9))")).matches("TIME '12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(10))")).matches("TIME '12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(11))")).matches("TIME '12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(12))")).matches("TIME '12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(10))")).matches("TIME '12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(11))")).matches("TIME '12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(12))")).matches("TIME '12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIME(11))")).matches("TIME '12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIME(12))")).matches("TIME '12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912' AS TIME(12))")).matches("TIME '12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(0))")).matches("TIME '12:34:56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(1))")).matches("TIME '12:34:56.1'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(2))")).matches("TIME '12:34:56.11'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(3))")).matches("TIME '12:34:56.111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(4))")).matches("TIME '12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(5))")).matches("TIME '12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(6))")).matches("TIME '12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(7))")).matches("TIME '12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(8))")).matches("TIME '12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(9))")).matches("TIME '12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(9))")).matches("TIME '12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(9))")).matches("TIME '12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(10))")).matches("TIME '12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(10))")).matches("TIME '12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(11))")).matches("TIME '12:34:56.11111111111'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(0))")).matches("TIME '12:34:57'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(1))")).matches("TIME '12:34:56.6'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(2))")).matches("TIME '12:34:56.56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(3))")).matches("TIME '12:34:56.556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(4))")).matches("TIME '12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(5))")).matches("TIME '12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(6))")).matches("TIME '12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(7))")).matches("TIME '12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(8))")).matches("TIME '12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(9))")).matches("TIME '12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(9))")).matches("TIME '12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(9))")).matches("TIME '12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(10))")).matches("TIME '12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(10))")).matches("TIME '12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(11))")).matches("TIME '12:34:56.55555555556'");

        // 5-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '12001-05-01 12:34:56' AS TIME(0))")).matches("TIME '12:34:56'");

        // 5-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-12001-05-01 12:34:56' AS TIME(0))")).matches("TIME '12:34:56'");

        // round up, wrap-around
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(0))")).matches("TIME '00:00:00'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(1))")).matches("TIME '00:00:00.0'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(2))")).matches("TIME '00:00:00.00'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(3))")).matches("TIME '00:00:00.000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(4))")).matches("TIME '00:00:00.0000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(5))")).matches("TIME '00:00:00.00000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(6))")).matches("TIME '00:00:00.000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(7))")).matches("TIME '00:00:00.0000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(8))")).matches("TIME '00:00:00.00000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(9))")).matches("TIME '00:00:00.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(9))")).matches("TIME '00:00:00.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(9))")).matches("TIME '00:00:00.000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(10))")).matches("TIME '00:00:00.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(10))")).matches("TIME '00:00:00.0000000000'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(11))")).matches("TIME '00:00:00.00000000000'");
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("+08:35"))
                .build();

        // Should be equivalent to CAST(CAST(x AS TIMESTAMP(p) WITH TIME ZONE) AS TIME(p) WITH TIME ZONE)

        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234567+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345678+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456789+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234567891+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345678912+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456789123+08:35'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.0000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.00000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.0000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.00000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.000000000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.10+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.100+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.10000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.100000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.10000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.100000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.10000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.100000000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.120+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1200+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.120000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1200000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.120000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1200000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.120000000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1230+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12300+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1230000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12300000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1230000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12300000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123000000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12340+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123400+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12340000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123400000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12340000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123400000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123450+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234500+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123450000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234500000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123450000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234560+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345600+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234560000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345600000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345670+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456700+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234567000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345670000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456700000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456780+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234567800+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345678000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456780000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1234567890+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345678900+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456789000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.12345678910+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456789100+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.123456789120+08:35'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111111+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111111+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111111+08:35'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555555556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555555556+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555555556+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555555556+08:35'");

        // 5-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '12001-05-01 12:34:56' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");

        // 5-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-12001-05-01 12:34:56' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+08:35'");

        // round up, wrap-around
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.9999999999' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.99999999999' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000000+08:35'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000000+08:35'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 23:59:59.999999999999' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000000+08:35'");
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789123' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123 Pacific/Apia'");

        // source < target
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.0 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.00 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.0000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.00000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.10 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.100 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.10000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.100000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.120 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1200 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.120000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1230 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12300 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12340 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123400 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123450 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567800 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678000 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567890 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678900 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789000 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678910 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567891' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789100 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678912' AS TIMESTAMP(12) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789120 Pacific/Apia'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1111111111' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.11111111111' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.111111111111' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111 Pacific/Apia'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:57 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(1) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.6 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(2) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.56 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(3) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(4) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(5) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(6) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(7) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(8) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.5555555555' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(9) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.55555555555' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556 Pacific/Apia'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(10) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556 Pacific/Apia'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.555555555555' AS TIMESTAMP(11) WITH TIME ZONE)")).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556 Pacific/Apia'");

        // 5-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '12001-05-01 12:34:56' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '12001-05-01 12:34:56 Pacific/Apia'");

        // 5-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-12001-05-01 12:34:56' AS TIMESTAMP(0) WITH TIME ZONE)")).matches("TIMESTAMP '-12001-05-01 12:34:56 Pacific/Apia'");

        // Overflow
        assertThatThrownBy(() -> assertions.expression("CAST(TIMESTAMP '123001-05-01 12:34:56' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Out of range for timestamp with time zone: 3819379822496000");
        assertThatThrownBy(() -> assertions.expression("CAST(TIMESTAMP '-123001-05-01 12:34:56' AS TIMESTAMP WITH TIME ZONE)").evaluate())
                .hasMessage("Out of range for timestamp with time zone: -3943693439888000");
    }

    @Test
    public void testCastToJson()
    {
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.1\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.12\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.123\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.1234\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.12345\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.123456\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.1234567\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.12345678\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.123456789\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567890' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.1234567890\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678901' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.12345678901\"'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789012' AS JSON)")).matches("JSON '\"2020-05-01 12:34:56.123456789012\"'");

        // 6-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '123001-05-01 12:34:56.123456789012' AS JSON)")).matches("JSON '\"+123001-05-01 12:34:56.123456789012\"'");

        // 6-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-123001-05-01 12:34:56.123456789012' AS JSON)")).matches("JSON '\"-123001-05-01 12:34:56.123456789012\"'");
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1234");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12345");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123456");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1234567");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12345678");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123456789");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.1234567890' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.1234567890");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.12345678901' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.12345678901");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-01 12:34:56.123456789012' AS VARCHAR)")).isEqualTo("2020-05-01 12:34:56.123456789012");

        // negative epoch
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1234' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1234");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12345' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12345");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123456' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123456");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1234567' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1234567");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12345678' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12345678");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123456789' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123456789");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.1234567890' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.1234567890");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.12345678901' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.12345678901");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-01 12:34:56.123456789012' AS VARCHAR)")).isEqualTo("1500-05-01 12:34:56.123456789012");

        // 6-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '123001-05-01 12:34:56' AS VARCHAR)")).isEqualTo("+123001-05-01 12:34:56");

        // 6-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-123001-05-01 12:34:56' AS VARCHAR)")).isEqualTo("-123001-05-01 12:34:56");
    }

    @Test
    public void testCastFromVarchar()
    {
        // round down
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111111'");

        // round up
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.555555555555' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555555555'");

        // negative epoch, round down
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111'");
        assertThat(assertions.expression("CAST('2020-05-01 12:34:56.111111111111' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111111'");

        // negative epoch, round up
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-01 12:34:57'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '1500-05-01 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(11))")).matches("TIMESTAMP '1500-05-01 12:34:56.55555555556'");
        assertThat(assertions.expression("CAST('1500-05-01 12:34:56.555555555555' AS TIMESTAMP(12))")).matches("TIMESTAMP '1500-05-01 12:34:56.555555555555'");

        // 6-digit year
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '123001-05-01 12:34:56'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '123001-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '123001-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '123001-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '123001-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '123001-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '123001-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '123001-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '123001-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '123001-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '123001-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '123001-05-01 12:34:56.11111111111'");
        assertThat(assertions.expression("CAST('123001-05-01 12:34:56.111111111111' AS TIMESTAMP(12))")).matches("TIMESTAMP '123001-05-01 12:34:56.111111111111'");

        // 6-digit year with + sign
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '123001-05-01 12:34:56'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '123001-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '123001-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '123001-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '123001-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '123001-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '123001-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '123001-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '123001-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '123001-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '123001-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '123001-05-01 12:34:56.11111111111'");
        assertThat(assertions.expression("CAST('+123001-05-01 12:34:56.111111111111' AS TIMESTAMP(12))")).matches("TIMESTAMP '123001-05-01 12:34:56.111111111111'");

        // 6-digit year with - sign
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '-123001-05-01 12:34:56'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '-123001-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '-123001-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '-123001-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '-123001-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '-123001-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '-123001-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '-123001-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '-123001-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '-123001-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '-123001-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '-123001-05-01 12:34:56.11111111111'");
        assertThat(assertions.expression("CAST('-123001-05-01 12:34:56.111111111111' AS TIMESTAMP(12))")).matches("TIMESTAMP '-123001-05-01 12:34:56.111111111111'");

        // values w/ time zone
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56 +01:23' AS TIMESTAMP(0))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.1 +01:23' AS TIMESTAMP(1))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.11 +01:23' AS TIMESTAMP(2))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.111 +01:23' AS TIMESTAMP(3))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.1111 +01:23' AS TIMESTAMP(4))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.11111 +01:23' AS TIMESTAMP(5))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.111111 +01:23' AS TIMESTAMP(6))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.1111111 +01:23' AS TIMESTAMP(7))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.11111111 +01:23' AS TIMESTAMP(8))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.111111111 +01:23' AS TIMESTAMP(9))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111111111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.1111111111 +01:23' AS TIMESTAMP(10))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.11111111111 +01:23' AS TIMESTAMP(11))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.11111111111'");
        assertThat(assertions.expression("CAST('2020-05-10 12:34:56.111111111111 +01:23' AS TIMESTAMP(12))"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.111111111111'");

        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(0))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(1))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(2))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(3))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(4))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(5))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(6))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(7))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(8))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(9))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(10))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(11))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 12:34:56.111111111111 xxx' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 12:34:56.111111111111 xxx");

        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(0))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(1))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(2))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(3))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(4))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(5))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(6))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(7))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(8))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(9))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(10))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(11))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10 xxx' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10 xxx");

        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(0))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(1))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(2))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(3))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(4))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(5))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(6))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(7))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(8))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(9))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(10))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(11))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
        assertThatThrownBy(() -> assertions.expression("CAST('2020-05-10T12:34:56' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2020-05-10T12:34:56");
    }

    @Test
    public void testLowerDigitsZeroed()
    {
        // round down
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(0)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.000000000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(3)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.111000000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(6)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(9)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111111000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(0)) AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(3)) AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(6)) AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.111' AS TIMESTAMP(0)) AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.000'");

        // round up
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(0)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:57.000000000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(3)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.556000000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(6)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(9)) AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-10 12:34:56.555555556000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(0)) AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:57.000000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(3)) AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.556000'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(6)) AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555555'");
        assertThat(assertions.expression("CAST(CAST(TIMESTAMP '2020-05-10 12:34:56.555' AS TIMESTAMP(0)) AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:57.000'");
    }

    @Test
    public void testRoundDown()
    {
        // positive epoch
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:56'");

        // positive epoch
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11111' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1111' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.111'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.111' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.11' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.1' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:56'");
    }

    @Test
    public void testRoundUp()
    {
        // positive epoch
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-10 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-10 12:34:56.55555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-10 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-10 12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-10 12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-10 12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-10 12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-10 12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-10 12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-10 12:34:56.56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-10 12:34:56.6'");

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-10 12:34:57'");

        // overflow to next second
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.5' AS timestamp(0))")).matches("TIMESTAMP '1970-01-01 00:00:01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.95' AS timestamp(1))")).matches("TIMESTAMP '1970-01-01 00:00:01.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.995' AS timestamp(2))")).matches("TIMESTAMP '1970-01-01 00:00:01.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.9995' AS timestamp(3))")).matches("TIMESTAMP '1970-01-01 00:00:01.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.99995' AS timestamp(4))")).matches("TIMESTAMP '1970-01-01 00:00:01.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999995' AS timestamp(5))")).matches("TIMESTAMP '1970-01-01 00:00:01.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.9999995' AS timestamp(6))")).matches("TIMESTAMP '1970-01-01 00:00:01.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.99999995' AS timestamp(7))")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999995' AS timestamp(8))")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.9999999995' AS timestamp(9))")).matches("TIMESTAMP '1970-01-01 00:00:01.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.99999999995' AS timestamp(10))")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999995' AS timestamp(11))")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000000'");

        // overflow to next second -- maximal precision
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(0))")).matches("TIMESTAMP '1970-01-01 00:00:01'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(1))")).matches("TIMESTAMP '1970-01-01 00:00:01.0'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(2))")).matches("TIMESTAMP '1970-01-01 00:00:01.00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(3))")).matches("TIMESTAMP '1970-01-01 00:00:01.000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(4))")).matches("TIMESTAMP '1970-01-01 00:00:01.0000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(5))")).matches("TIMESTAMP '1970-01-01 00:00:01.00000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(6))")).matches("TIMESTAMP '1970-01-01 00:00:01.000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(7))")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(8))")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(9))")).matches("TIMESTAMP '1970-01-01 00:00:01.000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(10))")).matches("TIMESTAMP '1970-01-01 00:00:01.0000000000'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00.999999999999' AS timestamp(11))")).matches("TIMESTAMP '1970-01-01 00:00:01.00000000000'");

        // negative epoch
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-10 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555555' AS TIMESTAMP(11))")).matches("TIMESTAMP '1500-05-10 12:34:56.55555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-10 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '1500-05-10 12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '1500-05-10 12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '1500-05-10 12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '1500-05-10 12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '1500-05-10 12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55555' AS TIMESTAMP(4))")).matches("TIMESTAMP '1500-05-10 12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5555' AS TIMESTAMP(3))")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.555' AS TIMESTAMP(2))")).matches("TIMESTAMP '1500-05-10 12:34:56.56'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.55' AS TIMESTAMP(1))")).matches("TIMESTAMP '1500-05-10 12:34:56.6'");

        assertThat(assertions.expression("CAST(TIMESTAMP '1500-05-10 12:34:56.5' AS TIMESTAMP(0))")).matches("TIMESTAMP '1500-05-10 12:34:57'");
    }

    @Test
    public void testToIso8601()
    {
        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56')"))
                .hasType(createVarcharType(22))
                .isEqualTo("2020-05-01T12:34:56");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1')"))
                .hasType(createVarcharType(24))
                .isEqualTo("2020-05-01T12:34:56.1");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12')"))
                .hasType(createVarcharType(25))
                .isEqualTo("2020-05-01T12:34:56.12");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123')"))
                .hasType(createVarcharType(26))
                .isEqualTo("2020-05-01T12:34:56.123");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234')"))
                .hasType(createVarcharType(27))
                .isEqualTo("2020-05-01T12:34:56.1234");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345')"))
                .hasType(createVarcharType(28))
                .isEqualTo("2020-05-01T12:34:56.12345");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456')"))
                .hasType(createVarcharType(29))
                .isEqualTo("2020-05-01T12:34:56.123456");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234567')"))
                .hasType(createVarcharType(30))
                .isEqualTo("2020-05-01T12:34:56.1234567");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345678')"))
                .hasType(createVarcharType(31))
                .isEqualTo("2020-05-01T12:34:56.12345678");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456789')"))
                .hasType(createVarcharType(32))
                .isEqualTo("2020-05-01T12:34:56.123456789");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.1234567890')"))
                .hasType(createVarcharType(33))
                .isEqualTo("2020-05-01T12:34:56.1234567890");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.12345678901')"))
                .hasType(createVarcharType(34))
                .isEqualTo("2020-05-01T12:34:56.12345678901");

        assertThat(assertions.expression("to_iso8601(TIMESTAMP '2020-05-01 12:34:56.123456789012')"))
                .hasType(createVarcharType(35))
                .isEqualTo("2020-05-01T12:34:56.123456789012");
    }

    @Test
    public void testFormat()
    {
        // round down
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56')")).isEqualTo("2020-05-10T12:34:56");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1')")).isEqualTo("2020-05-10T12:34:56.100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11')")).isEqualTo("2020-05-10T12:34:56.110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111')")).isEqualTo("2020-05-10T12:34:56.111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1111')")).isEqualTo("2020-05-10T12:34:56.111100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11111')")).isEqualTo("2020-05-10T12:34:56.111110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111111')")).isEqualTo("2020-05-10T12:34:56.111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1111111')")).isEqualTo("2020-05-10T12:34:56.111111100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11111111')")).isEqualTo("2020-05-10T12:34:56.111111110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111111111')")).isEqualTo("2020-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.1111111111')")).isEqualTo("2020-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.11111111111')")).isEqualTo("2020-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.111111111111')")).isEqualTo("2020-05-10T12:34:56.111111111");

        // round up
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56')")).isEqualTo("2020-05-10T12:34:56");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5')")).isEqualTo("2020-05-10T12:34:56.500");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55')")).isEqualTo("2020-05-10T12:34:56.550");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555')")).isEqualTo("2020-05-10T12:34:56.555");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5555')")).isEqualTo("2020-05-10T12:34:56.555500");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55555')")).isEqualTo("2020-05-10T12:34:56.555550");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555555')")).isEqualTo("2020-05-10T12:34:56.555555");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5555555')")).isEqualTo("2020-05-10T12:34:56.555555500");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55555555')")).isEqualTo("2020-05-10T12:34:56.555555550");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555555555')")).isEqualTo("2020-05-10T12:34:56.555555555");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.5555555555')")).isEqualTo("2020-05-10T12:34:56.555555556");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.55555555555')")).isEqualTo("2020-05-10T12:34:56.555555556");
        assertThat(assertions.expression("format('%s', TIMESTAMP '2020-05-10 12:34:56.555555555555')")).isEqualTo("2020-05-10T12:34:56.555555556");

        // 6-digit year in the future
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56')")).isEqualTo("+123001-05-10T12:34:56");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.1')")).isEqualTo("+123001-05-10T12:34:56.100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.11')")).isEqualTo("+123001-05-10T12:34:56.110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.111')")).isEqualTo("+123001-05-10T12:34:56.111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.1111')")).isEqualTo("+123001-05-10T12:34:56.111100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.11111')")).isEqualTo("+123001-05-10T12:34:56.111110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.111111')")).isEqualTo("+123001-05-10T12:34:56.111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.1111111')")).isEqualTo("+123001-05-10T12:34:56.111111100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.11111111')")).isEqualTo("+123001-05-10T12:34:56.111111110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.111111111')")).isEqualTo("+123001-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.1111111111')")).isEqualTo("+123001-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.11111111111')")).isEqualTo("+123001-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '123001-05-10 12:34:56.111111111111')")).isEqualTo("+123001-05-10T12:34:56.111111111");

        // 6-digit year in the past
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56')")).isEqualTo("-123001-05-10T12:34:56");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.1')")).isEqualTo("-123001-05-10T12:34:56.100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.11')")).isEqualTo("-123001-05-10T12:34:56.110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.111')")).isEqualTo("-123001-05-10T12:34:56.111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.1111')")).isEqualTo("-123001-05-10T12:34:56.111100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.11111')")).isEqualTo("-123001-05-10T12:34:56.111110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.111111')")).isEqualTo("-123001-05-10T12:34:56.111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.1111111')")).isEqualTo("-123001-05-10T12:34:56.111111100");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.11111111')")).isEqualTo("-123001-05-10T12:34:56.111111110");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.111111111')")).isEqualTo("-123001-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.1111111111')")).isEqualTo("-123001-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.11111111111')")).isEqualTo("-123001-05-10T12:34:56.111111111");
        assertThat(assertions.expression("format('%s', TIMESTAMP '-123001-05-10 12:34:56.111111111111')")).isEqualTo("-123001-05-10T12:34:56.111111111");
    }

    @Test
    public void testFormatDateTime()
    {
        // format_datetime supports up to millisecond precision

        // round down
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.000");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.100");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.110");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.1111111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.11111111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.111111111111', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.111");

        // round up
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.000");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.500");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.550");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.555");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.5555555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.55555555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
        assertThat(assertions.expression("format_datetime(TIMESTAMP '2020-05-10 12:34:56.555555555555', 'yyyy-MM-dd HH:mm:ss.SSS')")).isEqualTo("2020-05-10 12:34:56.556");
    }

    @Test
    public void testDateFormat()
    {
        // date_format supports up to millisecond precision

        // round down
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.000000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.100000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.110000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.1111111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.11111111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.111111111111', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.111000");

        // round up
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.000000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.500000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.550000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.555000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.5555555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.55555555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
        assertThat(assertions.expression("date_format(TIMESTAMP '2020-05-10 12:34:56.555555555555', '%Y-%m-%d %H:%i:%s.%f')")).isEqualTo("2020-05-10 12:34:56.556000");
    }

    @Test
    public void testWithTimeZone()
    {
        // round down
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.1', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.1 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.12', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.12 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.123', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.123 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.1234', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.1234 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.12345', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.12345 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.123456', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.123456 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.1234567', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.12345678', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.123456789', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.1234567891', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.12345678912', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912 America/Los_Angeles'");
        assertThat(assertions.expression("with_timezone(TIMESTAMP '2020-05-01 12:34:56.123456789123', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123 America/Los_Angeles'");
    }

    @Test
    public void testSequenceIntervalDayToSecond()
    {
        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56', TIMESTAMP '2020-01-01 12:34:59', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56', " +
                        "TIMESTAMP '2020-01-01 12:34:57', " +
                        "TIMESTAMP '2020-01-01 12:34:58', " +
                        "TIMESTAMP '2020-01-01 12:34:59']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1', TIMESTAMP '2020-01-01 12:34:59.1', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1', " +
                        "TIMESTAMP '2020-01-01 12:34:57.1', " +
                        "TIMESTAMP '2020-01-01 12:34:58.1', " +
                        "TIMESTAMP '2020-01-01 12:34:59.1']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12', TIMESTAMP '2020-01-01 12:34:59.12', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12', " +
                        "TIMESTAMP '2020-01-01 12:34:57.12', " +
                        "TIMESTAMP '2020-01-01 12:34:58.12', " +
                        "TIMESTAMP '2020-01-01 12:34:59.12']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123', TIMESTAMP '2020-01-01 12:34:59.123', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123', " +
                        "TIMESTAMP '2020-01-01 12:34:57.123', " +
                        "TIMESTAMP '2020-01-01 12:34:58.123', " +
                        "TIMESTAMP '2020-01-01 12:34:59.123']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1234', TIMESTAMP '2020-01-01 12:34:59.1234', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1234', " +
                        "TIMESTAMP '2020-01-01 12:34:57.1234', " +
                        "TIMESTAMP '2020-01-01 12:34:58.1234', " +
                        "TIMESTAMP '2020-01-01 12:34:59.1234']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12345', TIMESTAMP '2020-01-01 12:34:59.12345', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12345', " +
                        "TIMESTAMP '2020-01-01 12:34:57.12345', " +
                        "TIMESTAMP '2020-01-01 12:34:58.12345', " +
                        "TIMESTAMP '2020-01-01 12:34:59.12345']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123456', TIMESTAMP '2020-01-01 12:34:59.123456', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123456', " +
                        "TIMESTAMP '2020-01-01 12:34:57.123456', " +
                        "TIMESTAMP '2020-01-01 12:34:58.123456', " +
                        "TIMESTAMP '2020-01-01 12:34:59.123456']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1234567', TIMESTAMP '2020-01-01 12:34:59.1234567', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1234567', " +
                        "TIMESTAMP '2020-01-01 12:34:57.1234567', " +
                        "TIMESTAMP '2020-01-01 12:34:58.1234567', " +
                        "TIMESTAMP '2020-01-01 12:34:59.1234567']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12345678', TIMESTAMP '2020-01-01 12:34:59.12345678', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12345678', " +
                        "TIMESTAMP '2020-01-01 12:34:57.12345678', " +
                        "TIMESTAMP '2020-01-01 12:34:58.12345678', " +
                        "TIMESTAMP '2020-01-01 12:34:59.12345678']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123456789', TIMESTAMP '2020-01-01 12:34:59.123456789', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123456789', " +
                        "TIMESTAMP '2020-01-01 12:34:57.123456789', " +
                        "TIMESTAMP '2020-01-01 12:34:58.123456789', " +
                        "TIMESTAMP '2020-01-01 12:34:59.123456789']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1234567890', TIMESTAMP '2020-01-01 12:34:59.1234567890', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1234567890', " +
                        "TIMESTAMP '2020-01-01 12:34:57.1234567890', " +
                        "TIMESTAMP '2020-01-01 12:34:58.1234567890', " +
                        "TIMESTAMP '2020-01-01 12:34:59.1234567890']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12345678901', TIMESTAMP '2020-01-01 12:34:59.12345678901', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12345678901', " +
                        "TIMESTAMP '2020-01-01 12:34:57.12345678901', " +
                        "TIMESTAMP '2020-01-01 12:34:58.12345678901', " +
                        "TIMESTAMP '2020-01-01 12:34:59.12345678901']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123456789012', TIMESTAMP '2020-01-01 12:34:59.123456789012', INTERVAL '1' SECOND)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123456789012', " +
                        "TIMESTAMP '2020-01-01 12:34:57.123456789012', " +
                        "TIMESTAMP '2020-01-01 12:34:58.123456789012', " +
                        "TIMESTAMP '2020-01-01 12:34:59.123456789012']");
    }

    @Test
    public void testSequenceIntervalYearToMonth()
    {
        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56', TIMESTAMP '2020-04-01 12:34:56', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56', " +
                        "TIMESTAMP '2020-02-01 12:34:56', " +
                        "TIMESTAMP '2020-03-01 12:34:56', " +
                        "TIMESTAMP '2020-04-01 12:34:56']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1', TIMESTAMP '2020-04-01 12:34:56.1', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1', " +
                        "TIMESTAMP '2020-02-01 12:34:56.1', " +
                        "TIMESTAMP '2020-03-01 12:34:56.1', " +
                        "TIMESTAMP '2020-04-01 12:34:56.1']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12', TIMESTAMP '2020-04-01 12:34:59.12', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12', " +
                        "TIMESTAMP '2020-02-01 12:34:56.12', " +
                        "TIMESTAMP '2020-03-01 12:34:56.12', " +
                        "TIMESTAMP '2020-04-01 12:34:56.12']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123', TIMESTAMP '2020-04-01 12:34:59.123', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123', " +
                        "TIMESTAMP '2020-02-01 12:34:56.123', " +
                        "TIMESTAMP '2020-03-01 12:34:56.123', " +
                        "TIMESTAMP '2020-04-01 12:34:56.123']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1234', TIMESTAMP '2020-04-01 12:34:59.1234', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1234', " +
                        "TIMESTAMP '2020-02-01 12:34:56.1234', " +
                        "TIMESTAMP '2020-03-01 12:34:56.1234', " +
                        "TIMESTAMP '2020-04-01 12:34:56.1234']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12345', TIMESTAMP '2020-04-01 12:34:59.12345', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12345', " +
                        "TIMESTAMP '2020-02-01 12:34:56.12345', " +
                        "TIMESTAMP '2020-03-01 12:34:56.12345', " +
                        "TIMESTAMP '2020-04-01 12:34:56.12345']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123456', TIMESTAMP '2020-04-01 12:34:59.123456', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123456', " +
                        "TIMESTAMP '2020-02-01 12:34:56.123456', " +
                        "TIMESTAMP '2020-03-01 12:34:56.123456', " +
                        "TIMESTAMP '2020-04-01 12:34:56.123456']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1234567', TIMESTAMP '2020-04-01 12:34:59.1234567', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1234567', " +
                        "TIMESTAMP '2020-02-01 12:34:56.1234567', " +
                        "TIMESTAMP '2020-03-01 12:34:56.1234567', " +
                        "TIMESTAMP '2020-04-01 12:34:56.1234567']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12345678', TIMESTAMP '2020-04-01 12:34:59.12345678', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12345678', " +
                        "TIMESTAMP '2020-02-01 12:34:56.12345678', " +
                        "TIMESTAMP '2020-03-01 12:34:56.12345678', " +
                        "TIMESTAMP '2020-04-01 12:34:56.12345678']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123456789', TIMESTAMP '2020-04-01 12:34:59.123456789', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123456789', " +
                        "TIMESTAMP '2020-02-01 12:34:56.123456789', " +
                        "TIMESTAMP '2020-03-01 12:34:56.123456789', " +
                        "TIMESTAMP '2020-04-01 12:34:56.123456789']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.1234567890', TIMESTAMP '2020-04-01 12:34:59.1234567890', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.1234567890', " +
                        "TIMESTAMP '2020-02-01 12:34:56.1234567890', " +
                        "TIMESTAMP '2020-03-01 12:34:56.1234567890', " +
                        "TIMESTAMP '2020-04-01 12:34:56.1234567890']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.12345678901', TIMESTAMP '2020-04-01 12:34:59.12345678901', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.12345678901', " +
                        "TIMESTAMP '2020-02-01 12:34:56.12345678901', " +
                        "TIMESTAMP '2020-03-01 12:34:56.12345678901', " +
                        "TIMESTAMP '2020-04-01 12:34:56.12345678901']");

        assertThat(assertions.expression("sequence(TIMESTAMP '2020-01-01 12:34:56.123456789012', TIMESTAMP '2020-04-01 12:34:59.123456789012', INTERVAL '1' MONTH)"))
                .matches("ARRAY[" +
                        "TIMESTAMP '2020-01-01 12:34:56.123456789012', " +
                        "TIMESTAMP '2020-02-01 12:34:56.123456789012', " +
                        "TIMESTAMP '2020-03-01 12:34:56.123456789012', " +
                        "TIMESTAMP '2020-04-01 12:34:56.123456789012']");
    }

    @Test
    public void testDateDiff()
    {
        // date_diff truncates the fractional part

        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55', TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '1000'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1', TIMESTAMP '2020-05-10 12:34:56.2')")).matches("BIGINT '1100'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11', TIMESTAMP '2020-05-10 12:34:56.22')")).matches("BIGINT '1110'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111', TIMESTAMP '2020-05-10 12:34:56.222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111', TIMESTAMP '2020-05-10 12:34:56.2222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111', TIMESTAMP '2020-05-10 12:34:56.22222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111', TIMESTAMP '2020-05-10 12:34:56.222222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111', TIMESTAMP '2020-05-10 12:34:56.2222222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111', TIMESTAMP '2020-05-10 12:34:56.22222222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111', TIMESTAMP '2020-05-10 12:34:56.222222222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111111', TIMESTAMP '2020-05-10 12:34:56.2222222222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111111', TIMESTAMP '2020-05-10 12:34:56.22222222222')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111111', TIMESTAMP '2020-05-10 12:34:56.222222222222')")).matches("BIGINT '1111'");

        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55', TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '1000'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1', TIMESTAMP '2020-05-10 12:34:56.9')")).matches("BIGINT '1800'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11', TIMESTAMP '2020-05-10 12:34:56.99')")).matches("BIGINT '1880'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111', TIMESTAMP '2020-05-10 12:34:56.999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111', TIMESTAMP '2020-05-10 12:34:56.9999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111', TIMESTAMP '2020-05-10 12:34:56.99999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111', TIMESTAMP '2020-05-10 12:34:56.999999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111', TIMESTAMP '2020-05-10 12:34:56.9999999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111', TIMESTAMP '2020-05-10 12:34:56.99999999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111', TIMESTAMP '2020-05-10 12:34:56.999999999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.1111111111', TIMESTAMP '2020-05-10 12:34:56.9999999999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.11111111111', TIMESTAMP '2020-05-10 12:34:56.99999999999')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIMESTAMP '2020-05-10 12:34:55.111111111111', TIMESTAMP '2020-05-10 12:34:56.999999999999')")).matches("BIGINT '1888'");

        // coarser unit
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55', TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '1'");

        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1', TIMESTAMP '2020-05-10 12:34:56.2')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11', TIMESTAMP '2020-05-10 12:34:56.22')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111', TIMESTAMP '2020-05-10 12:34:56.222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111', TIMESTAMP '2020-05-10 12:34:56.2222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111', TIMESTAMP '2020-05-10 12:34:56.22222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111', TIMESTAMP '2020-05-10 12:34:56.222222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111', TIMESTAMP '2020-05-10 12:34:56.2222222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111', TIMESTAMP '2020-05-10 12:34:56.22222222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111', TIMESTAMP '2020-05-10 12:34:56.222222222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111111', TIMESTAMP '2020-05-10 12:34:56.2222222222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111111', TIMESTAMP '2020-05-10 12:34:56.22222222222')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111111', TIMESTAMP '2020-05-10 12:34:56.222222222222')")).matches("BIGINT '1'");

        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1', TIMESTAMP '2020-05-10 12:34:56.9')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11', TIMESTAMP '2020-05-10 12:34:56.99')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111', TIMESTAMP '2020-05-10 12:34:56.999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111', TIMESTAMP '2020-05-10 12:34:56.9999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111', TIMESTAMP '2020-05-10 12:34:56.99999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111', TIMESTAMP '2020-05-10 12:34:56.999999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111', TIMESTAMP '2020-05-10 12:34:56.9999999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111', TIMESTAMP '2020-05-10 12:34:56.99999999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111', TIMESTAMP '2020-05-10 12:34:56.999999999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.1111111111', TIMESTAMP '2020-05-10 12:34:56.9999999999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.11111111111', TIMESTAMP '2020-05-10 12:34:56.99999999999')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIMESTAMP '2020-05-10 11:34:55.111111111111', TIMESTAMP '2020-05-10 12:34:56.999999999999')")).matches("BIGINT '1'");
    }

    @Test
    public void testDateAdd()
    {
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.11')")).matches("TIMESTAMP '2020-05-10 12:34:56.11'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.111')")).matches("TIMESTAMP '2020-05-10 12:34:56.112'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1111')")).matches("TIMESTAMP '2020-05-10 12:34:56.1121'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.11111')")).matches("TIMESTAMP '2020-05-10 12:34:56.11211'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.112111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.1121111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.11111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.11211111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.111111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.112111111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.1111111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.1121111111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.11111111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.11211111111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.111111111111')")).matches("TIMESTAMP '2020-05-10 12:34:56.112111111111'");

        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.5')")).matches("TIMESTAMP '2020-05-10 12:34:56.5'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.55')")).matches("TIMESTAMP '2020-05-10 12:34:56.55'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.555')")).matches("TIMESTAMP '2020-05-10 12:34:56.556'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.5555')")).matches("TIMESTAMP '2020-05-10 12:34:56.5565'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.55555')")).matches("TIMESTAMP '2020-05-10 12:34:56.55655'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.556555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.5555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.5565555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.55555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.55655555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.555555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.556555555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.5555555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.5565555555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.55555555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.55655555555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '2020-05-10 12:34:56.555555555555')")).matches("TIMESTAMP '2020-05-10 12:34:56.556555555555'");

        assertThat(assertions.expression("date_add('millisecond', 1000, TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:34:57'");

        // negative epoch
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56')")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1')")).matches("TIMESTAMP '1500-05-10 12:34:56.1'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11')")).matches("TIMESTAMP '1500-05-10 12:34:56.11'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111')")).matches("TIMESTAMP '1500-05-10 12:34:56.112'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1111')")).matches("TIMESTAMP '1500-05-10 12:34:56.1121'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11111')")).matches("TIMESTAMP '1500-05-10 12:34:56.11211'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.112111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.1121111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.11211111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.112111111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.1111111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.1121111111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.11111111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.11211111111'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.111111111111')")).matches("TIMESTAMP '1500-05-10 12:34:56.112111111111'");

        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56')")).matches("TIMESTAMP '1500-05-10 12:34:56'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5')")).matches("TIMESTAMP '1500-05-10 12:34:56.5'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55')")).matches("TIMESTAMP '1500-05-10 12:34:56.55'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555')")).matches("TIMESTAMP '1500-05-10 12:34:56.556'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5555')")).matches("TIMESTAMP '1500-05-10 12:34:56.5565'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55555')")).matches("TIMESTAMP '1500-05-10 12:34:56.55655'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.556555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.5565555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.55655555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.556555555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.5555555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.5565555555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.55555555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.55655555555'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIMESTAMP '1500-05-10 12:34:56.555555555555')")).matches("TIMESTAMP '1500-05-10 12:34:56.556555555555'");
    }

    @Test
    public void testLastDayOfMonth()
    {
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1234')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12345')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123456')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1234567')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12345678')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123456789')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.1234567891')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.12345678912')")).matches("DATE '2020-05-31'");
        assertThat(assertions.expression("last_day_of_month(TIMESTAMP '2020-05-01 12:34:56.123456789123')")).matches("DATE '2020-05-31'");
    }

    @Test
    public void testCastToTimeWithDaylightSavings()
    {
        // The number of seconds since the beginning of the day for the America/Los_Angeles in 2020 doesn't match
        // that of 1970, the year of the epoch. Make sure the proper corrections are being performed.
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("America/Los_Angeles"))
                .build();

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10' as TIME(0))", session)).matches("TIME '09:26:10'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1' as TIME(1))", session)).matches("TIME '09:26:10.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11' as TIME(2))", session)).matches("TIME '09:26:10.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111' as TIME(3))", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1111' as TIME(4))", session)).matches("TIME '09:26:10.1111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11111' as TIME(5))", session)).matches("TIME '09:26:10.11111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111111' as TIME(6))", session)).matches("TIME '09:26:10.111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1111111' as TIME(7))", session)).matches("TIME '09:26:10.1111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11111111' as TIME(8))", session)).matches("TIME '09:26:10.11111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111111111' as TIME(9))", session)).matches("TIME '09:26:10.111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1111111111' as TIME(10))", session)).matches("TIME '09:26:10.1111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11111111111' as TIME(11))", session)).matches("TIME '09:26:10.11111111111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111111111111' as TIME(12))", session)).matches("TIME '09:26:10.111111111111'");
    }

    @Test
    public void testAtTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Pacific/Apia"))
                .build();

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.1 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.12 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.123 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.1234 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.12345 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.123456 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.1234567 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.12345678 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.123456789 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567891' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.1234567891 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678912' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.12345678912 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789123' AT TIME ZONE 'America/Los_Angeles'", session)).matches("TIMESTAMP '2020-04-30 16:34:56.123456789123 America/Los_Angeles'");

        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.1 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.12 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.123 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.1234 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.12345 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.123456 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.1234567 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.12345678 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.123456789 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567891' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.1234567891 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.12345678912' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.12345678912 +10:00'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789123' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIMESTAMP '2020-05-01 09:34:56.123456789123 +10:00'");
    }

    @Test
    public void testCastInvalidTimestamp()
    {
        assertThatThrownBy(() -> assertions.expression("CAST('ABC' AS TIMESTAMP)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: ABC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-00 00:00:00' AS TIMESTAMP)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-00 00:00:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-00-01 00:00:00' AS TIMESTAMP)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-00-01 00:00:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 25:00:00' AS TIMESTAMP)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 25:00:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:61:00' AS TIMESTAMP)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:61:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:00:61' AS TIMESTAMP)").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:00:61");

        assertThatThrownBy(() -> assertions.expression("CAST('ABC' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: ABC");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-00 00:00:00' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-00 00:00:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-00-01 00:00:00' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-00-01 00:00:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 25:00:00' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 25:00:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:61:00' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:61:00");
        assertThatThrownBy(() -> assertions.expression("CAST('2022-01-01 00:00:61' AS TIMESTAMP(12))").evaluate())
                .hasMessage("Value cannot be cast to timestamp: 2022-01-01 00:00:61");
    }

    @Test
    public void testCastFromVarcharContainingTimeZone()
    {
        assertThat(assertions.expression("CAST('2001-1-22 03:04:05.321 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.321'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04:05 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.000'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:00.000'");

        assertThat(assertions.expression("CAST('2001-1-22 +07:09' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 00:00:00.000'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04:05.321 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.321'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04:05 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:05.000'");

        assertThat(assertions.expression("CAST('2001-1-22 03:04 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 03:04:00.000'");

        assertThat(assertions.expression("CAST('2001-1-22 Asia/Oral' AS timestamp)"))
                .matches("TIMESTAMP '2001-1-22 00:00:00.000'");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2017-03-30 14:15:16.432'", "TIMESTAMP '2016-03-29 03:04:05.321'"))
                .matches("INTERVAL '366 11:11:11.111' DAY TO SECOND");

        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2016-03-29 03:04:05.321'", "TIMESTAMP '2017-03-30 14:15:16.432'"))
                .matches("INTERVAL '-366 11:11:11.111' DAY TO SECOND");
    }

    @Test
    public void testPlusInterval()
    {
        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321'", "INTERVAL '3' hour"))
                .matches("TIMESTAMP '2001-01-22 06:04:05.321'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' hour", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .matches("TIMESTAMP '2001-01-22 06:04:05.321'");

        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321'", "INTERVAL '3' day"))
                .matches("TIMESTAMP '2001-01-25 03:04:05.321'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' day", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .matches("TIMESTAMP '2001-01-25 03:04:05.321'");

        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321'", "INTERVAL '3' month"))
                .matches("TIMESTAMP '2001-04-22 03:04:05.321'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' month", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .matches("TIMESTAMP '2001-04-22 03:04:05.321'");

        assertThat(assertions.operator(ADD, "TIMESTAMP '2001-1-22 03:04:05.321'", "INTERVAL '3' year"))
                .matches("TIMESTAMP '2004-01-22 03:04:05.321'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' year", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .matches("TIMESTAMP '2004-01-22 03:04:05.321'");
    }

    @Test
    public void testMinusInterval()
    {
        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2001-1-22 03:04:05.321'", "INTERVAL '3' day"))
                .matches("TIMESTAMP '2001-01-19 03:04:05.321'");

        assertThat(assertions.operator(SUBTRACT, "TIMESTAMP '2001-1-22 03:04:05.321'", "INTERVAL '3' month"))
                .matches("TIMESTAMP '2000-10-22 03:04:05.321'");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22 03:04:05.321'", "TIMESTAMP '2001-1-22 03:04:05'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIMESTAMP '2001-1-22'", "TIMESTAMP '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.111'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.111'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("b", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TIMESTAMP '2001-1-22'")
                .binding("b", "TIMESTAMP '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.111'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.111'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.321'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.322'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.333'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.311'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.312'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TIMESTAMP '2001-1-22 03:04:05.321'")
                .binding("low", "TIMESTAMP '2001-1-22 03:04:05.333'")
                .binding("high", "TIMESTAMP '2001-1-22 03:04:05.111'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreatest()
    {
        assertThat(assertions.function("greatest", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'"))
                .matches("TIMESTAMP '2013-03-30 01:05'");

        assertThat(assertions.function("greatest", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'", "TIMESTAMP '2012-05-01 01:05'"))
                .matches("TIMESTAMP '2013-03-30 01:05'");
    }

    @Test
    public void testLeast()
    {
        assertThat(assertions.function("least", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'"))
                .matches("TIMESTAMP '2012-03-30 01:05'");

        assertThat(assertions.function("least", "TIMESTAMP '2013-03-30 01:05'", "TIMESTAMP '2012-03-30 01:05'", "TIMESTAMP '2012-05-01 01:05'"))
                .matches("TIMESTAMP '2012-03-30 01:05'");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null AS TIMESTAMP)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "TIMESTAMP '2012-03-30 01:05'"))
                .isEqualTo(false);
    }

    private static BiFunction<Session, QueryRunner, Object> timestamp(int precision, int year, int month, int day, int hour, int minute, int second, long picoOfSecond)
    {
        return (session, queryRunner) -> {
            LocalDateTime base = LocalDateTime.of(year, month, day, hour, minute, second);

            ZoneOffset offset = ZoneOffset.UTC;
            long epochMicros = base.toEpochSecond(offset) * MICROSECONDS_PER_SECOND + picoOfSecond / PICOSECONDS_PER_MICROSECOND;
            int picosOfMicro = (int) (picoOfSecond % PICOSECONDS_PER_MICROSECOND);

            return SqlTimestamp.newInstance(precision, epochMicros, picosOfMicro);
        };
    }
}
