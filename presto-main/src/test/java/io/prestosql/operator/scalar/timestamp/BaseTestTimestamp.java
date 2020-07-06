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
package io.prestosql.operator.scalar.timestamp;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.query.QueryAssertions;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;

import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.prestosql.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseTestTimestamp
{
    private static final TimeZoneKey SESSION_TIME_ZONE = DEFAULT_TIME_ZONE_KEY;

    private final boolean legacyTimestamp;
    protected QueryAssertions assertions;

    protected BaseTestTimestamp(boolean legacyTimestamp)
    {
        this.legacyTimestamp = legacyTimestamp;
    }

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("legacy_timestamp", String.valueOf(legacyTimestamp))
                .setTimeZoneKey(SESSION_TIME_ZONE)
                .build();
        assertions = new QueryAssertions(session);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
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

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP '2020-05-01 12:34:56.1234567890123'"))
                .hasMessage("line 1:8: TIMESTAMP precision must be in range [0, 12]");

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP '2020-13-01'"))
                .hasMessage("line 1:8: '2020-13-01' is not a valid timestamp literal");

        assertThatThrownBy(() -> assertions.expression("TIMESTAMP 'xxx'"))
                .hasMessage("line 1:8: 'xxx' is not a valid timestamp literal");

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
        // round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56' AS TIME)")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1' AS TIME)")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11' AS TIME)")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1111111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11111111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111111111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1111111111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11111111111' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111111111111' AS TIME)")).matches("TIME '12:34:56.111'");

        // round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56' AS TIME)")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5' AS TIME)")).matches("TIME '12:34:56.5'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55' AS TIME)")).matches("TIME '12:34:56.55'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555' AS TIME)")).matches("TIME '12:34:56.555'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5555555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55555555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555555555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5555555555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55555555555' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555555555555' AS TIME)")).matches("TIME '12:34:56.556'");
    }

    @Test
    public void testCastFromTime()
    {
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 123456789, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        // TODO: date part should be 2020-05-01. See https://github.com/prestosql/presto/issues/3845

        // round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '1970-01-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1110'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11100'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1110000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11100000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1110000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11100000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111000000000'");

        // round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '1970-01-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.5550'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.55500'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.5550000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.55500000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.5550000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.55500000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555000000000'");
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKeyForOffset(-5 * 60))
                .build();

        // Should be equivalent to CAST(CAST(x AS TIMESTAMP(p) WITH TIME ZONE) AS TIME(p) WITH TIME ZONE)

        // round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.1 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.11 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.1111111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.11111111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.111111111111' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.111 -05:00'");

        // round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.5 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.55 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.555 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.5555555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.55555555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2001-1-22 12:34:56.555555555555' AS TIME WITH TIME ZONE)", session)).matches("TIME '12:34:56.556 -05:00'");
    }

    @Test
    public void testCastFromTimeWithTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKeyForOffset(-8 * 60))
                .build();

        // should be equivalent to CAST(CAST(x, 'America/Los_Angeles') AS TIMESTAMP WITHOUT TIME ZONE)
        // TODO: date part should be 2020-05-01. See https://github.com/prestosql/presto/issues/3845

        // round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '1970-01-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1110'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11100'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1110000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11100000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.1110000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.11100000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 -08:00' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.111000000000'");

        // round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '1970-01-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.5550'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.55500'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.5550000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.55500000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.5550000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.55500000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 -08:00' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '1970-01-01 12:34:56.555000000000'");
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
        assertThatThrownBy(() -> assertions.expression("CAST(TIMESTAMP '123001-05-01 12:34:56' AS TIMESTAMP WITH TIME ZONE)"))
                .hasMessage("Out of range for timestamp with time zone: 3819379822496000");
        assertThatThrownBy(() -> assertions.expression("CAST(TIMESTAMP '-123001-05-01 12:34:56' AS TIMESTAMP WITH TIME ZONE)"))
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

        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10' as TIME)", session)).matches("TIME '09:26:10'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1' as TIME)", session)).matches("TIME '09:26:10.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11' as TIME)", session)).matches("TIME '09:26:10.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1111111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11111111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111111111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.1111111111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.11111111111' as TIME)", session)).matches("TIME '09:26:10.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-26 09:26:10.111111111111' as TIME)", session)).matches("TIME '09:26:10.111'");
    }

    private static BiFunction<Session, QueryRunner, Object> timestamp(int precision, int year, int month, int day, int hour, int minute, int second, long picoOfSecond)
    {
        return (session, queryRunner) -> {
            LocalDateTime base = LocalDateTime.of(year, month, day, hour, minute, second);

            ZoneOffset offset = ZoneOffset.UTC;
            if (SystemSessionProperties.isLegacyTimestamp(session)) {
                offset = session.getTimeZoneKey().getZoneId()
                        .getRules()
                        .getValidOffsets(base)
                        .get(0);
            }

            long epochMicros = base.toEpochSecond(offset) * MICROSECONDS_PER_SECOND + picoOfSecond / PICOSECONDS_PER_MICROSECOND;
            int picosOfMicro = (int) (picoOfSecond % PICOSECONDS_PER_MICROSECOND);

            if (SystemSessionProperties.isLegacyTimestamp(session)) {
                return SqlTimestamp.newLegacyInstance(precision, epochMicros, picosOfMicro, session.getTimeZoneKey());
            }

            return SqlTimestamp.newInstance(precision, epochMicros, picosOfMicro);
        };
    }
}
