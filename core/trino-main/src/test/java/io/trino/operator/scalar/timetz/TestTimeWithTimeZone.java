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
package io.trino.operator.scalar.timetz;

import io.trino.Session;
import io.trino.spi.type.SqlTimeWithTimeZone;
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

import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTimeWithTimeZone
{
    protected QueryAssertions assertions;

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
        assertThat(assertions.expression("TIME '12:34:56+08:35'"))
                .hasType(createTimeWithTimeZoneType(0))
                .isEqualTo(timeWithTimeZone(0, 12, 34, 56, 0, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56+08:35");

        assertThat(assertions.expression("TIME '12:34:56.1+08:35'"))
                .hasType(createTimeWithTimeZoneType(1))
                .isEqualTo(timeWithTimeZone(1, 12, 34, 56, 100_000_000_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.1+08:35");

        assertThat(assertions.expression("TIME '12:34:56.12+08:35'"))
                .hasType(createTimeWithTimeZoneType(2))
                .isEqualTo(timeWithTimeZone(2, 12, 34, 56, 120_000_000_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.12+08:35");

        assertThat(assertions.expression("TIME '12:34:56.123+08:35'"))
                .hasType(createTimeWithTimeZoneType(3))
                .isEqualTo(timeWithTimeZone(3, 12, 34, 56, 123_000_000_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.123+08:35");

        assertThat(assertions.expression("TIME '12:34:56.1234+08:35'"))
                .hasType(createTimeWithTimeZoneType(4))
                .isEqualTo(timeWithTimeZone(4, 12, 34, 56, 123_400_000_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.1234+08:35");

        assertThat(assertions.expression("TIME '12:34:56.12345+08:35'"))
                .hasType(createTimeWithTimeZoneType(5))
                .isEqualTo(timeWithTimeZone(5, 12, 34, 56, 123_450_000_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.12345+08:35");

        assertThat(assertions.expression("TIME '12:34:56.123456+08:35'"))
                .hasType(createTimeWithTimeZoneType(6))
                .isEqualTo(timeWithTimeZone(6, 12, 34, 56, 123_456_000_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.123456+08:35");

        assertThat(assertions.expression("TIME '12:34:56.1234567+08:35'"))
                .hasType(createTimeWithTimeZoneType(7))
                .isEqualTo(timeWithTimeZone(7, 12, 34, 56, 123_456_700_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.1234567+08:35");

        assertThat(assertions.expression("TIME '12:34:56.12345678+08:35'"))
                .hasType(createTimeWithTimeZoneType(8))
                .isEqualTo(timeWithTimeZone(8, 12, 34, 56, 123_456_780_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.12345678+08:35");

        assertThat(assertions.expression("TIME '12:34:56.123456789+08:35'"))
                .hasType(createTimeWithTimeZoneType(9))
                .isEqualTo(timeWithTimeZone(9, 12, 34, 56, 123_456_789_000L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.123456789+08:35");

        assertThat(assertions.expression("TIME '12:34:56.1234567891+08:35'"))
                .hasType(createTimeWithTimeZoneType(10))
                .isEqualTo(timeWithTimeZone(10, 12, 34, 56, 123_456_789_100L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.1234567891+08:35");

        assertThat(assertions.expression("TIME '12:34:56.12345678912+08:35'"))
                .hasType(createTimeWithTimeZoneType(11))
                .isEqualTo(timeWithTimeZone(11, 12, 34, 56, 123_456_789_120L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.12345678912+08:35");

        assertThat(assertions.expression("TIME '12:34:56.123456789123+08:35'"))
                .hasType(createTimeWithTimeZoneType(12))
                .isEqualTo(timeWithTimeZone(12, 12, 34, 56, 123_456_789_123L, 8 * 60 + 35))
                .asString().isEqualTo("12:34:56.123456789123+08:35");

        // negative offsets
        assertThat(assertions.expression("TIME '12:34:56-08:35'"))
                .hasType(createTimeWithTimeZoneType(0))
                .isEqualTo(timeWithTimeZone(0, 12, 34, 56, 0, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56-08:35");

        assertThat(assertions.expression("TIME '12:34:56.1-08:35'"))
                .hasType(createTimeWithTimeZoneType(1))
                .isEqualTo(timeWithTimeZone(1, 12, 34, 56, 100_000_000_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.1-08:35");

        assertThat(assertions.expression("TIME '12:34:56.12-08:35'"))
                .hasType(createTimeWithTimeZoneType(2))
                .isEqualTo(timeWithTimeZone(2, 12, 34, 56, 120_000_000_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.12-08:35");

        assertThat(assertions.expression("TIME '12:34:56.123-08:35'"))
                .hasType(createTimeWithTimeZoneType(3))
                .isEqualTo(timeWithTimeZone(3, 12, 34, 56, 123_000_000_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.123-08:35");

        assertThat(assertions.expression("TIME '12:34:56.1234-08:35'"))
                .hasType(createTimeWithTimeZoneType(4))
                .isEqualTo(timeWithTimeZone(4, 12, 34, 56, 123_400_000_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.1234-08:35");

        assertThat(assertions.expression("TIME '12:34:56.12345-08:35'"))
                .hasType(createTimeWithTimeZoneType(5))
                .isEqualTo(timeWithTimeZone(5, 12, 34, 56, 123_450_000_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.12345-08:35");

        assertThat(assertions.expression("TIME '12:34:56.123456-08:35'"))
                .hasType(createTimeWithTimeZoneType(6))
                .isEqualTo(timeWithTimeZone(6, 12, 34, 56, 123_456_000_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.123456-08:35");

        assertThat(assertions.expression("TIME '12:34:56.1234567-08:35'"))
                .hasType(createTimeWithTimeZoneType(7))
                .isEqualTo(timeWithTimeZone(7, 12, 34, 56, 123_456_700_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.1234567-08:35");

        assertThat(assertions.expression("TIME '12:34:56.12345678-08:35'"))
                .hasType(createTimeWithTimeZoneType(8))
                .isEqualTo(timeWithTimeZone(8, 12, 34, 56, 123_456_780_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.12345678-08:35");

        assertThat(assertions.expression("TIME '12:34:56.123456789-08:35'"))
                .hasType(createTimeWithTimeZoneType(9))
                .isEqualTo(timeWithTimeZone(9, 12, 34, 56, 123_456_789_000L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.123456789-08:35");

        assertThat(assertions.expression("TIME '12:34:56.1234567891-08:35'"))
                .hasType(createTimeWithTimeZoneType(10))
                .isEqualTo(timeWithTimeZone(10, 12, 34, 56, 123_456_789_100L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.1234567891-08:35");

        assertThat(assertions.expression("TIME '12:34:56.12345678912-08:35'"))
                .hasType(createTimeWithTimeZoneType(11))
                .isEqualTo(timeWithTimeZone(11, 12, 34, 56, 123_456_789_120L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.12345678912-08:35");

        assertThat(assertions.expression("TIME '12:34:56.123456789123-08:35'"))
                .hasType(createTimeWithTimeZoneType(12))
                .isEqualTo(timeWithTimeZone(12, 12, 34, 56, 123_456_789_123L, -(8 * 60 + 35)))
                .asString().isEqualTo("12:34:56.123456789123-08:35");

        // negative offset minutes
        assertThat(assertions.expression("TIME '12:34:56-00:35'"))
                .hasType(createTimeWithTimeZoneType(0))
                .isEqualTo(timeWithTimeZone(0, 12, 34, 56, 0, -35))
                .asString().isEqualTo("12:34:56-00:35");

        assertThat(assertions.expression("TIME '12:34:56.1-00:35'"))
                .hasType(createTimeWithTimeZoneType(1))
                .isEqualTo(timeWithTimeZone(1, 12, 34, 56, 100_000_000_000L, -35))
                .asString().isEqualTo("12:34:56.1-00:35");

        assertThat(assertions.expression("TIME '12:34:56.12-00:35'"))
                .hasType(createTimeWithTimeZoneType(2))
                .isEqualTo(timeWithTimeZone(2, 12, 34, 56, 120_000_000_000L, -35))
                .asString().isEqualTo("12:34:56.12-00:35");

        assertThat(assertions.expression("TIME '12:34:56.123-00:35'"))
                .hasType(createTimeWithTimeZoneType(3))
                .isEqualTo(timeWithTimeZone(3, 12, 34, 56, 123_000_000_000L, -35))
                .asString().isEqualTo("12:34:56.123-00:35");

        assertThat(assertions.expression("TIME '12:34:56.1234-00:35'"))
                .hasType(createTimeWithTimeZoneType(4))
                .isEqualTo(timeWithTimeZone(4, 12, 34, 56, 123_400_000_000L, -35))
                .asString().isEqualTo("12:34:56.1234-00:35");

        assertThat(assertions.expression("TIME '12:34:56.12345-00:35'"))
                .hasType(createTimeWithTimeZoneType(5))
                .isEqualTo(timeWithTimeZone(5, 12, 34, 56, 123_450_000_000L, -35))
                .asString().isEqualTo("12:34:56.12345-00:35");

        assertThat(assertions.expression("TIME '12:34:56.123456-00:35'"))
                .hasType(createTimeWithTimeZoneType(6))
                .isEqualTo(timeWithTimeZone(6, 12, 34, 56, 123_456_000_000L, -35))
                .asString().isEqualTo("12:34:56.123456-00:35");

        assertThat(assertions.expression("TIME '12:34:56.1234567-00:35'"))
                .hasType(createTimeWithTimeZoneType(7))
                .isEqualTo(timeWithTimeZone(7, 12, 34, 56, 123_456_700_000L, -35))
                .asString().isEqualTo("12:34:56.1234567-00:35");

        assertThat(assertions.expression("TIME '12:34:56.12345678-00:35'"))
                .hasType(createTimeWithTimeZoneType(8))
                .isEqualTo(timeWithTimeZone(8, 12, 34, 56, 123_456_780_000L, -35))
                .asString().isEqualTo("12:34:56.12345678-00:35");

        assertThat(assertions.expression("TIME '12:34:56.123456789-00:35'"))
                .hasType(createTimeWithTimeZoneType(9))
                .isEqualTo(timeWithTimeZone(9, 12, 34, 56, 123_456_789_000L, -35))
                .asString().isEqualTo("12:34:56.123456789-00:35");

        assertThat(assertions.expression("TIME '12:34:56.1234567891-00:35'"))
                .hasType(createTimeWithTimeZoneType(10))
                .isEqualTo(timeWithTimeZone(10, 12, 34, 56, 123_456_789_100L, -35))
                .asString().isEqualTo("12:34:56.1234567891-00:35");

        assertThat(assertions.expression("TIME '12:34:56.12345678912-00:35'"))
                .hasType(createTimeWithTimeZoneType(11))
                .isEqualTo(timeWithTimeZone(11, 12, 34, 56, 123_456_789_120L, -35))
                .asString().isEqualTo("12:34:56.12345678912-00:35");

        assertThat(assertions.expression("TIME '12:34:56.123456789123-00:35'"))
                .hasType(createTimeWithTimeZoneType(12))
                .isEqualTo(timeWithTimeZone(12, 12, 34, 56, 123_456_789_123L, -35))
                .asString().isEqualTo("12:34:56.123456789123-00:35");

        // limits
        assertThat(assertions.expression("TIME '12:34:56.123456789123+14:00'"))
                .hasType(createTimeWithTimeZoneType(12))
                .isEqualTo(timeWithTimeZone(12, 12, 34, 56, 123_456_789_123L, 14 * 60));

        assertThat(assertions.expression("TIME '12:34:56.123456789123-14:00'"))
                .hasType(createTimeWithTimeZoneType(12))
                .isEqualTo(timeWithTimeZone(12, 12, 34, 56, 123_456_789_123L, -14 * 60));

        assertThatThrownBy(assertions.expression("TIME '12:34:56.1234567891234+08:35'")::evaluate)
                .hasMessage("line 1:12: TIME WITH TIME ZONE precision must be in range [0, 12]: 13");

        assertThatThrownBy(assertions.expression("TIME '25:00:00+08:35'")::evaluate)
                .hasMessage("line 1:12: '25:00:00+08:35' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:65:00+08:35'")::evaluate)
                .hasMessage("line 1:12: '12:65:00+08:35' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:65+08:35'")::evaluate)
                .hasMessage("line 1:12: '12:00:65+08:35' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:00+15:00'")::evaluate)
                .hasMessage("line 1:12: '12:00:00+15:00' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:00-15:00'")::evaluate)
                .hasMessage("line 1:12: '12:00:00-15:00' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:00+14:01'")::evaluate)
                .hasMessage("line 1:12: '12:00:00+14:01' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:00-14:01'")::evaluate)
                .hasMessage("line 1:12: '12:00:00-14:01' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:00+13:60'")::evaluate)
                .hasMessage("line 1:12: '12:00:00+13:60' is not a valid TIME literal");

        assertThatThrownBy(assertions.expression("TIME '12:00:00-13:60'")::evaluate)
                .hasMessage("line 1:12: '12:00:00-13:60' is not a valid TIME literal");
    }

    @Test
    public void testCastToTime()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(2))")).matches("TIME '12:34:56.12'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(3))")).matches("TIME '12:34:56.123'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(4))")).matches("TIME '12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(5))")).matches("TIME '12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(6))")).matches("TIME '12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIME(7))")).matches("TIME '12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIME(8))")).matches("TIME '12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIME(9))")).matches("TIME '12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456789123'");

        // source < target
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(1))")).matches("TIME '12:34:56.0'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(2))")).matches("TIME '12:34:56.00'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(3))")).matches("TIME '12:34:56.000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(4))")).matches("TIME '12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(5))")).matches("TIME '12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(6))")).matches("TIME '12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(7))")).matches("TIME '12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(8))")).matches("TIME '12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(9))")).matches("TIME '12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(10))")).matches("TIME '12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(11))")).matches("TIME '12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIME(12))")).matches("TIME '12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(2))")).matches("TIME '12:34:56.10'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(3))")).matches("TIME '12:34:56.100'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(4))")).matches("TIME '12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(5))")).matches("TIME '12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(6))")).matches("TIME '12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(7))")).matches("TIME '12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(8))")).matches("TIME '12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(9))")).matches("TIME '12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(10))")).matches("TIME '12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(11))")).matches("TIME '12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(12))")).matches("TIME '12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(3))")).matches("TIME '12:34:56.120'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(4))")).matches("TIME '12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(5))")).matches("TIME '12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(6))")).matches("TIME '12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(7))")).matches("TIME '12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(8))")).matches("TIME '12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(9))")).matches("TIME '12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(10))")).matches("TIME '12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(11))")).matches("TIME '12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIME(12))")).matches("TIME '12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(4))")).matches("TIME '12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(5))")).matches("TIME '12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(6))")).matches("TIME '12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(7))")).matches("TIME '12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(8))")).matches("TIME '12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(9))")).matches("TIME '12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(10))")).matches("TIME '12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(11))")).matches("TIME '12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIME(12))")).matches("TIME '12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(5))")).matches("TIME '12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(6))")).matches("TIME '12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(7))")).matches("TIME '12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(8))")).matches("TIME '12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(9))")).matches("TIME '12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(11))")).matches("TIME '12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIME(12))")).matches("TIME '12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(6))")).matches("TIME '12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(7))")).matches("TIME '12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(8))")).matches("TIME '12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(9))")).matches("TIME '12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIME(12))")).matches("TIME '12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(7))")).matches("TIME '12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(8))")).matches("TIME '12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(9))")).matches("TIME '12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIME(8))")).matches("TIME '12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIME(9))")).matches("TIME '12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIME(9))")).matches("TIME '12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIME(10))")).matches("TIME '12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIME(11))")).matches("TIME '12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912+08:35' AS TIME(12))")).matches("TIME '12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(0))")).matches("TIME '12:34:56'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(1))")).matches("TIME '12:34:56.1'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(2))")).matches("TIME '12:34:56.11'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(3))")).matches("TIME '12:34:56.111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(4))")).matches("TIME '12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(5))")).matches("TIME '12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(6))")).matches("TIME '12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(6))")).matches("TIME '12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(7))")).matches("TIME '12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(7))")).matches("TIME '12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(8))")).matches("TIME '12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(8))")).matches("TIME '12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(9))")).matches("TIME '12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(9))")).matches("TIME '12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(9))")).matches("TIME '12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(10))")).matches("TIME '12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(10))")).matches("TIME '12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(11))")).matches("TIME '12:34:56.11111111111'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.5+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(0))")).matches("TIME '12:34:57'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(1))")).matches("TIME '12:34:56.6'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(2))")).matches("TIME '12:34:56.56'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(3))")).matches("TIME '12:34:56.556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(4))")).matches("TIME '12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(5))")).matches("TIME '12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(6))")).matches("TIME '12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(6))")).matches("TIME '12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(7))")).matches("TIME '12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(7))")).matches("TIME '12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(8))")).matches("TIME '12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(8))")).matches("TIME '12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(9))")).matches("TIME '12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(9))")).matches("TIME '12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(9))")).matches("TIME '12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(10))")).matches("TIME '12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(10))")).matches("TIME '12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(11))")).matches("TIME '12:34:56.55555555556'");

        // wrap-around
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(0))")).matches("TIME '00:00:00'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(1))")).matches("TIME '00:00:00.0'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(2))")).matches("TIME '00:00:00.00'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(3))")).matches("TIME '00:00:00.000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(4))")).matches("TIME '00:00:00.0000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(5))")).matches("TIME '00:00:00.00000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(6))")).matches("TIME '00:00:00.000000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(7))")).matches("TIME '00:00:00.0000000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(8))")).matches("TIME '00:00:00.00000000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(9))")).matches("TIME '00:00:00.000000000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(10))")).matches("TIME '00:00:00.0000000000'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(11))")).matches("TIME '00:00:00.00000000000'");
    }

    @Test
    public void testCurrentTime()
    {
        // round down
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 111111111, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("current_time(0)", session)).matches("TIME '12:34:56 +13:00'");
        assertThat(assertions.expression("current_time(1)", session)).matches("TIME '12:34:56.1 +13:00'");
        assertThat(assertions.expression("current_time(2)", session)).matches("TIME '12:34:56.11 +13:00'");
        assertThat(assertions.expression("current_time(3)", session)).matches("TIME '12:34:56.111 +13:00'");
        assertThat(assertions.expression("current_time(4)", session)).matches("TIME '12:34:56.1111 +13:00'");
        assertThat(assertions.expression("current_time(5)", session)).matches("TIME '12:34:56.11111 +13:00'");
        assertThat(assertions.expression("current_time(6)", session)).matches("TIME '12:34:56.111111 +13:00'");
        assertThat(assertions.expression("current_time(7)", session)).matches("TIME '12:34:56.1111111 +13:00'");
        assertThat(assertions.expression("current_time(8)", session)).matches("TIME '12:34:56.11111111 +13:00'");
        assertThat(assertions.expression("current_time(9)", session)).matches("TIME '12:34:56.111111111 +13:00'");
        assertThat(assertions.expression("current_time(10)", session)).matches("TIME '12:34:56.1111111110 +13:00'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_time(11)", session)).matches("TIME '12:34:56.11111111100 +13:00'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_time(12)", session)).matches("TIME '12:34:56.111111111000 +13:00'"); // Java instant provides p = 9 precision

        // round up
        session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 555555555, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("current_time(0)", session)).matches("TIME '12:34:57 +13:00'");
        assertThat(assertions.expression("current_time(1)", session)).matches("TIME '12:34:56.6 +13:00'");
        assertThat(assertions.expression("current_time(2)", session)).matches("TIME '12:34:56.56 +13:00'");
        assertThat(assertions.expression("current_time(3)", session)).matches("TIME '12:34:56.556 +13:00'");
        assertThat(assertions.expression("current_time(4)", session)).matches("TIME '12:34:56.5556 +13:00'");
        assertThat(assertions.expression("current_time(5)", session)).matches("TIME '12:34:56.55556 +13:00'");
        assertThat(assertions.expression("current_time(6)", session)).matches("TIME '12:34:56.555556 +13:00'");
        assertThat(assertions.expression("current_time(7)", session)).matches("TIME '12:34:56.5555556 +13:00'");
        assertThat(assertions.expression("current_time(8)", session)).matches("TIME '12:34:56.55555556 +13:00'");
        assertThat(assertions.expression("current_time(9)", session)).matches("TIME '12:34:56.555555555 +13:00'");
        assertThat(assertions.expression("current_time(10)", session)).matches("TIME '12:34:56.5555555550 +13:00'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_time(11)", session)).matches("TIME '12:34:56.55555555500 +13:00'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_time(12)", session)).matches("TIME '12:34:56.555555555000 +13:00'"); // Java instant provides p = 9 precision

        // round up at the boundary
        session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 23, 59, 59, 999999999, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        assertThat(assertions.expression("current_time(0)", session)).matches("TIME '00:00:00 +13:00'");
        assertThat(assertions.expression("current_time(1)", session)).matches("TIME '00:00:00.0 +13:00'");
        assertThat(assertions.expression("current_time(2)", session)).matches("TIME '00:00:00.00 +13:00'");
        assertThat(assertions.expression("current_time(3)", session)).matches("TIME '00:00:00.000 +13:00'");
        assertThat(assertions.expression("current_time(4)", session)).matches("TIME '00:00:00.0000 +13:00'");
        assertThat(assertions.expression("current_time(5)", session)).matches("TIME '00:00:00.00000 +13:00'");
        assertThat(assertions.expression("current_time(6)", session)).matches("TIME '00:00:00.000000 +13:00'");
        assertThat(assertions.expression("current_time(7)", session)).matches("TIME '00:00:00.0000000 +13:00'");
        assertThat(assertions.expression("current_time(8)", session)).matches("TIME '00:00:00.00000000 +13:00'");
        assertThat(assertions.expression("current_time(9)", session)).matches("TIME '23:59:59.999999999 +13:00'");
        assertThat(assertions.expression("current_time(10)", session)).matches("TIME '23:59:59.9999999990 +13:00'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_time(11)", session)).matches("TIME '23:59:59.99999999900 +13:00'"); // Java instant provides p = 9 precision
        assertThat(assertions.expression("current_time(12)", session)).matches("TIME '23:59:59.999999999000 +13:00'"); // Java instant provides p = 9 precision
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.12 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.123 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1234 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12345 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123456 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567891 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678912 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789123 +08:35'");

        // source < target
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.0 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.00 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.0000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.00000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.0000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.00000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.0000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.00000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.000000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.10 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.100 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.10000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.100000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.10000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.100000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.10000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.100000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.120 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1200 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.120000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1200000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.120000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1200000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.120000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1230 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12300 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1230000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12300000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1230000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12300000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.12340 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123400 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12340000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123400000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12340000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123400000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.123450 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234500 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123450000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234500000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123450000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1234560 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345600 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234560000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345600000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.12345670 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456700 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345670000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456700000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.123456780 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567800 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456780000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1234567890 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678900 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.12345678910 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789100 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912 +08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.123456789120 +08:35'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.1 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111111 +08:35'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.5 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.55555555556 +08:35'");

        // wrap-around
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '00:00:00.000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999 +08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000000 +08:35'");
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 111111111, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        // source = target
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123 +08:35'");

        // source < target
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1200 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1200000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1200000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12000000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1230 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12300 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1230000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12300000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1230000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12300000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123000000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12340 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123400 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12340000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123400000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12340000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123400000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123450 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234500 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123450000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234500000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345000000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123450000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234560 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345600 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234560000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345600000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456000000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345670 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456700 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345670000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456700000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456780 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567800 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678000 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456780000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567890 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678900 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789000 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678910 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789100 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912+08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789120 +08:35'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111 +08:35'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.5+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:57 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556 +08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556 +08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556 +08:35'");

        // negative offset
        assertThat(assertions.expression("CAST(TIME '12:34:56-08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1-08:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12-08:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123-08:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234-08:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345-08:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456-08:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567-08:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678-08:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789-08:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891-08:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912-08:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912 -08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123-08:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123 -08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56-00:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1-00:35' AS TIMESTAMP(1) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12-00:35' AS TIMESTAMP(2) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123-00:35' AS TIMESTAMP(3) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234-00:35' AS TIMESTAMP(4) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345-00:35' AS TIMESTAMP(5) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456-00:35' AS TIMESTAMP(6) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567-00:35' AS TIMESTAMP(7) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678-00:35' AS TIMESTAMP(8) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789-00:35' AS TIMESTAMP(9) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891-00:35' AS TIMESTAMP(10) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912-00:35' AS TIMESTAMP(11) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912 -00:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123-00:35' AS TIMESTAMP(12) WITH TIME ZONE)", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123 -00:35'");

        // 5-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '12001-05-01 12:34:56+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '12001-05-01 12:34:56 +08:35'");

        // 5-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-12001-05-01 12:34:56+08:35' AS TIMESTAMP(0) WITH TIME ZONE)", session)).matches("TIMESTAMP '-12001-05-01 12:34:56 +08:35'");
    }

    @Test
    public void testCastToTimestamp()
    {
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 111111111, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        // source = target
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123'");

        // source < target
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912+08:35' AS TIMESTAMP(12))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:56'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.5+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '2020-05-01 12:34:57'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(1))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.6'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(2))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.56'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(3))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(4))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(5))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(6))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(7))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(8))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(9))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(10))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIMESTAMP(11))", session)).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556'");

        // 5-digit year in the future
        assertThat(assertions.expression("CAST(TIMESTAMP '12001-05-01 12:34:56+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '12001-05-01 12:34:56'");

        // 5-digit year in the past
        assertThat(assertions.expression("CAST(TIMESTAMP '-12001-05-01 12:34:56+08:35' AS TIMESTAMP(0))", session)).matches("TIMESTAMP '-12001-05-01 12:34:56'");
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(TIME '12:34:56+08:35' AS VARCHAR)")).isEqualTo("12:34:56+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS VARCHAR)")).isEqualTo("12:34:56.1+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12+08:35' AS VARCHAR)")).isEqualTo("12:34:56.12+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123+08:35' AS VARCHAR)")).isEqualTo("12:34:56.123+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234+08:35' AS VARCHAR)")).isEqualTo("12:34:56.1234+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345+08:35' AS VARCHAR)")).isEqualTo("12:34:56.12345+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456+08:35' AS VARCHAR)")).isEqualTo("12:34:56.123456+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567+08:35' AS VARCHAR)")).isEqualTo("12:34:56.1234567+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678+08:35' AS VARCHAR)")).isEqualTo("12:34:56.12345678+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789+08:35' AS VARCHAR)")).isEqualTo("12:34:56.123456789+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567890+08:35' AS VARCHAR)")).isEqualTo("12:34:56.1234567890+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678901+08:35' AS VARCHAR)")).isEqualTo("12:34:56.12345678901+08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789012+08:35' AS VARCHAR)")).isEqualTo("12:34:56.123456789012+08:35");

        // negative offset
        assertThat(assertions.expression("CAST(TIME '12:34:56-08:35' AS VARCHAR)")).isEqualTo("12:34:56-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1-08:35' AS VARCHAR)")).isEqualTo("12:34:56.1-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12-08:35' AS VARCHAR)")).isEqualTo("12:34:56.12-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123-08:35' AS VARCHAR)")).isEqualTo("12:34:56.123-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234-08:35' AS VARCHAR)")).isEqualTo("12:34:56.1234-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345-08:35' AS VARCHAR)")).isEqualTo("12:34:56.12345-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456-08:35' AS VARCHAR)")).isEqualTo("12:34:56.123456-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567-08:35' AS VARCHAR)")).isEqualTo("12:34:56.1234567-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678-08:35' AS VARCHAR)")).isEqualTo("12:34:56.12345678-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789-08:35' AS VARCHAR)")).isEqualTo("12:34:56.123456789-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567890-08:35' AS VARCHAR)")).isEqualTo("12:34:56.1234567890-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678901-08:35' AS VARCHAR)")).isEqualTo("12:34:56.12345678901-08:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789012-08:35' AS VARCHAR)")).isEqualTo("12:34:56.123456789012-08:35");

        // negative minute offset
        assertThat(assertions.expression("CAST(TIME '12:34:56-00:35' AS VARCHAR)")).isEqualTo("12:34:56-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1-00:35' AS VARCHAR)")).isEqualTo("12:34:56.1-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12-00:35' AS VARCHAR)")).isEqualTo("12:34:56.12-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123-00:35' AS VARCHAR)")).isEqualTo("12:34:56.123-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234-00:35' AS VARCHAR)")).isEqualTo("12:34:56.1234-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345-00:35' AS VARCHAR)")).isEqualTo("12:34:56.12345-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456-00:35' AS VARCHAR)")).isEqualTo("12:34:56.123456-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567-00:35' AS VARCHAR)")).isEqualTo("12:34:56.1234567-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678-00:35' AS VARCHAR)")).isEqualTo("12:34:56.12345678-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789-00:35' AS VARCHAR)")).isEqualTo("12:34:56.123456789-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567890-00:35' AS VARCHAR)")).isEqualTo("12:34:56.1234567890-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678901-00:35' AS VARCHAR)")).isEqualTo("12:34:56.12345678901-00:35");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789012-00:35' AS VARCHAR)")).isEqualTo("12:34:56.123456789012-00:35");
    }

    @Test
    public void testCastFromVarchar()
    {
        // round down
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111111+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111+08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111111+08:35'");

        // round up
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.55555555556+08:35'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555+08:35' AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.555555555555+08:35'");

        // round up, wrap-around
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '00:00:00.000000000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000000+08:35'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999+08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000000+08:35'");
    }

    @Test
    public void testCastFromVarcharWithoutTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 111111111, assertions.getDefaultSession().getTimeZoneKey().getZoneId())))
                .build();

        // round down
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:56+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.1111111111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.11111111111+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.111111111111' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.111111111111+13:00'");

        // round up
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '12:34:57+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '12:34:56.6+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '12:34:56.56+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '12:34:56.556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555555556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '12:34:56.5555555556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '12:34:56.55555555556+13:00'");
        assertThat(assertions.expression("CAST('12:34:56.555555555555' AS TIME(12) WITH TIME ZONE)", session)).matches("TIME '12:34:56.555555555555+13:00'");

        // round up, wrap-around
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(0) WITH TIME ZONE)", session)).matches("TIME '00:00:00+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(1) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(2) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(3) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(4) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(5) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(6) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(7) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(8) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(9) WITH TIME ZONE)", session)).matches("TIME '00:00:00.000000000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(10) WITH TIME ZONE)", session)).matches("TIME '00:00:00.0000000000+13:00'");
        assertThat(assertions.expression("CAST('23:59:59.999999999999' AS TIME(11) WITH TIME ZONE)", session)).matches("TIME '00:00:00.00000000000+13:00'");
    }

    @Test
    public void testLowerDigitsZeroed()
    {
        // round down
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111111111+08:35' AS TIME(0) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.000000000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111111111+08:35' AS TIME(3) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.111000000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111111111+08:35' AS TIME(6) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.111111000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111111111+08:35' AS TIME(9) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111+08:35' AS TIME(0) WITH TIME ZONE) AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111+08:35' AS TIME(3) WITH TIME ZONE) AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111111+08:35' AS TIME(6) WITH TIME ZONE) AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.111+08:35' AS TIME(0) WITH TIME ZONE) AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.000+08:35'");

        // round up
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555555555+08:35' AS TIME(0) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:57.000000000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555555555+08:35' AS TIME(3) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.556000000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555555555+08:35' AS TIME(6) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.555556000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555555555+08:35' AS TIME(9) WITH TIME ZONE) AS TIME(12) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555+08:35' AS TIME(0) WITH TIME ZONE) AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:57.000000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555+08:35' AS TIME(3) WITH TIME ZONE) AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.556000+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555555+08:35' AS TIME(6) WITH TIME ZONE) AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555555+08:35'");
        assertThat(assertions.expression("CAST(CAST(TIME '12:34:56.555+08:35' AS TIME(0) WITH TIME ZONE) AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:57.000+08:35'");
    }

    @Test
    public void testRoundDown()
    {
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111+08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.111111111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.11111111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.1111111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.111111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.11111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.1111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.111+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.11+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.1+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:56+08:35'");
    }

    @Test
    public void testRoundUp()
    {
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555+08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '12:34:56.55555555556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '12:34:56.5555555556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '12:34:56.555555556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '12:34:56.55555556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '12:34:56.5555556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '12:34:56.555556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '12:34:56.55556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '12:34:56.5556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '12:34:56.556+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '12:34:56.56+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '12:34:56.6+08:35'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '12:34:57+08:35'");

        // wrap-around
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(0) WITH TIME ZONE)")).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(1) WITH TIME ZONE)")).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(2) WITH TIME ZONE)")).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(3) WITH TIME ZONE)")).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(4) WITH TIME ZONE)")).matches("TIME '00:00:00.0000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(5) WITH TIME ZONE)")).matches("TIME '00:00:00.00000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(6) WITH TIME ZONE)")).matches("TIME '00:00:00.000000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(7) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(8) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(9) WITH TIME ZONE)")).matches("TIME '00:00:00.000000000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(10) WITH TIME ZONE)")).matches("TIME '00:00:00.0000000000+08:35'");
        assertThat(assertions.expression("CAST(TIME '23:59:59.999999999999+08:35' AS TIME(11) WITH TIME ZONE)")).matches("TIME '00:00:00.00000000000+08:35'");
    }

    @Test
    public void testFormat()
    {
        assertThat(assertions.expression("format('%s', TIME '12:34:56+08:35')")).isEqualTo("12:34:56+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.1+08:35')")).isEqualTo("12:34:56.1+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.11+08:35')")).isEqualTo("12:34:56.11+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.111+08:35')")).isEqualTo("12:34:56.111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.1111+08:35')")).isEqualTo("12:34:56.1111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.11111+08:35')")).isEqualTo("12:34:56.11111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.111111+08:35')")).isEqualTo("12:34:56.111111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.1111111+08:35')")).isEqualTo("12:34:56.1111111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.11111111+08:35')")).isEqualTo("12:34:56.11111111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.111111111+08:35')")).isEqualTo("12:34:56.111111111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.1111111111+08:35')")).isEqualTo("12:34:56.1111111111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.11111111111+08:35')")).isEqualTo("12:34:56.11111111111+08:35");
        assertThat(assertions.expression("format('%s', TIME '12:34:56.111111111111+08:35')")).isEqualTo("12:34:56.111111111111+08:35");
    }

    @Test
    public void testDateDiff()
    {
        // date_diff truncates the fractional part

        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55+08:35', TIME '12:34:56+08:35')")).matches("BIGINT '1000'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1+08:35', TIME '12:34:56.2+08:35')")).matches("BIGINT '1100'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11+08:35', TIME '12:34:56.22+08:35')")).matches("BIGINT '1110'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111+08:35', TIME '12:34:56.222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1111+08:35', TIME '12:34:56.2222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11111+08:35', TIME '12:34:56.22222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111111+08:35', TIME '12:34:56.222222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1111111+08:35', TIME '12:34:56.2222222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11111111+08:35', TIME '12:34:56.22222222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111111111+08:35', TIME '12:34:56.222222222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1111111111+08:35', TIME '12:34:56.2222222222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11111111111+08:35', TIME '12:34:56.22222222222+08:35')")).matches("BIGINT '1111'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111111111111+08:35', TIME '12:34:56.222222222222+08:35')")).matches("BIGINT '1111'");

        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55+08:35', TIME '12:34:56+08:35')")).matches("BIGINT '1000'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1+08:35', TIME '12:34:56.9+08:35')")).matches("BIGINT '1800'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11+08:35', TIME '12:34:56.99+08:35')")).matches("BIGINT '1880'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111+08:35', TIME '12:34:56.999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1111+08:35', TIME '12:34:56.9999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11111+08:35', TIME '12:34:56.99999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111111+08:35', TIME '12:34:56.999999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1111111+08:35', TIME '12:34:56.9999999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11111111+08:35', TIME '12:34:56.99999999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111111111+08:35', TIME '12:34:56.999999999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.1111111111+08:35', TIME '12:34:56.9999999999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.11111111111+08:35', TIME '12:34:56.99999999999+08:35')")).matches("BIGINT '1888'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '12:34:55.111111111111+08:35', TIME '12:34:56.999999999999+08:35')")).matches("BIGINT '1888'");

        // coarser unit
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55+08:35', TIME '12:34:56+08:35')")).matches("BIGINT '1'");

        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1+08:35', TIME '12:34:56.2+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11+08:35', TIME '12:34:56.22+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111+08:35', TIME '12:34:56.222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1111+08:35', TIME '12:34:56.2222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11111+08:35', TIME '12:34:56.22222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111111+08:35', TIME '12:34:56.222222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1111111+08:35', TIME '12:34:56.2222222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11111111+08:35', TIME '12:34:56.22222222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111111111+08:35', TIME '12:34:56.222222222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1111111111+08:35', TIME '12:34:56.2222222222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11111111111+08:35', TIME '12:34:56.22222222222+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111111111111+08:35', TIME '12:34:56.222222222222+08:35')")).matches("BIGINT '1'");

        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1+08:35', TIME '12:34:56.9+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11+08:35', TIME '12:34:56.99+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111+08:35', TIME '12:34:56.999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1111+08:35', TIME '12:34:56.9999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11111+08:35', TIME '12:34:56.99999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111111+08:35', TIME '12:34:56.999999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1111111+08:35', TIME '12:34:56.9999999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11111111+08:35', TIME '12:34:56.99999999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111111111+08:35', TIME '12:34:56.999999999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.1111111111+08:35', TIME '12:34:56.9999999999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.11111111111+08:35', TIME '12:34:56.99999999999+08:35')")).matches("BIGINT '1'");
        assertThat(assertions.expression("date_diff('hour', TIME '11:34:55.111111111111+08:35', TIME '12:34:56.999999999999+08:35')")).matches("BIGINT '1'");

        // different offset
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56+09:36', TIME '12:34:56+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.1+09:36', TIME '12:34:56.1+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.12+09:36', TIME '12:34:56.12+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.123+09:36', TIME '12:34:56.123+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.1234+09:36', TIME '12:34:56.1234+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.12345+09:36', TIME '12:34:56.12345+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.123456+09:36', TIME '12:34:56.123456+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.1234567+09:36', TIME '12:34:56.1234567+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.12345678+09:36', TIME '12:34:56.12345678+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.123456789+09:36', TIME '12:34:56.123456789+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.1234567891+09:36', TIME '12:34:56.1234567891+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.12345678912+09:36', TIME '12:34:56.12345678912+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('millisecond', TIME '13:35:56.123456789123+09:36', TIME '12:34:56.123456789123+08:35')")).matches("BIGINT '0'");

        // result is modulo 24h
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00+14:00', TIME '00:00:00-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1+14:00', TIME '00:00:00.1-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12+14:00', TIME '00:00:00.12-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123+14:00', TIME '00:00:00.123-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1234+14:00', TIME '00:00:00.1234-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12345+14:00', TIME '00:00:00.12345-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123456+14:00', TIME '00:00:00.123456-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1234567+14:00', TIME '00:00:00.1234567-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12345678+14:00', TIME '00:00:00.12345678-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123456789+14:00', TIME '00:00:00.123456789-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1234567891+14:00', TIME '00:00:00.1234567891-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12345678912+14:00', TIME '00:00:00.12345678912-14:00')")).matches("BIGINT '14400'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123456789123+14:00', TIME '00:00:00.123456789123-14:00')")).matches("BIGINT '14400'");

        assertThat(assertions.expression("date_diff('second', TIME '00:00:00+14:00', TIME '00:00:00-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1+14:00', TIME '00:00:00.1-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12+14:00', TIME '00:00:00.12-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123+14:00', TIME '00:00:00.123-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1234+14:00', TIME '00:00:00.1234-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12345+14:00', TIME '00:00:00.12345-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123456+14:00', TIME '00:00:00.123456-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1234567+14:00', TIME '00:00:00.1234567-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12345678+14:00', TIME '00:00:00.12345678-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123456789+14:00', TIME '00:00:00.123456789-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.1234567891+14:00', TIME '00:00:00.1234567891-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.12345678912+14:00', TIME '00:00:00.12345678912-10:00')")).matches("BIGINT '0'");
        assertThat(assertions.expression("date_diff('second', TIME '00:00:00.123456789123+14:00', TIME '00:00:00.123456789123-10:00')")).matches("BIGINT '0'");
    }

    @Test
    public void testDateAdd()
    {
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56+08:35')")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.1+08:35')")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.10+08:35')")).matches("TIME '12:34:56.10+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.100+08:35')")).matches("TIME '12:34:56.101+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.1000+08:35')")).matches("TIME '12:34:56.1010+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.10000+08:35')")).matches("TIME '12:34:56.10100+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.100000+08:35')")).matches("TIME '12:34:56.101000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.1000000+08:35')")).matches("TIME '12:34:56.1010000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.10000000+08:35')")).matches("TIME '12:34:56.10100000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.100000000+08:35')")).matches("TIME '12:34:56.101000000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.1000000000+08:35')")).matches("TIME '12:34:56.1010000000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.10000000000+08:35')")).matches("TIME '12:34:56.10100000000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 1, TIME '12:34:56.100000000000+08:35')")).matches("TIME '12:34:56.101000000000+08:35'");

        assertThat(assertions.expression("date_add('millisecond', 1000, TIME '12:34:56+08:35')")).matches("TIME '12:34:57+08:35'");

        assertThat(assertions.expression("date_add('millisecond', 1, TIME '23:59:59.999+08:35')")).matches("TIME '00:00:00.000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', -1, TIME '00:00:00.000+08:35')")).matches("TIME '23:59:59.999+08:35'");

        // coarser unit
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56+08:35')")).matches("TIME '12:34:57+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.1+08:35')")).matches("TIME '12:34:57.1+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.12+08:35')")).matches("TIME '12:34:57.12+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.123+08:35')")).matches("TIME '12:34:57.123+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.1234+08:35')")).matches("TIME '12:34:57.1234+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.12345+08:35')")).matches("TIME '12:34:57.12345+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.123456+08:35')")).matches("TIME '12:34:57.123456+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.1234567+08:35')")).matches("TIME '12:34:57.1234567+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.12345678+08:35')")).matches("TIME '12:34:57.12345678+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.123456789+08:35')")).matches("TIME '12:34:57.123456789+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.1234567891+08:35')")).matches("TIME '12:34:57.1234567891+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.12345678912+08:35')")).matches("TIME '12:34:57.12345678912+08:35'");
        assertThat(assertions.expression("date_add('second', 1, TIME '12:34:56.123456789123+08:35')")).matches("TIME '12:34:57.123456789123+08:35'");

        // test possible overflow
        assertThat(assertions.expression("date_add('hour', 365 * 24, TIME '12:34:56+08:35')")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("date_add('hour', 365 * 24 + 1, TIME '12:34:56+08:35')")).matches("TIME '13:34:56+08:35'");

        assertThat(assertions.expression("date_add('minute', 365 * 24 * 60, TIME '12:34:56+08:35')")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("date_add('minute', 365 * 24 * 60 + 1, TIME '12:34:56+08:35')")).matches("TIME '12:35:56+08:35'");

        assertThat(assertions.expression("date_add('second', 365 * 24 * 60 * 60, TIME '12:34:56+08:35')")).matches("TIME '12:34:56+08:35'");
        assertThat(assertions.expression("date_add('second', 365 * 24 * 60 * 60 + 1, TIME '12:34:56+08:35')")).matches("TIME '12:34:57+08:35'");

        assertThat(assertions.expression("date_add('millisecond', BIGINT '365' * 24 * 60 * 60 * 1000, TIME '12:34:56.000+08:35')")).matches("TIME '12:34:56.000+08:35'");
        assertThat(assertions.expression("date_add('millisecond', BIGINT '365' * 24 * 60 * 60 * 1000 + 1, TIME '12:34:56.000+08:35')")).matches("TIME '12:34:56.001+08:35'");

        // wrap-around
        assertThat(assertions.expression("date_add('millisecond', 1000, TIME '23:59:59+08:35')")).matches("TIME '00:00:00+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 900, TIME '23:59:59.1+08:35')")).matches("TIME '00:00:00.0+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 890, TIME '23:59:59.11+08:35')")).matches("TIME '00:00:00.00+08:35'");
        assertThat(assertions.expression("date_add('millisecond', 889, TIME '23:59:59.111+08:35')")).matches("TIME '00:00:00.000+08:35'");
    }

    @Test
    public void testDateTrunc()
    {
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56+08:35')")).matches("TIME '12:34:56+08:35'");

        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.1+08:35')")).matches("TIME '12:34:56.1+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.11+08:35')")).matches("TIME '12:34:56.11+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.111+08:35')")).matches("TIME '12:34:56.111+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.1111+08:35')")).matches("TIME '12:34:56.1110+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.11111+08:35')")).matches("TIME '12:34:56.11100+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.111111+08:35')")).matches("TIME '12:34:56.111000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.1111111+08:35')")).matches("TIME '12:34:56.1110000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.11111111+08:35')")).matches("TIME '12:34:56.11100000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.111111111+08:35')")).matches("TIME '12:34:56.111000000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.1111111111+08:35')")).matches("TIME '12:34:56.1110000000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.11111111111+08:35')")).matches("TIME '12:34:56.11100000000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.111111111111+08:35')")).matches("TIME '12:34:56.111000000000+08:35'");

        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.5+08:35')")).matches("TIME '12:34:56.5+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.55+08:35')")).matches("TIME '12:34:56.55+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.555+08:35')")).matches("TIME '12:34:56.555+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.5555+08:35')")).matches("TIME '12:34:56.5550+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.55555+08:35')")).matches("TIME '12:34:56.55500+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.555555+08:35')")).matches("TIME '12:34:56.555000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.5555555+08:35')")).matches("TIME '12:34:56.5550000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.55555555+08:35')")).matches("TIME '12:34:56.55500000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.555555555+08:35')")).matches("TIME '12:34:56.555000000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.5555555555+08:35')")).matches("TIME '12:34:56.5550000000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.55555555555+08:35')")).matches("TIME '12:34:56.55500000000+08:35'");
        assertThat(assertions.expression("date_trunc('millisecond', TIME '12:34:56.555555555555+08:35')")).matches("TIME '12:34:56.555000000000+08:35'");

        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56+08:35')")).matches("TIME '12:34:56+08:35'");

        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.1+08:35')")).matches("TIME '12:34:56.0+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.11+08:35')")).matches("TIME '12:34:56.00+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.111+08:35')")).matches("TIME '12:34:56.000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.1111+08:35')")).matches("TIME '12:34:56.0000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.11111+08:35')")).matches("TIME '12:34:56.00000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.111111+08:35')")).matches("TIME '12:34:56.000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.1111111+08:35')")).matches("TIME '12:34:56.0000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.11111111+08:35')")).matches("TIME '12:34:56.00000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.111111111+08:35')")).matches("TIME '12:34:56.000000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.1111111111+08:35')")).matches("TIME '12:34:56.0000000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.11111111111+08:35')")).matches("TIME '12:34:56.00000000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.111111111111+08:35')")).matches("TIME '12:34:56.000000000000+08:35'");

        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.5+08:35')")).matches("TIME '12:34:56.0+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.55+08:35')")).matches("TIME '12:34:56.00+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.555+08:35')")).matches("TIME '12:34:56.000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.5555+08:35')")).matches("TIME '12:34:56.0000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.55555+08:35')")).matches("TIME '12:34:56.00000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.555555+08:35')")).matches("TIME '12:34:56.000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.5555555+08:35')")).matches("TIME '12:34:56.0000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.55555555+08:35')")).matches("TIME '12:34:56.00000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.555555555+08:35')")).matches("TIME '12:34:56.000000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.5555555555+08:35')")).matches("TIME '12:34:56.0000000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.55555555555+08:35')")).matches("TIME '12:34:56.00000000000+08:35'");
        assertThat(assertions.expression("date_trunc('second', TIME '12:34:56.555555555555+08:35')")).matches("TIME '12:34:56.000000000000+08:35'");

        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56+08:35')")).matches("TIME '12:34:00+08:35'");

        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.1+08:35')")).matches("TIME '12:34:00.0+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.11+08:35')")).matches("TIME '12:34:00.00+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.111+08:35')")).matches("TIME '12:34:00.000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.1111+08:35')")).matches("TIME '12:34:00.0000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.11111+08:35')")).matches("TIME '12:34:00.00000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.111111+08:35')")).matches("TIME '12:34:00.000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.1111111+08:35')")).matches("TIME '12:34:00.0000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.11111111+08:35')")).matches("TIME '12:34:00.00000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.111111111+08:35')")).matches("TIME '12:34:00.000000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.1111111111+08:35')")).matches("TIME '12:34:00.0000000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.11111111111+08:35')")).matches("TIME '12:34:00.00000000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.111111111111+08:35')")).matches("TIME '12:34:00.000000000000+08:35'");

        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.5+08:35')")).matches("TIME '12:34:00.0+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.55+08:35')")).matches("TIME '12:34:00.00+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.555+08:35')")).matches("TIME '12:34:00.000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.5555+08:35')")).matches("TIME '12:34:00.0000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.55555+08:35')")).matches("TIME '12:34:00.00000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.555555+08:35')")).matches("TIME '12:34:00.000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.5555555+08:35')")).matches("TIME '12:34:00.0000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.55555555+08:35')")).matches("TIME '12:34:00.00000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.555555555+08:35')")).matches("TIME '12:34:00.000000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.5555555555+08:35')")).matches("TIME '12:34:00.0000000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.55555555555+08:35')")).matches("TIME '12:34:00.00000000000+08:35'");
        assertThat(assertions.expression("date_trunc('minute', TIME '12:34:56.555555555555+08:35')")).matches("TIME '12:34:00.000000000000+08:35'");

        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56+08:35')")).matches("TIME '12:00:00+08:35'");

        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.1+08:35')")).matches("TIME '12:00:00.0+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.11+08:35')")).matches("TIME '12:00:00.00+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.111+08:35')")).matches("TIME '12:00:00.000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.1111+08:35')")).matches("TIME '12:00:00.0000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.11111+08:35')")).matches("TIME '12:00:00.00000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.111111+08:35')")).matches("TIME '12:00:00.000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.1111111+08:35')")).matches("TIME '12:00:00.0000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.11111111+08:35')")).matches("TIME '12:00:00.00000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.111111111+08:35')")).matches("TIME '12:00:00.000000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.1111111111+08:35')")).matches("TIME '12:00:00.0000000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.11111111111+08:35')")).matches("TIME '12:00:00.00000000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.111111111111+08:35')")).matches("TIME '12:00:00.000000000000+08:35'");

        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.5+08:35')")).matches("TIME '12:00:00.0+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.55+08:35')")).matches("TIME '12:00:00.00+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.555+08:35')")).matches("TIME '12:00:00.000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.5555+08:35')")).matches("TIME '12:00:00.0000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.55555+08:35')")).matches("TIME '12:00:00.00000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.555555+08:35')")).matches("TIME '12:00:00.000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.5555555+08:35')")).matches("TIME '12:00:00.0000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.55555555+08:35')")).matches("TIME '12:00:00.00000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.555555555+08:35')")).matches("TIME '12:00:00.000000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.5555555555+08:35')")).matches("TIME '12:00:00.0000000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.55555555555+08:35')")).matches("TIME '12:00:00.00000000000+08:35'");
        assertThat(assertions.expression("date_trunc('hour', TIME '12:34:56.555555555555+08:35')")).matches("TIME '12:00:00.000000000000+08:35'");
    }

    @Test
    public void testAtTimeZone()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Pacific/Apia"))
                .build();

        assertThat(assertions.expression("TIME '12:34:56-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.1+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.12+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.123+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.1234+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.12345+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.123456+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234567-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.1234567+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345678-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.12345678+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456789-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.123456789+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.1234567891-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.1234567891+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.12345678912-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.12345678912+08:35'");
        assertThat(assertions.expression("TIME '12:34:56.123456789123-07:09' AT TIME ZONE '+08:35'", session)).matches("TIME '04:18:56.123456789123+08:35'");

        assertThat(assertions.expression("TIME '12:34:56-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.1-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.1 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.12-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.12 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.123-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.123 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.1234-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.1234 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.12345-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.12345 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.123456-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.123456 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.1234567-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.1234567 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.12345678-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.12345678 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.123456789-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.123456789 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.1234567891-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.1234567891 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.12345678912-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.12345678912 +10:00'");
        assertThat(assertions.expression("TIME '12:34:56.123456789123-07:09' AT TIME ZONE INTERVAL '10' HOUR", session)).matches("TIME '05:43:56.123456789123 +10:00'");
    }

    private static BiFunction<Session, QueryRunner, Object> timeWithTimeZone(int precision, int hour, int minute, int second, long picoOfSecond, int offsetMinutes)
    {
        return (session, queryRunner) -> {
            long picos = (hour * 3600 + minute * 60 + second) * PICOSECONDS_PER_SECOND + picoOfSecond;
            return SqlTimeWithTimeZone.newInstance(precision, picos, offsetMinutes);
        };
    }
}
