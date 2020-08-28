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
package io.prestosql.operator.scalar.time;

import io.prestosql.Session;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZonedDateTime;

import static io.prestosql.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCastToTimestamp
{
    private static final TimeZoneKey SESSION_TIME_ZONE = DEFAULT_TIME_ZONE_KEY;

    protected QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setStart(Instant.from(ZonedDateTime.of(2020, 5, 1, 12, 34, 56, 123456789, SESSION_TIME_ZONE.getZoneId())))
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
    public void testCastToTimestamp()
    {
        // source = target
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.12'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.123'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567891'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678912'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789123' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789123'");

        // source < target
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.0'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.00'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.0000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.00000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.000000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.10'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.100'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.10000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.100000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.120'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1200000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12000000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.120000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1230000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12300000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123000000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12340000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123400000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234500000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345000000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123450000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234560000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345600000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456000000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345670000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456700000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567800'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678000'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456780000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1234567890'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678900'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.123456789' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789000'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.12345678910'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1234567891' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789100'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.12345678912' AS TIMESTAMP(12))")).matches("TIMESTAMP '2020-05-01 12:34:56.123456789120'");

        // source > target, round down
        assertThat(assertions.expression("CAST(TIME '12:34:56.1' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:56'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.1'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.11'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.1111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.111111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.11111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.1111111111'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.111111111111' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.11111111111'");

        // source > target, round up
        assertThat(assertions.expression("CAST(TIME '12:34:56.5' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(0))")).matches("TIMESTAMP '2020-05-01 12:34:57'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(1))")).matches("TIMESTAMP '2020-05-01 12:34:56.6'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(2))")).matches("TIMESTAMP '2020-05-01 12:34:56.56'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(3))")).matches("TIMESTAMP '2020-05-01 12:34:56.556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(4))")).matches("TIMESTAMP '2020-05-01 12:34:56.5556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(5))")).matches("TIMESTAMP '2020-05-01 12:34:56.55556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(6))")).matches("TIMESTAMP '2020-05-01 12:34:56.555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(7))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(8))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.5555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(9))")).matches("TIMESTAMP '2020-05-01 12:34:56.555555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.55555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");
        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(10))")).matches("TIMESTAMP '2020-05-01 12:34:56.5555555556'");

        assertThat(assertions.expression("CAST(TIME '12:34:56.555555555555' AS TIMESTAMP(11))")).matches("TIMESTAMP '2020-05-01 12:34:56.55555555556'");
    }
}
