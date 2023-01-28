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
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDateTrunc
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(TestingSession.DEFAULT_TIME_ZONE_KEY)
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
    public void testDateTruncYear()
    {
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-01-01 00:00:00'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-01-01 00:00:00.0'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-01-01 00:00:00.00'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-01-01 00:00:00.000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-01-01 00:00:00.0000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-01-01 00:00:00.00000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-01-01 00:00:00.000000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-01-01 00:00:00.0000000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-01-01 00:00:00.00000000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-01-01 00:00:00.000000000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-01-01 00:00:00.0000000000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-01-01 00:00:00.00000000000'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-01-01 00:00:00.000000000000'");
    }

    @Test
    public void testDateTruncQuarter()
    {
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-04-01 00:00:00'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-04-01 00:00:00.0'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-04-01 00:00:00.00'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-04-01 00:00:00.000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-04-01 00:00:00.0000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-04-01 00:00:00.00000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-04-01 00:00:00.000000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-04-01 00:00:00.0000000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-04-01 00:00:00.00000000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-04-01 00:00:00.000000000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-04-01 00:00:00.0000000000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-04-01 00:00:00.00000000000'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-04-01 00:00:00.000000000000'");
    }

    @Test
    public void testDateTruncMonth()
    {
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-01 00:00:00'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-01 00:00:00.0'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-01 00:00:00.00'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-01 00:00:00.000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-01 00:00:00.0000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-01 00:00:00.00000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-01 00:00:00.000000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000000'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000000'");
    }

    @Test
    public void testDateTruncWeek()
    {
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-04 00:00:00'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-04 00:00:00.0'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-04 00:00:00.00'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-04 00:00:00.000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-04 00:00:00.0000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-04 00:00:00.00000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-04 00:00:00.000000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-04 00:00:00.0000000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-04 00:00:00.00000000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-04 00:00:00.000000000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-04 00:00:00.0000000000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-04 00:00:00.00000000000'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-04 00:00:00.000000000000'");
    }

    @Test
    public void testDateTruncDay()
    {
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 00:00:00'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-10 00:00:00.0'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-10 00:00:00.00'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-10 00:00:00.000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-10 00:00:00.0000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-10 00:00:00.00000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-10 00:00:00.000000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-10 00:00:00.0000000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-10 00:00:00.00000000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-10 00:00:00.000000000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-10 00:00:00.0000000000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-10 00:00:00.00000000000'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-10 00:00:00.000000000000'");
    }

    @Test
    public void testDateTruncHour()
    {
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:00:00'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-10 12:00:00.0'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-10 12:00:00.00'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-10 12:00:00.000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-10 12:00:00.0000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-10 12:00:00.00000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-10 12:00:00.000000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-10 12:00:00.0000000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-10 12:00:00.00000000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-10 12:00:00.000000000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-10 12:00:00.0000000000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-10 12:00:00.00000000000'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-10 12:00:00.000000000000'");
    }

    @Test
    public void testDateTruncMinute()
    {
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:34:00'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-10 12:34:00.0'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-10 12:34:00.00'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-10 12:34:00.000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-10 12:34:00.0000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-10 12:34:00.00000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-10 12:34:00.000000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-10 12:34:00.0000000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-10 12:34:00.00000000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-10 12:34:00.000000000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-10 12:34:00.0000000000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-10 12:34:00.00000000000'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-10 12:34:00.000000000000'");
    }

    @Test
    public void testDateTruncSecond()
    {
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-10 12:34:56.0'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-10 12:34:56.00'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-10 12:34:56.000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-10 12:34:56.0000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-10 12:34:56.00000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-10 12:34:56.000000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-10 12:34:56.0000000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-10 12:34:56.00000000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-10 12:34:56.000000000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-10 12:34:56.0000000000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-10 12:34:56.00000000000'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-10 12:34:56.000000000000'");
    }

    @Test
    public void testDateTruncMilliSecond()
    {
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56')")).matches("TIMESTAMP '2020-05-10 12:34:56'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1')")).matches("TIMESTAMP '2020-05-10 12:34:56.1'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12')")).matches("TIMESTAMP '2020-05-10 12:34:56.12'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123')")).matches("TIMESTAMP '2020-05-10 12:34:56.123'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("TIMESTAMP '2020-05-10 12:34:56.1230'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("TIMESTAMP '2020-05-10 12:34:56.12300'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("TIMESTAMP '2020-05-10 12:34:56.123000'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("TIMESTAMP '2020-05-10 12:34:56.1230000'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("TIMESTAMP '2020-05-10 12:34:56.12300000'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("TIMESTAMP '2020-05-10 12:34:56.123000000'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("TIMESTAMP '2020-05-10 12:34:56.1230000000'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("TIMESTAMP '2020-05-10 12:34:56.12300000000'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("TIMESTAMP '2020-05-10 12:34:56.123000000000'");
    }
}
