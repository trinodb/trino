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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDateTrunc
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
    public void testDateTruncYear()
    {
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('year', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-01-01 00:00:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncQuarter()
    {
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('quarter', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-04-01 00:00:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncMonth()
    {
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('month', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-01 00:00:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncWeek()
    {
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('week', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-04 00:00:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncDay()
    {
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('day', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 00:00:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncHour()
    {
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('hour', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:00:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncMinute()
    {
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('minute', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:00.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncSecond()
    {
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.0 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.00 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.0000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.00000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.0000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.00000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.0000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.00000000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('second', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.000000000000 Asia/Kathmandu'");
    }

    @Test
    public void testDateTruncMilliSecond()
    {
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1230 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.12300 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.123000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1230000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.12300000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.123000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.1230000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.12300000000 Asia/Kathmandu'");
        assertThat(assertions.expression("date_trunc('millisecond', TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("TIMESTAMP '2020-05-10 12:34:56.123000000000 Asia/Kathmandu'");
    }
}
