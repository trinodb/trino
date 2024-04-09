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

import io.trino.spi.StandardErrorCode;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExtract
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void tearDown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testYear()
    {
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2020'");

        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2020'");
    }

    @Test
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '5'");

        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '5'");
    }

    @Test
    public void testWeek()
    {
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '19'");

        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '19'");
    }

    @Test
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '10'");

        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '10'");
    }

    @Test
    public void testDayOfMonth()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '10'");

        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '10'");
    }

    @Test
    public void testHour()
    {
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '12'");

        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '12'");
    }

    @Test
    public void testMinute()
    {
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '34'");

        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '34'");
    }

    @Test
    public void testSecond()
    {
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '56'");

        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '56'");

        // negative epoch
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.1')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.12')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.123')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.1234')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.12345')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.123456')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.1234567')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.12345678')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.123456789')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.1234567890')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.12345678901')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '1500-05-10 12:34:56.123456789012')")).matches("BIGINT '56'");

        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.1')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.12')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.123')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.1234')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.12345')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.123456')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.1234567')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.12345678')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.123456789')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.1234567890')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.12345678901')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '1500-05-10 12:34:56.123456789012')")).matches("BIGINT '56'");
    }

    @Test
    public void testMillisecond()
    {
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MILLISECOND FROM TIMESTAMP '2020-05-10 12:34:56')")::evaluate)
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:12: Invalid EXTRACT field: MILLISECOND");

        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '100'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '120'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '123'");
    }

    @Test
    public void testDayOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '7'");

        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '7'");
    }

    @Test
    public void testDow()
    {
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '7'");
    }

    @Test
    public void testDayOfYear()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '131'");

        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '131'");
    }

    @Test
    public void testDoy()
    {
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '131'");
    }

    @Test
    public void testQuarter()
    {
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2'");

        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2'");
    }

    @Test
    public void testWeekOfYear()
    {
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(WEEK_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56')")::evaluate)
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:12: Invalid EXTRACT field: WEEK_OF_YEAR");

        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '19'");
    }

    @Test
    public void testYearOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2020'");

        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2020'");
    }

    @Test
    public void testYow()
    {
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("BIGINT '2020'");
    }

    @Test
    public void testUnsupported()
    {
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567890')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678901')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789012')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
    }
}
