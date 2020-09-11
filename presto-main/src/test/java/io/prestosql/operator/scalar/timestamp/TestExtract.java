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
import io.prestosql.sql.query.QueryAssertions;
import io.prestosql.testing.TestingSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExtract
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(TestingSession.DEFAULT_TIME_ZONE_KEY)
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
    public void testYear()
    {
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
    public void testDay()
    {
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
    public void testHour()
    {
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
    public void testDayOfYear()
    {
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
    public void testQuarter()
    {
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
}
