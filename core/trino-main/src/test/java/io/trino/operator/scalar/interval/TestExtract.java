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
package io.trino.operator.scalar.interval;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExtract
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void tearDown()
    {
        assertions.close();
    }

    @Test
    public void testYear()
    {
        // YEAR is always the leading field, so it holds the whole magnitude
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '42' YEAR)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '42-07' YEAR TO MONTH)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '42-20' YEAR TO MONTH)")).matches("BIGINT '43'");

        assertThat(assertions.expression("YEAR(INTERVAL '42-20' YEAR TO MONTH)")).matches("BIGINT '43'");
    }

    @Test
    public void testMonth()
    {
        // leading field: the whole magnitude, not bounded by a year
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '7' MONTH)")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '30' MONTH)")).matches("BIGINT '30'");
        // trailing field: bounded by the year
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '42-07' YEAR TO MONTH)")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '42-20' YEAR TO MONTH)")).matches("BIGINT '8'");

        assertThat(assertions.expression("MONTH(INTERVAL '30' MONTH)")).matches("BIGINT '30'");
    }

    @Test
    public void testDay()
    {
        // DAY is always the leading field, so it holds the whole magnitude
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42' DAY)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12' DAY TO HOUR)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 30' DAY TO HOUR)")).matches("BIGINT '43'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34' DAY TO MINUTE)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '42'");
    }

    @Test
    public void testHour()
    {
        // leading field: the whole magnitude, not bounded by a day
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '6' HOUR)")).matches("BIGINT '6'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '28' HOUR)")).matches("BIGINT '28'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '28:34' HOUR TO MINUTE)")).matches("BIGINT '28'");
        // trailing field: bounded by the day
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '12'");

        assertThat(assertions.expression("HOUR(INTERVAL '28' HOUR)")).matches("BIGINT '28'");
    }

    @Test
    public void testMinute()
    {
        // leading field: the whole magnitude, not bounded by an hour
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '42' MINUTE)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '182' MINUTE)")).matches("BIGINT '182'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '182:56' MINUTE TO SECOND)")).matches("BIGINT '182'");
        // trailing field: bounded by the hour
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '34'");

        assertThat(assertions.expression("MINUTE(INTERVAL '182' MINUTE)")).matches("BIGINT '182'");
    }

    @Test
    public void testSecond()
    {
        // leading field: the whole magnitude, not bounded by a minute
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10' SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '120' SECOND)")).matches("BIGINT '120'");
        // trailing field: bounded by the minute
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '1 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '56'");

        assertThat(assertions.expression("SECOND(INTERVAL '120' SECOND)")).matches("BIGINT '120'");
    }

    @Test
    public void testCannotExtractFieldOutsideQualifier()
    {
        assertThatThrownBy(assertions.expression("EXTRACT(MONTH FROM INTERVAL '42' YEAR)")::evaluate)
                .hasMessageContaining("Cannot extract MONTH from interval year");
        assertThatThrownBy(assertions.expression("EXTRACT(YEAR FROM INTERVAL '7' MONTH)")::evaluate)
                .hasMessageContaining("Cannot extract YEAR from interval month");

        assertThatThrownBy(assertions.expression("EXTRACT(HOUR FROM INTERVAL '42' DAY)")::evaluate)
                .hasMessageContaining("Cannot extract HOUR from interval day");
        assertThatThrownBy(assertions.expression("EXTRACT(DAY FROM INTERVAL '12' HOUR)")::evaluate)
                .hasMessageContaining("Cannot extract DAY from interval hour");
        assertThatThrownBy(assertions.expression("EXTRACT(SECOND FROM INTERVAL '12:34' HOUR TO MINUTE)")::evaluate)
                .hasMessageContaining("Cannot extract SECOND from interval hour(2) to minute");
        assertThatThrownBy(assertions.expression("EXTRACT(DAY FROM INTERVAL '34:56' MINUTE TO SECOND)")::evaluate)
                .hasMessageContaining("Cannot extract DAY from interval minute(2) to second");

        // a day-time field cannot be extracted from a year-month interval, and vice versa
        assertThatThrownBy(assertions.expression("EXTRACT(YEAR FROM INTERVAL '1' DAY)")::evaluate)
                .hasMessageContaining("Cannot extract YEAR from interval day");
        assertThatThrownBy(assertions.expression("EXTRACT(HOUR FROM INTERVAL '1' YEAR)")::evaluate)
                .hasMessageContaining("Cannot extract HOUR from interval year");
    }
}
