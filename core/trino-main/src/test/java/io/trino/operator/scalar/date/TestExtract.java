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
package io.trino.operator.scalar.date;

import io.trino.spi.StandardErrorCode;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

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
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
        assertThat(assertions.expression("year(DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Test
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '2020-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '1960-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(DATE '2020-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(DATE '1960-05-10')")).matches("BIGINT '5'");
    }

    @Test
    public void testWeek()
    {
        assertThat(assertions.expression("EXTRACT(WEEK FROM DATE '2020-05-10')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM DATE '1960-05-10')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(DATE '2020-05-10')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(DATE '1960-05-10')")).matches("BIGINT '19'");
    }

    @Test
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '1960-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(DATE '1960-05-10')")).matches("BIGINT '10'");
    }

    @Test
    public void testDayOfMonth()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM DATE '1960-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(DATE '1960-05-10')")).matches("BIGINT '10'");
    }

    @Test
    public void testDayOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("day_of_week(DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Test
    public void testDow()
    {
        assertThat(assertions.expression("EXTRACT(DOW FROM DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM DATE '1960-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("dow(DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("dow(DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Test
    public void testDayOfYear()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '1960-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(DATE '1960-05-10')")).matches("BIGINT '131'");
    }

    @Test
    public void testDoy()
    {
        assertThat(assertions.expression("EXTRACT(DOY FROM DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM DATE '1960-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("doy(DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("doy(DATE '1960-05-10')")).matches("BIGINT '131'");
    }

    @Test
    public void testQuarter()
    {
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '2020-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '1960-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(DATE '2020-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Test
    public void testYearOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
        assertThat(assertions.expression("year_of_week(DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Test
    public void testYow()
    {
        assertThat(assertions.expression("EXTRACT(YOW FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
        assertThat(assertions.expression("yow(DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("yow(DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Test
    public void testUnsupported()
    {
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(HOUR FROM DATE '2020-05-10')")::evaluate)
                .hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MINUTE FROM DATE '2020-05-10')")::evaluate)
                .hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(SECOND FROM DATE '2020-05-10')")::evaluate)
                .hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM DATE '2020-05-10')")::evaluate)
                .hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM DATE '2020-05-10')")::evaluate)
                .hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
    }
}
