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
package io.prestosql.operator.scalar.date;

import io.prestosql.spi.PrestoException;
import io.prestosql.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExtract
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
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
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Test
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '2020-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '1960-05-10')")).matches("BIGINT '5'");
    }

    @Test
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '1960-05-10')")).matches("BIGINT '10'");
    }

    @Test
    public void testHour()
    {
        assertThatThrownBy(() -> assertions.expression("EXTRACT(HOUR FROM DATE '2020-05-10')"))
                .isInstanceOf(PrestoException.class)
                .hasMessage("line 1:26: Cannot extract HOUR from date");
    }

    @Test
    public void testMinute()
    {
        assertThatThrownBy(() -> assertions.expression("EXTRACT(MINUTE FROM DATE '2020-05-10')"))
                .isInstanceOf(PrestoException.class)
                .hasMessage("line 1:28: Cannot extract MINUTE from date");
    }

    @Test
    public void testSecond()
    {
        assertThatThrownBy(() -> assertions.expression("EXTRACT(SECOND FROM DATE '2020-05-10')"))
                .isInstanceOf(PrestoException.class)
                .hasMessage("line 1:28: Cannot extract SECOND from date");
    }

    @Test
    public void testTimeZoneHour()
    {
        assertThatThrownBy(() -> assertions.expression("EXTRACT(TIMEZONE_HOUR FROM DATE '2020-05-10')"))
                .isInstanceOf(PrestoException.class)
                .hasMessage("line 1:35: Cannot extract TIMEZONE_HOUR from date");
    }

    @Test
    public void testTimeZoneMinute()
    {
        assertThatThrownBy(() -> assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM DATE '2020-05-10')"))
                .isInstanceOf(PrestoException.class)
                .hasMessage("line 1:37: Cannot extract TIMEZONE_MINUTE from date");
    }

    @Test
    public void testDayOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Test
    public void testDayOfYear()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '1960-05-10')")).matches("BIGINT '131'");
    }

    @Test
    public void testQuarter()
    {
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '2020-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Test
    public void testYearOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
    }
}
