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
package io.prestosql.operator.scalar.interval;

import io.prestosql.operator.scalar.AbstractTestExtract;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExtract
        extends AbstractTestExtract
{
    @Override
    protected List<String> types()
    {
        return List.of("interval year to month", "interval day to second");
    }

    @Override
    public void testYear()
    {
        assertThat(assertions.expression("year(INTERVAL '2020' YEAR)")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(INTERVAL '5' MONTH)")).matches("BIGINT '0'");
        assertThat(assertions.expression("year(INTERVAL '2020-05' YEAR TO MONTH)")).matches("BIGINT '2020'");

        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '2020' YEAR)")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '5' MONTH)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '2020-05' YEAR TO MONTH)")).matches("BIGINT '2020'");
    }

    @Override
    public void testMonth()
    {
        assertThat(assertions.expression("month(INTERVAL '2020' YEAR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("month(INTERVAL '5' MONTH)")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(INTERVAL '2020-05' YEAR TO MONTH)")).matches("BIGINT '5'");

        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '2020' YEAR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '5' MONTH)")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '2020-05' YEAR TO MONTH)")).matches("BIGINT '5'");
    }

    @Override
    public void testDay()
    {
        assertThat(assertions.expression("day(INTERVAL '10' DAY)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("day(INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("day(INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '10'");

        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10' DAY)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '10'");
    }

    @Override
    public void testHour()
    {
        assertThat(assertions.expression("hour(INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("hour(INTERVAL '12' HOUR)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("hour(INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '12'");

        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '12' HOUR)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '12'");
    }

    @Override
    public void testMinute()
    {
        assertThat(assertions.expression("minute(INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("minute(INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("minute(INTERVAL '34' MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '34'");

        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34' MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '34'");
    }

    @Override
    public void testSecond()
    {
        assertThat(assertions.expression("second(INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("second(INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("second(INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("second(INTERVAL '56' SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '56'");

        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '56' SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '56'");
    }

    @Test
    public void testMillisecond()
    {
        assertThat(assertions.expression("millisecond(INTERVAL '10' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(INTERVAL '34' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(INTERVAL '56' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56' DAY TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '100'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '120'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(INTERVAL '10 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '123'");
    }
}
