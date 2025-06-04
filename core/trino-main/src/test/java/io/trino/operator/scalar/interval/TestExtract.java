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
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '42' YEAR)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '7' MONTH)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '20' MONTH)")).matches("BIGINT '1'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '42-07' YEAR TO MONTH)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM INTERVAL '42-20' YEAR TO MONTH)")).matches("BIGINT '43'");

        assertThat(assertions.expression("YEAR(INTERVAL '42' YEAR)")).matches("BIGINT '42'");
        assertThat(assertions.expression("YEAR(INTERVAL '7' MONTH)")).matches("BIGINT '0'");
        assertThat(assertions.expression("YEAR(INTERVAL '20' MONTH)")).matches("BIGINT '1'");
        assertThat(assertions.expression("YEAR(INTERVAL '42-07' YEAR TO MONTH)")).matches("BIGINT '42'");
        assertThat(assertions.expression("YEAR(INTERVAL '42-20' YEAR TO MONTH)")).matches("BIGINT '43'");
    }

    @Test
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '42' YEAR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '7' MONTH)")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM INTERVAL '42-07' YEAR TO MONTH)")).matches("BIGINT '7'");

        assertThat(assertions.expression("MONTH(INTERVAL '42' YEAR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("MONTH(INTERVAL '7' MONTH)")).matches("BIGINT '7'");
        assertThat(assertions.expression("MONTH(INTERVAL '42-07' YEAR TO MONTH)")).matches("BIGINT '7'");
    }

    @Test
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42' DAY)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '30' HOUR)")).matches("BIGINT '1'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '2000' MINUTE)")).matches("BIGINT '1'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '99999' SECOND)")).matches("BIGINT '1'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12' DAY TO HOUR)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 30' DAY TO HOUR)")).matches("BIGINT '43'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34' DAY TO MINUTE)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(DAY FROM INTERVAL '42 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '42'");

        assertThat(assertions.expression("DAY(INTERVAL '42' DAY)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '12' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("DAY(INTERVAL '30' HOUR)")).matches("BIGINT '1'");
        assertThat(assertions.expression("DAY(INTERVAL '42' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("DAY(INTERVAL '2000' MINUTE)")).matches("BIGINT '1'");
        assertThat(assertions.expression("DAY(INTERVAL '42' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("DAY(INTERVAL '99999' SECOND)")).matches("BIGINT '1'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12' DAY TO HOUR)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 30' DAY TO HOUR)")).matches("BIGINT '43'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34' DAY TO MINUTE)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.12' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.123' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.1234' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.12345' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.123456' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.1234567' DAY TO SECOND)")).matches("BIGINT '42'");
        assertThat(assertions.expression("DAY(INTERVAL '42 12:34:56.12345678' DAY TO SECOND)")).matches("BIGINT '42'");
    }

    @Test
    public void testHour()
    {
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '42' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '6' HOUR)")).matches("BIGINT '6'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '28' HOUR)")).matches("BIGINT '4'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '60' MINUTE)")).matches("BIGINT '1'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '180' MINUTE)")).matches("BIGINT '3'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '60' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '1 12:34' DAY TO MINUTE)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.1' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.12' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.123' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.1234' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.12345' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.123456' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.1234567' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM INTERVAL '34:56.12345678' MINUTE TO SECOND)")).matches("BIGINT '0'");

        assertThat(assertions.expression("HOUR(INTERVAL '42' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '6' HOUR)")).matches("BIGINT '6'");
        assertThat(assertions.expression("HOUR(INTERVAL '28' HOUR)")).matches("BIGINT '4'");
        assertThat(assertions.expression("HOUR(INTERVAL '60' MINUTE)")).matches("BIGINT '1'");
        assertThat(assertions.expression("HOUR(INTERVAL '180' MINUTE)")).matches("BIGINT '3'");
        assertThat(assertions.expression("HOUR(INTERVAL '60' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '12'");
        assertThat(assertions.expression("HOUR(INTERVAL '1 12:34' DAY TO MINUTE)")).matches("BIGINT '12'");
        assertThat(assertions.expression("HOUR(INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("HOUR(INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '12'");
        assertThat(assertions.expression("HOUR(INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '12'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.1' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.12' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.123' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.1234' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.12345' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.123456' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.1234567' MINUTE TO SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("HOUR(INTERVAL '34:56.12345678' MINUTE TO SECOND)")).matches("BIGINT '0'");
    }

    @Test
    public void testMinute()
    {
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '1' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '6' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '28' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '42' MINUTE)")).matches("BIGINT '42'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '182' MINUTE)")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '10' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '120' SECOND)")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '1 12:34' DAY TO MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.1' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.12' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.123' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.1234' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.12345' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.123456' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.1234567' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM INTERVAL '34:56.12345678' MINUTE TO SECOND)")).matches("BIGINT '34'");

        assertThat(assertions.expression("MINUTE(INTERVAL '1' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("MINUTE(INTERVAL '6' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("MINUTE(INTERVAL '28' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("MINUTE(INTERVAL '42' MINUTE)")).matches("BIGINT '42'");
        assertThat(assertions.expression("MINUTE(INTERVAL '182' MINUTE)")).matches("BIGINT '2'");
        assertThat(assertions.expression("MINUTE(INTERVAL '10' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("MINUTE(INTERVAL '120' SECOND)")).matches("BIGINT '2'");
        assertThat(assertions.expression("MINUTE(INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("MINUTE(INTERVAL '1 12:34' DAY TO MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.1' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.12' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.123' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.1234' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.12345' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.123456' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.1234567' MINUTE TO SECOND)")).matches("BIGINT '34'");
        assertThat(assertions.expression("MINUTE(INTERVAL '34:56.12345678' MINUTE TO SECOND)")).matches("BIGINT '34'");
    }

    @Test
    public void testSecond()
    {
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '1' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '6' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '28' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '42' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '182' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '10' SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '120' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '1 12:34' DAY TO MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '1 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.1' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.12' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.123' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.1234' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.12345' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.123456' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.1234567' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM INTERVAL '34:56.12345678' MINUTE TO SECOND)")).matches("BIGINT '56'");

        assertThat(assertions.expression("SECOND(INTERVAL '1' DAY)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '6' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '28' HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '42' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '182' MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '10' SECOND)")).matches("BIGINT '10'");
        assertThat(assertions.expression("SECOND(INTERVAL '120' SECOND)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '1 12' DAY TO HOUR)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '1 12:34' DAY TO MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '1 12:34:56' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '1 12:34:56.1' DAY TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '12:34' HOUR TO MINUTE)")).matches("BIGINT '0'");
        assertThat(assertions.expression("SECOND(INTERVAL '12:34:56' HOUR TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.1' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.12' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.123' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.1234' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.12345' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.123456' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.1234567' MINUTE TO SECOND)")).matches("BIGINT '56'");
        assertThat(assertions.expression("SECOND(INTERVAL '34:56.12345678' MINUTE TO SECOND)")).matches("BIGINT '56'");
    }
}
