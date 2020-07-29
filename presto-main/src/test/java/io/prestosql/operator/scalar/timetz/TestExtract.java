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
package io.prestosql.operator.scalar.timetz;

import io.prestosql.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExtract
{
    protected QueryAssertions assertions;

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
    public void testHour()
    {
        assertThat(assertions.expression("hour(TIME '12:34:56+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1234+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12345+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123456+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1234567+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12345678+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123456789+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '12'");
    }

    @Test
    public void testMinute()
    {
        assertThat(assertions.expression("minute(TIME '12:34:56+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1234+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12345+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123456+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1234567+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12345678+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123456789+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '34'");
    }

    @Test
    public void testSecond()
    {
        assertThat(assertions.expression("second(TIME '12:34:56+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1234+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12345+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123456+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1234567+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12345678+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123456789+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '56'");
    }

    @Test
    public void testMillisecond()
    {
        assertThat(assertions.expression("millisecond(TIME '12:34:56+08:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1+08:35')")).matches("BIGINT '100'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12+08:35')")).matches("BIGINT '120'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1234+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12345+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123456+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1234567+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12345678+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123456789+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '123'");
    }
}
