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
package io.trino.operator.scalar.timetz;

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
    public void testHour()
    {
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1234+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12345+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123456+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1234567+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12345678+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123456789+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '12'");

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
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1234+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12345+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123456+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1234567+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12345678+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123456789+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '34'");

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
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1234+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12345+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123456+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1234567+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12345678+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123456789+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '56'");

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
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MILLISECOND FROM TIME '12:34:56+08:35')")::evaluate)
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:12: Invalid EXTRACT field: MILLISECOND");

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

    @Test
    public void testTimezoneHour()
    {
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234567+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345678+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456789+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '8'");

        assertThat(assertions.expression("timezone_hour(TIME '12:34:56+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234567+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345678+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456789+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '8'");

        // negative offsets
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234567-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345678-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456789-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234567890-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345678901-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456789012-08:35')")).matches("BIGINT '-8'");

        assertThat(assertions.expression("timezone_hour(TIME '12:34:56-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234567-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345678-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456789-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234567890-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345678901-08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456789012-08:35')")).matches("BIGINT '-8'");

        // negative offset minutes
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234567-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345678-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456789-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.1234567890-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.12345678901-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIME '12:34:56.123456789012-00:35')")).matches("BIGINT '0'");

        assertThat(assertions.expression("timezone_hour(TIME '12:34:56-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234567-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345678-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456789-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.1234567890-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.12345678901-00:35')")).matches("BIGINT '0'");
        assertThat(assertions.expression("timezone_hour(TIME '12:34:56.123456789012-00:35')")).matches("BIGINT '0'");
    }

    @Test
    public void testTimezoneMinute()
    {
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234567+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345678+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456789+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '35'");

        assertThat(assertions.expression("timezone_minute(TIME '12:34:56+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234567+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345678+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456789+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234567890+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345678901+08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456789012+08:35')")).matches("BIGINT '35'");

        // negative offsets
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234567-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345678-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456789-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234567890-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345678901-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456789012-08:35')")).matches("BIGINT '-35'");

        assertThat(assertions.expression("timezone_minute(TIME '12:34:56-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234567-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345678-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456789-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234567890-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345678901-08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456789012-08:35')")).matches("BIGINT '-35'");

        // negative offset minutes
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234567-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345678-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456789-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.1234567890-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.12345678901-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIME '12:34:56.123456789012-00:35')")).matches("BIGINT '-35'");

        assertThat(assertions.expression("timezone_minute(TIME '12:34:56-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234567-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345678-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456789-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.1234567890-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.12345678901-00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIME '12:34:56.123456789012-00:35')")).matches("BIGINT '-35'");
    }

    @Test
    public void testUnsupported()
    {
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.1-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.12-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.123-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.1234-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.12345-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.123456-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.1234567-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.12345678-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.123456789-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.1234567890-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.12345678901-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(YEAR FROM TIME '12:34:56.123456789012-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.1-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.12-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.123-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.1234-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.12345-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.123456-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.1234567-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.12345678-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.123456789-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.1234567890-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.12345678901-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(MONTH FROM TIME '12:34:56.123456789012-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.1-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.12-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.123-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.1234-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.12345-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.123456-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.1234567-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.12345678-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.123456789-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.1234567890-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.12345678901-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
        assertTrinoExceptionThrownBy(assertions.expression("EXTRACT(DAY FROM TIME '12:34:56.123456789012-00:35')")::evaluate).hasErrorCode(StandardErrorCode.TYPE_MISMATCH);
    }
}
