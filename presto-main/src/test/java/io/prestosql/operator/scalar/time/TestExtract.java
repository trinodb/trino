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
package io.prestosql.operator.scalar.time;

import io.prestosql.spi.PrestoException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testYear()
    {
        for (int i = 0; i <= 12; i++) {
            int precision = i;
            assertThatThrownBy(() -> assertions.expression(format("EXTRACT(YEAR FROM CAST(NULL AS TIME(%s)))", precision)))
                    .isInstanceOf(PrestoException.class)
                    .hasMessage(format("line 1:26: Cannot extract YEAR from time(%s)", precision));
        }
    }

    @Test
    public void testMonth()
    {
        for (int i = 0; i <= 12; i++) {
            int precision = i;
            assertThatThrownBy(() -> assertions.expression(format("EXTRACT(MONTH FROM CAST(NULL AS TIME(%s)))", precision)))
                    .isInstanceOf(PrestoException.class)
                    .hasMessage(format("line 1:27: Cannot extract MONTH from time(%s)", precision));
        }
    }

    @Test
    public void testDay()
    {
        for (int i = 0; i <= 12; i++) {
            int precision = i;
            assertThatThrownBy(() -> assertions.expression(format("EXTRACT(DAY FROM CAST(NULL AS TIME(%s)))", precision)))
                    .isInstanceOf(PrestoException.class)
                    .hasMessage(format("line 1:25: Cannot extract DAY from time(%s)", precision));
        }
    }

    @Test
    public void testHour()
    {
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1234')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12345')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123456')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1234567')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12345678')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123456789')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.1234567890')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.12345678901')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIME '12:34:56.123456789012')")).matches("BIGINT '12'");

        assertThat(assertions.expression("hour(TIME '12:34:56')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1234')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12345')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123456')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1234567')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12345678')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123456789')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.1234567890')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.12345678901')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIME '12:34:56.123456789012')")).matches("BIGINT '12'");
    }

    @Test
    public void testMinute()
    {
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1234')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12345')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123456')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1234567')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12345678')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123456789')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.1234567890')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.12345678901')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIME '12:34:56.123456789012')")).matches("BIGINT '34'");

        assertThat(assertions.expression("minute(TIME '12:34:56')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1234')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12345')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123456')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1234567')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12345678')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123456789')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.1234567890')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.12345678901')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIME '12:34:56.123456789012')")).matches("BIGINT '34'");
    }

    @Test
    public void testSecond()
    {
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1234')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12345')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123456')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1234567')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12345678')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123456789')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.1234567890')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.12345678901')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIME '12:34:56.123456789012')")).matches("BIGINT '56'");

        assertThat(assertions.expression("second(TIME '12:34:56')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1234')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12345')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123456')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1234567')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12345678')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123456789')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.1234567890')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.12345678901')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIME '12:34:56.123456789012')")).matches("BIGINT '56'");
    }

    @Test
    public void testMillisecond()
    {
        assertThatThrownBy(() -> assertions.expression("EXTRACT(MILLISECOND FROM TIME '12:34:56')"))
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:8: Invalid EXTRACT field: MILLISECOND");

        assertThat(assertions.expression("millisecond(TIME '12:34:56')")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1')")).matches("BIGINT '100'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12')")).matches("BIGINT '120'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1234')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12345')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123456')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1234567')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12345678')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123456789')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.1234567890')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.12345678901')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIME '12:34:56.123456789012')")).matches("BIGINT '123'");
    }

    @Test
    public void testTimezoneHour()
    {
        for (int i = 0; i <= 12; i++) {
            int precision = i;
            assertThatThrownBy(() -> assertions.expression(format("EXTRACT(TIMEZONE_HOUR FROM CAST(NULL AS TIME(%s)))", precision)))
                    .isInstanceOf(PrestoException.class)
                    .hasMessage(format("line 1:35: Cannot extract TIMEZONE_HOUR from time(%s)", precision));
        }
    }

    @Test
    public void testTimezoneMinute()
    {
        for (int i = 0; i <= 12; i++) {
            int precision = i;
            assertThatThrownBy(() -> assertions.expression(format("EXTRACT(TIMEZONE_MINUTE FROM CAST(NULL AS TIME(%s)))", precision)))
                    .isInstanceOf(PrestoException.class)
                    .hasMessage(format("line 1:37: Cannot extract TIMEZONE_MINUTE from time(%s)", precision));
        }
    }
}
