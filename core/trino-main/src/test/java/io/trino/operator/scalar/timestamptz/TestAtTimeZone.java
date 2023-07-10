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
package io.trino.operator.scalar.timestamptz;

import io.trino.Session;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestAtTimeZone
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Pacific/Apia"))
                .build();
        assertions = new QueryAssertions(session);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAtTimeZone()
    {
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.1 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.12 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.123 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1234 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.1234 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12345 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.12345 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.123456 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1234567 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.1234567 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12345678 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.12345678 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456789 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.123456789 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1234567891 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.1234567891 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12345678912 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.12345678912 America/Los_Angeles'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456789123 +07:09', 'America/Los_Angeles')")).matches("TIMESTAMP '2020-04-30 22:25:56.123456789123 America/Los_Angeles'");
    }

    @Test
    public void testAtTimeZoneWithOffset()
    {
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.1 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.12 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.123 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1234 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.1234 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12345 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.12345 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.123456 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1234567 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.1234567 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12345678 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.12345678 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456789 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.123456789 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.1234567891 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.1234567891 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.12345678912 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.12345678912 +03:04'");
        assertThat(assertions.expression("at_timezone(TIMESTAMP '2020-05-01 12:34:56.123456789123 +07:09', INTERVAL '03:04' HOUR TO MINUTE)")).matches("TIMESTAMP '2020-05-01 08:29:56.123456789123 +03:04'");
    }
}
