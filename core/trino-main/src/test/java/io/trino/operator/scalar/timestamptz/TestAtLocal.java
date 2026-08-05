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
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAtLocal
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("America/Los_Angeles"))
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
    public void testTimestampWithTimeZone()
    {
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 +07:09' AT LOCAL"))
                .matches("TIMESTAMP '2020-04-30 22:25:56 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123 +07:09' AT LOCAL"))
                .matches("TIMESTAMP '2020-04-30 22:25:56.123 America/Los_Angeles'");
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56.123456789 +07:09' AT LOCAL"))
                .matches("TIMESTAMP '2020-04-30 22:25:56.123456789 America/Los_Angeles'");
    }

    @Test
    public void testTimestamp()
    {
        // Timestamp without time zone is treated as being in the session time zone, so AT LOCAL is the identity.
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56' AT LOCAL"))
                .matches("TIMESTAMP '2020-05-01 12:34:56 America/Los_Angeles'");
    }

    @Test
    public void testTime()
    {
        // Session zone is America/Los_Angeles (-07:00 in May; PDT). A naive TIME
        // is interpreted in the session zone, so AT LOCAL attaches the session offset.
        assertThat(assertions.expression("TIME '12:34:56' AT LOCAL"))
                .matches("TIME '12:34:56-07:00'");
    }

    @Test
    public void testTimeWithTimeZone()
    {
        // Re-anchor TIME from +07:09 to session offset -07:00.
        assertThat(assertions.expression("TIME '12:34:56+07:09' AT LOCAL"))
                .matches("TIME '22:25:56-07:00'");
    }

    @Test
    public void testFollowsSessionTimeZone()
    {
        // Same instant rendered in Pacific/Apia (+13:00 in May 2020).
        Session pacific = testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Pacific/Apia")).build();
        try (QueryAssertions pacificAssertions = new QueryAssertions(pacific)) {
            assertThat(pacificAssertions.expression("TIMESTAMP '2020-05-01 12:34:56 +07:09' AT LOCAL"))
                    .matches("TIMESTAMP '2020-05-01 18:25:56 Pacific/Apia'");
        }
    }

    @Test
    public void testEquivalentToAtTimeZoneWithSessionZone()
    {
        // AT LOCAL is sugar for AT TIME ZONE <session zone>
        assertThat(assertions.expression("TIMESTAMP '2020-05-01 12:34:56 +07:09' AT LOCAL"))
                .matches("TIMESTAMP '2020-05-01 12:34:56 +07:09' AT TIME ZONE 'America/Los_Angeles'");
    }
}
