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
package io.prestosql.operator.scalar.timestamptz;

import io.prestosql.Session;
import io.prestosql.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampWithTimeZoneToTimeCast
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .setTimeZoneKey(getTimeZoneKey("Pacific/Apia"))
                .build();
        assertions = new QueryAssertions(session);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCastToTime()
    {
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56 +07:09' AS TIME)")).matches("TIME '12:34:56'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1 +07:09' AS TIME)")).matches("TIME '12:34:56.1'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11 +07:09' AS TIME)")).matches("TIME '12:34:56.11'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");

        // round down
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.1111111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.11111111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.111111111111 +07:09' AS TIME)")).matches("TIME '12:34:56.111'");

        // round up
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.5555555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.55555555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
        assertThat(assertions.expression("CAST(TIMESTAMP '2020-05-10 12:34:56.555555555555 +07:09' AS TIME)")).matches("TIME '12:34:56.556'");
    }
}
