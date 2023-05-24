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
package io.trino.operator.scalar.timestamp;

import io.trino.Session;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHumanReadableSeconds
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(TestingSession.DEFAULT_TIME_ZONE_KEY)
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
    public void testToHumanRedableSecondsFormat()
    {
        assertThat(assertions.function("human_readable_seconds", "0"))
                .hasType(VARCHAR)
                .isEqualTo("0 seconds");

        assertThat(assertions.function("human_readable_seconds", "1"))
                .hasType(VARCHAR)
                .isEqualTo("1 second");

        assertThat(assertions.function("human_readable_seconds", "60"))
                .hasType(VARCHAR)
                .isEqualTo("1 minute");

        assertThat(assertions.function("human_readable_seconds", "-60"))
                .hasType(VARCHAR)
                .isEqualTo("1 minute");

        assertThat(assertions.function("human_readable_seconds", "61"))
                .hasType(VARCHAR)
                .isEqualTo("1 minute, 1 second");

        assertThat(assertions.function("human_readable_seconds", "-61"))
                .hasType(VARCHAR)
                .isEqualTo("1 minute, 1 second");

        assertThat(assertions.expression("human_readable_seconds(535333.9513888889)"))
                .hasType(VARCHAR)
                .isEqualTo("6 days, 4 hours, 42 minutes, 14 seconds");

        assertThat(assertions.expression("human_readable_seconds(535333.2513888889)"))
                .hasType(VARCHAR)
                .isEqualTo("6 days, 4 hours, 42 minutes, 13 seconds");

        assertThat(assertions.function("human_readable_seconds", "56363463"))
                .hasType(VARCHAR)
                .isEqualTo("93 weeks, 1 day, 8 hours, 31 minutes, 3 seconds");

        assertThat(assertions.function("human_readable_seconds", "3660"))
                .hasType(VARCHAR)
                .isEqualTo("1 hour, 1 minute");

        assertThat(assertions.function("human_readable_seconds", "3601"))
                .hasType(VARCHAR)
                .isEqualTo("1 hour, 1 second");

        assertThat(assertions.function("human_readable_seconds", "8003"))
                .hasType(VARCHAR)
                .isEqualTo("2 hours, 13 minutes, 23 seconds");

        assertThat(assertions.function("human_readable_seconds", "NULL"))
                .isNull(VARCHAR);

        // check for NaN
        assertTrinoExceptionThrownBy(() -> assertions.function("human_readable_seconds", "0.0E0 / 0.0E0").evaluate())
                .hasMessage("Invalid argument found: NaN");

        // check for infinity
        assertTrinoExceptionThrownBy(() -> assertions.function("human_readable_seconds", "1.0E0 / 0.0E0").evaluate())
                .hasMessage("Invalid argument found: Infinity");
    }
}
