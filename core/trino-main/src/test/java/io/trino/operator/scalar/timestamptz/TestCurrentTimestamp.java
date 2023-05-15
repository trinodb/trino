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
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCurrentTimestamp
{
    @Test
    public void testRoundUp()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            Session session = Session.builder(assertions.getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                    .setStart(Instant.ofEpochSecond(0, 999_999_999))
                    .build();

            assertThat(assertions.expression("current_timestamp(0)", session)).matches("TIMESTAMP '1970-01-01 00:00:01 UTC'");
            assertThat(assertions.expression("current_timestamp(1)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.0 UTC'");
            assertThat(assertions.expression("current_timestamp(2)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.00 UTC'");
            assertThat(assertions.expression("current_timestamp(3)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.000 UTC'");
            assertThat(assertions.expression("current_timestamp(4)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.0000 UTC'");
            assertThat(assertions.expression("current_timestamp(5)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.00000 UTC'");
            assertThat(assertions.expression("current_timestamp(6)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.000000 UTC'");
            assertThat(assertions.expression("current_timestamp(7)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.0000000 UTC'");
            assertThat(assertions.expression("current_timestamp(8)", session)).matches("TIMESTAMP '1970-01-01 00:00:01.00000000 UTC'");
            assertThat(assertions.expression("current_timestamp(9)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.999999999 UTC'");
            assertThat(assertions.expression("current_timestamp(10)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.9999999990 UTC'");
            assertThat(assertions.expression("current_timestamp(11)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.99999999900 UTC'");
            assertThat(assertions.expression("current_timestamp(12)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.999999999000 UTC'");
        }
    }

    @Test
    public void testRoundDown()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            Session session = Session.builder(assertions.getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                    .setStart(Instant.ofEpochSecond(0, 1))
                    .build();

            assertThat(assertions.expression("current_timestamp(0)", session)).matches("TIMESTAMP '1970-01-01 00:00:00 UTC'");
            assertThat(assertions.expression("current_timestamp(1)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.0 UTC'");
            assertThat(assertions.expression("current_timestamp(2)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.00 UTC'");
            assertThat(assertions.expression("current_timestamp(3)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.000 UTC'");
            assertThat(assertions.expression("current_timestamp(4)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.0000 UTC'");
            assertThat(assertions.expression("current_timestamp(5)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.00000 UTC'");
            assertThat(assertions.expression("current_timestamp(6)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.000000 UTC'");
            assertThat(assertions.expression("current_timestamp(7)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.0000000 UTC'");
            assertThat(assertions.expression("current_timestamp(8)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.00000000 UTC'");
            assertThat(assertions.expression("current_timestamp(9)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.000000001 UTC'");
            assertThat(assertions.expression("current_timestamp(10)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.0000000010 UTC'");
            assertThat(assertions.expression("current_timestamp(11)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.00000000100 UTC'");
            assertThat(assertions.expression("current_timestamp(12)", session)).matches("TIMESTAMP '1970-01-01 00:00:00.000000001000 UTC'");
        }
    }
}
