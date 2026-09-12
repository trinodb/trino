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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestToUnixTimeNanos
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testToUnixTimeNanos()
    {
        // p=0..3: short form (long timestamp, millis precision)
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00 UTC')")).matches("BIGINT '1609459200000000000'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.1 UTC')")).matches("BIGINT '1609459200100000000'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.12 UTC')")).matches("BIGINT '1609459200120000000'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.123 UTC')")).matches("BIGINT '1609459200123000000'");
        // p=4..12: long form (LongTimestampWithTimeZone, sub-millisecond precision)
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.1234 UTC')")).matches("BIGINT '1609459200123400000'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.12345 UTC')")).matches("BIGINT '1609459200123450000'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.123456 UTC')")).matches("BIGINT '1609459200123456000'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.1234567 UTC')")).matches("BIGINT '1609459200123456700'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.12345678 UTC')")).matches("BIGINT '1609459200123456780'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.123456789 UTC')")).matches("BIGINT '1609459200123456789'");
        // p=10..12: sub-nanosecond picos are truncated (integer division)
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.1234567890 UTC')")).matches("BIGINT '1609459200123456789'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.12345678901 UTC')")).matches("BIGINT '1609459200123456789'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '2021-01-01 00:00:00.123456789012 UTC')")).matches("BIGINT '1609459200123456789'");
    }

    @Test
    public void testToUnixTimeNanosEpochBoundary()
    {
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '1970-01-01 00:00:00 UTC')")).matches("BIGINT '0'");
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '1970-01-01 00:00:00.000000000 UTC')")).matches("BIGINT '0'");
    }

    @Test
    public void testToUnixTimeNanosBeforeEpoch()
    {
        assertThat(assertions.expression("to_unixtime_nanos(TIMESTAMP '1969-12-31 23:59:59.999999999 UTC')")).matches("BIGINT '-1'");
    }
}
