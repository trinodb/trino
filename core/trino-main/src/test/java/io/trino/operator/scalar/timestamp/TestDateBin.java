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
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDateBin
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
    public void testDateBin()
    {
        // 15-minute bins aligned to 2001-01-01 00:00:00; source falls in [15:30, 15:45)
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01 00:00:00')")).matches("TIMESTAMP '2020-02-11 15:30:00'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.1', TIMESTAMP '2001-01-01 00:00:00.0')")).matches("TIMESTAMP '2020-02-11 15:30:00.0'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.12', TIMESTAMP '2001-01-01 00:00:00.00')")).matches("TIMESTAMP '2020-02-11 15:30:00.00'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.123', TIMESTAMP '2001-01-01 00:00:00.000')")).matches("TIMESTAMP '2020-02-11 15:30:00.000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.1234', TIMESTAMP '2001-01-01 00:00:00.0000')")).matches("TIMESTAMP '2020-02-11 15:30:00.0000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.12345', TIMESTAMP '2001-01-01 00:00:00.00000')")).matches("TIMESTAMP '2020-02-11 15:30:00.00000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.123456', TIMESTAMP '2001-01-01 00:00:00.000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.1234567', TIMESTAMP '2001-01-01 00:00:00.0000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.0000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.12345678', TIMESTAMP '2001-01-01 00:00:00.00000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.00000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.123456789', TIMESTAMP '2001-01-01 00:00:00.000000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.000000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.1234567890', TIMESTAMP '2001-01-01 00:00:00.0000000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.0000000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.12345678901', TIMESTAMP '2001-01-01 00:00:00.00000000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.00000000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17.123456789012', TIMESTAMP '2001-01-01 00:00:00.000000000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.000000000000'");
    }

    @Test
    public void testDateBinOnBoundary()
    {
        // source exactly on a bin boundary — should return the boundary itself
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:30:00', TIMESTAMP '2001-01-01 00:00:00')")).matches("TIMESTAMP '2020-02-11 15:30:00'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:30:00.000000', TIMESTAMP '2001-01-01 00:00:00.000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:30:00.000000000000', TIMESTAMP '2001-01-01 00:00:00.000000000000')")).matches("TIMESTAMP '2020-02-11 15:30:00.000000000000'");
    }

    @Test
    public void testDateBinSourceBeforeOrigin()
    {
        // source before origin — floorDiv correctly rounds toward negative infinity
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2000-12-31 23:44:17', TIMESTAMP '2001-01-01 00:00:00')")).matches("TIMESTAMP '2000-12-31 23:30:00'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2000-12-31 23:44:17.123456', TIMESTAMP '2001-01-01 00:00:00.000000')")).matches("TIMESTAMP '2000-12-31 23:30:00.000000'");
        assertThat(assertions.expression("date_bin(INTERVAL '15' MINUTE, TIMESTAMP '2000-12-31 23:44:17.123456789012', TIMESTAMP '2001-01-01 00:00:00.000000000000')")).matches("TIMESTAMP '2000-12-31 23:30:00.000000000000'");
    }

    @Test
    public void testDateBinSubSecondStride()
    {
        // 500ms stride — source at .345 falls in [.000, .500)
        assertThat(assertions.expression("date_bin(INTERVAL '0.5' SECOND, TIMESTAMP '2020-02-11 15:44:17.345', TIMESTAMP '2001-01-01 00:00:00.000')")).matches("TIMESTAMP '2020-02-11 15:44:17.000'");
        assertThat(assertions.expression("date_bin(INTERVAL '0.5' SECOND, TIMESTAMP '2020-02-11 15:44:17.500', TIMESTAMP '2001-01-01 00:00:00.000')")).matches("TIMESTAMP '2020-02-11 15:44:17.500'");
        assertThat(assertions.expression("date_bin(INTERVAL '0.5' SECOND, TIMESTAMP '2020-02-11 15:44:17.999', TIMESTAMP '2001-01-01 00:00:00.000')")).matches("TIMESTAMP '2020-02-11 15:44:17.500'");
    }

    @Test
    public void testDateBinInvalidStride()
    {
        assertTrinoExceptionThrownBy(assertions.expression("date_bin(INTERVAL '0' SECOND, TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01 00:00:00')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }
}
