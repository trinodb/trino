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
package io.trino.operator.scalar.interval;

import io.trino.sql.query.QueryAssertions;
import io.trino.type.SqlIntervalDayTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIntervalDayTime
{
    protected QueryAssertions assertions;

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
    public void testLiterals()
    {
        assertThat(assertions.expression("INTERVAL '12 10:45:32.123' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '12 10:45:32.12' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '12 10:45:32' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '12 10:45' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO SECOND"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10:45' DAY TO MINUTE"))
                .isEqualTo(interval(12, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO MINUTE"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO MINUTE"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO HOUR"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO HOUR"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '30' DAY"))
                .isEqualTo(interval(30, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '90' DAY"))
                .isEqualTo(interval(90, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10:45:32.123' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '10:45:32.12' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '10:45:32' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '10:45' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10:45' HOUR TO MINUTE"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR TO MINUTE"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '45:32.123' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '45:32.12' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '45:32' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '45' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '45' MINUTE"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '32.123' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 123));

        assertThat(assertions.expression("INTERVAL '32.12' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 120));

        assertThat(assertions.expression("INTERVAL '32' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 0));

        // Invalid literals
        assertThatThrownBy(assertions.expression("INTERVAL '12X' DAY")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY value: 12X");

        assertThatThrownBy(assertions.expression("INTERVAL '12 10' DAY")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY value: 12 10");

        assertThatThrownBy(assertions.expression("INTERVAL '12 X' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY TO HOUR value: 12 X");

        assertThatThrownBy(assertions.expression("INTERVAL '12 -10' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY TO HOUR value: 12 -10");

        assertThatThrownBy(assertions.expression("INTERVAL '--12 -10' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY TO HOUR value: --12 -10");

        // Invalid qualifiers (DAY TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '12' DAY TO DAY")::evaluate)
                .hasMessage("line 1:33: mismatched input 'DAY'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' DAY TO YEAR")::evaluate)
                .hasMessage("line 1:36: mismatched input 'YEAR'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' DAY TO MONTH")::evaluate)
                .hasMessage("line 1:36: mismatched input 'MONTH'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        // Invalid qualifiers (HOUR TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '10' HOUR TO HOUR")::evaluate)
                .hasMessage("line 1:26: Invalid interval qualifier: HOUR TO HOUR");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' HOUR TO YEAR")::evaluate)
                .hasMessage("line 1:37: mismatched input 'YEAR'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' HOUR TO MONTH")::evaluate)
                .hasMessage("line 1:37: mismatched input 'MONTH'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' HOUR TO DAY")::evaluate)
                .hasMessage("line 1:37: mismatched input 'DAY'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        // Invalid qualifiers (MINUTE TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '45' MINUTE TO MINUTE")::evaluate)
                .hasMessage("line 1:26: Invalid interval qualifier: MINUTE TO MINUTE");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO YEAR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'YEAR'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO MONTH")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MONTH'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO DAY")::evaluate)
                .hasMessage("line 1:39: mismatched input 'DAY'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO HOUR")::evaluate)
                .hasMessage("line 1:29: Invalid interval qualifier: MINUTE TO HOUR");

        // Invalid qualifiers (SECOND TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '32' SECOND TO SECOND")::evaluate)
                .hasMessage("line 1:36: mismatched input 'SECOND'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO YEAR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'YEAR'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO MONTH")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MONTH'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO DAY")::evaluate)
                .hasMessage("line 1:39: mismatched input 'DAY'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO HOUR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'HOUR'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO MINUTE")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MINUTE'. Expecting: ')', ','");
    }

    private static SqlIntervalDayTime interval(int day, int hour, int minute, int second, int milliseconds)
    {
        return new SqlIntervalDayTime(day, hour, minute, second, milliseconds);
    }
}
