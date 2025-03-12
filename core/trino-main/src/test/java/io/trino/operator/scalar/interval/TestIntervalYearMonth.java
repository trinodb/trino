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
import io.trino.type.SqlIntervalYearMonth;
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
public class TestIntervalYearMonth
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
        assertThat(assertions.expression("INTERVAL '124-30' YEAR TO MONTH"))
                .isEqualTo(interval(124, 30));

        assertThat(assertions.expression("INTERVAL '124' YEAR TO MONTH"))
                .isEqualTo(interval(124, 0));

        assertThat(assertions.expression("INTERVAL '30' MONTH"))
                .isEqualTo(interval(0, 30));

        assertThat(assertions.expression("INTERVAL '32767' YEAR"))
                .isEqualTo(interval(32767, 0));

        assertThat(assertions.expression("INTERVAL '32767' MONTH"))
                .isEqualTo(interval(0, 32767));

        assertThat(assertions.expression("INTERVAL '32767-32767' YEAR TO MONTH"))
                .isEqualTo(interval(32767, 32767));

        // Invalid literals
        assertThatThrownBy(assertions.expression("INTERVAL '124X' YEAR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR value: 124X");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR value: 124-30");

        assertThatThrownBy(assertions.expression("INTERVAL '124-X' YEAR TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR TO MONTH value: 124-X");

        assertThatThrownBy(assertions.expression("INTERVAL '124--30' YEAR TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR TO MONTH value: 124--30");

        assertThatThrownBy(assertions.expression("INTERVAL '--124--30' YEAR TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR TO MONTH value: --124--30");

        // Invalid qualifiers (YEAR TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '32767' YEAR TO YEAR")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: YEAR TO YEAR");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO DAY")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: YEAR TO DAY");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: YEAR TO HOUR");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO MINUTE")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: YEAR TO MINUTE");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO SECOND")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: YEAR TO SECOND");

        // Invalid qualifiers (MONTH TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '30' MONTH TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: MONTH TO MONTH");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO YEAR")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: MONTH TO YEAR");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO DAY")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: MONTH TO DAY");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: MONTH TO HOUR");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO MINUTE")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: MONTH TO MINUTE");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO SECOND")::evaluate)
                .hasMessage("line 1:12: Invalid interval qualifier: MONTH TO SECOND");
    }

    private static SqlIntervalYearMonth interval(int year, int month)
    {
        return new SqlIntervalYearMonth(year, month);
    }
}
