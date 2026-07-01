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
package io.trino.plugin.functions.python;

import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestPythonIntervalResults
{
    @Test
    void testDeclaredQualifierAndPrecision()
            throws IOException
    {
        assertThat(execute("interval day(2)", "timedelta(seconds=1)")).isEqualTo(0L);
        assertThat(execute("interval day(2)", "timedelta(seconds=-1)")).isEqualTo(0L);
        assertThat(execute("interval hour(2)", "timedelta(hours=1, minutes=59)")).isEqualTo(3_600_000_000L);
        assertThat(execute("interval hour(2)", "timedelta(hours=-1, minutes=-59)")).isEqualTo(-3_600_000_000L);
        assertThat(execute("interval hour(2)", "timedelta(hours=99)")).isEqualTo(356_400_000_000L);
        assertThat(execute("interval hour(2)", "timedelta(hours=-99)")).isEqualTo(-356_400_000_000L);
        assertThat(execute("interval second(2,0)", "timedelta(milliseconds=500)")).isEqualTo(1_000_000L);
        assertThat(execute("interval second(2,6)", "timedelta(milliseconds=500)")).isEqualTo(500_000L);
        assertThat(execute("interval second(3,6)", "timedelta(seconds=100)")).isEqualTo(100_000_000L);
        assertThat(execute("interval year(2)", "13")).isEqualTo(12L);
        assertThat(execute("interval year(2)", "-13")).isEqualTo(-12L);
    }

    @Test
    void testDeclaredLeadingPrecision()
    {
        for (String[] example : new String[][] {
                {"interval hour(2)", "timedelta(hours=100)"},
                {"interval hour(2)", "timedelta(hours=-100)"},
                {"interval second(2,6)", "timedelta(seconds=100)"},
                {"interval second(2,0)", "timedelta(milliseconds=99500)"},
                {"interval month(2)", "100"},
                {"array(interval second(2,6))", "[timedelta(seconds=100)]"},
                {"row(value interval second(2,6))", "(timedelta(seconds=100),)"},
                {"map(bigint, interval second(2,6))", "{1: timedelta(seconds=100)}"},
                {"map(interval second(2,6), bigint)", "{timedelta(seconds=100): 1}"},
        }) {
            assertTrinoExceptionThrownBy(() -> execute(example[0], example[1]))
                    .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                    .hasMessageContaining("Function result cannot be converted to");
        }
    }

    private static Object execute(String sqlType, String expression)
            throws IOException
    {
        Type type = TESTING_TYPE_MANAGER.fromSqlType(sqlType);
        try (PythonEngine engine = new PythonEngine("from datetime import timedelta\ndef result():\n    return " + expression + "\n")) {
            engine.setup(type, List.of(), "result");
            return engine.execute(new Object[0]);
        }
    }
}
