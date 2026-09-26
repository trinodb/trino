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
package io.trino.operator.scalar;

import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.RegexLibrary.REGULATOR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRegulatorRegexpFunctions
        extends AbstractTestRegexpFunctions
{
    public TestRegulatorRegexpFunctions()
    {
        super(REGULATOR);
    }

    @Test
    public void testMultilineBeginLineOperations()
    {
        assertMultilineBeginLineOperations();
    }

    @Test
    public void testUnsupportedPatternsDoNotFallBack()
    {
        for (String pattern : new String[] {"(?=a)", "(?<=a)", "(a)\\1", "(?>a)", "a++", "a{1001}", "\\G", "\\Z"}) {
            assertTrinoExceptionThrownBy(assertions.function("regexp_like", "'aa'", "'" + pattern + "'")::evaluate)
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        }
    }

    @Test
    public void testLargeGroupIndex()
    {
        for (String function : new String[] {"regexp_extract", "regexp_extract_all"}) {
            assertTrinoExceptionThrownBy(assertions.function(function, "'a'", "'(a)'", "9223372036854775807")::evaluate)
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessage("Pattern has 1 groups. Cannot access group 9223372036854775807");
        }
    }

    @Test
    public void testTryUnsupportedPattern()
    {
        assertThat(assertions.expression("TRY(regexp_like('aa', '(a)\\1'))"))
                .isNull(BOOLEAN);
    }
}
