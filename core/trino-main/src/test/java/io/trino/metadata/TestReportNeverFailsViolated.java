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
package io.trino.metadata;

import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.NEGATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestReportNeverFailsViolated
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(getClass())
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction(neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long lyingNeverFails(@SqlType(StandardTypes.BOOLEAN) boolean fail)
    {
        if (fail) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "lying_never_fails: caller asked to fail");
        }
        return 0;
    }

    @ScalarFunction(deterministic = false, neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long lyingNonDeterministicNeverFails(@SqlType(StandardTypes.BOOLEAN) boolean fail)
    {
        if (fail) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "lying_non_deterministic_never_fails: caller asked to fail");
        }
        return 0;
    }

    @ScalarOperator(value = NEGATION, neverFails = true)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lyingNeverFailsOperator(@SqlType(StandardTypes.BOOLEAN) boolean fail)
    {
        if (fail) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "lying_never_fails_operator: caller asked to fail");
        }
        return false;
    }

    @Test
    void testIncorrectNeverFails()
    {
        assertThat(assertions.expression("lying_never_fails(false)"))
                .matches("BIGINT '0'");

        assertThatThrownBy(assertions.expression("lying_never_fails(true)")::evaluate)
                .hasMessage("Function system.builtin.lying_never_fails(boolean):bigint declared as never failing threw io.trino.spi.TrinoException: lying_never_fails: caller asked to fail")
                .hasStackTraceContaining("lying_never_fails: caller asked to fail");
    }

    @Test
    void testIncorrectNonDeterministicNeverFails()
    {
        assertThat(assertions.expression("lying_non_deterministic_never_fails(false)"))
                .matches("BIGINT '0'");

        assertThatThrownBy(assertions.expression("lying_non_deterministic_never_fails(true)")::evaluate)
                .hasMessage("Function system.builtin.lying_non_deterministic_never_fails(boolean):bigint declared as never failing threw io.trino.spi.TrinoException: lying_non_deterministic_never_fails: caller asked to fail")
                .hasStackTraceContaining("lying_non_deterministic_never_fails: caller asked to fail");
    }

    @Test
    void testIncorrectNeverFailsOperator()
    {
        assertThat(assertions.expression("-CAST(false AS boolean)"))
                .matches("false");

        assertThatThrownBy(assertions.expression("-CAST(true AS boolean)")::evaluate)
                .hasMessage("Function system.builtin.$operator$negation(boolean):boolean declared as never failing threw io.trino.spi.TrinoException: lying_never_fails_operator: caller asked to fail")
                .hasStackTraceContaining("lying_never_fails_operator: caller asked to fail");
    }
}
