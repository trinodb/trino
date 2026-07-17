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

import io.trino.metadata.InternalFunctionBundle;
import io.trino.sql.query.QueryAssertions;
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
public class TestCustomFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(CustomFunctions.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCustomAdd()
    {
        assertThat(assertions.function("custom_add", "123", "456"))
                .isEqualTo(579L);
    }

    @Test
    public void testSliceIsNull()
    {
        assertThat(assertions.function("custom_is_null", "CAST(NULL AS VARCHAR)"))
                .isEqualTo(true);

        assertThat(assertions.function("custom_is_null", "'not null'"))
                .isEqualTo(false);
    }

    @Test
    public void testLongIsNull()
    {
        assertThat(assertions.function("custom_is_null", "CAST(NULL AS BIGINT)"))
                .isEqualTo(true);

        assertThat(assertions.function("custom_is_null", "0"))
                .isEqualTo(false);
    }

    @Test
    public void testIdentityFunction()
    {
        assertThat(assertions.function("identity&function", "\"identity.function\"(123)"))
                .isEqualTo(123L);

        assertThat(assertions.function("identity.function", "\"identity&function\"(123)"))
                .isEqualTo(123L);
    }

    @Test
    public void testConnectorSessionBinding()
    {
        assertThat(assertions.function("connector_session_varchar", "'hello'"))
                .isEqualTo("hello user");
    }

    @Test
    public void testComplexConnectorSessionBinding()
    {
        assertThat(assertions.function("connector_session_row", "row('goodbye')"))
                .isEqualTo("goodbye user");
    }

    @Test
    public void testNamedArguments()
    {
        // Positional call still works.
        assertThat(assertions.function("named_subtract", "10", "3"))
                .isEqualTo(7L);

        // Both arguments by name, in declared order.
        assertThat(assertions.expression("named_subtract(\"left\" => 10, \"right\" => 3)"))
                .isEqualTo(7L);

        // Both arguments by name, swapped.
        assertThat(assertions.expression("named_subtract(\"right\" => 3, \"left\" => 10)"))
                .isEqualTo(7L);

        // Mixed positional then named.
        assertThat(assertions.expression("named_subtract(10, \"right\" => 3)"))
                .isEqualTo(7L);

        // Positional value followed by a named arg that targets the already-filled slot.
        assertThatThrownBy(() -> assertions.expression("named_subtract(10, \"left\" => 3)").evaluate())
                .hasMessageContaining("Named argument left for function named_subtract refers to parameter position 0, which is already supplied positionally");

        // Unknown name reports the offending identifier at its column.
        assertThatThrownBy(() -> assertions.expression("named_subtract(\"left\" => 1, bogus => 2)").evaluate())
                .hasMessageContaining("No argument named bogus for function named_subtract");
    }
}
