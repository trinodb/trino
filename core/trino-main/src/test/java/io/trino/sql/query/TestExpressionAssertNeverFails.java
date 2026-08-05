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
package io.trino.sql.query;

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
class TestExpressionAssertNeverFails
{
    private QueryAssertions assertions;

    @BeforeAll
    void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    void testNeverFails()
    {
        // infallible function
        assertThat(assertions.expression("abs(a)").binding("a", "DOUBLE '1'"))
                .neverFails()
                .isEqualTo(1.0);

        // fallible function
        assertThatThrownBy(assertThat(assertions.expression("abs(a)").binding("a", "BIGINT '1'"))::neverFails)
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        [Expression abs(field::bigint) may fail]\s
                        Expecting value to be false but was true""");

        // infallible operator
        assertThat(assertions.expression("CAST(a AS double)").binding("a", "DECIMAL '1'"))
                .neverFails()
                .isEqualTo(1.0);

        // fallible operator
        assertThatThrownBy(assertThat(assertions.expression("CAST(a AS bigint)").binding("a", "DOUBLE '1'"))::neverFails)
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        [Expression Cast(field::double, bigint) may fail]\s
                        Expecting value to be false but was true""");
    }

    @Test
    void testCouldFail()
    {
        // infallible function
        assertThatThrownBy(assertThat(assertions.expression("abs(a)").binding("a", "DOUBLE '1'"))::couldFail)
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        [Expression abs(field::double) may fail]\s
                        Expecting value to be true but was false""");

        // fallible function
        assertThat(assertions.expression("abs(a)").binding("a", "BIGINT '1'"))
                .couldFail()
                .isEqualTo(1L);

        // infallible operator
        assertThatThrownBy(assertThat(assertions.expression("CAST(a AS double)").binding("a", "DECIMAL '1'"))::couldFail)
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        [Expression Cast(field::decimal(1,0), double) may fail]\s
                        Expecting value to be true but was false""");

        // fallible operator
        assertThat(assertions.expression("CAST(a AS bigint)").binding("a", "DOUBLE '1'"))
                .couldFail()
                .isEqualTo(1L);
    }

    @Test
    void testNoBinding()
    {
        assertThatThrownBy(assertThat(assertions.expression("cast(DECIMAL '1.0' as DOUBLE)"))::neverFails)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("Expected ProjectNode but got different plan root, the expression may have been constant-folded — use binding\\(\\) to preserve it: io.trino.sql.planner.plan.ValuesNode@[0-9a-f]+");

        assertThatThrownBy(assertThat(assertions.expression("cast(DECIMAL '1.0' as DOUBLE)"))::couldFail)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("Expected ProjectNode but got different plan root, the expression may have been constant-folded — use binding\\(\\) to preserve it: io.trino.sql.planner.plan.ValuesNode@[0-9a-f]+");
    }
}
