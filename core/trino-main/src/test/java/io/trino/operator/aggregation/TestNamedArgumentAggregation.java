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
package io.trino.operator.aggregation;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.Name;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestNamedArgumentAggregation
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.extractFunctions(NamedDiffSum.class));
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAggregateNamedArgumentsBoundByName()
    {
        // The aggregate computes sum(a - b). Non-commutative, so binding order is observable.
        assertThat(assertions.query("SELECT named_diff_sum(a, b) FROM (VALUES (BIGINT '10', BIGINT '3')) t(a, b)"))
                .matches("VALUES BIGINT '7'");
        assertThat(assertions.query("SELECT named_diff_sum(a => a, b => b) FROM (VALUES (BIGINT '10', BIGINT '3')) t(a, b)"))
                .matches("VALUES BIGINT '7'");
        // Swapped named arguments: relies on the planner applying the analyzer's binding.
        assertThat(assertions.query("SELECT named_diff_sum(b => b, a => a) FROM (VALUES (BIGINT '10', BIGINT '3')) t(a, b)"))
                .matches("VALUES BIGINT '7'");
    }

    @Test
    public void testWindowNamedArgumentsBoundByName()
    {
        // Window form of the aggregate must apply the same binding.
        assertThat(assertions.query(
                "SELECT named_diff_sum(b => b, a => a) OVER () FROM (VALUES (BIGINT '10', BIGINT '3')) t(a, b)"))
                .matches("VALUES BIGINT '7'");
    }

    @AggregationFunction("named_diff_sum")
    public static final class NamedDiffSum
    {
        private NamedDiffSum() {}

        @InputFunction
        public static void input(
                @AggregationState NullableLongState state,
                @SqlType(StandardTypes.BIGINT) @Name("a") long a,
                @SqlType(StandardTypes.BIGINT) @Name("b") long b)
        {
            state.setValue(state.getValue() + (a - b));
            state.setNull(false);
        }

        @CombineFunction
        public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState other)
        {
            if (other.isNull()) {
                return;
            }
            if (state.isNull()) {
                state.setValue(other.getValue());
                state.setNull(false);
            }
            else {
                state.setValue(state.getValue() + other.getValue());
            }
        }

        @OutputFunction(StandardTypes.BIGINT)
        public static void output(@AggregationState NullableLongState state, BlockBuilder out)
        {
            NullableLongState.write(BIGINT, state, out);
        }
    }
}
