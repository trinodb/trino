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
import io.trino.spi.block.Block;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryFailedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestParametricScalar
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(InvalidParametricScalarFunctions.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testException()
    {
        assertThatThrownBy(() -> assertions.expression("incompatible_argument_type(1)").evaluate())
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Expected implementation system.builtin.incompatible_argument_type(@SqlType(BIGINT) long):long but java types do not match");

        assertThatThrownBy(() -> assertions.expression("incompatible_noargs()").evaluate())
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Expected implementation system.builtin.incompatible_noargs():long but java types do not match");

        assertThatThrownBy(() -> assertions.expression("incompatible_return_bigint(1)").evaluate())
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Expected implementation system.builtin.incompatible_return_bigint(@SqlType(BIGINT) long):long but java types do not match");

        assertThatThrownBy(() -> assertions.expression("incompatible_return_row_array()").evaluate())
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Expected implementation system.builtin.incompatible_return_row_array():Block but java types do not match");
    }

    public static class InvalidParametricScalarFunctions
    {
        @SqlType(BIGINT)
        @ScalarFunction("incompatible_argument_type")
        public long incompatibleArgumentType(@SqlType(BIGINT) int value)
        {
            return 0;
        }

        @SqlType(INTEGER)
        @ScalarFunction("incompatible_noargs")
        public Block incompatibleNoArgs()
        {
            return null;
        }

        @SqlType(BIGINT)
        @ScalarFunction("incompatible_return_bigint")
        public Block incompatibleReturn(@SqlType(BIGINT) long value)
        {
            return null;
        }

        @SqlType("array(row(varchar))")
        @ScalarFunction("incompatible_return_row_array")
        public int rowArray()
        {
            return 0;
        }
    }
}
