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

import com.google.common.base.Joiner;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestArrayFunctions
{
    private QueryAssertions assertions;

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
    public void testArrayConstructor()
    {
        // large constant array
        assertThat(assertions.expression("array[" + Joiner.on(", ").join(nCopies(20000, "rand()")) + "]"))
                .hasType(new ArrayType(DOUBLE));

        // large non-constant array
        // 1900 is close to the max number of elements for the expression below. The actual limit depends
        // currently depends on the number of bytecodes in the expression containing the array constructor
        assertThat(assertions.expression("array[a, " + Joiner.on(", ").join(nCopies(1900, "1")) + "]")
                .binding("a", "1"))
                .hasType(new ArrayType(INTEGER));

        assertThat(assertions.expression("array[a, b, c]")
                .binding("a", "1")
                .binding("b", "2")
                .binding("c", "3"))
                .matches("ARRAY[1, 2, 3]");
    }

    @Test
    public void testArrayConcat()
    {
        assertThat(assertions.expression("CONCAT(" + Joiner.on(", ").join(nCopies(127, "array[1]")) + ")"))
                .hasType(new ArrayType(INTEGER))
                .matches("ARRAY[%s]".formatted(Joiner.on(",").join(nCopies(127, 1))));

        assertThat(assertions.function("concat", "ARRAY[1]", "ARRAY[2]", "ARRAY[3]"))
                .matches("ARRAY[1, 2, 3]");

        assertTrinoExceptionThrownBy(assertions.expression("CONCAT(" + Joiner.on(", ").join(nCopies(128, "array[1]")) + ")")::evaluate)
                .hasMessage("line 1:12: Too many arguments for function call concat()");
    }
}
