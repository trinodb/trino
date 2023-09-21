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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayTrimFunction
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
    public void testTrimArray()
    {
        assertThat(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "2"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));
        assertThat(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "0"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3, 4));
        assertThat(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));
        assertThat(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1));
        assertThat(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "4"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("trim_array", "ARRAY['a', 'b', 'c', 'd']", "1"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));
        assertThat(assertions.function("trim_array", "ARRAY['a', 'b', null, 'd']", "1"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(asList("a", "b", null));
        assertThat(assertions.function("trim_array", "ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5, 6]]", "1"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2, 3)));

        assertTrinoExceptionThrownBy(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "5")::evaluate)
                .hasMessage("size must not exceed array cardinality 4: 5");
        assertTrinoExceptionThrownBy(assertions.function("trim_array", "ARRAY[1, 2, 3, 4]", "-1")::evaluate)
                .hasMessage("size must not be negative: -1");
    }
}
