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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.operator.scalar.ZipFunction.MAX_ARITY;
import static io.trino.operator.scalar.ZipFunction.MIN_ARITY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestZipFunction
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
    public void testSameLength()
    {
        assertThat(assertions.function("zip", "ARRAY[1, 2]", "ARRAY['a', 'b']"))
                .hasType(zipReturnType(INTEGER, createVarcharType(1)))
                .isEqualTo(list(list(1, "a"), list(2, "b")));

        assertThat(assertions.function("zip", "ARRAY[1, 2]", "ARRAY['a', VARCHAR 'b']"))
                .hasType(zipReturnType(INTEGER, VARCHAR))
                .isEqualTo(list(list(1, "a"), list(2, "b")));

        assertThat(assertions.function("zip", "ARRAY[1, 2, 3, 4]", "ARRAY['a', 'b', 'c', 'd']"))
                .hasType(zipReturnType(INTEGER, createVarcharType(1)))
                .isEqualTo(list(list(1, "a"), list(2, "b"), list(3, "c"), list(4, "d")));

        assertThat(assertions.function("zip", "ARRAY[1, 2]", "ARRAY['a', 'b']", " ARRAY['c', 'd']"))
                .hasType(zipReturnType(INTEGER, createVarcharType(1), createVarcharType(1)))
                .isEqualTo(list(list(1, "a", "c"), list(2, "b", "d")));

        assertThat(assertions.function("zip", "ARRAY[1, 2]", "ARRAY['a', 'b']", "ARRAY['c', 'd']", "ARRAY['e', 'f']"))
                .hasType(zipReturnType(INTEGER, createVarcharType(1), createVarcharType(1), createVarcharType(1)))
                .isEqualTo(list(list(1, "a", "c", "e"), list(2, "b", "d", "f")));

        assertThat(assertions.function("zip", "ARRAY[]", "ARRAY[]"))
                .hasType(zipReturnType(UNKNOWN, UNKNOWN))
                .isEqualTo(list());

        assertThat(assertions.function("zip", "ARRAY[]", "ARRAY[]", "ARRAY[]"))
                .hasType(zipReturnType(UNKNOWN, UNKNOWN, UNKNOWN))
                .isEqualTo(list());

        assertThat(assertions.function("zip", "ARRAY[]", "ARRAY[]", "ARRAY[]", "ARRAY[]"))
                .hasType(zipReturnType(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN))
                .isEqualTo(list());

        assertThat(assertions.function("zip", "ARRAY[NULL]", "ARRAY[NULL]"))
                .hasType(zipReturnType(UNKNOWN, UNKNOWN))
                .isEqualTo(list(list(null, null)));

        assertThat(assertions.function("zip", "ARRAY[ARRAY[1, 1], ARRAY[1, 2]]", "ARRAY[ARRAY[2, 1], ARRAY[2, 2]]"))
                .hasType(zipReturnType(new ArrayType(INTEGER), new ArrayType(INTEGER)))
                .isEqualTo(list(list(list(1, 1), list(2, 1)), list(list(1, 2), list(2, 2))));
    }

    @Test
    public void testDifferentLength()
    {
        assertThat(assertions.function("zip", "ARRAY[1]", "ARRAY['a', 'b']"))
                .hasType(zipReturnType(INTEGER, createVarcharType(1)))
                .isEqualTo(list(list(1, "a"), list(null, "b")));

        assertThat(assertions.function("zip", "ARRAY[NULL, 2]", "ARRAY['a']"))
                .hasType(zipReturnType(INTEGER, createVarcharType(1)))
                .isEqualTo(list(list(null, "a"), list(2, null)));

        assertThat(assertions.function("zip", "ARRAY[]", "ARRAY[1]", "ARRAY[1, 2]", "ARRAY[1, 2, 3]"))
                .hasType(zipReturnType(UNKNOWN, INTEGER, INTEGER, INTEGER))
                .isEqualTo(list(list(null, 1, 1, 1), list(null, null, 2, 2), list(null, null, null, 3)));

        assertThat(assertions.function("zip", "ARRAY[]", "ARRAY[NULL]", "ARRAY[NULL, NULL]"))
                .hasType(zipReturnType(UNKNOWN, UNKNOWN, UNKNOWN))
                .isEqualTo(list(list(null, null, null), list(null, null, null)));
    }

    @Test
    public void testWithNull()
    {
        assertThat(assertions.function("zip", "CAST(NULL AS ARRAY(UNKNOWN))", "ARRAY[]", "ARRAY[1]"))
                .isNull(zipReturnType(UNKNOWN, UNKNOWN, INTEGER));
    }

    @Test
    public void testAllArities()
    {
        for (int arity = MIN_ARITY; arity <= MAX_ARITY; arity++) {
            List<String> arguments = IntStream.rangeClosed(1, arity)
                    .mapToObj(index -> "ARRAY[" + index + "]")
                    .collect(toList());
            Type[] types = IntStream.rangeClosed(1, arity)
                    .mapToObj(index -> INTEGER)
                    .toArray(Type[]::new);
            assertThat(assertions.function("zip", arguments))
                    .hasType(zipReturnType(types))
                    .isEqualTo(list(IntStream.rangeClosed(1, arity).boxed().collect(toList())));
        }
    }

    private static Type zipReturnType(Type... types)
    {
        return new ArrayType(RowType.anonymous(list(types)));
    }

    @SafeVarargs
    private static <T> List<T> list(T... a)
    {
        return asList(a);
    }
}
