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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMapTransformKeysFunction
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
    public void testEmpty()
    {
        assertThat(assertions.expression("transform_keys(a, (k, v) -> NULL)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> v)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> 0)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(INTEGER, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> true)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(BOOLEAN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> 'key')")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(createVarcharType(3), UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k + CAST(v as BIGINT))")
                .binding("a", "CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR))"))
                .hasType(mapType(BIGINT, VARCHAR))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("transform_keys(a, (k, v) -> v)")
                .binding("a", "CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR))"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testNullKey()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> NULL)")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])")
                .evaluate())
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> v)")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY['a', 'b', NULL])")
                .evaluate())
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> k + v)")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY[1, 2, NULL])")
                .evaluate())
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> TRY_CAST(v as BIGINT))")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY['1', '2', 'Invalid'])")
                .evaluate())
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> element_at(map(ARRAY[1, 2], ARRAY['one', 'two']), k))")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY[1.0E0, 1.4E0, 1.7E0])")
                .evaluate())
                .hasMessage("map key cannot be null");
    }

    @Test
    public void testDuplicateKeys()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> k % 3)")
                .binding("a", "map(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'd'])")
                .evaluate())
                .hasMessage("Duplicate map keys (1) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> k % 2 = 0)")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])")
                .evaluate())
                .hasMessage("Duplicate map keys (false) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> k - floor(k))")
                .binding("a", "map(ARRAY[1.5E0, 2.5E0, 3.5E0], ARRAY['a', 'b', 'c'])")
                .evaluate())
                .hasMessage("Duplicate map keys (0.5) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> v)")
                .binding("a", "map(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'b'])")
                .evaluate())
                .hasMessage("Duplicate map keys (b) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> substr(k, 1, 3))")
                .binding("a", "map(ARRAY['abc1', 'cba2', 'abc3'], ARRAY[1, 2, 3])")
                .evaluate())
                .hasMessage("Duplicate map keys (abc) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> array_sort(k || v))")
                .binding("a", "map(ARRAY[ARRAY[1], ARRAY[2]], ARRAY[2, 1])")
                .evaluate())
                .hasMessage("Duplicate map keys ([1, 2]) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> DATE '2001-08-22')")
                .binding("a", "map(ARRAY[1, 2], ARRAY[null, null])")
                .evaluate())
                .hasMessage("Duplicate map keys (2001-08-22) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.expression("transform_keys(a, (k, v) -> TIMESTAMP '2001-08-22 03:04:05.321')")
                .binding("a", "map(ARRAY[1, 2], ARRAY[null, null])")
                .evaluate())
                .hasMessage("Duplicate map keys (2001-08-22 03:04:05.321) are not allowed");
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.expression("transform_keys(a, (k, v) -> k + v)")
                .binding("a", "map(ARRAY[1, 2, 3, 4], ARRAY[10, 20, 30, 40])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(11, 10, 22, 20, 33, 30, 44, 40));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> v * v)")
                .binding("a", "map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, 3, 4])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, 1, 4, 2, 9, 3, 16, 4));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k || CAST(v as VARCHAR))")
                .binding("a", "map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, 3, 4])"))
                .hasType(mapType(VARCHAR, INTEGER))
                .isEqualTo(ImmutableMap.of("a1", 1, "b2", 2, "c3", 3, "d4", 4));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> map(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three'])[k])")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY[1.0E0, 1.4E0, 1.7E0])"))
                .hasType(mapType(createVarcharType(5), DOUBLE))
                .isEqualTo(ImmutableMap.of("one", 1.0, "two", 1.4, "three", 1.7));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> date_add('year', 1, k))")
                .binding("a", "map(ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '2010-05-10 12:34:56.123456789'], ARRAY[1, 2])"))
                .matches("map_from_entries(ARRAY[(TIMESTAMP '2021-05-10 12:34:56.123456789', 1), (TIMESTAMP '2011-05-10 12:34:56.123456789', 2)])");

        Map<String, Integer> expectedStringIntMap = new HashMap<>();
        expectedStringIntMap.put("a1", 1);
        expectedStringIntMap.put("b0", null);
        expectedStringIntMap.put("c3", 3);
        expectedStringIntMap.put("d4", 4);
        assertThat(assertions.expression("transform_keys(a, (k, v) -> k || COALESCE(CAST(v as VARCHAR), '0'))")
                .binding("a", "map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, NULL, 3, 4])"))
                .hasType(mapType(VARCHAR, INTEGER))
                .isEqualTo(expectedStringIntMap);
    }

    @Test
    public void testTypeCombinations()
    {
        assertThat(assertions.expression("transform_keys(a, (k, v) -> k + v)")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY[25, 26, 27])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(50, 25, 52, 26, 54, 27));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k + v)")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(50.5, 25.5, 52.5, 26.5, 54.5, 27.5));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k % 2 = 0 OR v)")
                .binding("a", "map(ARRAY[25, 26], ARRAY[false, true])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(ImmutableMap.of(false, false, true, true));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> to_base(k, 16) || substr(v, 1, 1))")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY['abc', 'def', 'xyz'])"))
                .hasType(mapType(VARCHAR, createVarcharType(3)))
                .isEqualTo(ImmutableMap.of("19a", "abc", "1ad", "def", "1bx", "xyz"));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> ARRAY[CAST(k AS VARCHAR)] || v)")
                .binding("a", "map(ARRAY[25, 26], ARRAY[ARRAY['a'], ARRAY['b']])"))
                .hasType(mapType(new ArrayType(VARCHAR), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(ImmutableList.of("25", "a"), ImmutableList.of("a"), ImmutableList.of("26", "b"), ImmutableList.of("b")));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> CAST(k * 2 AS BIGINT) + v)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25, 26, 27])"))
                .hasType(mapType(BIGINT, INTEGER))
                .isEqualTo(ImmutableMap.of(76L, 25, 79L, 26, 82L, 27));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k + v)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(51.0, 25.5, 53.0, 26.5, 55.0, 27.5));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> CAST(k AS BIGINT) % 2 = 0 OR v)")
                .binding("a", "map(ARRAY[25.2E0, 26.2E0], ARRAY[false, true])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(ImmutableMap.of(false, false, true, true));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> CAST(k AS VARCHAR) || substr(v, 1, 1))")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY['abc', 'def', 'xyz'])"))
                .hasType(mapType(VARCHAR, createVarcharType(3)))
                .isEqualTo(ImmutableMap.of("2.55E1a", "abc", "2.65E1d", "def", "2.75E1x", "xyz"));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> ARRAY[CAST(k AS VARCHAR)] || v)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0], ARRAY[ARRAY['a'], ARRAY['b']])"))
                .hasType(mapType(new ArrayType(VARCHAR), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(ImmutableList.of("2.55E1", "a"), ImmutableList.of("a"), ImmutableList.of("2.65E1", "b"), ImmutableList.of("b")));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> if(k, 2 * v, 3 * v))")
                .binding("a", "map(ARRAY[true, false], ARRAY[25, 26])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(50, 25, 78, 26));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> if(k, 2 * v, 3 * v))")
                .binding("a", "map(ARRAY[false, true], ARRAY[25.5E0, 26.5E0])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(76.5, 25.5, 53.0, 26.5));

        Map<Boolean, Boolean> expectedBoolBoolMap = new HashMap<>();
        expectedBoolBoolMap.put(false, true);
        expectedBoolBoolMap.put(true, null);
        assertThat(assertions.expression("transform_keys(a, (k, v) -> if(k, NOT v, v IS NULL))")
                .binding("a", "map(ARRAY[true, false], ARRAY[true, NULL])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(expectedBoolBoolMap);

        assertThat(assertions.expression("transform_keys(a, (k, v) -> if(k, substr(v, 1, 2), substr(v, 1, 1)))")
                .binding("a", "map(ARRAY[false, true], ARRAY['abc', 'def'])"))
                .hasType(mapType(createVarcharType(3), createVarcharType(3)))
                .isEqualTo(ImmutableMap.of("a", "abc", "de", "def"));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> if(k, reverse(v), v))")
                .binding("a", "map(ARRAY[true, false], ARRAY[ARRAY['a', 'b'], ARRAY['x', 'y']])"))
                .hasType(mapType(new ArrayType(createVarcharType(1)), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(ImmutableList.of("b", "a"), ImmutableList.of("a", "b"), ImmutableList.of("x", "y"), ImmutableList.of("x", "y")));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> length(k) + v)")
                .binding("a", "map(ARRAY['a', 'ab', 'abc'], ARRAY[25, 26, 27])"))
                .hasType(mapType(BIGINT, INTEGER))
                .isEqualTo(ImmutableMap.of(26L, 25, 28L, 26, 30L, 27));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> length(k) + v)")
                .binding("a", "map(ARRAY['a', 'ab', 'abc'], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(26.5, 25.5, 28.5, 26.5, 30.5, 27.5));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k = 'b' OR v)")
                .binding("a", "map(ARRAY['a', 'b'], ARRAY[false, true])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(ImmutableMap.of(false, false, true, true));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k || v)")
                .binding("a", "map(ARRAY['a', 'x'], ARRAY['bc', 'yz'])"))
                .hasType(mapType(VARCHAR, createVarcharType(2)))
                .isEqualTo(ImmutableMap.of("abc", "bc", "xyz", "yz"));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> k || v)")
                .binding("a", "map(ARRAY['x', 'y'], ARRAY[ARRAY['a'], ARRAY['b']])"))
                .hasType(mapType(new ArrayType(createVarcharType(1)), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(ImmutableList.of("x", "a"), ImmutableList.of("a"), ImmutableList.of("y", "b"), ImmutableList.of("b")));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25, 26])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(28, 25, 33, 26));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25.5E0, 26.5E0])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(28.5, 25.5, 33.5, 26.5));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> contains(k, 3) AND v)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[false, true])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(ImmutableMap.of(false, false, true, true));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY['abc', 'xyz'])"))
                .hasType(mapType(new ArrayType(VARCHAR), createVarcharType(3)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of("1", "2", "abc"), "abc", ImmutableList.of("3", "4", "xyz"), "xyz"));

        assertThat(assertions.expression("transform_keys(a, (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY['a'], ARRAY['a', 'b']])"))
                .hasType(mapType(new ArrayType(VARCHAR), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(ImmutableList.of("1", "2", "a"), ImmutableList.of("a"), ImmutableList.of("3", "4", "a", "b"), ImmutableList.of("a", "b")));
    }
}
