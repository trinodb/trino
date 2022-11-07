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
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMapFilterFunction
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
        assertThat(assertions.expression("map_filter(a, (k, v) -> true)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("map_filter(a, (k, v) -> false)")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("map_filter(a, (k, v) -> CAST (NULL AS BOOLEAN))")
                .binding("a", "map(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("map_filter(a, (k, v) -> true)")
                .binding("a", "CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT, VARCHAR))"))
                .hasType(mapType(BIGINT, VARCHAR))
                .isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testNull()
    {
        Map<Integer, Void> oneToNullMap = new HashMap<>();
        oneToNullMap.put(1, null);
        assertThat(assertions.expression("map_filter(a, (k, v) -> v IS NULL)")
                .binding("a", "map(ARRAY[1], ARRAY[NULL])"))
                .hasType(mapType(INTEGER, UNKNOWN))
                .isEqualTo(oneToNullMap);

        assertThat(assertions.expression("map_filter(a, (k, v) -> v IS NOT NULL)")
                .binding("a", "map(ARRAY[1], ARRAY[NULL])"))
                .hasType(mapType(INTEGER, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("map_filter(a, (k, v) -> v IS NULL)")
                .binding("a", "map(ARRAY[1], ARRAY[CAST (NULL AS INTEGER)])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(oneToNullMap);

        Map<Integer, Void> sequenceToNullMap = new HashMap<>();
        sequenceToNullMap.put(1, null);
        sequenceToNullMap.put(2, null);
        sequenceToNullMap.put(3, null);
        assertThat(assertions.expression("map_filter(a, (k, v) -> v IS NULL)")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY[NULL, NULL, NULL])"))
                .hasType(mapType(INTEGER, UNKNOWN))
                .isEqualTo(sequenceToNullMap);

        assertThat(assertions.expression("map_filter(a, (k, v) -> v IS NOT NULL)")
                .binding("a", "map(ARRAY[1, 2, 3], ARRAY[NULL, NULL, NULL])"))
                .hasType(mapType(INTEGER, UNKNOWN))
                .isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.expression("map_filter(a, (x, y) -> x <= 6 OR y = 5)")
                .binding("a", "map(ARRAY[5, 6, 7, 8], ARRAY[5, 6, 6, 5])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(5, 5, 6, 6, 8, 5));

        assertThat(assertions.expression("map_filter(a, (x, y) -> x <= 6 OR y = 5)")
                .binding("a", "map(ARRAY[5 + RANDOM(1), 6, 7, 8], ARRAY[5, 6, 6, 5])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(5, 5, 6, 6, 8, 5));

        assertThat(assertions.expression("map_filter(a, (k, v) -> v IS NOT NULL)")
                .binding("a", "map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, NULL, 4])"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(ImmutableMap.of("a", 1, "b", 2, "d", 4));

        assertThat(assertions.expression("map_filter(a, (k, v) -> v)")
                .binding("a", "map(ARRAY['a', 'b', 'c'], ARRAY[TRUE, FALSE, NULL])"))
                .hasType(mapType(createVarcharType(1), BOOLEAN))
                .isEqualTo(ImmutableMap.of("a", true));

        assertThat(assertions.expression("map_filter(a, (k, v) -> year(k) = 1111)")
                .binding("a", "map(ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789'], ARRAY[1, 2])"))
                .matches("map_from_entries(array[(TIMESTAMP '1111-05-10 12:34:56.123456789', 2)])");
    }

    @Test
    public void testTypeCombinations()
    {
        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25 OR v = 27)")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY[25, 26, 27])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(25, 25, 27, 27));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25 OR v = 27.5E0)")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(INTEGER, DOUBLE))
                .isEqualTo(ImmutableMap.of(25, 25.5, 27, 27.5));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25 OR v)")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY[false, null, true])"))
                .hasType(mapType(INTEGER, BOOLEAN))
                .isEqualTo(ImmutableMap.of(25, false, 27, true));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25 OR v = 'xyz')")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY['abc', 'def', 'xyz'])"))
                .hasType(mapType(INTEGER, createVarcharType(3)))
                .isEqualTo(ImmutableMap.of(25, "abc", 27, "xyz"));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25 OR cardinality(v) = 3)")
                .binding("a", "map(ARRAY[25, 26, 27], ARRAY[ARRAY['a', 'b'], ARRAY['a', 'c'], ARRAY['a', 'b', 'c']])"))
                .hasType(mapType(INTEGER, new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(25, ImmutableList.of("a", "b"), 27, ImmutableList.of("a", "b", "c")));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25.5E0 OR v = 27)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25, 26, 27])"))
                .hasType(mapType(DOUBLE, INTEGER))
                .isEqualTo(ImmutableMap.of(25.5, 25, 27.5, 27));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25.5E0 OR v = 27.5E0)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(25.5, 25.5, 27.5, 27.5));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25.5E0 OR v)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[false, null, true])"))
                .hasType(mapType(DOUBLE, BOOLEAN))
                .isEqualTo(ImmutableMap.of(25.5, false, 27.5, true));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25.5E0 OR v = 'xyz')")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY['abc', 'def', 'xyz'])"))
                .hasType(mapType(DOUBLE, createVarcharType(3)))
                .isEqualTo(ImmutableMap.of(25.5, "abc", 27.5, "xyz"));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 25.5E0 OR cardinality(v) = 3)")
                .binding("a", "map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[ARRAY['a', 'b'], ARRAY['a', 'c'], ARRAY['a', 'b', 'c']])"))
                .hasType(mapType(DOUBLE, new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(25.5, ImmutableList.of("a", "b"), 27.5, ImmutableList.of("a", "b", "c")));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k AND v = 25)")
                .binding("a", "map(ARRAY[true, false], ARRAY[25, 26])"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 25));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k OR v > 100)")
                .binding("a", "map(ARRAY[false, true], ARRAY[25.5E0, 26.5E0])"))
                .hasType(mapType(BOOLEAN, DOUBLE))
                .isEqualTo(ImmutableMap.of(true, 26.5));

        Map<Boolean, Boolean> falseToNullMap = new HashMap<>();
        falseToNullMap.put(false, null);
        assertThat(assertions.expression("map_filter(a, (k, v) -> NOT k OR v)")
                .binding("a", "map(ARRAY[true, false], ARRAY[false, null])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(falseToNullMap);

        assertThat(assertions.expression("map_filter(a, (k, v) -> NOT k AND v = 'abc')")
                .binding("a", "map(ARRAY[false, true], ARRAY['abc', 'def'])"))
                .hasType(mapType(BOOLEAN, createVarcharType(3)))
                .isEqualTo(ImmutableMap.of(false, "abc"));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k OR cardinality(v) = 3)")
                .binding("a", "map(ARRAY[true, false], ARRAY[ARRAY['a', 'b'], ARRAY['a', 'b', 'c']])"))
                .hasType(mapType(BOOLEAN, new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(true, ImmutableList.of("a", "b"), false, ImmutableList.of("a", "b", "c")));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 's0' OR v = 27)")
                .binding("a", "map(ARRAY['s0', 's1', 's2'], ARRAY[25, 26, 27])"))
                .hasType(mapType(createVarcharType(2), INTEGER))
                .isEqualTo(ImmutableMap.of("s0", 25, "s2", 27));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 's0' OR v = 27.5E0)")
                .binding("a", "map(ARRAY['s0', 's1', 's2'], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(createVarcharType(2), DOUBLE))
                .isEqualTo(ImmutableMap.of("s0", 25.5, "s2", 27.5));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 's0' OR v)")
                .binding("a", "map(ARRAY['s0', 's1', 's2'], ARRAY[false, null, true])"))
                .hasType(mapType(createVarcharType(2), BOOLEAN))
                .isEqualTo(ImmutableMap.of("s0", false, "s2", true));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 's0' OR v = 'xyz')")
                .binding("a", "map(ARRAY['s0', 's1', 's2'], ARRAY['abc', 'def', 'xyz'])"))
                .hasType(mapType(createVarcharType(2), createVarcharType(3)))
                .isEqualTo(ImmutableMap.of("s0", "abc", "s2", "xyz"));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = 's0' OR cardinality(v) = 3)")
                .binding("a", "map(ARRAY['s0', 's1', 's2'], ARRAY[ARRAY['a', 'b'], ARRAY['a', 'c'], ARRAY['a', 'b', 'c']])"))
                .hasType(mapType(createVarcharType(2), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of("s0", ImmutableList.of("a", "b"), "s2", ImmutableList.of("a", "b", "c")));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = ARRAY[1, 2] OR v = 27)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[]], ARRAY[25, 26, 27])"))
                .hasType(mapType(new ArrayType(INTEGER), INTEGER))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1, 2), 25, ImmutableList.of(), 27));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = ARRAY[1, 2] OR v = 27.5E0)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[]], ARRAY[25.5E0, 26.5E0, 27.5E0])"))
                .hasType(mapType(new ArrayType(INTEGER), DOUBLE))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1, 2), 25.5, ImmutableList.of(), 27.5));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = ARRAY[1, 2] OR v)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[]], ARRAY[false, null, true])"))
                .hasType(mapType(new ArrayType(INTEGER), BOOLEAN))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1, 2), false, ImmutableList.of(), true));

        assertThat(assertions.expression("map_filter(a, (k, v) -> k = ARRAY[1, 2] OR v = 'xyz')")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[]], ARRAY['abc', 'def', 'xyz'])"))
                .hasType(mapType(new ArrayType(INTEGER), createVarcharType(3)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1, 2), "abc", ImmutableList.of(), "xyz"));

        assertThat(assertions.expression("map_filter(a, (k, v) -> cardinality(k) = 0 OR cardinality(v) = 3)")
                .binding("a", "map(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[]], ARRAY[ARRAY['a', 'b'], ARRAY['a', 'b', 'c'], ARRAY['a', 'c']])"))
                .hasType(mapType(new ArrayType(INTEGER), new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(3, 4), ImmutableList.of("a", "b", "c"), ImmutableList.of(), ImmutableList.of("a", "c")));
    }
}
