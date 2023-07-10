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
import io.trino.spi.type.RowType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.util.MoreMaps.asMap;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMapZipWithFunction
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
    public void testBasic()
    {
        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> k + v1 + v2)")
                        .binding("a", "map(ARRAY [1, 2, 3], ARRAY [10, 20, 30])")
                        .binding("b", "map(ARRAY [1, 2, 3], ARRAY [1, 4, 9])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, 12, 2, 26, 3, 42));

        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> v1)")
                        .binding("a", "map(ARRAY ['a', 'b'], ARRAY [1, 2])")
                        .binding("b", "map(ARRAY ['c', 'd'], ARRAY [30, 40])"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(asMap(asList("a", "b", "c", "d"), asList(1, 2, null, null)));

        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> v2)")
                        .binding("a", "map(ARRAY ['a', 'b'], ARRAY [1, 2])")
                        .binding("b", "map(ARRAY ['c', 'd'], ARRAY [30, 40])"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(asMap(asList("a", "b", "c", "d"), asList(null, null, 30, 40)));

        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> (v1, v2))")
                        .binding("a", "map(ARRAY ['a', 'b', 'c'], ARRAY [1, 2, 3])")
                        .binding("b", "map(ARRAY ['b', 'c', 'd', 'e'], ARRAY ['x', 'y', 'z', null])"))
                .hasType(mapType(createVarcharType(1), RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(1)))))
                .isEqualTo(ImmutableMap.of(
                        "a", asList(1, null),
                        "b", ImmutableList.of(2, "x"),
                        "c", ImmutableList.of(3, "y"),
                        "d", asList(null, "z"),
                        "e", asList(null, null)));
    }

    @Test
    public void testTypes()
    {
        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> v1 * v2 - k)")
                        .binding("a", "map(ARRAY [25, 26, 27], ARRAY [25, 26, 27])")
                        .binding("b", "map(ARRAY [25, 26, 27], ARRAY [1, 2, 3])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(25, 0, 26, 26, 27, 54));
        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> v1 + v2 - k)")
                        .binding("a", "map(ARRAY [25.5E0, 26.75E0, 27.875E0], ARRAY [25, 26, 27])")
                        .binding("b", "map(ARRAY [25.5E0, 26.75E0, 27.875E0], ARRAY [1, 2, 3])"))
                .hasType(mapType(DOUBLE, DOUBLE))
                .isEqualTo(ImmutableMap.of(25.5, 0.5, 26.75, 1.25, 27.875, 2.125));
        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> k AND v1 % v2 = 0)")
                        .binding("a", "map(ARRAY [true, false], ARRAY [25, 26])")
                        .binding("b", "map(ARRAY [true, false], ARRAY [1, 2])"))
                .hasType(mapType(BOOLEAN, BOOLEAN))
                .isEqualTo(ImmutableMap.of(true, true, false, false));
        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> k || ':' || CAST(v1/v2 AS VARCHAR))")
                        .binding("a", "map(ARRAY ['s0', 's1', 's2'], ARRAY [25, 26, 27])")
                        .binding("b", "map(ARRAY ['s0', 's1', 's2'], ARRAY [1, 2, 3])"))
                .hasType(mapType(createVarcharType(2), VARCHAR))
                .isEqualTo(ImmutableMap.of("s0", "s0:25", "s1", "s1:13", "s2", "s2:9"));
        assertThat(assertions.expression(
                "map_zip_with(a, b, (k, v1, v2) -> if(v1 % v2 = 0, reverse(k), k))")
                        .binding("a", "map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [25, 26])")
                        .binding("b", "map(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [5, 6])"))
                .hasType(mapType(new ArrayType(INTEGER), new ArrayType(INTEGER)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of(2, 1), ImmutableList.of(3, 4), ImmutableList.of(3, 4)));
    }
}
