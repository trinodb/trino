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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestZipWithFunction
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
        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "ARRAY[]")
                .binding("b", "ARRAY[]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(UNKNOWN, UNKNOWN))))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY['a', 'b']"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), INTEGER))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("a", 1), ImmutableList.of("b", 2)));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY[VARCHAR 'a', VARCHAR 'b']"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(VARCHAR, INTEGER))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("a", 1), ImmutableList.of("b", 2)));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x + y)")
                .binding("a", "ARRAY[1, 1]")
                .binding("b", "ARRAY[1, 2]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2, 3));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x * y)")
                .binding("a", "CAST(ARRAY[3, 5] AS ARRAY(BIGINT))")
                .binding("b", "CAST(ARRAY[1, 2] AS ARRAY(BIGINT))"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(3L, 10L));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x OR y)")
                .binding("a", "ARRAY[true, false]")
                .binding("b", "ARRAY[false, true]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, true));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> concat(x, y))")
                .binding("a", "ARRAY['a', 'b']")
                .binding("b", "ARRAY['c', 'd']"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("ac", "bd"));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> map_concat(x, y))")
                .binding("a", "ARRAY[MAP(ARRAY[CAST ('a' AS VARCHAR)], ARRAY[1]), MAP(ARRAY[VARCHAR 'b'], ARRAY[2])]")
                .binding("b", "ARRAY[MAP(ARRAY['c'], ARRAY[3]), MAP()]"))
                .hasType(new ArrayType(mapType(VARCHAR, INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableMap.of("a", 1, "c", 3), ImmutableMap.of("b", 2)));
    }

    @Test
    public void testDifferentLength()
    {
        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "ARRAY[1]")
                .binding("b", "ARRAY['a', 'bc']"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(2), INTEGER))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("a", 1), asList("bc", null)));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "ARRAY[NULL, 2]")
                .binding("b", "ARRAY['a']"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), INTEGER))))
                .isEqualTo(ImmutableList.of(asList("a", null), asList(null, 2)));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x + y)")
                .binding("a", "ARRAY[NULL, NULL]")
                .binding("b", "ARRAY[NULL, 2, 1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(null, null, null));
    }

    @Test
    public void testWithNull()
    {
        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "CAST(NULL AS ARRAY(UNKNOWN))")
                .binding("b", "ARRAY[]"))
                .isNull(new ArrayType(RowType.anonymous(ImmutableList.of(UNKNOWN, UNKNOWN))));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> (y, x))")
                .binding("a", "ARRAY[NULL]")
                .binding("b", "ARRAY[NULL]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(UNKNOWN, UNKNOWN))))
                .isEqualTo(ImmutableList.of(asList(null, null)));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x IS NULL AND y IS NULL)")
                .binding("a", "ARRAY[NULL]")
                .binding("b", "ARRAY[NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x IS NULL OR y IS NULL)")
                .binding("a", "ARRAY['a', NULL]")
                .binding("b", "ARRAY[NULL, 1]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, true));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> x + y)")
                .binding("a", "ARRAY[1, NULL]")
                .binding("b", "ARRAY[3, 4]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(4, null));

        assertThat(assertions.expression("zip_with(a, b, (x, y) -> NULL)")
                .binding("a", "ARRAY['a', 'b']")
                .binding("b", "ARRAY[1, 3]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList(null, null));
    }
}
