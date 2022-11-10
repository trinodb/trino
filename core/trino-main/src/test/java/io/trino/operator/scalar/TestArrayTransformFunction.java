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
import io.trino.Session;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayTransformFunction
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
        assertThat(assertions.expression("transform(a, x -> 9)")
                .binding("a", "ARRAY[5, 6]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(9, 9));
        assertThat(assertions.expression("transform(a, x -> x + 1)")
                .binding("a", "ARRAY[5, 6]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(6, 7));
        assertThat(assertions.expression("transform(a, x -> x + 1)")
                .binding("a", "ARRAY[5 + RANDOM(1), 6]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(6, 7));
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.expression("transform(a, x -> x + 1)")
                .binding("a", "ARRAY[3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(4));
        assertThat(assertions.expression("transform(a, x -> x + 1)")
                .binding("a", "ARRAY[NULL, NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(null, null));
        assertThat(assertions.expression("transform(a, x -> x + 1)")
                .binding("a", "ARRAY[NULL, 3, NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(null, 4, null));

        assertThat(assertions.expression("transform(a, x -> x IS NULL)")
                .binding("a", "ARRAY[3]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(false));
        assertThat(assertions.expression("transform(a, x -> x IS NULL)")
                .binding("a", "ARRAY[NULL, NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, true));
        assertThat(assertions.expression("transform(a, x -> x IS NULL)")
                .binding("a", "ARRAY[NULL, 3, NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false, true));
    }

    @Test
    public void testSessionDependent()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Pacific/Kiritimati"))
                .build();

        assertThat(assertions.expression("transform(a, x -> x || current_timezone())", session)
                .binding("a", "ARRAY['timezone: ', 'tz: ']"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("timezone: Pacific/Kiritimati", "tz: Pacific/Kiritimati"));
    }

    @Test
    public void testInstanceexpression()
    {
        assertThat(assertions.expression("transform(a, x -> concat(ARRAY[1], x))")
                .binding("a", "ARRAY[2, 3, 4, NULL, 5]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(asList(ImmutableList.of(1, 2), ImmutableList.of(1, 3), ImmutableList.of(1, 4), null, ImmutableList.of(1, 5)));
    }

    @Test
    public void testTypeCombinations()
    {
        assertThat(assertions.expression("transform(a, x -> x + 1)")
                .binding("a", "ARRAY[25, 26]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(26, 27));
        assertThat(assertions.expression("transform(a, x -> x + 1.0E0)")
                .binding("a", "ARRAY[25, 26]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(26.0, 27.0));
        assertThat(assertions.expression("transform(a, x -> x = 25)")
                .binding("a", "ARRAY[25, 26]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));
        assertThat(assertions.expression("transform(a, x -> to_base(x, 16))")
                .binding("a", "ARRAY[25, 26]"))
                .hasType(new ArrayType(createVarcharType(64)))
                .isEqualTo(ImmutableList.of("19", "1a"));
        assertThat(assertions.expression("transform(a, x -> ARRAY[x + 1])")
                .binding("a", "ARRAY[25, 26]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(26), ImmutableList.of(27)));

        assertThat(assertions.expression("transform(a, x -> CAST(x AS BIGINT))")
                .binding("a", "ARRAY[25.6E0, 27.3E0]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(26L, 27L));
        assertThat(assertions.expression("transform(a, x -> x + 1.0E0)")
                .binding("a", "ARRAY[25.6E0, 27.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(26.6, 28.3));
        assertThat(assertions.expression("transform(a, x -> x = 25.6E0)")
                .binding("a", "ARRAY[25.6E0, 27.3E0]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));
        assertThat(assertions.expression("transform(a, x -> CAST(x AS VARCHAR))")
                .binding("a", "ARRAY[25.6E0, 27.3E0]"))
                .hasType(new ArrayType(createUnboundedVarcharType()))
                .isEqualTo(ImmutableList.of("2.56E1", "2.73E1"));
        assertThat(assertions.expression("transform(a, x -> MAP(ARRAY[x + 1], ARRAY[true]))")
                .binding("a", "ARRAY[25.6E0, 27.3E0]"))
                .hasType(new ArrayType(mapType(DOUBLE, BOOLEAN)))
                .isEqualTo(ImmutableList.of(ImmutableMap.of(26.6, true), ImmutableMap.of(28.3, true)));

        assertThat(assertions.expression("transform(a, x -> if(x, 25, 26))")
                .binding("a", "ARRAY[true, false]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(25, 26));
        assertThat(assertions.expression("transform(a, x -> if(x, 25.6E0, 28.9E0))")
                .binding("a", "ARRAY[false, true]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(28.9, 25.6));
        assertThat(assertions.expression("transform(a, x -> not x)")
                .binding("a", "ARRAY[true, false]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(false, true));
        assertThat(assertions.expression("transform(a, x -> CAST(x AS VARCHAR))")
                .binding("a", "ARRAY[false, true]"))
                .hasType(new ArrayType(createUnboundedVarcharType()))
                .isEqualTo(ImmutableList.of("false", "true"));
        assertThat(assertions.expression("transform(a, x -> ARRAY[x])")
                .binding("a", "ARRAY[true, false]"))
                .hasType(new ArrayType(new ArrayType(BOOLEAN)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(true), ImmutableList.of(false)));

        assertThat(assertions.expression("transform(a, x -> from_base(x, 16))")
                .binding("a", "ARRAY['41', '42']"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(65L, 66L));
        assertThat(assertions.expression("transform(a, x -> CAST(x AS DOUBLE))")
                .binding("a", "ARRAY['25.6E0', '27.3E0']"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(25.6, 27.3));
        assertThat(assertions.expression("transform(a, x -> 'abc' = x)")
                .binding("a", "ARRAY['abc', 'def']"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));
        assertThat(assertions.expression("transform(a, x -> x || x)")
                .binding("a", "ARRAY['abc', 'def']"))
                .hasType(new ArrayType(createUnboundedVarcharType()))
                .isEqualTo(ImmutableList.of("abcabc", "defdef"));
        assertThat(assertions.expression("transform(a, x -> ROW(x, CAST(x AS INTEGER), x > '3'))")
                .binding("a", "ARRAY['123', '456']"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(3), INTEGER, BOOLEAN))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("123", 123, false), ImmutableList.of("456", 456, true)));

        assertThat(assertions.expression("transform(a, x -> from_base(x[3], 10))")
                .binding("a", "ARRAY[ARRAY['abc', null, '123'], ARRAY['def', 'x', '456']]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(123L, 456L));
        assertThat(assertions.expression("transform(a, x -> CAST(x[3] AS DOUBLE))")
                .binding("a", "ARRAY[ARRAY['abc', null, '123'], ARRAY['def', 'x', '456']]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(123.0, 456.0));
        assertThat(assertions.expression("transform(a, x -> x[2] IS NULL)")
                .binding("a", "ARRAY[ARRAY['abc', null, '123'], ARRAY['def', 'x', '456']]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));
        assertThat(assertions.expression("transform(a, x -> x[2])")
                .binding("a", "ARRAY[ARRAY['abc', null, '123'], ARRAY['def', 'x', '456']]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(asList(null, "x"));
        assertThat(assertions.expression("transform(a, x -> map_keys(x))")
                .binding("a", "ARRAY[MAP(ARRAY['abc', 'def'], ARRAY[123, 456]), MAP(ARRAY['ghi', 'jkl'], ARRAY[234, 567])]"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("abc", "def"), ImmutableList.of("ghi", "jkl")));
    }
}
