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

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static com.google.common.math.LongMath.factorial;
import static io.trino.operator.scalar.ArrayCombinationsFunction.combinationCount;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestArrayCombinationsFunction
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
    public void testCombinationCount()
    {
        for (int n = 0; n < 5; n++) {
            for (int k = 0; k <= n; k++) {
                assertThat(combinationCount(n, k))
                        .isEqualTo(factorial(n) / factorial(n - k) / factorial(k));
            }
        }

        assertThat(combinationCount(42, 7)).isEqualTo(26978328);
        assertThat(combinationCount(100, 4)).isEqualTo(3921225);
    }

    @Test
    public void testBasic()
    {
        assertThat(assertions.function("combinations", "ARRAY['bar', 'foo', 'baz', 'foo']", "0"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(ImmutableList.of()));

        assertThat(assertions.function("combinations", "ARRAY['bar', 'foo', 'baz', 'foo']", "1"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("bar"),
                        ImmutableList.of("foo"),
                        ImmutableList.of("baz"),
                        ImmutableList.of("foo")));

        assertThat(assertions.function("combinations", "ARRAY['bar', 'foo', 'baz', 'foo']", "2"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("bar", "foo"),
                        ImmutableList.of("bar", "baz"),
                        ImmutableList.of("foo", "baz"),
                        ImmutableList.of("bar", "foo"),
                        ImmutableList.of("foo", "foo"),
                        ImmutableList.of("baz", "foo")));

        assertThat(assertions.function("combinations", "ARRAY['bar', 'foo', 'baz', 'foo']", "3"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("bar", "foo", "baz"),
                        ImmutableList.of("bar", "foo", "foo"),
                        ImmutableList.of("bar", "baz", "foo"),
                        ImmutableList.of("foo", "baz", "foo")));

        assertThat(assertions.function("combinations", "ARRAY['bar', 'foo', 'baz', 'foo']", "4"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("bar", "foo", "baz", "foo")));

        assertThat(assertions.function("combinations", "ARRAY['bar', 'foo', 'baz', 'foo']", "5"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("combinations", "ARRAY['a', 'bb', 'ccc', 'dddd']", "2"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(4))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("a", "bb"),
                        ImmutableList.of("a", "ccc"),
                        ImmutableList.of("bb", "ccc"),
                        ImmutableList.of("a", "dddd"),
                        ImmutableList.of("bb", "dddd"),
                        ImmutableList.of("ccc", "dddd")));
    }

    @Test
    public void testLimits()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("combinations", "sequence(1, 40)", "-1").evaluate())
                .hasMessage("combination size must not be negative: -1");

        assertTrinoExceptionThrownBy(() -> assertions.function("combinations", "sequence(1, 40)", "10").evaluate())
                .hasMessage("combination size must not exceed 5: 10");

        assertTrinoExceptionThrownBy(() -> assertions.function("combinations", "sequence(1, 100)", "5").evaluate())
                .hasMessage("combinations exceed max size");
    }

    @Test
    public void testCardinality()
    {
        for (int n = 0; n < 5; n++) {
            for (int k = 0; k <= n; k++) {
                String array = "ARRAY" + ContiguousSet.closedOpen(0, n).asList();
                assertThat(assertions.expression(format("cardinality(combinations(%s, %s))", array, k)))
                        .hasType(BIGINT)
                        .isEqualTo(factorial(n) / factorial(n - k) / factorial(k));
            }
        }
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.function("combinations", "CAST(NULL AS array(bigint))", "2"))
                .isNull(new ArrayType(new ArrayType(BIGINT)));

        assertThat(assertions.function("combinations", "ARRAY['foo', NULL, 'bar']", "2"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(
                        asList("foo", null),
                        asList("foo", "bar"),
                        asList(null, "bar")));

        assertThat(assertions.function("combinations", "ARRAY [NULL, NULL, NULL]", "2"))
                .hasType(new ArrayType(new ArrayType(UNKNOWN)))
                .isEqualTo(ImmutableList.of(
                        asList(null, null),
                        asList(null, null),
                        asList(null, null)));

        assertThat(assertions.function("combinations", "ARRAY [NULL, 3, NULL]", "2"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(
                        asList(null, 3),
                        asList(null, null),
                        asList(3, null)));
    }

    @Test
    public void testTypeCombinations()
    {
        assertThat(assertions.function("combinations", "ARRAY[1, 2, 3]", "2"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of(1, 2),
                        ImmutableList.of(1, 3),
                        ImmutableList.of(2, 3)));

        assertThat(assertions.function("combinations", "ARRAY[1.1E0, 2.1E0, 3.1E0]", "2"))
                .hasType(new ArrayType(new ArrayType(DOUBLE)))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of(1.1, 2.1),
                        ImmutableList.of(1.1, 3.1),
                        ImmutableList.of(2.1, 3.1)));

        assertThat(assertions.function("combinations", "ARRAY[true, false, true]", "2"))
                .hasType(new ArrayType(new ArrayType(BOOLEAN)))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of(true, false),
                        ImmutableList.of(true, true),
                        ImmutableList.of(false, true)));

        assertThat(assertions.function("combinations", "ARRAY[ARRAY['A1', 'A2'], ARRAY['B1'], ARRAY['C1', 'C2']]", "2"))
                .hasType(new ArrayType(new ArrayType(new ArrayType(createVarcharType(2)))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of(ImmutableList.of("A1", "A2"), ImmutableList.of("B1")),
                        ImmutableList.of(ImmutableList.of("A1", "A2"), ImmutableList.of("C1", "C2")),
                        ImmutableList.of(ImmutableList.of("B1"), ImmutableList.of("C1", "C2"))));

        assertThat(assertions.function("combinations", "ARRAY['\u4FE1\u5FF5\u7231', '\u5E0C\u671B', '\u671B']", "2"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of("\u4FE1\u5FF5\u7231", "\u5E0C\u671B"),
                        ImmutableList.of("\u4FE1\u5FF5\u7231", "\u671B"),
                        ImmutableList.of("\u5E0C\u671B", "\u671B")));

        assertThat(assertions.function("combinations", "ARRAY[]", "2"))
                .hasType(new ArrayType(new ArrayType(UNKNOWN)))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("combinations", "ARRAY['']", "2"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(0))))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("combinations", "ARRAY['', '']", "2"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(0))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("", "")));
    }
}
