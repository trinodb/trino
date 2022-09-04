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

import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestConditions
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
    public void testLike()
    {
        // like
        assertThat(assertions.expression("a like 'X_monkeyX_' escape 'X'")
                .binding("a", "'_monkey_'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a like 'monkey'")
                .binding("a", "'monkey'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a like 'mon%'")
                .binding("a", "'monkey'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a like '%key'")
                .binding("a", "'monkey'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a like 'm____y'")
                .binding("a", "'monkey'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a like 'lion'")
                .binding("a", "'monkey'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a like 'monkey'")
                .binding("a", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a like null")
                .binding("a", "'monkey'"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a like 'monkey' escape null")
                .binding("a", "'monkey'"))
                .isNull(BOOLEAN);

        // not like
        assertThat(assertions.expression("a not like 'X_monkeyX_' escape 'X'")
                .binding("a", "'_monkey_'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a not like 'monkey'")
                .binding("a", "'monkey'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a not like 'mon%'")
                .binding("a", "'monkey'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a not like '%key'")
                .binding("a", "'monkey'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a not like 'm____y'")
                .binding("a", "'monkey'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a not like 'lion'")
                .binding("a", "'monkey'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a not like 'monkey'")
                .binding("a", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a not like null")
                .binding("a", "'monkey'"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a not like 'monkey' escape null")
                .binding("a", "'monkey'"))
                .isNull(BOOLEAN);

        assertThatThrownBy(() -> assertions.expression("a like 'monkey' escape 'foo'")
                .binding("a", "'monkey'")
                .evaluate())
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape string must be a single character");
    }

    @Test
    public void testDistinctFrom()
    {
        // distinct from
        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "null")
                .binding("b", "null"))
                .isEqualTo(false);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "null")
                .binding("b", "1"))
                .isEqualTo(true);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "1")
                .binding("b", "null"))
                .isEqualTo(true);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "1")
                .binding("b", "1"))
                .isEqualTo(false);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "1")
                .binding("b", "2"))
                .isEqualTo(true);

        // not distinct from
        assertThat(assertions.expression("a IS NOT DISTINCT FROM b")
                .binding("a", "null")
                .binding("b", "null"))
                .isEqualTo(true);

        assertThat(assertions.expression("a IS NOT DISTINCT FROM b")
                .binding("a", "null")
                .binding("b", "1"))
                .isEqualTo(false);

        assertThat(assertions.expression("a IS NOT DISTINCT FROM b")
                .binding("a", "1")
                .binding("b", "null"))
                .isEqualTo(false);

        assertThat(assertions.expression("a IS NOT DISTINCT FROM b")
                .binding("a", "1")
                .binding("b", "1"))
                .isEqualTo(true);

        assertThat(assertions.expression("a IS NOT DISTINCT FROM b")
                .binding("a", "1")
                .binding("b", "2"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        // between
        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "2")
                .binding("high", "4"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "3")
                .binding("high", "3"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "2")
                .binding("high", "3"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "4")
                .binding("high", "2"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "2")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "5")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "null")
                .binding("low", "3")
                .binding("high", "4"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "null")
                .binding("high", "4"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "2")
                .binding("high", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "3")
                .binding("low", "3")
                .binding("high", "4000000000"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "5")
                .binding("low", "3")
                .binding("high", "4000000000"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "5")
                .binding("low", "BIGINT '3'")
                .binding("high", "4"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "BIGINT '3'")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "'b'")
                .binding("high", "'b'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "'c'")
                .binding("high", "'c'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "'b'")
                .binding("high", "'c'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "'c'")
                .binding("high", "'d'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "'d'")
                .binding("high", "'b'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'b'")
                .binding("low", "'c'")
                .binding("high", "'d'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'e'")
                .binding("low", "'c'")
                .binding("high", "'d'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "null")
                .binding("low", "'b'")
                .binding("high", "'d'"))
                .matches("CAST(null AS boolean)");

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "null")
                .binding("high", "'d'"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value between low and high")
                .binding("value", "'c'")
                .binding("low", "'b'")
                .binding("high", "null"))
                .isNull(BOOLEAN);

        // not between
        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "2")
                .binding("high", "4"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "3")
                .binding("high", "3"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "2")
                .binding("high", "3"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "4")
                .binding("high", "2"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "2")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "5")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "null")
                .binding("low", "3")
                .binding("high", "4"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "null")
                .binding("high", "4"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "2")
                .binding("high", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "3")
                .binding("low", "3")
                .binding("high", "4000000000"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "5")
                .binding("low", "3")
                .binding("high", "4000000000"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "5")
                .binding("low", "BIGINT '3'")
                .binding("high", "4"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "BIGINT '3'")
                .binding("low", "3")
                .binding("high", "4"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "'b'")
                .binding("high", "'b'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "'c'")
                .binding("high", "'c'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "'b'")
                .binding("high", "'c'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "'c'")
                .binding("high", "'d'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "'d'")
                .binding("high", "'b'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'b'")
                .binding("low", "'c'")
                .binding("high", "'d'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'e'")
                .binding("low", "'c'")
                .binding("high", "'d'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "null")
                .binding("low", "'b'")
                .binding("high", "'d'"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "null")
                .binding("high", "'d'"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not between low and high")
                .binding("value", "'c'")
                .binding("low", "'b'")
                .binding("high", "null"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testIn()
    {
        assertThat(assertions.expression("value in (2, 4, 3, 5)")
                .binding("value", "3"))
                .isEqualTo(true);

        assertThat(assertions.expression("value not in (2, 4, 3, 5)")
                .binding("value", "3"))
                .isEqualTo(false);

        assertThat(assertions.expression("value in (2, 4, 9, 5)")
                .binding("value", "3"))
                .isEqualTo(false);

        assertThat(assertions.expression("value in (2, null, 3, 5)")
                .binding("value", "3"))
                .isEqualTo(true);

        assertThat(assertions.expression("value in ('bar', 'baz', 'foo', 'blah')")
                .binding("value", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value in ('bar', 'baz', 'buz', 'blah')")
                .binding("value", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value in ('bar', null, 'foo', 'blah')")
                .binding("value", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value in (2, null, 3, 5)")
                .binding("value", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value in (2, null)")
                .binding("value", "3"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not in (2, null, 3, 5)")
                .binding("value", "null"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("value not in (2, null)")
                .binding("value", "3"))
                .isNull(BOOLEAN);

        // Because of the failing in-list item 5 / 0, the in-predicate cannot be simplified.
        // It is instead processed with the use of generated code which applies the short-circuit
        // logic and finds a match without evaluating the failing item.
        /*assertFunction("3 in (2, 4, 3, 5 / 0)", BOOLEAN, true);*/
    }

    @Test
    public void testSearchCase()
    {
        assertThat(assertions.expression("""
                        case
                            when value then 33
                        end
                        """)
                .binding("value", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case
                            when value then BIGINT '33'
                        end
                        """)
                .binding("value", "true"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case
                            when value then 1
                            else 33
                        end
                        """)
                .binding("value", "false"))
                .matches("33");

        assertThat(assertions.expression("""
                        case
                            when value then 10000000000
                            else 33
                        end
                        """)
                .binding("value", "false"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 1
                            when condition2 then 1
                            when condition3 then 33
                            else 1
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "false")
                .binding("condition3", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case
                            when condition1 then BIGINT '1'
                            when condition2 then 1
                            when condition3 then 33
                            else 1
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "false")
                .binding("condition3", "true"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 10000000000
                            when condition2 then 1
                            when condition3 then 33
                            else 1
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "false")
                .binding("condition3", "true"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case
                            when value then 1
                        end
                        """)
                .binding("value", "false"))
                .matches("CAST(null AS integer)");

        assertThat(assertions.expression("""
                        case
                            when value then null
                            else 'foo'
                        end
                        """)
                .binding("value", "true"))
                .isNull(createVarcharType(3));

        assertThat(assertions.expression("""
                        case
                            when condition1 then 1
                            when condition2 then 33
                        end
                        """)
                .binding("condition1", "null")
                .binding("condition2", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 10000000000
                            when condition2 then 33
                        end
                        """)
                .binding("condition1", "null")
                .binding("condition2", "true"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 1.0E0
                            when condition2 then 33
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("33E0");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 2.2
                            when condition2 then 2.2
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .hasType(createDecimalType(2, 1))
                .matches("2.2");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 1234567890.0987654321
                            when condition2 then 3.3
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("CAST(3.3 AS decimal(20, 10))");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 1
                            when condition2 then 2.2
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("CAST(2.2 AS decimal(11, 1))");

        assertThat(assertions.expression("""
                        case
                            when condition1 then 1.1
                            when condition2 then 33E0
                        end
                        """)
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("33E0");
    }

    @Test
    public void testSimpleCase()
    {
        assertThat(assertions.expression("""
                        case value
                            when condition then CAST(null AS varchar)
                            else 'foo'
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "true"))
                .matches("CAST(null AS varchar)");

        assertThat(assertions.expression("""
                        case value
                            when condition then 33
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case value
                            when condition then BIGINT '33'
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "true"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case value
                            when condition then 1
                            else 33
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "false"))
                .matches("33");

        assertThat(assertions.expression("""
                        case value
                            when condition then 10000000000
                            else 33
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "false"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1
                            when condition2 then 1
                            when condition3 then 33
                            else 1
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("condition2", "false")
                .binding("condition3", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case value
                            when condition then 1
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "false"))
                .isNull(INTEGER);

        assertThat(assertions.expression("""
                        case value
                            when condition then null
                            else 'foo'
                        end
                        """)
                .binding("value", "true")
                .binding("condition", "true"))
                .isNull(createVarcharType(3));

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 10000000000
                            when condition2 then 33
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "null")
                .binding("condition2", "true"))
                .matches("BIGINT '33'");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1
                            when condition2 then 33
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "null")
                .binding("condition2", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case value
                            when condition then 1
                            else 33
                        end
                        """)
                .binding("value", "null")
                .binding("condition", "true"))
                .matches("33");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1E0
                            when condition2 then 33
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("33E0");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 2.2
                            when condition2 then 2.2
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("2.2");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1234567890.0987654321
                            when condition2 then 3.3
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("CAST(3.3 AS decimal(20, 10))");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1
                            when condition2 then 2.2
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("CAST(2.2 AS decimal(11, 1))");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1.1
                            when condition2 then 33E0
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("condition2", "true"))
                .matches("33E0");

        assertThat(assertions.expression("""
                        case value
                            when condition1 then result1
                            when condition2 then result2
                        end
                        """)
                .binding("value", "true")
                .binding("condition1", "false")
                .binding("result1", "1.1")
                .binding("condition2", "true")
                .binding("result2", "33.0E0"))
                .matches("33.0E0");
    }

    @Test
    public void testSimpleCaseWithCoercions()
    {
        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1
                            when condition2 then 2
                        end
                        """)
                .binding("value", "8")
                .binding("condition1", "double '76.1'")
                .binding("condition2", "real '8.1'"))
                .isNull(INTEGER);

        assertThat(assertions.expression("""
                        case value
                            when condition1 then 1
                            when condition2 then 2
                        end
                        """)
                .binding("value", "8")
                .binding("condition1", "9")
                .binding("condition2", "cast(NULL as decimal)"))
                .isNull(INTEGER);
    }
}
