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
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.operator.scalar.InvokeFunction.INVOKE_FUNCTION;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLambdaExpression
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(new InternalFunctionBundle(APPLY_FUNCTION, INVOKE_FUNCTION));
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
        assertThat(assertions.expression("apply(a, x -> x + 1)")
                .binding("a", "5"))
                .isEqualTo(6);
        assertThat(assertions.expression("apply(a, x -> x + 1)")
                .binding("a", "5 + RANDOM(1)"))
                .isEqualTo(6);
    }

    @Test
    public void testParameterName()
    {
        // parameter which is not valid identifier in Java
        String nonLetters = "a.b c; d ' \n \\n \"";
        assertThat(assertions.expression("apply(a, " + quote(nonLetters) + " -> " + quote(nonLetters) + " * 2)")
                .binding("a", "5"))
                .isEqualTo(10);
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.expression("apply(a, x -> x + 1)")
                .binding("a", "3"))
                .isEqualTo(4);
        assertThat(assertions.expression("apply(a, x -> x + 1)")
                .binding("a", "NULL"))
                .isNull(INTEGER);
        assertThat(assertions.expression("apply(a, x -> x + 1)")
                .binding("a", "CAST (NULL AS INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.expression("apply(a, x -> x IS NULL)")
                .binding("a", "3"))
                .isEqualTo(false);
        assertThat(assertions.expression("apply(a, x -> x IS NULL)")
                .binding("a", "NULL"))
                .isEqualTo(true);
        assertThat(assertions.expression("apply(a, x -> x IS NULL)")
                .binding("a", "CAST (NULL AS INTEGER)"))
                .isEqualTo(true);
    }

    @Test
    public void testUnreferencedLambdaArgument()
    {
        assertThat(assertions.expression("apply(a, x -> 6)")
                .binding("a", "5"))
                .isEqualTo(6);
    }

    @Test
    public void testLambdaWithoutArgument()
    {
        assertThat(assertions.expression("invoke(() -> 42)"))
                .isEqualTo(42);
    }

    @Test
    public void testSessionDependent()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Pacific/Kiritimati"))
                .build();

        assertThat(assertions.expression("apply(a, x -> x || current_timezone())", session)
                .binding("a", "'timezone: '"))
                .hasType(VARCHAR)
                .isEqualTo("timezone: Pacific/Kiritimati");
    }

    @Test
    public void testInstanceFunction()
    {
        assertThat(assertions.expression("apply(a, x -> concat(ARRAY [1], x))")
                .binding("a", "ARRAY[2]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));
    }

    @Test
    public void testNestedLambda()
    {
        assertThat(assertions.expression("apply(a, x -> apply(x + 7, y -> apply(y * 3, z -> z * 5) + 1) * 2)")
                .binding("a", "11"))
                .isEqualTo(542);
        assertThat(assertions.expression("apply(a, x -> apply(x + 7, x -> apply(x * 3, x -> x * 5) + 1) * 2)")
                .binding("a", "11"))
                .isEqualTo(542);
    }

    @Test
    public void testRowAccess()
    {
        assertThat(assertions.expression("apply(CAST(a AS ROW(x INTEGER, y VARCHAR)), r -> r[1])")
                .binding("a", "ROW(1, 'a')"))
                .isEqualTo(1);
        assertThat(assertions.expression("apply(a, r -> r[2])")
                .binding("a", "CAST(ROW(1, 'a') AS ROW(x INTEGER, y VARCHAR))"))
                .isEqualTo("a");
    }

    @Test
    public void testBind()
    {
        assertThat(assertions.expression("apply(a, \"$internal$bind\"(b, (x, y) -> x + y))")
                .binding("a", "90")
                .binding("b", "9"))
                .isEqualTo(99);
        assertThat(assertions.expression("invoke(\"$internal$bind\"(a, x -> x + 1))")
                .binding("a", "8"))
                .isEqualTo(9);
        assertThat(assertions.expression("apply(a, \"$internal$bind\"(b, c, (x, y, z) -> x + y + z))")
                .binding("a", "900")
                .binding("b", "90")
                .binding("c", "9"))
                .isEqualTo(999);
        assertThat(assertions.expression("invoke(\"$internal$bind\"(a, b, (x, y) -> x + y))")
                .binding("a", "90")
                .binding("b", "9"))
                .isEqualTo(99);
    }

    @Test
    public void testCoercion()
    {
        assertThat(assertions.expression("apply(a, x -> x + 9.0E0)")
                .binding("a", "90"))
                .isEqualTo(99.0);

        assertThat(assertions.expression("apply(a, \"$internal$bind\"(b, (x, y) -> x + y))")
                .binding("a", "90")
                .binding("b", "9.0E0"))
                .isEqualTo(99.0);
        assertThat(assertions.expression("invoke(\"$internal$bind\"(a, x -> x + 1.0E0))")
                .binding("a", "8"))
                .isEqualTo(9.0);
    }

    @Test
    public void testTypeCombinations()
    {
        assertThat(assertions.expression("apply(a, x -> x + 1)")
                .binding("a", "25"))
                .isEqualTo(26);
        assertThat(assertions.expression("apply(a, x -> x + 1.0E0)")
                .binding("a", "25"))
                .isEqualTo(26.0);
        assertThat(assertions.expression("apply(a, x -> x = 25)")
                .binding("a", "25"))
                .isEqualTo(true);
        assertThat(assertions.expression("apply(a, x -> to_base(x, 16))")
                .binding("a", "25"))
                .hasType(createVarcharType(64))
                .isEqualTo("19");
        assertThat(assertions.expression("apply(a, x -> ARRAY[x + 1])")
                .binding("a", "25"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(26));

        assertThat(assertions.expression("apply(a, x -> CAST(x AS BIGINT))")
                .binding("a", "25.6E0"))
                .isEqualTo(26L);
        assertThat(assertions.expression("apply(a, x -> x + 1.0E0)")
                .binding("a", "25.6E0"))
                .isEqualTo(26.6);
        assertThat(assertions.expression("apply(a, x -> x = 25.6E0)")
                .binding("a", "25.6E0"))
                .isEqualTo(true);
        assertThat(assertions.expression("apply(a, x -> CAST(x AS VARCHAR))")
                .binding("a", "25.6E0"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("2.56E1");
        assertThat(assertions.expression("apply(a, x -> MAP(ARRAY[x + 1], ARRAY[true]))")
                .binding("a", "25.6E0"))
                .hasType(mapType(DOUBLE, BOOLEAN))
                .isEqualTo(ImmutableMap.of(26.6, true));

        assertThat(assertions.expression("apply(a, x -> if(x, 25, 26))")
                .binding("a", "true"))
                .isEqualTo(25);
        assertThat(assertions.expression("apply(a, x -> if(x, 25.6E0, 28.9E0))")
                .binding("a", "false"))
                .isEqualTo(28.9);
        assertThat(assertions.expression("apply(a, x -> not x)")
                .binding("a", "true"))
                .isEqualTo(false);
        assertThat(assertions.expression("apply(a, x -> CAST(x AS VARCHAR))")
                .binding("a", "false"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("false");
        assertThat(assertions.expression("apply(a, x -> ARRAY[x])")
                .binding("a", "true"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.expression("apply(a, x -> from_base(x, 16))")
                .binding("a", "'41'"))
                .isEqualTo(65L);
        assertThat(assertions.expression("apply(a, x -> CAST(x AS DOUBLE))")
                .binding("a", "'25.6E0'"))
                .isEqualTo(25.6);
        assertThat(assertions.expression("apply(a, x -> 'abc' = x)")
                .binding("a", "'abc'"))
                .isEqualTo(true);
        assertThat(assertions.expression("apply(a, x -> x || x)")
                .binding("a", "'abc'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("abcabc");
        assertThat(assertions.expression("apply(a, x -> ROW(x, CAST(x AS INTEGER), x > '0'))")
                .binding("a", "'123'"))
                .hasType(RowType.anonymous(ImmutableList.of(createVarcharType(3), INTEGER, BOOLEAN)))
                .isEqualTo(ImmutableList.of("123", 123, true));

        assertThat(assertions.expression("apply(a, x -> from_base(x[3], 10))")
                .binding("a", "ARRAY['abc', NULL, '123']"))
                .isEqualTo(123L);
        assertThat(assertions.expression("apply(a, x -> CAST(x[3] AS DOUBLE))")
                .binding("a", "ARRAY['abc', NULL, '123']"))
                .isEqualTo(123.0);
        assertThat(assertions.expression("apply(a, x -> x[2] IS NULL)")
                .binding("a", "ARRAY['abc', NULL, '123']"))
                .isEqualTo(true);
        assertThat(assertions.expression("apply(a, x -> x[2])")
                .binding("a", "ARRAY['abc', NULL, '123']"))
                .isNull(createVarcharType(3));
        assertThat(assertions.expression("apply(a, x -> map_keys(x))")
                .binding("a", "MAP(ARRAY['abc', 'def'], ARRAY[123, 456])"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("abc", "def"));
    }

    @Test
    public void testFunctionParameter()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("count(x -> x)").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (<function>) for function count. Expected: count(), count(t) T");
        assertTrinoExceptionThrownBy(() -> assertions.expression("max(x -> x)").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (<function>) for function max. Expected: max(t) T:orderable, max(e, bigint) E:orderable");
        assertTrinoExceptionThrownBy(() -> assertions.expression("sqrt(x -> x)").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (<function>) for function sqrt. Expected: sqrt(double)");
        assertTrinoExceptionThrownBy(() -> assertions.expression("sqrt(x -> x, 123, x -> x)").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (<function>, integer, <function>) for function sqrt. Expected: sqrt(double)");
        assertTrinoExceptionThrownBy(() -> assertions.expression("pow(x -> x, 123)").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (<function>, integer) for function pow. Expected: pow(double, double)");
        assertTrinoExceptionThrownBy(() -> assertions.expression("pow(123, x -> x)").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (integer, <function>) for function pow. Expected: pow(double, double)");
    }

    private static String quote(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
