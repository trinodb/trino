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
package io.trino.plugin.pinot.query.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotFilterExpressionRewriter
{
    private static final FunctionName JSON_EXTRACT_SCALAR = new FunctionName("json_extract_scalar");
    private static final FunctionName JSON_EXTRACT = new FunctionName("json_extract");
    private static final FunctionName JSON_ARRAY_CONTAINS = new FunctionName("json_array_contains");
    private static final FunctionName CONTAINS = new FunctionName("contains");

    private static final Variable JSON = new Variable("json", VARCHAR);
    private static final Variable VENDOR = new Variable("vendor", VARCHAR);

    private static final Map<String, ColumnHandle> ASSIGNMENTS = ImmutableMap.of(
            "json", new PinotColumnHandle("json", VARCHAR),
            "vendor", new PinotColumnHandle("vendor", VARCHAR));

    private static final ConnectorExpressionRewriter<String> REWRITER = new ConnectorExpressionRewriter<>(ImmutableSet.of(
            new RewriteLogicalExpression(),
            new RewriteEquality(),
            new RewriteBooleanComparison(),
            new RewriteJsonExtractScalarIsNull(),
            new RewriteJsonContains(),
            new RewriteJsonArrayContains()));

    @Test
    public void testJsonExtractScalarIsNull()
    {
        assertThat(rewrite(isNull(jsonExtractScalar("$.selector"))))
                .contains("JSON_MATCH(\"json\", '\"$.selector\" IS NULL')");
    }

    @Test
    public void testJsonContains()
    {
        assertThat(rewrite(contains(varcharArray("O", "F"), jsonExtractScalar("$.status"))))
                .contains("JSON_MATCH(\"json\", '\"$.status\" IN (''O'', ''F'')')");
        assertThat(rewrite(contains(integerArray(5, 7), cast(INTEGER, jsonExtractScalar("$.number")))))
                .contains("JSON_MATCH(\"json\", '\"$.number\" IN (5, 7)')");
    }

    @Test
    public void testJsonContainsUnsupported()
    {
        // integer values without a cast would compare a number with a string
        assertThat(rewrite(contains(integerArray(5), jsonExtractScalar("$.number")))).isEmpty();
        // cast type must match the array element type
        assertThat(rewrite(contains(integerArray(5), cast(BIGINT, jsonExtractScalar("$.number"))))).isEmpty();
        assertThat(rewrite(contains(varcharArray("a"), new Call(VARCHAR, new FunctionName("lower"), List.of(JSON))))).isEmpty();
    }

    @Test
    public void testJsonArrayContains()
    {
        assertThat(rewrite(jsonArrayContains("$.tags", new Constant(utf8Slice("a"), VARCHAR))))
                .contains("JSON_MATCH(\"json\", '\"$.tags[*]\" = ''a''')");
        assertThat(rewrite(jsonArrayContains("$.numbers", new Constant(5L, BIGINT))))
                .contains("JSON_MATCH(\"json\", '\"$.numbers[*]\" = 5')");
    }

    @Test
    public void testQuotesAreEscaped()
    {
        assertThat(rewrite(jsonArrayContains("$.tags", new Constant(utf8Slice("it's"), VARCHAR))))
                .contains("JSON_MATCH(\"json\", '\"$.tags[*]\" = ''it''''s''')");
    }

    @Test
    public void testBooleanComparison()
    {
        ConnectorExpression arrayContains = jsonArrayContains("$.tags", new Constant(utf8Slice("a"), VARCHAR));
        String match = "JSON_MATCH(\"json\", '\"$.tags[*]\" = ''a''')";
        assertThat(rewrite(booleanComparison(EQUAL_OPERATOR_FUNCTION_NAME, arrayContains, true))).contains(match);
        assertThat(rewrite(booleanComparison(EQUAL_OPERATOR_FUNCTION_NAME, arrayContains, false))).contains("NOT(" + match + ")");
        assertThat(rewrite(booleanComparison(NOT_EQUAL_OPERATOR_FUNCTION_NAME, arrayContains, true))).contains("NOT(" + match + ")");
        assertThat(rewrite(booleanComparison(NOT_EQUAL_OPERATOR_FUNCTION_NAME, arrayContains, false))).contains(match);
    }

    @Test
    public void testLogicalExpressions()
    {
        ConnectorExpression equality = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(VENDOR, new Constant(utf8Slice("vendor1"), VARCHAR)));
        ConnectorExpression isNull = isNull(jsonExtractScalar("$.selector"));
        String isNullPql = "JSON_MATCH(\"json\", '\"$.selector\" IS NULL')";

        assertThat(rewrite(new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(equality, isNull))))
                .contains("(\"vendor\" = 'vendor1' AND " + isNullPql + ")");
        assertThat(rewrite(new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(equality, isNull))))
                .contains("(\"vendor\" = 'vendor1' OR " + isNullPql + ")");
        assertThat(rewrite(new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(equality, isNull))))))
                .contains("NOT((\"vendor\" = 'vendor1' OR " + isNullPql + "))");
    }

    @Test
    public void testUnsupportedArgumentFailsWholeExpression()
    {
        ConnectorExpression unsupported = new Call(BOOLEAN, new FunctionName("starts_with"), List.of(VENDOR, new Constant(utf8Slice("v"), VARCHAR)));
        assertThat(rewrite(unsupported)).isEmpty();
        assertThat(rewrite(new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(isNull(jsonExtractScalar("$.a")), unsupported)))).isEmpty();
    }

    @Test
    public void testAggregateColumnIsNotPushedDown()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("json", new PinotColumnHandle("json", VARCHAR, "max(json)", false, true, true, Optional.empty(), Optional.empty()));
        assertThat(REWRITER.rewrite(TestingConnectorSession.SESSION, isNull(jsonExtractScalar("$.a")), assignments)).isEmpty();
    }

    private static Optional<String> rewrite(ConnectorExpression expression)
    {
        return REWRITER.rewrite(TestingConnectorSession.SESSION, expression, ASSIGNMENTS);
    }

    private static Call jsonExtractScalar(String path)
    {
        return new Call(VARCHAR, JSON_EXTRACT_SCALAR, List.of(JSON, new Constant(utf8Slice(path), VARCHAR)));
    }

    private static Call isNull(ConnectorExpression expression)
    {
        return new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(expression));
    }

    private static Call cast(Type type, ConnectorExpression expression)
    {
        return new Call(type, CAST_FUNCTION_NAME, List.of(expression));
    }

    private static Call contains(Constant array, ConnectorExpression value)
    {
        return new Call(BOOLEAN, CONTAINS, List.of(array, value));
    }

    private static Call jsonArrayContains(String path, Constant value)
    {
        Call extract = new Call(VARCHAR, JSON_EXTRACT, List.of(JSON, new Constant(utf8Slice(path), VARCHAR)));
        return new Call(BOOLEAN, JSON_ARRAY_CONTAINS, ImmutableList.of(extract, value));
    }

    private static Call booleanComparison(FunctionName operator, ConnectorExpression expression, boolean value)
    {
        return new Call(BOOLEAN, operator, List.of(expression, new Constant(value, BOOLEAN)));
    }

    private static Constant varcharArray(String... values)
    {
        ArrayType type = new ArrayType(VARCHAR);
        var builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            VARCHAR.writeSlice(builder, utf8Slice(value));
        }
        return new Constant(builder.build(), type);
    }

    private static Constant integerArray(int... values)
    {
        ArrayType type = new ArrayType(INTEGER);
        var builder = INTEGER.createBlockBuilder(null, values.length);
        for (int value : values) {
            INTEGER.writeLong(builder, value);
        }
        return new Constant(builder.build(), type);
    }
}
