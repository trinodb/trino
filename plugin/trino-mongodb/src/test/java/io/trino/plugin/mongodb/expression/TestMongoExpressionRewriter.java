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
package io.trino.plugin.mongodb.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.mongodb.expression.MongoExpressions.documentOf;
import static io.trino.plugin.mongodb.expression.MongoExpressions.toDecimal;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoExpressionRewriter
{
    private final ConnectorSession connectorSession;
    private final ConnectorExpressionRewriter<FilterExpression> mongoExpressionRewriter;

    private TestMongoExpressionRewriter()
    {
        this.mongoExpressionRewriter = new MongoExpressionRewriter().getRewriter();
        this.connectorSession = testSessionBuilder().build().toConnectorSession();
    }

    @Test
    public void testRewriteVarcharConstant()
    {
        ConnectorExpression varcharConstant = new Constant(Slices.wrappedBuffer("value".getBytes(UTF_8)), VARCHAR);
        assertRewrite(varcharConstant, createLiteralExpression("value"));
    }

    @Test
    public void testRewriteExactNumericConstant()
    {
        ConnectorExpression tinyintConstant = new Constant(1, TINYINT);
        assertRewrite(tinyintConstant, createLiteralExpression(1));

        ConnectorExpression smallintConstant = new Constant(15, SMALLINT);
        assertRewrite(smallintConstant, createLiteralExpression(15));

        ConnectorExpression intConstant = new Constant(123456, INTEGER);
        assertRewrite(intConstant, createLiteralExpression(123456));

        ConnectorExpression bigintConstant = new Constant(123456789, BIGINT);
        assertRewrite(bigintConstant, createLiteralExpression(123456789));

        long shortDecimalValue = Decimals.encodeShortScaledValue(new BigDecimal("3.14"), 2);
        ConnectorExpression shortDecimalConstant = new Constant(shortDecimalValue, createDecimalType(3, 2));
        assertRewrite(shortDecimalConstant, createLiteralExpression(toDecimal("3.14")));

        Int128 longDecimalValue = Decimals.encodeScaledValue(new BigDecimal("1234567890.123456789"), 9);
        ConnectorExpression longDecimalConstant = new Constant(longDecimalValue, createDecimalType(19, 9));
        assertRewrite(longDecimalConstant, createLiteralExpression(toDecimal("1234567890.123456789")));
    }

    @Test
    public void testRewriteWithNullConstant()
    {
        ConnectorExpression varcharConstant = new Constant(null, VARCHAR);
        assertRewrite(varcharConstant, createLiteralExpression(null));

        ConnectorExpression tinyintConstant = new Constant(null, TINYINT);
        assertRewrite(tinyintConstant, createLiteralExpression(null));

        ConnectorExpression smallintConstant = new Constant(null, SMALLINT);
        assertRewrite(smallintConstant, createLiteralExpression(null));

        ConnectorExpression intConstant = new Constant(null, INTEGER);
        assertRewrite(intConstant, createLiteralExpression(null));

        ConnectorExpression bigintConstant = new Constant(null, BIGINT);
        assertRewrite(bigintConstant, createLiteralExpression(null));

        ConnectorExpression shortDecimalConstant = new Constant(null, createDecimalType(3, 2));
        assertRewrite(shortDecimalConstant, createLiteralExpression(null));

        ConnectorExpression longDecimalConstant = new Constant(null, createDecimalType(19, 9));
        assertRewrite(longDecimalConstant, createLiteralExpression(null));
    }

    @Test
    public void testRewriteVariable()
    {
        ConnectorExpression variable = new Variable("col", BIGINT);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        assertRewrite(variable, createVariableExpression("$col", new ExpressionInfo("$col", BIGINT)), assignments);
    }

    @Test
    public void testRewriteIsNull()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression isNullExpression = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(variable));
        assertRewrite(
                isNullExpression,
                createDocumentExpression(
                        documentOf("$or", ImmutableList.of(
                                documentOf("$eq", Arrays.asList("$col", null)),
                                documentOf("$eq", Arrays.asList("$col", documentOf("$undefined", true))),
                                documentOf("$not", documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))))))),
                assignments);
    }

    @Test
    public void testRewriteIsNotNull()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression isNullExpression = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(variable));
        ConnectorExpression isNotNullExpression = new Call(BOOLEAN, NOT_FUNCTION_NAME, ImmutableList.of(isNullExpression));
        assertRewrite(
                isNotNullExpression,
                createDocumentExpression(
                        documentOf("$not", ImmutableList.of(
                                documentOf("$or", ImmutableList.of(
                                        documentOf("$eq", Arrays.asList("$col", null)),
                                        documentOf("$eq", Arrays.asList("$col", documentOf("$undefined", true))),
                                        documentOf("$not", documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))))))))),
                assignments);
    }

    @Test
    public void testRewriteIn()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        List<ConnectorExpression> constants = ImmutableList.of(
                new Constant(10, BIGINT),
                new Constant(15, BIGINT),
                new Constant(null, BIGINT));
        ConnectorExpression arrayExpression = new Call(BOOLEAN, ARRAY_CONSTRUCTOR_FUNCTION_NAME, constants);
        ConnectorExpression inExpression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, ImmutableList.of(variable, arrayExpression));
        assertRewrite(
                inExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$in", ImmutableList.of("$col", Arrays.asList(10, 15, null)))))),
                assignments);
    }

    @Test
    public void testRewriteNotIn()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        List<ConnectorExpression> constants = ImmutableList.of(
                new Constant(10, BIGINT),
                new Constant(15, BIGINT),
                new Constant(null, BIGINT));
        ConnectorExpression arrayExpression = new Call(BOOLEAN, ARRAY_CONSTRUCTOR_FUNCTION_NAME, constants);
        ConnectorExpression inExpression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, ImmutableList.of(variable, arrayExpression));
        ConnectorExpression notInExpression = new Call(BOOLEAN, NOT_FUNCTION_NAME, ImmutableList.of(inExpression));
        assertRewrite(
                notInExpression,
                createDocumentExpression(
                        documentOf("$not", ImmutableList.of(
                                documentOf("$and", ImmutableList.of(
                                        documentOf("$gt", Arrays.asList("$col", null)),
                                        documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                        documentOf("$in", ImmutableList.of("$col", Arrays.asList(10, 15, null)))))))),
                assignments);
    }

    @Test
    public void testRewriteEqual()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression equalExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                equalExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$eq", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteNotEqual()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression notEqualExpression = new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                notEqualExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$ne", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteLessThan()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression lessThanExpression = new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                lessThanExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$lt", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteGreaterThan()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression greaterThanExpression = new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                greaterThanExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$gt", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteLessThanOrEqual()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression lessThanOrEqualExpression = new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                lessThanOrEqualExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$lte", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteGreaterThanOrEqual()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression greaterThanOrEqualExpression = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                greaterThanOrEqualExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$in", ImmutableList.of(documentOf("$type", "$col"), ImmutableList.of("long", "int"))),
                                documentOf("$gte", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteEqualWithVarchar()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", VARCHAR));
        ConnectorExpression variable = new Variable("col", VARCHAR);
        ConnectorExpression constant = new Constant(Slices.wrappedBuffer("value".getBytes(UTF_8)), VARCHAR);

        ConnectorExpression equalExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                equalExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$eq", ImmutableList.of(MongoExpressions.toString("$col"), "value"))))),
                assignments);
    }

    @Test
    public void testRewriteEqualWithInteger()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", INTEGER));
        ConnectorExpression variable = new Variable("col", VARCHAR);
        ConnectorExpression constant = new Constant(10, INTEGER);

        ConnectorExpression equalExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                equalExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$gt", Arrays.asList("$col", null)),
                                documentOf("$eq", ImmutableList.of(documentOf("$type", "$col"), "int")),
                                documentOf("$eq", ImmutableList.of("$col", 10))))),
                assignments);
    }

    @Test
    public void testRewriteOr()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of(
                "col1", createColumnHandle("col1", BIGINT),
                "col2", createColumnHandle("col2", BIGINT));
        ConnectorExpression variable1 = new Variable("col1", BIGINT);
        ConnectorExpression constant1 = new Constant(10, BIGINT);
        ConnectorExpression variable2 = new Variable("col2", BIGINT);
        ConnectorExpression constant2 = new Constant(20, BIGINT);

        ConnectorExpression leftExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable1, constant1));
        ConnectorExpression rightExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable2, constant2));

        ConnectorExpression orExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, ImmutableList.of(leftExpression, rightExpression));
        assertRewrite(
                orExpression,
                createDocumentExpression(
                        documentOf("$or", ImmutableList.of(
                                documentOf("$and", ImmutableList.of(
                                        documentOf("$gt", Arrays.asList("$col1", null)),
                                        documentOf("$in", ImmutableList.of(documentOf("$type", "$col1"), ImmutableList.of("long", "int"))),
                                        documentOf("$eq", ImmutableList.of("$col1", 10)))),
                                documentOf("$and", ImmutableList.of(
                                        documentOf("$gt", Arrays.asList("$col2", null)),
                                        documentOf("$in", ImmutableList.of(documentOf("$type", "$col2"), ImmutableList.of("long", "int"))),
                                        documentOf("$eq", ImmutableList.of("$col2", 20))))))),
                assignments);
    }

    @Test
    public void testRewriteAnd()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of(
                "col1", createColumnHandle("col1", BIGINT),
                "col2", createColumnHandle("col2", BIGINT));
        ConnectorExpression variable1 = new Variable("col1", BIGINT);
        ConnectorExpression constant1 = new Constant(10, BIGINT);
        ConnectorExpression variable2 = new Variable("col2", BIGINT);
        ConnectorExpression constant2 = new Constant(20, BIGINT);

        ConnectorExpression leftExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable1, constant1));
        ConnectorExpression rightExpression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable2, constant2));

        ConnectorExpression andExpression = new Call(BOOLEAN, AND_FUNCTION_NAME, ImmutableList.of(leftExpression, rightExpression));
        assertRewrite(
                andExpression,
                createDocumentExpression(
                        documentOf("$and", ImmutableList.of(
                                documentOf("$and", ImmutableList.of(
                                        documentOf("$gt", Arrays.asList("$col1", null)),
                                        documentOf("$in", ImmutableList.of(documentOf("$type", "$col1"), ImmutableList.of("long", "int"))),
                                        documentOf("$eq", ImmutableList.of("$col1", 10)))),
                                documentOf("$and", ImmutableList.of(
                                        documentOf("$gt", Arrays.asList("$col2", null)),
                                        documentOf("$in", ImmutableList.of(documentOf("$type", "$col2"), ImmutableList.of("long", "int"))),
                                        documentOf("$eq", ImmutableList.of("$col2", 20))))))),
                assignments);
    }

    @Test
    public void testUnsupportedRewriteExpression()
    {
        Slice slice = Slices.wrappedBuffer("value".getBytes(UTF_8));
        ConnectorExpression charConstant = new Constant(slice, createCharType(8));
        assertThat(mongoExpressionRewriter.rewrite(connectorSession, charConstant, ImmutableMap.of()))
                .isEmpty();
    }

    private void assertRewrite(ConnectorExpression expression, FilterExpression expectedExpression)
    {
        assertRewrite(expression, expectedExpression, ImmutableMap.of());
    }

    private void assertRewrite(ConnectorExpression expression, FilterExpression expectedExpression, Map<String, ColumnHandle> assignments)
    {
        Optional<FilterExpression> actualExpression = mongoExpressionRewriter.rewrite(connectorSession, expression, assignments);
        assertThat(actualExpression).isEqualTo(Optional.of(expectedExpression));
    }

    private static FilterExpression createLiteralExpression(Object expression)
    {
        return new FilterExpression(expression, FilterExpression.ExpressionType.LITERAL, Optional.empty());
    }

    private static FilterExpression createVariableExpression(Object expression, ExpressionInfo expressionInfo)
    {
        return new FilterExpression(expression, FilterExpression.ExpressionType.VARIABLE, Optional.of(expressionInfo));
    }

    private static FilterExpression createDocumentExpression(Object expression)
    {
        return new FilterExpression(expression, FilterExpression.ExpressionType.DOCUMENT, Optional.empty());
    }

    private static MongoColumnHandle createColumnHandle(String baseName, Type type, String... dereferenceNames)
    {
        return new MongoColumnHandle(
                baseName,
                ImmutableList.copyOf(dereferenceNames),
                type,
                false,
                false,
                Optional.empty());
    }
}
