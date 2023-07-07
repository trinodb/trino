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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.mongodb.expression.MongoConnectorExpressionRewriterBuilder;
import io.trino.plugin.mongodb.expression.MongoExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.bson.types.ObjectId;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.documentOf;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.toDate;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.toDecimal;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.toObjectId;
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
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.MICRO_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoConnectorExpressionRewriterBuilder
{
    private final ConnectorSession connectorSession;
    private final ConnectorExpressionRewriter<MongoExpression> mongoExpressionRewriter;

    private TestMongoConnectorExpressionRewriterBuilder()
    {
        this.mongoExpressionRewriter = MongoConnectorExpressionRewriterBuilder.newBuilder()
                .addDefaultRules()
                .build();
        this.connectorSession = testSessionBuilder().build().toConnectorSession();
    }

    @Test
    public void testRewriteCharConstant()
    {
        Slice slice = Slices.wrappedBuffer("value".getBytes(UTF_8));
        ConnectorExpression expression = new Constant(slice, createCharType(8));
        assertRewrite(expression, "value   ");
    }

    @Test
    public void testRewriteVarcharConstant()
    {
        ConnectorExpression expression = new Constant(Slices.wrappedBuffer("value".getBytes(UTF_8)), VARCHAR);
        assertRewrite(expression, "value");
    }

    @Test
    public void testRewriteExactNumericConstant()
    {
        ConnectorExpression tinyintValue = new Constant(1, TINYINT);
        assertRewrite(tinyintValue, 1);

        ConnectorExpression smallintValue = new Constant(15, SMALLINT);
        assertRewrite(smallintValue, 15);

        ConnectorExpression intValue = new Constant(123456, INTEGER);
        assertRewrite(intValue, 123456);

        ConnectorExpression bigintValue = new Constant(123456789, BIGINT);
        assertRewrite(bigintValue, 123456789);

        long shortDecimal = Decimals.encodeShortScaledValue(new BigDecimal("3.14"), 2);
        ConnectorExpression shortDecimalValue = new Constant(shortDecimal, createDecimalType(3, 2));
        assertRewrite(shortDecimalValue, toDecimal("3.14"));

        Int128 longDecimal = Decimals.encodeScaledValue(new BigDecimal("1234567890.123456789"), 9);
        ConnectorExpression longDecimalValue = new Constant(longDecimal, createDecimalType(19, 9));
        assertRewrite(longDecimalValue, toDecimal("1234567890.123456789"));
    }

    @Test
    public void testRewriteBooleanConstant()
    {
        ConnectorExpression expression = new Constant(true, BOOLEAN);
        assertRewrite(expression, true);
    }

    @Test
    public void testRewriteDateTimeConstant()
    {
        ConnectorExpression expression = new Constant(Instant.ofEpochSecond(LocalTime.of(9, 39, 5).toNanoOfDay()).toEpochMilli(), createTimeType(6));
        assertRewrite(expression, toDate("1970-01-01T09:39:05Z"));

        expression = new Constant(LocalDate.of(2022, 10, 12).toEpochDay(), DATE);
        assertRewrite(expression, toDate("2022-10-12"));

        TemporalAccessor parseResult = ISO_OFFSET_DATE_TIME.parse("2020-01-01T09:39:05Z");
        long timestamp = TimeUnit.DAYS.toMicros(parseResult.getLong(EPOCH_DAY)) + parseResult.getLong(MICRO_OF_DAY);
        expression = new Constant(timestamp, createTimestampType(3));
        assertRewrite(expression, toDate("2020-01-01T09:39:05"));

        parseResult = ISO_OFFSET_DATE_TIME.parse("2003-01-01T05:27:35.495Z");
        long timestampWithZone = packDateTimeWithZone(parseResult.getLong(INSTANT_SECONDS) * 1000 + parseResult.getLong(MILLI_OF_SECOND), getTimeZoneKey(ZoneId.from(parseResult).getId()));
        expression = new Constant(timestampWithZone, createTimestampWithTimeZoneType(3));
        assertRewrite(expression, toDate("2003-01-01T05:27:35.495"));
    }

    @Test
    public void testRewriteObjectIdConstant()
    {
        ConnectorExpression expression = new Constant(Slices.wrappedBuffer(new ObjectId("6216f0c6c432d45190f25e7c").toByteArray()), OBJECT_ID);
        assertRewrite(expression, toObjectId("6216f0c6c432d45190f25e7c"));
    }

    @Test(dataProvider = "typeProvider")
    public void testRewriteNullConstant(Type type)
    {
        ConnectorExpression nullValue = new Constant(null, type);
        assertRewrite(nullValue, null);
    }

    @DataProvider
    public static Object[][] typeProvider()
    {
        return new Object[][] {
                {BOOLEAN},
                {TINYINT},
                {SMALLINT},
                {INTEGER},
                {BIGINT},
                {createDecimalType(9)}, // ShortDecimalType
                {createDecimalType(34)}, // LongDecimalType
                {createCharType(3)},
                {VARCHAR},
                {OBJECT_ID},
                {createTimeType(12)},
                {DATE},
                {createTimestampType(6)}, // ShortTimestampType
                {createTimestampWithTimeZoneType(3)}, // ShortTimestampWithTimeZoneType
        };
    }

    @Test(dataProvider = "unsupportedTypeProvider")
    public void testRewriteUnsupportedConstantType(Type type)
    {
        ConnectorExpression dummyConstant = new Constant(new Object(), type);
        assertThat(mongoExpressionRewriter.rewrite(connectorSession, dummyConstant, ImmutableMap.of()))
                .isEmpty();
    }

    @DataProvider
    public static Object[][] unsupportedTypeProvider()
    {
        return new Object[][] {
                {VARBINARY},
                {REAL},
                {DOUBLE},
                {createTimeWithTimeZoneType(3)}, // ShortTimeWithTimeZoneType
                {createTimeWithTimeZoneType(12)}, // LongTimeWithTimeZoneType
                {createTimestampType(9)}, // LongTimestampType
                {createTimestampWithTimeZoneType(6)}, // LongTimestampWithTimeZoneType
        };
    }

    @Test
    public void testRewriteVariable()
    {
        ConnectorExpression expression = new Variable("col", BIGINT);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        assertRewrite(expression, "$col", assignments);
    }

    @Test
    public void testRewriteIsNull()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression expression = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(variable));
        assertRewrite(
                expression,
                documentOf("$or", ImmutableList.of(
                        documentOf("$eq", Arrays.asList("$col", null)),
                        documentOf("$eq", Arrays.asList("$col", documentOf("$undefined", true))))),
                assignments);
    }

    @Test
    public void testRewriteIsNotNull()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression notExpression = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(variable));
        ConnectorExpression isNotNullExpression = new Call(BOOLEAN, NOT_FUNCTION_NAME, ImmutableList.of(notExpression));
        assertRewrite(
                isNotNullExpression,
                documentOf("$not", ImmutableList.of(
                        documentOf("$or", ImmutableList.of(
                                documentOf("$eq", Arrays.asList("$col", null)),
                                documentOf("$eq", Arrays.asList("$col", documentOf("$undefined", true))))))),
                assignments);
    }

    @Test
    public void testRewriteIn()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        List<ConnectorExpression> constants = ImmutableList.of(
                new Constant(10, BIGINT),
                new Constant(15, BIGINT));
        ConnectorExpression arrayExpression = new Call(BOOLEAN, ARRAY_CONSTRUCTOR_FUNCTION_NAME, constants);
        ConnectorExpression expression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, ImmutableList.of(variable, arrayExpression));
        assertRewrite(expression, documentOf("$in", ImmutableList.of("$col", ImmutableList.of(10, 15))), assignments);
    }

    @Test
    public void testRewriteComparison()
    {
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", createColumnHandle("col", BIGINT));
        ConnectorExpression variable = new Variable("col", BIGINT);
        ConnectorExpression constant = new Constant(10, BIGINT);

        ConnectorExpression expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(expression, documentOf("$eq", ImmutableList.of("$col", 10)), assignments);

        expression = new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                expression,
                documentOf("$and", ImmutableList.of(
                        documentOf("$ne", ImmutableList.of("$col", 10)),
                        documentOf("$gt", Arrays.asList("$col", null)),
                        documentOf("$gt", Arrays.asList(10, null)))),
                assignments);

        expression = new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                expression,
                documentOf("$and", ImmutableList.of(
                        documentOf("$lt", ImmutableList.of("$col", 10)),
                        documentOf("$gt", Arrays.asList("$col", null)),
                        documentOf("$gt", Arrays.asList(10, null)))),
                assignments);

        expression = new Call(BOOLEAN, GREATER_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(expression, documentOf("$gt", ImmutableList.of("$col", 10)), assignments);

        expression = new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(
                expression,
                documentOf("$and", ImmutableList.of(
                        documentOf("$lte", ImmutableList.of("$col", 10)),
                        documentOf("$gt", Arrays.asList("$col", null)),
                        documentOf("$gt", Arrays.asList(10, null)))),
                assignments);

        expression = new Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(variable, constant));
        assertRewrite(expression, documentOf("$gte", ImmutableList.of("$col", 10)), assignments);
    }

    @Test
    public void testRewriteLogicalExpression()
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

        ConnectorExpression logicalExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, ImmutableList.of(leftExpression, rightExpression));
        assertRewrite(logicalExpression, documentOf("$or", ImmutableList.of(documentOf("$eq", ImmutableList.of("$col1", 10)), documentOf("$eq", ImmutableList.of("$col2", 20)))), assignments);

        logicalExpression = new Call(BOOLEAN, AND_FUNCTION_NAME, ImmutableList.of(leftExpression, rightExpression));
        assertRewrite(logicalExpression, documentOf("$and", ImmutableList.of(documentOf("$eq", ImmutableList.of("$col1", 10)), documentOf("$eq", ImmutableList.of("$col2", 20)))), assignments);
    }

    private void assertRewrite(ConnectorExpression expression, Object expectedValue)
    {
        assertRewrite(expression, expectedValue, ImmutableMap.of());
    }

    private void assertRewrite(ConnectorExpression expression, Object expectedValue, Map<String, ColumnHandle> assignments)
    {
        Optional<MongoExpression> actualExpression = mongoExpressionRewriter.rewrite(connectorSession, expression, assignments);
        Optional<MongoExpression> expectedExpression = Optional.of(new MongoExpression(expectedValue));
        assertThat(actualExpression).isEqualTo(expectedExpression);
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
