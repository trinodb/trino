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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.Signature;
import io.trino.operator.scalar.Re2JCastToRegexpFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.transaction.TestingTransactionManager;
import io.trino.type.Re2JRegexp;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.function.BiPredicate;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreCase;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.FunctionId.toFunctionId;
import static io.trino.metadata.LiteralFunction.LITERAL_FUNCTION_NAME;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.scalar.JoniRegexpCasts.castVarcharToJoniRegexp;
import static io.trino.operator.scalar.JsonFunctions.castVarcharToJsonPath;
import static io.trino.operator.scalar.StringFunctions.castVarcharToCodePoints;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.type.Re2JRegexpType.RE2J_REGEXP_SIGNATURE;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLiteralEncoder
{
    private final Metadata metadata = createTestMetadataManager();
    private final LiteralEncoder encoder = new LiteralEncoder(metadata);

    private final ResolvedFunction literalFunction = new ResolvedFunction(
            new BoundSignature(LITERAL_FUNCTION_NAME, VARBINARY, ImmutableList.of(VARBINARY)),
            new LiteralFunction(metadata::getBlockEncodingSerde).getFunctionMetadata().getFunctionId(),
            ImmutableMap.of(),
            ImmutableSet.of());

    private final ResolvedFunction base64Function = new ResolvedFunction(
            new BoundSignature("from_base64", VARBINARY, ImmutableList.of(VARCHAR)),
            toFunctionId(new Signature("from_base64", VARBINARY.getTypeSignature(), ImmutableList.of(new TypeSignature("varchar", typeVariable("x"))))),
            ImmutableMap.of(),
            ImmutableSet.of());

    @Test
    public void testEncodeUnknown()
    {
        assertEncode(null, UNKNOWN, "null");
    }

    @Test
    public void testEncodeBigint()
    {
        assertEncode(null, BIGINT, "CAST(null AS bigint)");
        assertEncode(123L, BIGINT, "BIGINT '123'");
    }

    @Test
    public void testEncodeDecimal()
    {
        assertEncode(123L, createDecimalType(7, 1), "CAST(DECIMAL '12.3' AS decimal(7, 1))");
    }

    @Test
    public void testEncodeChar()
    {
        assertEncode(utf8Slice("hello"), createCharType(5), "CAST('hello' AS char(5))");
        assertEncode(utf8Slice("hello"), createCharType(13), "CAST('hello' AS char(13))");
    }

    @Test
    public void testEncodeVarchar()
    {
        assertEncode(utf8Slice("hello"), createVarcharType(5), "'hello'");
        assertEncode(utf8Slice("hello"), createVarcharType(13), "CAST('hello' AS varchar(13))");
        assertEncode(utf8Slice("hello"), VARCHAR, "VARCHAR 'hello'");
    }

    @Test
    public void testEncodeVarbinary()
    {
        assertEncodeCaseInsensitively(utf8Slice("hello"), VARBINARY, literalVarbinary("hello".getBytes(UTF_8)));
        assertEncodeCaseInsensitively(utf8Slice("hello"), VARBINARY, literalVarbinary("hello".getBytes(UTF_8)));
    }

    @Test
    public void testEncodeTimestamp()
    {
        for (int precision = 0; precision <= 12; precision++) {
            assertEncode(null, createTimestampType(precision), format("CAST(null AS timestamp(%s))", precision));
        }

        assertEncode(1603710138_000000L, createTimestampType(0), "TIMESTAMP '2020-10-26 11:02:18'");
        assertEncode(1603710138_100000L, createTimestampType(1), "TIMESTAMP '2020-10-26 11:02:18.1'");
        assertEncode(1603710138_120000L, createTimestampType(2), "TIMESTAMP '2020-10-26 11:02:18.12'");
        assertEncode(1603710138_123000L, createTimestampType(3), "TIMESTAMP '2020-10-26 11:02:18.123'");
        assertEncode(1603710138_123400L, createTimestampType(4), "TIMESTAMP '2020-10-26 11:02:18.1234'");
        assertEncode(1603710138_123450L, createTimestampType(5), "TIMESTAMP '2020-10-26 11:02:18.12345'");
        assertEncode(1603710138_123456L, createTimestampType(6), "TIMESTAMP '2020-10-26 11:02:18.123456'");
        assertEncode(new LongTimestamp(1603710138_123456L, 100000), createTimestampType(7), "TIMESTAMP '2020-10-26 11:02:18.1234561'");
        assertEncode(new LongTimestamp(1603710138_123456L, 120000), createTimestampType(8), "TIMESTAMP '2020-10-26 11:02:18.12345612'");
        assertEncode(new LongTimestamp(1603710138_123456L, 123000), createTimestampType(9), "TIMESTAMP '2020-10-26 11:02:18.123456123'");
        assertEncode(new LongTimestamp(1603710138_123456L, 123400), createTimestampType(10), "TIMESTAMP '2020-10-26 11:02:18.1234561234'");
        assertEncode(new LongTimestamp(1603710138_123456L, 123450), createTimestampType(11), "TIMESTAMP '2020-10-26 11:02:18.12345612345'");
        assertEncode(new LongTimestamp(1603710138_123456L, 123456), createTimestampType(12), "TIMESTAMP '2020-10-26 11:02:18.123456123456'");

        assertEncode(1603710138_000000L, createTimestampType(1), "TIMESTAMP '2020-10-26 11:02:18.0'");
        assertEncode(1603710138_000000L, createTimestampType(2), "TIMESTAMP '2020-10-26 11:02:18.00'");
        assertEncode(1603710138_000000L, createTimestampType(3), "TIMESTAMP '2020-10-26 11:02:18.000'");
        assertEncode(1603710138_000000L, createTimestampType(4), "TIMESTAMP '2020-10-26 11:02:18.0000'");
        assertEncode(1603710138_000000L, createTimestampType(5), "TIMESTAMP '2020-10-26 11:02:18.00000'");
        assertEncode(1603710138_000000L, createTimestampType(6), "TIMESTAMP '2020-10-26 11:02:18.000000'");
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(7), "TIMESTAMP '2020-10-26 11:02:18.0000000'");
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(8), "TIMESTAMP '2020-10-26 11:02:18.00000000'");
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(9), "TIMESTAMP '2020-10-26 11:02:18.000000000'");
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(10), "TIMESTAMP '2020-10-26 11:02:18.0000000000'");
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(11), "TIMESTAMP '2020-10-26 11:02:18.00000000000'");
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(12), "TIMESTAMP '2020-10-26 11:02:18.000000000000'");
    }

    @Test
    public void testEncodeTimestampWithTimeZone()
    {
        for (int precision = 0; precision <= 12; precision++) {
            assertEncode(null, createTimestampWithTimeZoneType(precision), format("CAST(null AS timestamp(%s) with time zone)", precision));
        }

        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(0), "TIMESTAMP '2020-10-26 11:02:18 UTC'");
        assertEncode(packDateTimeWithZone(1603710138_100L, UTC_KEY), createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-10-26 11:02:18.1 UTC'");
        assertEncode(packDateTimeWithZone(1603710138_120L, UTC_KEY), createTimestampWithTimeZoneType(2), "TIMESTAMP '2020-10-26 11:02:18.12 UTC'");
        assertEncode(packDateTimeWithZone(1603710138_123L, UTC_KEY), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-10-26 11:02:18.123 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 100000000, UTC_KEY), createTimestampWithTimeZoneType(4), "TIMESTAMP '2020-10-26 11:02:18.1231 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 120000000, UTC_KEY), createTimestampWithTimeZoneType(5), "TIMESTAMP '2020-10-26 11:02:18.12312 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123000000, UTC_KEY), createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-10-26 11:02:18.123123 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123400000, UTC_KEY), createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-10-26 11:02:18.1231234 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123450000, UTC_KEY), createTimestampWithTimeZoneType(8), "TIMESTAMP '2020-10-26 11:02:18.12312345 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456000, UTC_KEY), createTimestampWithTimeZoneType(9), "TIMESTAMP '2020-10-26 11:02:18.123123456 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456700, UTC_KEY), createTimestampWithTimeZoneType(10), "TIMESTAMP '2020-10-26 11:02:18.1231234567 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456780, UTC_KEY), createTimestampWithTimeZoneType(11), "TIMESTAMP '2020-10-26 11:02:18.12312345678 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456789, UTC_KEY), createTimestampWithTimeZoneType(12), "TIMESTAMP '2020-10-26 11:02:18.123123456789 UTC'");

        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(1), "TIMESTAMP '2020-10-26 11:02:18.0 UTC'");
        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(2), "TIMESTAMP '2020-10-26 11:02:18.00 UTC'");
        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-10-26 11:02:18.000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(4), "TIMESTAMP '2020-10-26 11:02:18.0000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(5), "TIMESTAMP '2020-10-26 11:02:18.00000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-10-26 11:02:18.000000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(7), "TIMESTAMP '2020-10-26 11:02:18.0000000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(8), "TIMESTAMP '2020-10-26 11:02:18.00000000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(9), "TIMESTAMP '2020-10-26 11:02:18.000000000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(10), "TIMESTAMP '2020-10-26 11:02:18.0000000000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(11), "TIMESTAMP '2020-10-26 11:02:18.00000000000 UTC'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(12), "TIMESTAMP '2020-10-26 11:02:18.000000000000 UTC'");

        // with zone
        assertEncode(packDateTimeWithZone(1603710138_000L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(0), "TIMESTAMP '2020-10-26 12:02:18 Europe/Warsaw'");
        assertEncode(packDateTimeWithZone(1603710138_123L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-10-26 12:02:18.123 Europe/Warsaw'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123000000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-10-26 12:02:18.123123 Europe/Warsaw'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(9), "TIMESTAMP '2020-10-26 12:02:18.123123456 Europe/Warsaw'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456789, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(12), "TIMESTAMP '2020-10-26 12:02:18.123123456789 Europe/Warsaw'");

        // DST change forward
        assertEncode(packDateTimeWithZone(1585445478_000L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(0), "TIMESTAMP '2020-03-29 03:31:18 Europe/Warsaw'");
        assertEncode(packDateTimeWithZone(1585445478_123L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-03-29 03:31:18.123 Europe/Warsaw'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1585445478_123L, 123000000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(6), "TIMESTAMP '2020-03-29 03:31:18.123123 Europe/Warsaw'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1585445478_123L, 123456000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(9), "TIMESTAMP '2020-03-29 03:31:18.123123456 Europe/Warsaw'");
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1585445478_123L, 123456789, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(12), "TIMESTAMP '2020-03-29 03:31:18.123123456789 Europe/Warsaw'");

        // DST change backward - no direct representation
        assertRoundTrip(packDateTimeWithZone(1603589478_000L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(0), Long::equals);
        assertRoundTrip(packDateTimeWithZone(1603589478_123L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(3), Long::equals);
        assertRoundTrip(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603589478_123L, 123000000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(6), LongTimestampWithTimeZone::equals);
        assertRoundTrip(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603589478_123L, 123456000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(9), LongTimestampWithTimeZone::equals);
        assertRoundTrip(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603589478_123L, 123456789, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(12), LongTimestampWithTimeZone::equals);
    }

    @Test
    public void testEncodeRegex()
    {
        assertRoundTrip(castVarcharToJoniRegexp(utf8Slice("[a-z]")), LIKE_PATTERN, (left, right) -> left.pattern().equals(right.pattern()));
        assertRoundTrip(castVarcharToJoniRegexp(utf8Slice("[a-z]")), JONI_REGEXP, (left, right) -> left.pattern().equals(right.pattern()));
        assertRoundTrip(castVarcharToRe2JRegexp(utf8Slice("[a-z]")), metadata.getType(RE2J_REGEXP_SIGNATURE), (left, right) -> left.pattern().equals(right.pattern()));
    }

    @Test
    public void testEncodeJsonPath()
    {
        assertRoundTrip(castVarcharToJsonPath(utf8Slice("$.foo")), JSON_PATH, (left, right) -> left.pattern().equals(right.pattern()));
    }

    @Test
    public void testEncodeCodePoints()
    {
        assertRoundTrip(castVarcharToCodePoints(utf8Slice("hello")), CODE_POINTS, Arrays::equals);
    }

    private void assertEncode(Object value, Type type, String expected)
    {
        Expression expression = encoder.toExpression(value, type);
        assertEquals(getExpressionType(expression), type);
        assertEquals(getExpressionValue(expression), value);
        assertEquals(formatSql(expression), expected);
    }

    /**
     * @deprecated Use {@link #assertEncode} instead.
     */
    @Deprecated
    private void assertEncodeCaseInsensitively(Object value, Type type, String expected)
    {
        Expression expression = encoder.toExpression(value, type);
        assertEquals(getExpressionType(expression), type);
        assertEquals(getExpressionValue(expression), value);
        assertEqualsIgnoreCase(formatSql(expression), expected);
    }

    private <T> void assertRoundTrip(T value, Type type, BiPredicate<T, T> predicate)
    {
        Expression expression = encoder.toExpression(value, type);
        assertEquals(getExpressionType(expression), type);
        @SuppressWarnings("unchecked")
        T decodedValue = (T) getExpressionValue(expression);
        assertTrue(predicate.test(value, decodedValue));
    }

    private Object getExpressionValue(Expression expression)
    {
        return new ExpressionInterpreter(expression, metadata, TEST_SESSION, getExpressionTypes(expression)).evaluate();
    }

    private Type getExpressionType(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(expression);
        Type expressionType = expressionTypes.get(NodeRef.of(expression));
        verify(expressionType != null, "No type found");
        return expressionType;
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression)
    {
        return transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, transactionSession -> {
                    ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                            metadata,
                            new AllowAllAccessControl(),
                            transactionSession,
                            TypeProvider.empty(),
                            emptyMap(),
                            node -> new IllegalStateException("Unexpected node: " + node),
                            WarningCollector.NOOP,
                            false);
                    expressionAnalyzer.analyze(expression, Scope.create());
                    Map<NodeRef<Expression>, Type> expressionTypes = expressionAnalyzer.getExpressionTypes();
                    return expressionTypes;
                });
    }

    private String literalVarbinary(byte[] value)
    {
        return "\"" + literalFunction.toQualifiedName() + "\"" +
                "(\"" + base64Function.toQualifiedName() + "\"" +
                "('" + Base64.getEncoder().encodeToString(value) + "'))";
    }

    private static Re2JRegexp castVarcharToRe2JRegexp(Slice value)
    {
        return Re2JCastToRegexpFunction.castToRegexp(Integer.MAX_VALUE, 5, false, VarcharType.UNBOUNDED_LENGTH, value);
    }
}
