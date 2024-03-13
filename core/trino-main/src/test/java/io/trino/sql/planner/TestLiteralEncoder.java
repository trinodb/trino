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
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.Re2JCastToRegexpFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.Signature;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NodeRef;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.type.Re2JRegexp;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.function.BiPredicate;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.LiteralFunction.LITERAL_FUNCTION_NAME;
import static io.trino.operator.scalar.JoniRegexpCasts.castVarcharToJoniRegexp;
import static io.trino.operator.scalar.JsonFunctions.castVarcharToJsonPath;
import static io.trino.operator.scalar.StringFunctions.castVarcharToCodePoints;
import static io.trino.spi.function.FunctionId.toFunctionId;
import static io.trino.spi.function.FunctionKind.SCALAR;
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
import static io.trino.sql.ir.IrUtils.isEffectivelyLiteral;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.LikeFunctions.likePattern;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.type.Re2JRegexpType.RE2J_REGEXP_SIGNATURE;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLiteralEncoder
{
    private final LiteralEncoder encoder = new LiteralEncoder(PLANNER_CONTEXT);

    private final ResolvedFunction literalFunction = new ResolvedFunction(
            new BoundSignature(
                    builtinFunctionName(LITERAL_FUNCTION_NAME),
                    VARBINARY,
                    ImmutableList.of(VARBINARY)),
            GlobalSystemConnector.CATALOG_HANDLE,
            new LiteralFunction(PLANNER_CONTEXT.getBlockEncodingSerde()).getFunctionMetadata().getFunctionId(),
            SCALAR,
            true,
            new FunctionNullability(false, ImmutableList.of(false)),
            ImmutableMap.of(),
            ImmutableSet.of());

    private final ResolvedFunction base64Function = new ResolvedFunction(
            new BoundSignature(
                    builtinFunctionName("from_base64"),
                    VARBINARY,
                    ImmutableList.of(VARCHAR)),
            GlobalSystemConnector.CATALOG_HANDLE,
            toFunctionId(
                    "from_base64",
                    Signature.builder()
                            .returnType(VARBINARY)
                            .argumentType(new TypeSignature("varchar", typeVariable("x")))
                            .build()),
            SCALAR,
            true,
            new FunctionNullability(false, ImmutableList.of(false)),
            ImmutableMap.of(),
            ImmutableSet.of());

    @Test
    public void testEncodeUnknown()
    {
        assertEncode(null, UNKNOWN);
    }

    @Test
    public void testEncodeBigint()
    {
        assertEncode(null, BIGINT);
        assertEncode(123L, BIGINT);
    }

    @Test
    public void testEncodeDecimal()
    {
        assertEncode(123L, createDecimalType(7, 1));
    }

    @Test
    public void testEncodeChar()
    {
        assertEncode(utf8Slice("hello"), createCharType(5));
        assertEncode(utf8Slice("hello"), createCharType(13));
    }

    @Test
    public void testEncodeVarchar()
    {
        assertEncode(utf8Slice("hello"), createVarcharType(5));
        assertEncode(utf8Slice("hello"), createVarcharType(13));
        assertEncode(utf8Slice("hello"), VARCHAR);
    }

    @Test
    public void testEncodeVarbinary()
    {
        assertEncodeCaseInsensitively(utf8Slice("hello"), VARBINARY);
        assertEncodeCaseInsensitively(utf8Slice("hello"), VARBINARY);
    }

    @Test
    public void testEncodeTimestamp()
    {
        for (int precision = 0; precision <= 12; precision++) {
            assertEncode(null, createTimestampType(precision));
        }

        assertEncode(1603710138_000000L, createTimestampType(0));
        assertEncode(1603710138_100000L, createTimestampType(1));
        assertEncode(1603710138_120000L, createTimestampType(2));
        assertEncode(1603710138_123000L, createTimestampType(3));
        assertEncode(1603710138_123400L, createTimestampType(4));
        assertEncode(1603710138_123450L, createTimestampType(5));
        assertEncode(1603710138_123456L, createTimestampType(6));
        assertEncode(new LongTimestamp(1603710138_123456L, 100000), createTimestampType(7));
        assertEncode(new LongTimestamp(1603710138_123456L, 120000), createTimestampType(8));
        assertEncode(new LongTimestamp(1603710138_123456L, 123000), createTimestampType(9));
        assertEncode(new LongTimestamp(1603710138_123456L, 123400), createTimestampType(10));
        assertEncode(new LongTimestamp(1603710138_123456L, 123450), createTimestampType(11));
        assertEncode(new LongTimestamp(1603710138_123456L, 123456), createTimestampType(12));

        assertEncode(1603710138_000000L, createTimestampType(1));
        assertEncode(1603710138_000000L, createTimestampType(2));
        assertEncode(1603710138_000000L, createTimestampType(3));
        assertEncode(1603710138_000000L, createTimestampType(4));
        assertEncode(1603710138_000000L, createTimestampType(5));
        assertEncode(1603710138_000000L, createTimestampType(6));
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(7));
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(8));
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(9));
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(10));
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(11));
        assertEncode(new LongTimestamp(1603710138_000000L, 0), createTimestampType(12));
    }

    @Test
    public void testEncodeTimestampWithTimeZone()
    {
        for (int precision = 0; precision <= 12; precision++) {
            assertEncode(null, createTimestampWithTimeZoneType(precision));
        }

        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(0));
        assertEncode(packDateTimeWithZone(1603710138_100L, UTC_KEY), createTimestampWithTimeZoneType(1));
        assertEncode(packDateTimeWithZone(1603710138_120L, UTC_KEY), createTimestampWithTimeZoneType(2));
        assertEncode(packDateTimeWithZone(1603710138_123L, UTC_KEY), createTimestampWithTimeZoneType(3));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 100000000, UTC_KEY), createTimestampWithTimeZoneType(4));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 120000000, UTC_KEY), createTimestampWithTimeZoneType(5));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123000000, UTC_KEY), createTimestampWithTimeZoneType(6));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123400000, UTC_KEY), createTimestampWithTimeZoneType(7));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123450000, UTC_KEY), createTimestampWithTimeZoneType(8));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456000, UTC_KEY), createTimestampWithTimeZoneType(9));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456700, UTC_KEY), createTimestampWithTimeZoneType(10));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456780, UTC_KEY), createTimestampWithTimeZoneType(11));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456789, UTC_KEY), createTimestampWithTimeZoneType(12));

        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(1));
        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(2));
        assertEncode(packDateTimeWithZone(1603710138_000L, UTC_KEY), createTimestampWithTimeZoneType(3));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(4));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(5));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(6));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(7));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(8));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(9));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(10));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(11));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_000L, 0, UTC_KEY), createTimestampWithTimeZoneType(12));

        // with zone
        assertEncode(packDateTimeWithZone(1603710138_000L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(0));
        assertEncode(packDateTimeWithZone(1603710138_123L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(3));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123000000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(6));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(9));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1603710138_123L, 123456789, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(12));

        // DST change forward
        assertEncode(packDateTimeWithZone(1585445478_000L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(0));
        assertEncode(packDateTimeWithZone(1585445478_123L, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(3));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1585445478_123L, 123000000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(6));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1585445478_123L, 123456000, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(9));
        assertEncode(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1585445478_123L, 123456789, TimeZoneKey.getTimeZoneKey("Europe/Warsaw")), createTimestampWithTimeZoneType(12));

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
        assertRoundTrip(castVarcharToJoniRegexp(utf8Slice("[a-z]")), JONI_REGEXP, (left, right) -> left.pattern().equals(right.pattern()));
        assertRoundTrip(castVarcharToRe2JRegexp(utf8Slice("[a-z]")), PLANNER_CONTEXT.getTypeManager().getType(RE2J_REGEXP_SIGNATURE), (left, right) -> left.pattern().equals(right.pattern()));
    }

    @Test
    public void testEncodeLikePattern()
    {
        assertRoundTrip(likePattern(utf8Slice("abc")), LIKE_PATTERN, (left, right) -> left.getPattern().equals(right.getPattern()));
        assertRoundTrip(likePattern(utf8Slice("abc_")), LIKE_PATTERN, (left, right) -> left.getPattern().equals(right.getPattern()));
        assertRoundTrip(likePattern(utf8Slice("abc%")), LIKE_PATTERN, (left, right) -> left.getPattern().equals(right.getPattern()));

        assertRoundTrip(likePattern(utf8Slice("a_b%cX%X_"), utf8Slice("/")), LIKE_PATTERN, (left, right) -> left.getPattern().equals(right.getPattern()));
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

    private void assertEncode(Object value, Type type)
    {
        Expression expression = encoder.toExpression(value, type);
        assertThat(getExpressionType(expression)).isEqualTo(type);
        assertThat(getExpressionValue(expression)).isEqualTo(value);
    }

    /**
     * @deprecated Use {@link #assertEncode} instead.
     */
    @Deprecated
    private void assertEncodeCaseInsensitively(Object value, Type type)
    {
        Expression expression = encoder.toExpression(value, type);
        assertThat(isEffectivelyLiteral(PLANNER_CONTEXT, TEST_SESSION, expression))
                .describedAs("isEffectivelyLiteral returned false for: " + expression)
                .isTrue();
        assertThat(getExpressionType(expression)).isEqualTo(type);
        assertThat(getExpressionValue(expression)).isEqualTo(value);
    }

    private <T> void assertRoundTrip(T value, Type type, BiPredicate<T, T> predicate)
    {
        Expression expression = encoder.toExpression(value, type);
        assertThat(isEffectivelyLiteral(PLANNER_CONTEXT, TEST_SESSION, expression))
                .describedAs("isEffectivelyLiteral returned false for: " + expression)
                .isTrue();
        assertThat(getExpressionType(expression)).isEqualTo(type);
        @SuppressWarnings("unchecked")
        T decodedValue = (T) getExpressionValue(expression);
        assertThat(predicate.test(value, decodedValue)).isTrue();
    }

    private Object getExpressionValue(Expression expression)
    {
        return new IrExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION, getExpressionTypes(expression)).evaluate();
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
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, transactionSession -> {
                    return new IrTypeAnalyzer(PLANNER_CONTEXT).getTypes(transactionSession, TypeProvider.empty(), expression);
                });
    }

    private String literalVarbinary(byte[] value)
    {
        return "%s(%s('%s'))".formatted(serializeResolvedFunction(literalFunction), serializeResolvedFunction(base64Function), Base64.getEncoder().encodeToString(value));
    }

    private static String serializeResolvedFunction(ResolvedFunction function)
    {
        CatalogSchemaFunctionName name = function.toCatalogSchemaFunctionName();
        return "%s.\"%s\".\"%s\"".formatted(name.getCatalogName(), name.getSchemaName(), name.getFunctionName());
    }

    private static Re2JRegexp castVarcharToRe2JRegexp(Slice value)
    {
        return Re2JCastToRegexpFunction.castToRegexp(Integer.MAX_VALUE, 5, false, VarcharType.UNBOUNDED_LENGTH, value);
    }
}
