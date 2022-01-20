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
package io.trino.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolResolver;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionBuilder;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.ExpressionTestUtils.getTypes;
import static io.trino.sql.ExpressionTestUtils.resolveFunctionCalls;
import static io.trino.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.function.Function.identity;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestExpressionInterpreter
{
    private static final int TEST_CHAR_TYPE_LENGTH = 17;
    private static final int TEST_VARCHAR_TYPE_LENGTH = 17;
    private static final TypeProvider SYMBOL_TYPES = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("bound_integer"), INTEGER)
            .put(new Symbol("bound_long"), BIGINT)
            .put(new Symbol("bound_string"), createVarcharType(TEST_VARCHAR_TYPE_LENGTH))
            .put(new Symbol("bound_varbinary"), VarbinaryType.VARBINARY)
            .put(new Symbol("bound_double"), DOUBLE)
            .put(new Symbol("bound_boolean"), BOOLEAN)
            .put(new Symbol("bound_date"), DATE)
            .put(new Symbol("bound_time"), TIME)
            .put(new Symbol("bound_timestamp"), TIMESTAMP_MILLIS)
            .put(new Symbol("bound_pattern"), VARCHAR)
            .put(new Symbol("bound_null_string"), VARCHAR)
            .put(new Symbol("bound_decimal_short"), createDecimalType(5, 2))
            .put(new Symbol("bound_decimal_long"), createDecimalType(23, 3))
            .put(new Symbol("time"), BIGINT) // for testing reserved identifiers
            .put(new Symbol("unbound_integer"), INTEGER)
            .put(new Symbol("unbound_long"), BIGINT)
            .put(new Symbol("unbound_long2"), BIGINT)
            .put(new Symbol("unbound_long3"), BIGINT)
            .put(new Symbol("unbound_char"), createCharType(TEST_CHAR_TYPE_LENGTH))
            .put(new Symbol("unbound_string"), VARCHAR)
            .put(new Symbol("unbound_double"), DOUBLE)
            .put(new Symbol("unbound_boolean"), BOOLEAN)
            .put(new Symbol("unbound_date"), DATE)
            .put(new Symbol("unbound_time"), TIME)
            .put(new Symbol("unbound_timestamp"), TIMESTAMP_MILLIS)
            .put(new Symbol("unbound_interval"), INTERVAL_DAY_TIME)
            .put(new Symbol("unbound_pattern"), VARCHAR)
            .put(new Symbol("unbound_null_string"), VARCHAR)
            .buildOrThrow());

    private static final SymbolResolver INPUTS = symbol -> {
        switch (symbol.getName().toLowerCase(ENGLISH)) {
            case "bound_integer":
                return 1234L;
            case "bound_long":
                return 1234L;
            case "bound_string":
                return utf8Slice("hello");
            case "bound_double":
                return 12.34;
            case "bound_date":
                return new LocalDate(2001, 8, 22).toDateMidnight(UTC).getMillis();
            case "bound_time":
                return new LocalTime(3, 4, 5, 321).toDateTime(new DateTime(0, UTC)).getMillis();
            case "bound_timestamp":
                return scaleEpochMillisToMicros(new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC).getMillis());
            case "bound_pattern":
                return utf8Slice("%el%");
            case "bound_timestamp_with_timezone":
                return SqlTimestampWithTimeZone.newInstance(3, new DateTime(1970, 1, 1, 1, 0, 0, 999, UTC).getMillis(), 0, getTimeZoneKey("Z"));
            case "bound_varbinary":
                return Slices.wrappedBuffer((byte) 0xab);
            case "bound_decimal_short":
                return 12345L;
            case "bound_decimal_long":
                return Int128.valueOf("12345678901234567890123");
        }

        return symbol.toSymbolReference();
    };

    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testAnd()
    {
        assertOptimizedEquals("true AND false", "false");
        assertOptimizedEquals("false AND true", "false");
        assertOptimizedEquals("false AND false", "false");

        assertOptimizedEquals("true AND NULL", "NULL");
        assertOptimizedEquals("false AND NULL", "false");
        assertOptimizedEquals("NULL AND true", "NULL");
        assertOptimizedEquals("NULL AND false", "false");
        assertOptimizedEquals("NULL AND NULL", "NULL");

        assertOptimizedEquals("unbound_string='z' AND true", "unbound_string='z'");
        assertOptimizedEquals("unbound_string='z' AND false", "false");
        assertOptimizedEquals("true AND unbound_string='z'", "unbound_string='z'");
        assertOptimizedEquals("false AND unbound_string='z'", "false");

        assertOptimizedEquals("bound_string='z' AND bound_long=1+1", "bound_string='z' AND bound_long=2");
    }

    @Test
    public void testOr()
    {
        assertOptimizedEquals("true OR true", "true");
        assertOptimizedEquals("true OR false", "true");
        assertOptimizedEquals("false OR true", "true");
        assertOptimizedEquals("false OR false", "false");

        assertOptimizedEquals("true OR NULL", "true");
        assertOptimizedEquals("NULL OR true", "true");
        assertOptimizedEquals("NULL OR NULL", "NULL");

        assertOptimizedEquals("false OR NULL", "NULL");
        assertOptimizedEquals("NULL OR false", "NULL");

        assertOptimizedEquals("bound_string='z' OR true", "true");
        assertOptimizedEquals("bound_string='z' OR false", "bound_string='z'");
        assertOptimizedEquals("true OR bound_string='z'", "true");
        assertOptimizedEquals("false OR bound_string='z'", "bound_string='z'");

        assertOptimizedEquals("bound_string='z' OR bound_long=1+1", "bound_string='z' OR bound_long=2");
    }

    @Test
    public void testComparison()
    {
        assertOptimizedEquals("NULL = NULL", "NULL");

        assertOptimizedEquals("'a' = 'b'", "false");
        assertOptimizedEquals("'a' = 'a'", "true");
        assertOptimizedEquals("'a' = NULL", "NULL");
        assertOptimizedEquals("NULL = 'a'", "NULL");
        assertOptimizedEquals("bound_integer = 1234", "true");
        assertOptimizedEquals("bound_integer = 12340000000", "false");
        assertOptimizedEquals("bound_long = BIGINT '1234'", "true");
        assertOptimizedEquals("bound_long = 1234", "true");
        assertOptimizedEquals("bound_double = 12.34", "true");
        assertOptimizedEquals("bound_string = 'hello'", "true");
        assertOptimizedEquals("unbound_long = bound_long", "unbound_long = 1234");

        assertOptimizedEquals("10151082135029368 = 10151082135029369", "false");

        assertOptimizedEquals("bound_varbinary = X'a b'", "true");
        assertOptimizedEquals("bound_varbinary = X'a d'", "false");

        assertOptimizedEquals("1.1 = 1.1", "true");
        assertOptimizedEquals("9876543210.9874561203 = 9876543210.9874561203", "true");
        assertOptimizedEquals("bound_decimal_short = 123.45", "true");
        assertOptimizedEquals("bound_decimal_long = 12345678901234567890.123", "true");
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertOptimizedEquals("NULL IS DISTINCT FROM NULL", "false");

        assertOptimizedEquals("3 IS DISTINCT FROM 4", "true");
        assertOptimizedEquals("3 IS DISTINCT FROM BIGINT '4'", "true");
        assertOptimizedEquals("3 IS DISTINCT FROM 4000000000", "true");
        assertOptimizedEquals("3 IS DISTINCT FROM 3", "false");
        assertOptimizedEquals("3 IS DISTINCT FROM NULL", "true");
        assertOptimizedEquals("NULL IS DISTINCT FROM 3", "true");

        assertOptimizedEquals("10151082135029368 IS DISTINCT FROM 10151082135029369", "true");

        assertOptimizedEquals("1.1 IS DISTINCT FROM 1.1", "false");
        assertOptimizedEquals("9876543210.9874561203 IS DISTINCT FROM NULL", "true");
        assertOptimizedEquals("bound_decimal_short IS DISTINCT FROM NULL", "true");
        assertOptimizedEquals("bound_decimal_long IS DISTINCT FROM 12345678901234567890.123", "false");
        assertOptimizedMatches("unbound_integer IS DISTINCT FROM 1", "unbound_integer IS DISTINCT FROM 1");
        assertOptimizedMatches("unbound_integer IS DISTINCT FROM NULL", "unbound_integer IS NOT NULL");
        assertOptimizedMatches("NULL IS DISTINCT FROM unbound_integer", "unbound_integer IS NOT NULL");
    }

    @Test
    public void testIsNull()
    {
        assertOptimizedEquals("NULL IS NULL", "true");
        assertOptimizedEquals("1 IS NULL", "false");
        assertOptimizedEquals("10000000000 IS NULL", "false");
        assertOptimizedEquals("BIGINT '1' IS NULL", "false");
        assertOptimizedEquals("1.0 IS NULL", "false");
        assertOptimizedEquals("'a' IS NULL", "false");
        assertOptimizedEquals("true IS NULL", "false");
        assertOptimizedEquals("NULL+1 IS NULL", "true");
        assertOptimizedEquals("unbound_string IS NULL", "unbound_string IS NULL");
        assertOptimizedEquals("unbound_long+(1+1) IS NULL", "unbound_long+2 IS NULL");
        assertOptimizedEquals("1.1 IS NULL", "false");
        assertOptimizedEquals("9876543210.9874561203 IS NULL", "false");
        assertOptimizedEquals("bound_decimal_short IS NULL", "false");
        assertOptimizedEquals("bound_decimal_long IS NULL", "false");
    }

    @Test
    public void testIsNotNull()
    {
        assertOptimizedEquals("NULL IS NOT NULL", "false");
        assertOptimizedEquals("1 IS NOT NULL", "true");
        assertOptimizedEquals("10000000000 IS NOT NULL", "true");
        assertOptimizedEquals("BIGINT '1' IS NOT NULL", "true");
        assertOptimizedEquals("1.0 IS NOT NULL", "true");
        assertOptimizedEquals("'a' IS NOT NULL", "true");
        assertOptimizedEquals("true IS NOT NULL", "true");
        assertOptimizedEquals("NULL+1 IS NOT NULL", "false");
        assertOptimizedEquals("unbound_string IS NOT NULL", "unbound_string IS NOT NULL");
        assertOptimizedEquals("unbound_long+(1+1) IS NOT NULL", "unbound_long+2 IS NOT NULL");
        assertOptimizedEquals("1.1 IS NOT NULL", "true");
        assertOptimizedEquals("9876543210.9874561203 IS NOT NULL", "true");
        assertOptimizedEquals("bound_decimal_short IS NOT NULL", "true");
        assertOptimizedEquals("bound_decimal_long IS NOT NULL", "true");
    }

    @Test
    public void testNullIf()
    {
        assertOptimizedEquals("nullif(true, true)", "NULL");
        assertOptimizedEquals("nullif(true, false)", "true");
        assertOptimizedEquals("nullif(NULL, false)", "NULL");
        assertOptimizedEquals("nullif(true, NULL)", "true");

        assertOptimizedEquals("nullif('a', 'a')", "NULL");
        assertOptimizedEquals("nullif('a', 'b')", "'a'");
        assertOptimizedEquals("nullif(NULL, 'b')", "NULL");
        assertOptimizedEquals("nullif('a', NULL)", "'a'");

        assertOptimizedEquals("nullif(1, 1)", "NULL");
        assertOptimizedEquals("nullif(1, 2)", "1");
        assertOptimizedEquals("nullif(1, BIGINT '2')", "1");
        assertOptimizedEquals("nullif(1, 20000000000)", "1");
        assertOptimizedEquals("nullif(1.0E0, 1)", "NULL");
        assertOptimizedEquals("nullif(10000000000.0E0, 10000000000)", "NULL");
        assertOptimizedEquals("nullif(1.1E0, 1)", "1.1E0");
        assertOptimizedEquals("nullif(1.1E0, 1.1E0)", "NULL");
        assertOptimizedEquals("nullif(1, 2-1)", "NULL");
        assertOptimizedEquals("nullif(NULL, NULL)", "NULL");
        assertOptimizedEquals("nullif(1, NULL)", "1");
        assertOptimizedEquals("nullif(unbound_long, 1)", "nullif(unbound_long, 1)");
        assertOptimizedEquals("nullif(unbound_long, unbound_long2)", "nullif(unbound_long, unbound_long2)");
        assertOptimizedEquals("nullif(unbound_long, unbound_long2+(1+1))", "nullif(unbound_long, unbound_long2+2)");

        assertOptimizedEquals("nullif(1.1, 1.2)", "1.1");
        assertOptimizedEquals("nullif(9876543210.9874561203, 9876543210.9874561203)", "NULL");
        assertOptimizedEquals("nullif(bound_decimal_short, 123.45)", "NULL");
        assertOptimizedEquals("nullif(bound_decimal_long, 12345678901234567890.123)", "NULL");
        assertOptimizedEquals("nullif(ARRAY[CAST(1 AS bigint)], ARRAY[CAST(1 AS bigint)]) IS NULL", "true");
        assertOptimizedEquals("nullif(ARRAY[CAST(1 AS bigint)], ARRAY[CAST(NULL AS bigint)]) IS NULL", "false");
        assertOptimizedEquals("nullif(ARRAY[CAST(NULL AS bigint)], ARRAY[CAST(NULL AS bigint)]) IS NULL", "false");
    }

    @Test
    public void testNegative()
    {
        assertOptimizedEquals("-(1)", "-1");
        assertOptimizedEquals("-(BIGINT '1')", "BIGINT '-1'");
        assertOptimizedEquals("-(unbound_long+1)", "-(unbound_long+1)");
        assertOptimizedEquals("-(1+1)", "-2");
        assertOptimizedEquals("-(1+ BIGINT '1')", "BIGINT '-2'");
        assertOptimizedEquals("-(CAST(NULL AS bigint))", "NULL");
        assertOptimizedEquals("-(unbound_long+(1+1))", "-(unbound_long+2)");
        assertOptimizedEquals("-(1.1+1.2)", "-2.3");
        assertOptimizedEquals("-(9876543210.9874561203-9876543210.9874561203)", "CAST(0 AS decimal(20,10))");
        assertOptimizedEquals("-(bound_decimal_short+123.45)", "-246.90");
        assertOptimizedEquals("-(bound_decimal_long-12345678901234567890.123)", "CAST(0 AS decimal(20,10))");
    }

    @Test
    public void testArithmeticUnary()
    {
        assertOptimizedEquals("-rand()", "-rand()");
        assertOptimizedEquals("-(0 / 0)", "-(0 / 0)");
        assertOptimizedEquals("-(-(0 / 0))", "0 / 0");
        assertOptimizedEquals("-(-(-(0 / 0)))", "-(0 / 0)");
        assertOptimizedEquals("+rand()", "rand()");
        assertOptimizedEquals("+(0 / 0)", "0 / 0");
        assertOptimizedEquals("+++(0 / 0)", "0 / 0");
    }

    @Test
    public void testNot()
    {
        assertOptimizedEquals("not true", "false");
        assertOptimizedEquals("not false", "true");
        assertOptimizedEquals("not NULL", "NULL");
        assertOptimizedEquals("not 1=1", "false");
        assertOptimizedEquals("not 1=BIGINT '1'", "false");
        assertOptimizedEquals("not 1!=1", "true");
        assertOptimizedEquals("not unbound_long=1", "not unbound_long=1");
        assertOptimizedEquals("not unbound_long=(1+1)", "not unbound_long=2");
    }

    @Test
    public void testFunctionCall()
    {
        assertOptimizedEquals("abs(-5)", "5");
        assertOptimizedEquals("abs(-10-5)", "15");
        assertOptimizedEquals("abs(-bound_integer + 1)", "1233");
        assertOptimizedEquals("abs(-bound_long + 1)", "1233");
        assertOptimizedEquals("abs(-bound_long + BIGINT '1')", "1233");
        assertOptimizedEquals("abs(-bound_long)", "1234");
        assertOptimizedEquals("abs(unbound_long)", "abs(unbound_long)");
        assertOptimizedEquals("abs(unbound_long + 1)", "abs(unbound_long + 1)");
    }

    @Test
    public void testNonDeterministicFunctionCall()
    {
        // optimize should do nothing
        assertOptimizedEquals("random()", "random()");

        // evaluate should execute
        Object value = evaluate("random()");
        assertTrue(value instanceof Double);
        double randomValue = (double) value;
        assertTrue(0 <= randomValue && randomValue < 1);
    }

    @Test
    public void testBetween()
    {
        assertOptimizedEquals("3 BETWEEN 2 AND 4", "true");
        assertOptimizedEquals("2 BETWEEN 3 AND 4", "false");
        assertOptimizedEquals("NULL BETWEEN 2 AND 4", "NULL");
        assertOptimizedEquals("3 BETWEEN NULL AND 4", "NULL");
        assertOptimizedEquals("3 BETWEEN 2 AND NULL", "NULL");

        assertOptimizedEquals("'cc' BETWEEN 'b' AND 'd'", "true");
        assertOptimizedEquals("'b' BETWEEN 'cc' AND 'd'", "false");
        assertOptimizedEquals("NULL BETWEEN 'b' AND 'd'", "NULL");
        assertOptimizedEquals("'cc' BETWEEN NULL AND 'd'", "NULL");
        assertOptimizedEquals("'cc' BETWEEN 'b' AND NULL", "NULL");

        assertOptimizedEquals("bound_integer BETWEEN 1000 AND 2000", "true");
        assertOptimizedEquals("bound_integer BETWEEN 3 AND 4", "false");
        assertOptimizedEquals("bound_long BETWEEN 1000 AND 2000", "true");
        assertOptimizedEquals("bound_long BETWEEN 3 AND 4", "false");
        assertOptimizedEquals("bound_long BETWEEN bound_integer AND (bound_long + 1)", "true");
        assertOptimizedEquals("bound_string BETWEEN 'e' AND 'i'", "true");
        assertOptimizedEquals("bound_string BETWEEN 'a' AND 'b'", "false");

        assertOptimizedEquals("bound_long BETWEEN unbound_long AND 2000 + 1", "1234 BETWEEN unbound_long AND 2001");
        assertOptimizedEquals(
                "bound_string BETWEEN unbound_string AND 'bar'",
                format("CAST('hello' AS varchar(%s)) BETWEEN unbound_string AND 'bar'", TEST_VARCHAR_TYPE_LENGTH));

        assertOptimizedEquals("1.15 BETWEEN 1.1 AND 1.2", "true");
        assertOptimizedEquals("9876543210.98745612035 BETWEEN 9876543210.9874561203 AND 9876543210.9874561204", "true");
        assertOptimizedEquals("123.455 BETWEEN bound_decimal_short AND 123.46", "true");
        assertOptimizedEquals("12345678901234567890.1235 BETWEEN bound_decimal_long AND 12345678901234567890.123", "false");
    }

    @Test
    public void testExtract()
    {
        DateTime dateTime = new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC);
        double seconds = dateTime.getMillis() / 1000.0;

        assertOptimizedEquals("extract(YEAR FROM from_unixtime(" + seconds + ",'UTC'))", "2001");
        assertOptimizedEquals("extract(QUARTER FROM from_unixtime(" + seconds + ",'UTC'))", "3");
        assertOptimizedEquals("extract(MONTH FROM from_unixtime(" + seconds + ",'UTC'))", "8");
        assertOptimizedEquals("extract(WEEK FROM from_unixtime(" + seconds + ",'UTC'))", "34");
        assertOptimizedEquals("extract(DOW FROM from_unixtime(" + seconds + ",'UTC'))", "3");
        assertOptimizedEquals("extract(DOY FROM from_unixtime(" + seconds + ",'UTC'))", "234");
        assertOptimizedEquals("extract(DAY FROM from_unixtime(" + seconds + ",'UTC'))", "22");
        assertOptimizedEquals("extract(HOUR FROM from_unixtime(" + seconds + ",'UTC'))", "3");
        assertOptimizedEquals("extract(MINUTE FROM from_unixtime(" + seconds + ",'UTC'))", "4");
        assertOptimizedEquals("extract(SECOND FROM from_unixtime(" + seconds + ",'UTC'))", "5");
        assertOptimizedEquals("extract(TIMEZONE_HOUR FROM from_unixtime(" + seconds + ", 7, 9))", "7");
        assertOptimizedEquals("extract(TIMEZONE_MINUTE FROM from_unixtime(" + seconds + ", 7, 9))", "9");

        assertOptimizedEquals("extract(YEAR FROM bound_timestamp)", "2001");
        assertOptimizedEquals("extract(QUARTER FROM bound_timestamp)", "3");
        assertOptimizedEquals("extract(MONTH FROM bound_timestamp)", "8");
        assertOptimizedEquals("extract(WEEK FROM bound_timestamp)", "34");
        assertOptimizedEquals("extract(DOW FROM bound_timestamp)", "3");
        assertOptimizedEquals("extract(DOY FROM bound_timestamp)", "234");
        assertOptimizedEquals("extract(DAY FROM bound_timestamp)", "22");
        assertOptimizedEquals("extract(HOUR FROM bound_timestamp)", "3");
        assertOptimizedEquals("extract(MINUTE FROM bound_timestamp)", "4");
        assertOptimizedEquals("extract(SECOND FROM bound_timestamp)", "5");
        // todo reenable when cast as timestamp with time zone is implemented
        // todo add bound timestamp with time zone
        //assertOptimizedEquals("extract(TIMEZONE_HOUR FROM bound_timestamp)", "0");
        //assertOptimizedEquals("extract(TIMEZONE_MINUTE FROM bound_timestamp)", "0");

        assertOptimizedEquals("extract(YEAR FROM unbound_timestamp)", "extract(YEAR FROM unbound_timestamp)");
        assertOptimizedEquals("extract(SECOND FROM bound_timestamp + INTERVAL '3' SECOND)", "8");
    }

    @Test
    public void testIn()
    {
        assertOptimizedEquals("3 IN (2, 4, 3, 5)", "true");
        assertOptimizedEquals("3 IN (2, 4, 9, 5)", "false");
        assertOptimizedEquals("3 IN (2, NULL, 3, 5)", "true");

        assertOptimizedEquals("'foo' IN ('bar', 'baz', 'foo', 'blah')", "true");
        assertOptimizedEquals("'foo' IN ('bar', 'baz', 'buz', 'blah')", "false");
        assertOptimizedEquals("'foo' IN ('bar', NULL, 'foo', 'blah')", "true");

        assertOptimizedEquals("NULL IN (2, NULL, 3, 5)", "NULL");
        assertOptimizedEquals("3 IN (2, NULL)", "NULL");

        assertOptimizedEquals("bound_integer IN (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("bound_integer IN (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 IN (2, bound_integer, 3, 5)", "true");
        assertOptimizedEquals("99 IN (2, bound_integer, 3, 5)", "false");
        assertOptimizedEquals("bound_integer IN (2, bound_integer, 3, 5)", "true");

        assertOptimizedEquals("bound_long IN (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("bound_long IN (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 IN (2, bound_long, 3, 5)", "true");
        assertOptimizedEquals("99 IN (2, bound_long, 3, 5)", "false");
        assertOptimizedEquals("bound_long IN (2, bound_long, 3, 5)", "true");

        assertOptimizedEquals("bound_string IN ('bar', 'hello', 'foo', 'blah')", "true");
        assertOptimizedEquals("bound_string IN ('bar', 'baz', 'foo', 'blah')", "false");
        assertOptimizedEquals("'hello' IN ('bar', bound_string, 'foo', 'blah')", "true");
        assertOptimizedEquals("'baz' IN ('bar', bound_string, 'foo', 'blah')", "false");

        assertOptimizedEquals("bound_long IN (2, 1234, unbound_long, 5)", "true");
        assertOptimizedEquals("bound_string IN ('bar', 'hello', unbound_string, 'blah')", "true");

        assertOptimizedEquals("bound_long IN (2, 4, unbound_long, unbound_long2, 9)", "1234 IN (unbound_long, unbound_long2)");
        assertOptimizedEquals("unbound_long IN (2, 4, bound_long, unbound_long2, 5)", "unbound_long IN (2, 4, 1234, unbound_long2, 5)");

        assertOptimizedEquals("1.15 IN (1.1, 1.2, 1.3, 1.15)", "true");
        assertOptimizedEquals("9876543210.98745612035 IN (9876543210.9874561203, 9876543210.9874561204, 9876543210.98745612035)", "true");
        assertOptimizedEquals("bound_decimal_short IN (123.455, 123.46, 123.45)", "true");
        assertOptimizedEquals("bound_decimal_long IN (12345678901234567890.123, 9876543210.9874561204, 9876543210.98745612035)", "true");
        assertOptimizedEquals("bound_decimal_long IN (9876543210.9874561204, NULL, 9876543210.98745612035)", "NULL");

        assertOptimizedEquals("unbound_integer IN (1)", "unbound_integer = 1");
        assertOptimizedEquals("unbound_long IN (unbound_long2)", "unbound_long = unbound_long2");

        assertOptimizedEquals("3 in (2, 4, 3, 5 / 0)", "3 in (2, 4, 3, 5 / 0)");
        assertOptimizedEquals("null in (2, 4, 3, 5 / 0)", "null in (2, 4, 3, 5 / 0)");
        assertOptimizedEquals("3 in (2, 4, 3, null, 5 / 0)", "3 in (2, 4, 3, null, 5 / 0)");
        assertOptimizedEquals("null in (2, 4, null, 5 / 0)", "null in (2, 4, null, 5 / 0)");
        assertOptimizedEquals("3 in (5 / 0, 5 / 0)", "3 in (5 / 0, 5 / 0)");
        assertTrinoExceptionThrownBy(() -> evaluate("3 in (2, 4, 3, 5 / 0)"))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertOptimizedEquals("0 / 0 in (2, 4, 3, 5)", "0 / 0 in (2, 4, 3, 5)");
        assertOptimizedEquals("0 / 0 in (2, 4, 2, 4)", "0 / 0 in (2, 4)");
        assertOptimizedEquals("0 / 0 in (rand(), 2, 4)", "0 / 0 in (2, 4, rand())");
        assertOptimizedEquals("0 / 0 in (2, 2)", "0 / 0 = 2");
    }

    @Test
    public void testInComplexTypes()
    {
        assertEvaluatedEquals("ARRAY[1] IN (ARRAY[1])", "true");
        assertEvaluatedEquals("ARRAY[1] IN (ARRAY[2])", "false");
        assertEvaluatedEquals("ARRAY[1] IN (ARRAY[2], ARRAY[1])", "true");
        assertEvaluatedEquals("ARRAY[1] IN (NULL)", "NULL");
        assertEvaluatedEquals("ARRAY[1] IN (NULL, ARRAY[1])", "true");
        assertEvaluatedEquals("ARRAY[1, 2, NULL] IN (ARRAY[2, NULL], ARRAY[1, NULL])", "false");
        assertEvaluatedEquals("ARRAY[1, NULL] IN (ARRAY[2, NULL], NULL)", "NULL");
        assertEvaluatedEquals("ARRAY[NULL] IN (ARRAY[NULL])", "NULL");
        assertEvaluatedEquals("ARRAY[1] IN (ARRAY[NULL])", "NULL");
        assertEvaluatedEquals("ARRAY[NULL] IN (ARRAY[1])", "NULL");
        assertEvaluatedEquals("ARRAY[1, NULL] IN (ARRAY[1, NULL])", "NULL");
        assertEvaluatedEquals("ARRAY[1, NULL] IN (ARRAY[2, NULL])", "false");
        assertEvaluatedEquals("ARRAY[1, NULL] IN (ARRAY[1, NULL], ARRAY[2, NULL])", "NULL");
        assertEvaluatedEquals("ARRAY[1, NULL] IN (ARRAY[1, NULL], ARRAY[2, NULL], ARRAY[1, NULL])", "NULL");
        assertEvaluatedEquals("ARRAY[ARRAY[1, 2], ARRAY[3, 4]] in (ARRAY[ARRAY[1, 2], ARRAY[3, NULL]])", "NULL");

        assertEvaluatedEquals("ROW(1) IN (ROW(1))", "true");
        assertEvaluatedEquals("ROW(1) IN (ROW(2))", "false");
        assertEvaluatedEquals("ROW(1) IN (ROW(2), ROW(1), ROW(2))", "true");
        assertEvaluatedEquals("ROW(1) IN (NULL)", "NULL");
        assertEvaluatedEquals("ROW(1) IN (NULL, ROW(1))", "true");
        assertEvaluatedEquals("ROW(1, NULL) IN (ROW(2, NULL), NULL)", "NULL");
        assertEvaluatedEquals("ROW(NULL) IN (ROW(NULL))", "NULL");
        assertEvaluatedEquals("ROW(1) IN (ROW(NULL))", "NULL");
        assertEvaluatedEquals("ROW(NULL) IN (ROW(1))", "NULL");
        assertEvaluatedEquals("ROW(1, NULL) IN (ROW(1, NULL))", "NULL");
        assertEvaluatedEquals("ROW(1, NULL) IN (ROW(2, NULL))", "false");
        assertEvaluatedEquals("ROW(1, NULL) IN (ROW(1, NULL), ROW(2, NULL))", "NULL");
        assertEvaluatedEquals("ROW(1, NULL) IN (ROW(1, NULL), ROW(2, NULL), ROW(1, NULL))", "NULL");

        assertEvaluatedEquals("map(ARRAY[1], ARRAY[1]) IN (map(ARRAY[1], ARRAY[1]))", "true");
        assertEvaluatedEquals("map(ARRAY[1], ARRAY[1]) IN (NULL)", "NULL");
        assertEvaluatedEquals("map(ARRAY[1], ARRAY[1]) IN (NULL, map(ARRAY[1], ARRAY[1]))", "true");
        assertEvaluatedEquals("map(ARRAY[1], ARRAY[1]) IN (map(ARRAY[1, 2], ARRAY[1, NULL]))", "false");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 2], ARRAY[2, NULL]), NULL)", "NULL");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 2], ARRAY[1, NULL]))", "NULL");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 3], ARRAY[1, NULL]))", "false");
        assertEvaluatedEquals("map(ARRAY[1], ARRAY[NULL]) IN (map(ARRAY[1], ARRAY[NULL]))", "NULL");
        assertEvaluatedEquals("map(ARRAY[1], ARRAY[1]) IN (map(ARRAY[1], ARRAY[NULL]))", "NULL");
        assertEvaluatedEquals("map(ARRAY[1], ARRAY[NULL]) IN (map(ARRAY[1], ARRAY[1]))", "NULL");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 2], ARRAY[1, NULL]))", "NULL");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 3], ARRAY[1, NULL]))", "false");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 2], ARRAY[2, NULL]))", "false");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 2], ARRAY[1, NULL]), map(ARRAY[1, 2], ARRAY[2, NULL]))", "NULL");
        assertEvaluatedEquals("map(ARRAY[1, 2], ARRAY[1, NULL]) IN (map(ARRAY[1, 2], ARRAY[1, NULL]), map(ARRAY[1, 2], ARRAY[2, NULL]), map(ARRAY[1, 2], ARRAY[1, NULL]))", "NULL");
    }

    @Test
    public void testDereference()
    {
        assertOptimizedEquals("CAST(ROW(1, true) AS ROW(id BIGINT, value BOOLEAN)).value", "true");
        assertOptimizedEquals("CAST(ROW(1, null) AS ROW(id BIGINT, value BOOLEAN)).value", "null");
        assertOptimizedEquals("CAST(ROW(0 / 0, true) AS ROW(id DOUBLE, value BOOLEAN)).value", "CAST(ROW(0 / 0, true) AS ROW(id DOUBLE, value BOOLEAN)).value");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(ROW(0 / 0, true) AS ROW(id DOUBLE, value BOOLEAN)).value"))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testCurrentUser()
    {
        assertOptimizedEquals("current_user", "'" + TEST_SESSION.getUser() + "'");
    }

    @Test
    public void testCastToString()
    {
        // integer
        assertOptimizedEquals("CAST(123 AS varchar(20))", "'123'");
        assertOptimizedEquals("CAST(-123 AS varchar(20))", "'-123'");

        // bigint
        assertOptimizedEquals("CAST(BIGINT '123' AS varchar)", "'123'");
        assertOptimizedEquals("CAST(12300000000 AS varchar)", "'12300000000'");
        assertOptimizedEquals("CAST(-12300000000 AS varchar)", "'-12300000000'");

        // double
        assertOptimizedEquals("CAST(123.0E0 AS varchar)", "'123.0'");
        assertOptimizedEquals("CAST(-123.0E0 AS varchar)", "'-123.0'");
        assertOptimizedEquals("CAST(123.456E0 AS varchar)", "'123.456'");
        assertOptimizedEquals("CAST(-123.456E0 AS varchar)", "'-123.456'");

        // boolean
        assertOptimizedEquals("CAST(true AS varchar)", "'true'");
        assertOptimizedEquals("CAST(false AS varchar)", "'false'");

        // string
        assertOptimizedEquals("VARCHAR 'xyz'", "'xyz'");

        // NULL
        assertOptimizedEquals("CAST(NULL AS varchar)", "NULL");

        // decimal
        assertOptimizedEquals("CAST(1.1 AS varchar)", "'1.1'");
        // TODO enabled when DECIMAL is default for literal: assertOptimizedEquals("CAST(12345678901234567890.123 AS varchar)", "'12345678901234567890.123'");
    }

    @Test
    public void testCastBigintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(12300000000 AS varchar(11))", "'12300000000'");
        assertEvaluatedEquals("CAST(12300000000 AS varchar(50))", "'12300000000'");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(12300000000 AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 12300000000 cannot be represented as varchar(3)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(-12300000000 AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -12300000000 cannot be represented as varchar(3)");
    }

    @Test
    public void testCastIntegerToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(1234 AS varchar(4))", "'1234'");
        assertEvaluatedEquals("CAST(1234 AS varchar(50))", "'1234'");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(1234 AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 1234 cannot be represented as varchar(3)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(-1234 AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -1234 cannot be represented as varchar(3)");
    }

    @Test
    public void testCastSmallintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(SMALLINT '1234' AS varchar(4))", "'1234'");
        assertEvaluatedEquals("CAST(SMALLINT '1234' AS varchar(50))", "'1234'");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(SMALLINT '1234' AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 1234 cannot be represented as varchar(3)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(SMALLINT '-1234' AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -1234 cannot be represented as varchar(3)");
    }

    @Test
    public void testCastTinyintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(TINYINT '123' AS varchar(3))", "'123'");
        assertEvaluatedEquals("CAST(TINYINT '123' AS varchar(50))", "'123'");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(TINYINT '123' AS varchar(2))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 123 cannot be represented as varchar(2)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(TINYINT '-123' AS varchar(2))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -123 cannot be represented as varchar(2)");
    }

    @Test
    public void testCastDecimalToBoundedVarchar()
    {
        // short decimal
        assertEvaluatedEquals("CAST(DECIMAL '12.4' AS varchar(4))", "'12.4'");
        assertEvaluatedEquals("CAST(DECIMAL '12.4' AS varchar(50))", "'12.4'");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '12.4' AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 12.4 cannot be represented as varchar(3)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '-12.4' AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -12.4 cannot be represented as varchar(3)");

        // the trailing 0 does not fit in the type
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '12.40' AS varchar(4))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 12.40 cannot be represented as varchar(4)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '-12.40' AS varchar(5))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -12.40 cannot be represented as varchar(5)");

        // long decimal
        assertEvaluatedEquals("CAST(DECIMAL '100000000000000000.1' AS varchar(20))", "'100000000000000000.1'");
        assertEvaluatedEquals("CAST(DECIMAL '100000000000000000.1' AS varchar(50))", "'100000000000000000.1'");

        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '100000000000000000.1' AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 100000000000000000.1 cannot be represented as varchar(3)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '-100000000000000000.1' AS varchar(3))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -100000000000000000.1 cannot be represented as varchar(3)");

        // the trailing 0 does not fit in the type
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '100000000000000000.10' AS varchar(20))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 100000000000000000.10 cannot be represented as varchar(20)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DECIMAL '-100000000000000000.10' AS varchar(21))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -100000000000000000.10 cannot be represented as varchar(21)");
    }

    @Test
    public void testCastDoubleToBoundedVarchar()
    {
        // NaN
        assertEvaluatedEquals("CAST(0e0 / 0e0 AS varchar(3))", "'NaN'");
        assertEvaluatedEquals("CAST(0e0 / 0e0 AS varchar(50))", "'NaN'");

        // Infinity
        assertEvaluatedEquals("CAST(DOUBLE 'Infinity' AS varchar(8))", "'Infinity'");
        assertEvaluatedEquals("CAST(DOUBLE 'Infinity' AS varchar(50))", "'Infinity'");

        // incorrect behavior: the string representation is not compliant with the SQL standard
        assertEvaluatedEquals("CAST(0e0 AS varchar(3))", "'0.0'");
        assertEvaluatedEquals("CAST(DOUBLE '0' AS varchar(3))", "'0.0'");
        assertEvaluatedEquals("CAST(DOUBLE '-0' AS varchar(4))", "'-0.0'");
        assertEvaluatedEquals("CAST(DOUBLE '0' AS varchar(50))", "'0.0'");

        assertEvaluatedEquals("CAST(12e0 AS varchar(4))", "'12.0'");
        assertEvaluatedEquals("CAST(12e2 AS varchar(6))", "'1200.0'");
        assertEvaluatedEquals("CAST(12e-2 AS varchar(4))", "'0.12'");

        assertEvaluatedEquals("CAST(12e0 AS varchar(50))", "'12.0'");
        assertEvaluatedEquals("CAST(12e2 AS varchar(50))", "'1200.0'");
        assertEvaluatedEquals("CAST(12e-2 AS varchar(50))", "'0.12'");

        assertEvaluatedEquals("CAST(-12e0 AS varchar(5))", "'-12.0'");
        assertEvaluatedEquals("CAST(-12e2 AS varchar(7))", "'-1200.0'");
        assertEvaluatedEquals("CAST(-12e-2 AS varchar(5))", "'-0.12'");

        assertEvaluatedEquals("CAST(-12e0 AS varchar(50))", "'-12.0'");
        assertEvaluatedEquals("CAST(-12e2 AS varchar(50))", "'-1200.0'");
        assertEvaluatedEquals("CAST(-12e-2 AS varchar(50))", "'-0.12'");

        // the string representation is compliant with the SQL standard
        assertEvaluatedEquals("CAST(12345678.9e0 AS varchar(12))", "'1.23456789E7'");
        assertEvaluatedEquals("CAST(0.00001e0 AS varchar(6))", "'1.0E-5'");

        // the result value does not fit in the type (also, it is not compliant with the SQL standard)
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(12e0 AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 12.0 cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(-12e2 AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -1200.0 cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(0e0 AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 0.0 cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(0e0 / 0e0 AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value NaN cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DOUBLE 'Infinity' AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value Infinity cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(1200000e0 AS varchar(5))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 1200000.0 cannot be represented as varchar(5)");
    }

    @Test
    public void testCastRealToBoundedVarchar()
    {
        // NaN
        assertEvaluatedEquals("CAST(REAL '0e0' / REAL '0e0' AS varchar(3))", "'NaN'");
        assertEvaluatedEquals("CAST(REAL '0e0' / REAL '0e0' AS varchar(50))", "'NaN'");

        // Infinity
        assertEvaluatedEquals("CAST(REAL 'Infinity' AS varchar(8))", "'Infinity'");
        assertEvaluatedEquals("CAST(REAL 'Infinity' AS varchar(50))", "'Infinity'");

        // incorrect behavior: the string representation is not compliant with the SQL standard
        assertEvaluatedEquals("CAST(REAL '0' AS varchar(3))", "'0.0'");
        assertEvaluatedEquals("CAST(REAL '-0' AS varchar(4))", "'-0.0'");
        assertEvaluatedEquals("CAST(REAL '0' AS varchar(50))", "'0.0'");

        assertEvaluatedEquals("CAST(REAL '12' AS varchar(4))", "'12.0'");
        assertEvaluatedEquals("CAST(REAL '12e2' AS varchar(6))", "'1200.0'");
        assertEvaluatedEquals("CAST(REAL '12e-2' AS varchar(4))", "'0.12'");

        assertEvaluatedEquals("CAST(REAL '12' AS varchar(50))", "'12.0'");
        assertEvaluatedEquals("CAST(REAL '12e2' AS varchar(50))", "'1200.0'");
        assertEvaluatedEquals("CAST(REAL '12e-2' AS varchar(50))", "'0.12'");

        assertEvaluatedEquals("CAST(REAL '-12' AS varchar(5))", "'-12.0'");
        assertEvaluatedEquals("CAST(REAL '-12e2' AS varchar(7))", "'-1200.0'");
        assertEvaluatedEquals("CAST(REAL '-12e-2' AS varchar(5))", "'-0.12'");

        assertEvaluatedEquals("CAST(REAL '-12' AS varchar(50))", "'-12.0'");
        assertEvaluatedEquals("CAST(REAL '-12e2' AS varchar(50))", "'-1200.0'");
        assertEvaluatedEquals("CAST(REAL '-12e-2' AS varchar(50))", "'-0.12'");

        // the string representation is compliant with the SQL standard
        assertEvaluatedEquals("CAST(REAL '12345678.9e0' AS varchar(12))", "'1.2345679E7'");
        assertEvaluatedEquals("CAST(REAL '0.00001e0' AS varchar(6))", "'1.0E-5'");

        // the result value does not fit in the type (also, it is not compliant with the SQL standard)
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(REAL '12' AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 12.0 cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(REAL '-12e2' AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -1200.0 cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(REAL '0' AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 0.0 cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(REAL '0e0' / REAL '0e0' AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value NaN cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(REAL 'Infinity' AS varchar(1))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value Infinity cannot be represented as varchar(1)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(REAL '1200000' AS varchar(5))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 1200000.0 cannot be represented as varchar(5)");
    }

    @Test
    public void testCastDateToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(DATE '2013-02-02' AS varchar(10))", "'2013-02-02'");
        // according to the SQL standard, this literal is incorrect. Year should be unsigned. https://github.com/trinodb/trino/issues/10677
        assertEvaluatedEquals("CAST(DATE '-2013-02-02' AS varchar(50))", "'-2013-02-02'");

        // the result value does not fit in the type
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DATE '2013-02-02' AS varchar(9))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value 2013-02-02 cannot be represented as varchar(9)");
        assertTrinoExceptionThrownBy(() -> evaluate("CAST(DATE '-2013-02-02' AS varchar(9))"))
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value -2013-02-02 cannot be represented as varchar(9)");
    }

    @Test
    public void testCastToBoolean()
    {
        // integer
        assertOptimizedEquals("CAST(123 AS boolean)", "true");
        assertOptimizedEquals("CAST(-123 AS boolean)", "true");
        assertOptimizedEquals("CAST(0 AS boolean)", "false");

        // bigint
        assertOptimizedEquals("CAST(12300000000 AS boolean)", "true");
        assertOptimizedEquals("CAST(-12300000000 AS boolean)", "true");
        assertOptimizedEquals("CAST(BIGINT '0' AS boolean)", "false");

        // boolean
        assertOptimizedEquals("CAST(true AS boolean)", "true");
        assertOptimizedEquals("CAST(false AS boolean)", "false");

        // string
        assertOptimizedEquals("CAST('true' AS boolean)", "true");
        assertOptimizedEquals("CAST('false' AS boolean)", "false");
        assertOptimizedEquals("CAST('t' AS boolean)", "true");
        assertOptimizedEquals("CAST('f' AS boolean)", "false");
        assertOptimizedEquals("CAST('1' AS boolean)", "true");
        assertOptimizedEquals("CAST('0' AS boolean)", "false");

        // NULL
        assertOptimizedEquals("CAST(NULL AS boolean)", "NULL");

        // double
        assertOptimizedEquals("CAST(123.45E0 AS boolean)", "true");
        assertOptimizedEquals("CAST(-123.45E0 AS boolean)", "true");
        assertOptimizedEquals("CAST(0.0E0 AS boolean)", "false");

        // decimal
        assertOptimizedEquals("CAST(0.00 AS boolean)", "false");
        assertOptimizedEquals("CAST(7.8 AS boolean)", "true");
        assertOptimizedEquals("CAST(12345678901234567890.123 AS boolean)", "true");
        assertOptimizedEquals("CAST(00000000000000000000.000 AS boolean)", "false");
    }

    @Test
    public void testCastToBigint()
    {
        // integer
        assertOptimizedEquals("CAST(0 AS bigint)", "0");
        assertOptimizedEquals("CAST(123 AS bigint)", "123");
        assertOptimizedEquals("CAST(-123 AS bigint)", "-123");

        // bigint
        assertOptimizedEquals("CAST(BIGINT '0' AS bigint)", "0");
        assertOptimizedEquals("CAST(BIGINT '123' AS bigint)", "123");
        assertOptimizedEquals("CAST(BIGINT '-123' AS bigint)", "-123");

        // double
        assertOptimizedEquals("CAST(123.0E0 AS bigint)", "123");
        assertOptimizedEquals("CAST(-123.0E0 AS bigint)", "-123");
        assertOptimizedEquals("CAST(123.456E0 AS bigint)", "123");
        assertOptimizedEquals("CAST(-123.456E0 AS bigint)", "-123");

        // boolean
        assertOptimizedEquals("CAST(true AS bigint)", "1");
        assertOptimizedEquals("CAST(false AS bigint)", "0");

        // string
        assertOptimizedEquals("CAST('123' AS bigint)", "123");
        assertOptimizedEquals("CAST('-123' AS bigint)", "-123");

        // NULL
        assertOptimizedEquals("CAST(NULL AS bigint)", "NULL");

        // decimal
        assertOptimizedEquals("CAST(DECIMAL '1.01' AS bigint)", "1");
        assertOptimizedEquals("CAST(DECIMAL '7.8' AS bigint)", "8");
        assertOptimizedEquals("CAST(DECIMAL '1234567890.123' AS bigint)", "1234567890");
        assertOptimizedEquals("CAST(DECIMAL '00000000000000000000.000' AS bigint)", "0");
    }

    @Test
    public void testCastToInteger()
    {
        // integer
        assertOptimizedEquals("CAST(0 AS integer)", "0");
        assertOptimizedEquals("CAST(123 AS integer)", "123");
        assertOptimizedEquals("CAST(-123 AS integer)", "-123");

        // bigint
        assertOptimizedEquals("CAST(BIGINT '0' AS integer)", "0");
        assertOptimizedEquals("CAST(BIGINT '123' AS integer)", "123");
        assertOptimizedEquals("CAST(BIGINT '-123' AS integer)", "-123");

        // double
        assertOptimizedEquals("CAST(123.0E0 AS integer)", "123");
        assertOptimizedEquals("CAST(-123.0E0 AS integer)", "-123");
        assertOptimizedEquals("CAST(123.456E0 AS integer)", "123");
        assertOptimizedEquals("CAST(-123.456E0 AS integer)", "-123");

        // boolean
        assertOptimizedEquals("CAST(true AS integer)", "1");
        assertOptimizedEquals("CAST(false AS integer)", "0");

        // string
        assertOptimizedEquals("CAST('123' AS integer)", "123");
        assertOptimizedEquals("CAST('-123' AS integer)", "-123");

        // NULL
        assertOptimizedEquals("CAST(NULL AS integer)", "NULL");
    }

    @Test
    public void testCastToDouble()
    {
        // integer
        assertOptimizedEquals("CAST(0 AS double)", "0.0E0");
        assertOptimizedEquals("CAST(123 AS double)", "123.0E0");
        assertOptimizedEquals("CAST(-123 AS double)", "-123.0E0");

        // bigint
        assertOptimizedEquals("CAST(BIGINT '0' AS double)", "0.0E0");
        assertOptimizedEquals("CAST(12300000000 AS double)", "12300000000.0E0");
        assertOptimizedEquals("CAST(-12300000000 AS double)", "-12300000000.0E0");

        // double
        assertOptimizedEquals("CAST(123.0E0 AS double)", "123.0E0");
        assertOptimizedEquals("CAST(-123.0E0 AS double)", "-123.0E0");
        assertOptimizedEquals("CAST(123.456E0 AS double)", "123.456E0");
        assertOptimizedEquals("CAST(-123.456E0 AS double)", "-123.456E0");

        // string
        assertOptimizedEquals("CAST('0' AS double)", "0.0E0");
        assertOptimizedEquals("CAST('123' AS double)", "123.0E0");
        assertOptimizedEquals("CAST('-123' AS double)", "-123.0E0");
        assertOptimizedEquals("CAST('123.0E0' AS double)", "123.0E0");
        assertOptimizedEquals("CAST('-123.0E0' AS double)", "-123.0E0");
        assertOptimizedEquals("CAST('123.456E0' AS double)", "123.456E0");
        assertOptimizedEquals("CAST('-123.456E0' AS double)", "-123.456E0");

        // NULL
        assertOptimizedEquals("CAST(NULL AS double)", "NULL");

        // boolean
        assertOptimizedEquals("CAST(true AS double)", "1.0E0");
        assertOptimizedEquals("CAST(false AS double)", "0.0E0");

        // decimal
        assertOptimizedEquals("CAST(1.01 AS double)", "DOUBLE '1.01'");
        assertOptimizedEquals("CAST(7.8 AS double)", "DOUBLE '7.8'");
        assertOptimizedEquals("CAST(1234567890.123 AS double)", "DOUBLE '1234567890.123'");
        assertOptimizedEquals("CAST(00000000000000000000.000 AS double)", "DOUBLE '0.0'");
    }

    @Test
    public void testCastToDecimal()
    {
        // long
        assertOptimizedEquals("CAST(0 AS decimal(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("CAST(123 AS decimal(3,0))", "DECIMAL '123'");
        assertOptimizedEquals("CAST(-123 AS decimal(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("CAST(-123 AS decimal(20,10))", "CAST(-123 AS decimal(20,10))");

        // double
        assertOptimizedEquals("CAST(0E0 AS decimal(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("CAST(123.2E0 AS decimal(4,1))", "DECIMAL '123.2'");
        assertOptimizedEquals("CAST(-123.0E0 AS decimal(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("CAST(-123.55E0 AS decimal(20,10))", "CAST(-123.55 AS decimal(20,10))");

        // string
        assertOptimizedEquals("CAST('0' AS decimal(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("CAST('123.2' AS decimal(4,1))", "DECIMAL '123.2'");
        assertOptimizedEquals("CAST('-123.0' AS decimal(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("CAST('-123.55' AS decimal(20,10))", "CAST(-123.55 AS decimal(20,10))");

        // NULL
        assertOptimizedEquals("CAST(NULL AS decimal(1,0))", "NULL");
        assertOptimizedEquals("CAST(NULL AS decimal(20,10))", "NULL");

        // boolean
        assertOptimizedEquals("CAST(true AS decimal(1,0))", "DECIMAL '1'");
        assertOptimizedEquals("CAST(false AS decimal(4,1))", "DECIMAL '000.0'");
        assertOptimizedEquals("CAST(true AS decimal(3,0))", "DECIMAL '001'");
        assertOptimizedEquals("CAST(false AS decimal(20,10))", "CAST(0 AS decimal(20,10))");

        // decimal
        assertOptimizedEquals("CAST(0.0 AS decimal(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("CAST(123.2 AS decimal(4,1))", "DECIMAL '123.2'");
        assertOptimizedEquals("CAST(-123.0 AS decimal(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("CAST(-123.55 AS decimal(20,10))", "CAST(-123.55 AS decimal(20,10))");
    }

    @Test
    public void testCastOptimization()
    {
        assertOptimizedEquals("CAST(bound_integer AS varchar)", "'1234'");
        assertOptimizedEquals("CAST(bound_long AS varchar)", "'1234'");
        assertOptimizedEquals("CAST(bound_integer + 1 AS varchar)", "'1235'");
        assertOptimizedEquals("CAST(bound_long + 1 AS varchar)", "'1235'");
        assertOptimizedEquals("CAST(unbound_string AS varchar)", "CAST(unbound_string AS varchar)");
        assertOptimizedMatches("CAST(unbound_string AS varchar)", "unbound_string");
        assertOptimizedMatches("CAST(unbound_integer AS integer)", "unbound_integer");
        assertOptimizedMatches("CAST(unbound_string AS varchar(10))", "CAST(unbound_string AS varchar(10))");
    }

    @Test
    public void testTryCast()
    {
        assertOptimizedEquals("TRY_CAST(NULL AS bigint)", "NULL");
        assertOptimizedEquals("TRY_CAST(123 AS bigint)", "123");
        assertOptimizedEquals("TRY_CAST(NULL AS integer)", "NULL");
        assertOptimizedEquals("TRY_CAST(123 AS integer)", "123");
        assertOptimizedEquals("TRY_CAST('foo' AS varchar)", "'foo'");
        assertOptimizedEquals("TRY_CAST('foo' AS bigint)", "NULL");
        assertOptimizedEquals("TRY_CAST(unbound_string AS bigint)", "TRY_CAST(unbound_string AS bigint)");
        assertOptimizedEquals("TRY_CAST('foo' AS decimal(2,1))", "NULL");
    }

    @Test
    public void testReservedWithDoubleQuotes()
    {
        assertOptimizedEquals("\"time\"", "\"time\"");
    }

    @Test
    public void testSearchCase()
    {
        assertOptimizedEquals("CASE " +
                        "WHEN true THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 1 " +
                        "ELSE 33 " +
                        "END",
                "33");

        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 10000000000 " +
                        "ELSE 33 " +
                        "END",
                "33");

        assertOptimizedEquals("CASE " +
                        "WHEN bound_long = 1234 THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE " +
                        "WHEN true THEN bound_long " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 1 " +
                        "ELSE bound_long " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE " +
                        "WHEN bound_integer = 1234 THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE " +
                        "WHEN true THEN bound_integer " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 1 " +
                        "ELSE bound_integer " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE " +
                        "WHEN bound_long = 1234 THEN 33 " +
                        "ELSE unbound_long " +
                        "END",
                "33");
        assertOptimizedEquals("CASE " +
                        "WHEN true THEN bound_long " +
                        "ELSE unbound_long " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE " +
                        "WHEN false THEN unbound_long " +
                        "ELSE bound_long " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE " +
                        "WHEN bound_integer = 1234 THEN 33 " +
                        "ELSE unbound_integer " +
                        "END",
                "33");
        assertOptimizedEquals("CASE " +
                        "WHEN true THEN bound_integer " +
                        "ELSE unbound_integer " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE " +
                        "WHEN false THEN unbound_integer " +
                        "ELSE bound_integer " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE " +
                        "WHEN unbound_long = 1234 THEN 33 " +
                        "ELSE 1 " +
                        "END",
                "" +
                        "CASE " +
                        "WHEN unbound_long = 1234 THEN 33 " +
                        "ELSE 1 " +
                        "END");

        assertOptimizedMatches("CASE WHEN 0 / 0 = 0 THEN 1 END",
                "CASE WHEN ((0 / 0) = 0) THEN 1 END");

        assertOptimizedMatches("IF(false, 1, 0 / 0)", "0 / 0");

        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 2.2 " +
                        "WHEN true THEN 2.2 " +
                        "END",
                "2.2");

        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 1234567890.0987654321 " +
                        "WHEN true THEN 3.3 " +
                        "END",
                "CAST(3.3 AS decimal(20,10))");

        assertOptimizedEquals("CASE " +
                        "WHEN false THEN 1 " +
                        "WHEN true THEN 2.2 " +
                        "END",
                "2.2");

        assertOptimizedEquals("CASE WHEN ARRAY[CAST(1 AS bigint)] = ARRAY[CAST(1 AS bigint)] THEN 'matched' ELSE 'not_matched' END", "'matched'");
        assertOptimizedEquals("CASE WHEN ARRAY[CAST(2 AS bigint)] = ARRAY[CAST(1 AS bigint)] THEN 'matched' ELSE 'not_matched' END", "'not_matched'");
        assertOptimizedEquals("CASE WHEN ARRAY[CAST(NULL AS bigint)] = ARRAY[CAST(1 AS bigint)] THEN 'matched' ELSE 'not_matched' END", "'not_matched'");

        assertOptimizedEquals("CASE WHEN 0 / 0 = 0 THEN 'a' ELSE 'b' END", "CASE WHEN 0 / 0 = 0 THEN 'a' ELSE 'b' END");
        assertOptimizedEquals("CASE WHEN true THEN 'a' WHEN 0 / 0 = 0 THEN 'b' ELSE 'c' END", "'a'");
        assertOptimizedEquals("CASE WHEN 0 / 0 = 0 THEN 'a' WHEN true THEN 'b' ELSE 'c' END", "CASE WHEN 0 / 0 = 0 THEN 'a' ELSE 'b' END");
        assertOptimizedEquals("CASE WHEN 0 / 0 = 0 THEN 'a' WHEN false THEN 'b' ELSE 'c' END", "CASE WHEN 0 / 0 = 0 THEN 'a' ELSE 'c' END");
        assertOptimizedEquals("CASE WHEN 0 / 0 = 0 THEN 'a' WHEN false THEN 'b' END", "CASE WHEN 0 / 0 = 0 THEN 'a' END");
        assertOptimizedEquals("CASE WHEN false THEN 'a' WHEN false THEN 'b' ELSE 'c' END", "'c'");
        assertOptimizedEquals("CASE WHEN false THEN 'a' WHEN false THEN 'b' END", "null");
        assertOptimizedEquals("CASE WHEN 0 > 1 THEN 'a' WHEN 1 > 2 THEN 'b' END", "null");
        assertOptimizedEquals("CASE WHEN true THEN 0 / 0 WHEN false THEN 1 END", "0 / 0");
        assertOptimizedEquals("CASE WHEN false THEN 1 WHEN false THEN 2 ELSE 0 / 0 END", "0 / 0");

        assertEvaluatedEquals("CASE WHEN false THEN 0 / 0 WHEN true THEN 1 ELSE 0 / 0 END", "1");
        assertEvaluatedEquals("CASE WHEN true THEN 1 WHEN 0 / 0 = 0 THEN 2 ELSE 0 / 0 END", "1");
    }

    @Test
    public void testSimpleCase()
    {
        assertOptimizedEquals("CASE 1 " +
                        "WHEN 1 THEN 32 + 1 " +
                        "WHEN 1 THEN 34 " +
                        "END",
                "33");

        assertOptimizedEquals("CASE NULL " +
                        "WHEN true THEN 33 " +
                        "END",
                "NULL");
        assertOptimizedEquals("CASE NULL " +
                        "WHEN true THEN 33 " +
                        "ELSE 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE 33 " +
                        "WHEN NULL THEN 1 " +
                        "ELSE 33 " +
                        "END",
                "33");

        assertOptimizedEquals("CASE NULL " +
                        "WHEN true THEN 3300000000 " +
                        "END",
                "NULL");
        assertOptimizedEquals("CASE NULL " +
                        "WHEN true THEN 3300000000 " +
                        "ELSE 3300000000 " +
                        "END",
                "3300000000");
        assertOptimizedEquals("CASE 33 " +
                        "WHEN NULL THEN 3300000000 " +
                        "ELSE 33 " +
                        "END",
                "33");

        assertOptimizedEquals("CASE true " +
                        "WHEN true THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE true " +
                        "WHEN false THEN 1 " +
                        "ELSE 33 END",
                "33");

        assertOptimizedEquals("CASE bound_long " +
                        "WHEN 1234 THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE 1234 " +
                        "WHEN bound_long THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE true " +
                        "WHEN true THEN bound_long " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE true " +
                        "WHEN false THEN 1 " +
                        "ELSE bound_long " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE bound_integer " +
                        "WHEN 1234 THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE 1234 " +
                        "WHEN bound_integer THEN 33 " +
                        "END",
                "33");
        assertOptimizedEquals("CASE true " +
                        "WHEN true THEN bound_integer " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE true " +
                        "WHEN false THEN 1 " +
                        "ELSE bound_integer " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE bound_long " +
                        "WHEN 1234 THEN 33 " +
                        "ELSE unbound_long " +
                        "END",
                "33");
        assertOptimizedEquals("CASE true " +
                        "WHEN true THEN bound_long " +
                        "ELSE unbound_long " +
                        "END",
                "1234");
        assertOptimizedEquals("CASE true " +
                        "WHEN false THEN unbound_long " +
                        "ELSE bound_long " +
                        "END",
                "1234");

        assertOptimizedEquals("CASE unbound_long " +
                        "WHEN 1234 THEN 33 " +
                        "ELSE 1 " +
                        "END",
                "" +
                        "CASE unbound_long " +
                        "WHEN 1234 THEN 33 " +
                        "ELSE 1 " +
                        "END");

        assertOptimizedEquals("CASE 33 " +
                        "WHEN 0 THEN 0 " +
                        "WHEN 33 THEN unbound_long " +
                        "ELSE 1 " +
                        "END",
                "unbound_long");
        assertOptimizedEquals("CASE 33 " +
                        "WHEN 0 THEN 0 " +
                        "WHEN 33 THEN 1 " +
                        "WHEN unbound_long THEN 2 " +
                        "ELSE 1 " +
                        "END",
                "1");
        assertOptimizedEquals("CASE 33 " +
                        "WHEN unbound_long THEN 0 " +
                        "WHEN 1 THEN 1 " +
                        "WHEN 33 THEN 2 " +
                        "ELSE 0 " +
                        "END",
                "CASE 33 " +
                        "WHEN unbound_long THEN 0 " +
                        "ELSE 2 " +
                        "END");
        assertOptimizedEquals("CASE 33 " +
                        "WHEN 0 THEN 0 " +
                        "WHEN 1 THEN 1 " +
                        "ELSE unbound_long " +
                        "END",
                "unbound_long");
        assertOptimizedEquals("CASE 33 " +
                        "WHEN unbound_long THEN 0 " +
                        "WHEN 1 THEN 1 " +
                        "WHEN unbound_long2 THEN 2 " +
                        "ELSE 3 " +
                        "END",
                "CASE 33 " +
                        "WHEN unbound_long THEN 0 " +
                        "WHEN unbound_long2 THEN 2 " +
                        "ELSE 3 " +
                        "END");

        assertOptimizedEquals("CASE true " +
                        "WHEN unbound_long = 1 THEN 1 " +
                        "WHEN 0 / 0 = 0 THEN 2 " +
                        "ELSE 33 END",
                "" +
                        "CASE true " +
                        "WHEN unbound_long = 1 THEN 1 " +
                        "WHEN 0 / 0 = 0 THEN 2 ELSE 33 " +
                        "END");

        assertOptimizedEquals("CASE bound_long " +
                        "WHEN unbound_long + 123 * 10  THEN 1 = 1 " +
                        "ELSE 1 = 2 " +
                        "END",
                "" +
                        "CASE bound_long WHEN unbound_long + 1230 THEN true " +
                        "ELSE false " +
                        "END");

        assertOptimizedEquals("CASE bound_long " +
                        "WHEN unbound_long THEN 2 + 2 " +
                        "END",
                "" +
                        "CASE bound_long " +
                        "WHEN unbound_long THEN 4 " +
                        "END");

        assertOptimizedEquals("CASE bound_long " +
                        "WHEN unbound_long THEN 2 + 2 " +
                        "WHEN 1 THEN NULL " +
                        "WHEN 2 THEN NULL " +
                        "END",
                "" +
                        "CASE bound_long " +
                        "WHEN unbound_long THEN 4 " +
                        "END");

        assertOptimizedMatches("CASE 1 " +
                        "WHEN unbound_long THEN 1 " +
                        "WHEN 0 / 0 THEN 2 " +
                        "ELSE 1 " +
                        "END",
                "" +
                        "CASE BIGINT '1' " +
                        "WHEN unbound_long THEN 1 " +
                        "WHEN CAST((0 / 0) AS bigint) THEN 2 " +
                        "ELSE 1 " +
                        "END");

        assertOptimizedMatches("CASE 1 " +
                        "WHEN 0 / 0 THEN 1 " +
                        "WHEN 0 / 0 THEN 2 " +
                        "ELSE 1 " +
                        "END",
                "" +
                        "CASE 1 " +
                        "WHEN 0 / 0 THEN 1 " +
                        "WHEN 0 / 0 THEN 2 " +
                        "ELSE 1 " +
                        "END");

        assertOptimizedEquals("CASE true " +
                        "WHEN false THEN 2.2 " +
                        "WHEN true THEN 2.2 " +
                        "END",
                "2.2");

        // TODO enabled WHEN DECIMAL is default for literal:
//        assertOptimizedEquals("CASE true " +
//                        "WHEN false THEN 1234567890.0987654321 " +
//                        "WHEN true THEN 3.3 " +
//                        "END",
//                "CAST(3.3 AS decimal(20,10))");

        assertOptimizedEquals("CASE true " +
                        "WHEN false THEN 1 " +
                        "WHEN true THEN 2.2 " +
                        "END",
                "2.2");

        assertOptimizedEquals("CASE ARRAY[CAST(1 AS bigint)] WHEN ARRAY[CAST(1 AS bigint)] THEN 'matched' ELSE 'not_matched' END", "'matched'");
        assertOptimizedEquals("CASE ARRAY[CAST(2 AS bigint)] WHEN ARRAY[CAST(1 AS bigint)] THEN 'matched' ELSE 'not_matched' END", "'not_matched'");
        assertOptimizedEquals("CASE ARRAY[CAST(NULL AS bigint)] WHEN ARRAY[CAST(1 AS bigint)] THEN 'matched' ELSE 'not_matched' END", "'not_matched'");

        assertOptimizedEquals("CASE null WHEN 0 / 0 THEN 0 / 0 ELSE 1 END", "1");
        assertOptimizedEquals("CASE null WHEN 1 THEN 2 ELSE 0 / 0 END", "0 / 0");
        assertOptimizedEquals("CASE 0 / 0 WHEN 1 THEN 2 ELSE 3 END", "CASE 0 / 0 WHEN 1 THEN 2 ELSE 3 END");
        assertOptimizedEquals("CASE 1 WHEN 0 / 0 THEN 2 ELSE 3 END", "CASE 1 WHEN 0 / 0 THEN 2 ELSE 3 END");
        assertOptimizedEquals("CASE 1 WHEN 2 THEN 2 WHEN 0 / 0 THEN 3 ELSE 4 END", "CASE 1 WHEN 0 / 0 THEN 3 ELSE 4 END");
        assertOptimizedEquals("CASE 1 WHEN 0 / 0 THEN 2 END", "CASE 1 WHEN 0 / 0 THEN 2 END");
        assertOptimizedEquals("CASE 1 WHEN 2 THEN 2 WHEN 3 THEN 3 END", "null");

        assertEvaluatedEquals("CASE null WHEN 0 / 0 THEN 0 / 0 ELSE 1 END", "1");
        assertEvaluatedEquals("CASE 1 WHEN 2 THEN 0 / 0 ELSE 3 END", "3");
        assertEvaluatedEquals("CASE 1 WHEN 1 THEN 2 WHEN 1 THEN 0 / 0 END", "2");
        assertEvaluatedEquals("CASE 1 WHEN 1 THEN 2 ELSE 0 / 0 END", "2");
    }

    @Test
    public void testCoalesce()
    {
        assertOptimizedEquals("coalesce(unbound_long * (2 * 3), 1 - 1, NULL)", "coalesce(unbound_long * 6, 0)");
        assertOptimizedEquals("coalesce(unbound_long * (2 * 3), 1.0E0/2.0E0, NULL)", "coalesce(unbound_long * 6, 0.5E0)");
        assertOptimizedEquals("coalesce(unbound_long, 2, 1.0E0/2.0E0, 12.34E0, NULL)", "coalesce(unbound_long, 2.0E0, 0.5E0, 12.34E0)");
        assertOptimizedEquals("coalesce(unbound_integer * (2 * 3), 1 - 1, NULL)", "coalesce(6 * unbound_integer, 0)");
        assertOptimizedEquals("coalesce(unbound_integer * (2 * 3), 1.0E0/2.0E0, NULL)", "coalesce(6 * unbound_integer, 0.5E0)");
        assertOptimizedEquals("coalesce(unbound_integer, 2, 1.0E0/2.0E0, 12.34E0, NULL)", "coalesce(unbound_integer, 2.0E0, 0.5E0, 12.34E0)");
        assertOptimizedMatches("coalesce(0 / 0 < 1, unbound_boolean, 0 / 0 = 0)",
                "COALESCE(((0 / 0) < 1), unbound_boolean, ((0 / 0) = 0))");
        assertOptimizedMatches("coalesce(unbound_long, unbound_long)", "unbound_long");
        assertOptimizedMatches("coalesce(2 * unbound_long, 2 * unbound_long)", "unbound_long * BIGINT '2'");
        assertOptimizedMatches("coalesce(unbound_long, unbound_long2, unbound_long)", "coalesce(unbound_long, unbound_long2)");
        assertOptimizedMatches("coalesce(unbound_long, unbound_long2, unbound_long, unbound_long3)", "coalesce(unbound_long, unbound_long2, unbound_long3)");
        assertOptimizedEquals("coalesce(6, unbound_long2, unbound_long, unbound_long3)", "6");
        assertOptimizedEquals("coalesce(2 * 3, unbound_long2, unbound_long, unbound_long3)", "6");
        assertOptimizedMatches("coalesce(random(), random(), 5)", "coalesce(random(), random(), 5E0)");
        assertOptimizedMatches("coalesce(unbound_long, coalesce(unbound_long, 1))", "coalesce(unbound_long, BIGINT '1')");
        assertOptimizedMatches("coalesce(coalesce(unbound_long, coalesce(unbound_long, 1)), unbound_long2)", "coalesce(unbound_long, BIGINT '1')");
        assertOptimizedMatches("coalesce(unbound_long, 2, coalesce(unbound_long, 1))", "coalesce(unbound_long, BIGINT '2')");
        assertOptimizedMatches("coalesce(coalesce(unbound_long, coalesce(unbound_long2, unbound_long3)), 1)", "coalesce(unbound_long, unbound_long2, unbound_long3, BIGINT '1')");
        assertOptimizedMatches("coalesce(unbound_double, coalesce(random(), unbound_double))", "coalesce(unbound_double, random())");

        assertOptimizedEquals("coalesce(null, coalesce(null, null))", "null");
        assertOptimizedEquals("coalesce(null, coalesce(null, coalesce(null, null, 1)))", "1");
        assertOptimizedEquals("coalesce(1, 0 / 0)", "1");
        assertOptimizedEquals("coalesce(0 / 0, 1)", "coalesce(0 / 0, 1)");
        assertOptimizedEquals("coalesce(0 / 0, 1, null)", "coalesce(0 / 0, 1)");
        assertOptimizedEquals("coalesce(1, coalesce(0 / 0, 2))", "1");
        assertOptimizedEquals("coalesce(0 / 0, null, 1 / 0, null, 0 / 0)", "coalesce(0 / 0, 1 / 0)");
        assertOptimizedEquals("coalesce(0 / 0, null, 0 / 0, null)", "0 / 0");
        assertOptimizedEquals("coalesce(0 / 0, null, coalesce(0 / 0, null))", "0 / 0");
        assertOptimizedEquals("coalesce(rand(), rand(), 1, rand())", "coalesce(rand(), rand(), 1)");

        assertEvaluatedEquals("coalesce(1, 0 / 0)", "1");
        assertTrinoExceptionThrownBy(() -> evaluate("coalesce(0 / 0, 1)"))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testIf()
    {
        assertOptimizedEquals("IF(2 = 2, 3, 4)", "3");
        assertOptimizedEquals("IF(1 = 2, 3, 4)", "4");
        assertOptimizedEquals("IF(1 = 2, BIGINT '3', 4)", "4");
        assertOptimizedEquals("IF(1 = 2, 3000000000, 4)", "4");

        assertOptimizedEquals("IF(true, 3, 4)", "3");
        assertOptimizedEquals("IF(false, 3, 4)", "4");
        assertOptimizedEquals("IF(NULL, 3, 4)", "4");

        assertOptimizedEquals("IF(true, 3, NULL)", "3");
        assertOptimizedEquals("IF(false, 3, NULL)", "NULL");
        assertOptimizedEquals("IF(true, NULL, 4)", "NULL");
        assertOptimizedEquals("IF(false, NULL, 4)", "4");
        assertOptimizedEquals("IF(true, NULL, NULL)", "NULL");
        assertOptimizedEquals("IF(false, NULL, NULL)", "NULL");

        assertOptimizedEquals("IF(true, 3.5E0, 4.2E0)", "3.5E0");
        assertOptimizedEquals("IF(false, 3.5E0, 4.2E0)", "4.2E0");

        assertOptimizedEquals("IF(true, 'foo', 'bar')", "'foo'");
        assertOptimizedEquals("IF(false, 'foo', 'bar')", "'bar'");

        assertOptimizedEquals("IF(true, 1.01, 1.02)", "1.01");
        assertOptimizedEquals("IF(false, 1.01, 1.02)", "1.02");
        assertOptimizedEquals("IF(true, 1234567890.123, 1.02)", "1234567890.123");
        assertOptimizedEquals("IF(false, 1.01, 1234567890.123)", "1234567890.123");

        // todo optimize case statement
        assertOptimizedEquals("IF(unbound_boolean, 1 + 2, 3 + 4)", "CASE WHEN unbound_boolean THEN (1 + 2) ELSE (3 + 4) END");
        assertOptimizedEquals("IF(unbound_boolean, BIGINT '1' + 2, 3 + 4)", "CASE WHEN unbound_boolean THEN (BIGINT '1' + 2) ELSE (3 + 4) END");

        assertOptimizedEquals("IF(true, 0 / 0)", "0 / 0");
        assertOptimizedEquals("IF(true, 1, 0 / 0)", "1");
        assertOptimizedEquals("IF(false, 1, 0 / 0)", "0 / 0");
        assertOptimizedEquals("IF(false, 0 / 0, 1)", "1");
        assertOptimizedEquals("IF(0 / 0 = 0, 1, 2)", "IF(0 / 0 = 0, 1, 2)");

        assertEvaluatedEquals("IF(true, 1, 0 / 0)", "1");
        assertEvaluatedEquals("IF(false, 0 / 0, 1)", "1");
        assertTrinoExceptionThrownBy(() -> evaluate("IF(0 / 0 = 0, 1, 2)"))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testLike()
    {
        assertOptimizedEquals("'a' LIKE 'a'", "true");
        assertOptimizedEquals("'' LIKE 'a'", "false");
        assertOptimizedEquals("'abc' LIKE 'a'", "false");

        assertOptimizedEquals("'a' LIKE '_'", "true");
        assertOptimizedEquals("'' LIKE '_'", "false");
        assertOptimizedEquals("'abc' LIKE '_'", "false");

        assertOptimizedEquals("'a' LIKE '%'", "true");
        assertOptimizedEquals("'' LIKE '%'", "true");
        assertOptimizedEquals("'abc' LIKE '%'", "true");

        assertOptimizedEquals("'abc' LIKE '___'", "true");
        assertOptimizedEquals("'ab' LIKE '___'", "false");
        assertOptimizedEquals("'abcd' LIKE '___'", "false");

        assertOptimizedEquals("'abc' LIKE 'abc'", "true");
        assertOptimizedEquals("'xyz' LIKE 'abc'", "false");
        assertOptimizedEquals("'abc0' LIKE 'abc'", "false");
        assertOptimizedEquals("'0abc' LIKE 'abc'", "false");

        assertOptimizedEquals("'abc' LIKE 'abc%'", "true");
        assertOptimizedEquals("'abc0' LIKE 'abc%'", "true");
        assertOptimizedEquals("'0abc' LIKE 'abc%'", "false");

        assertOptimizedEquals("'abc' LIKE '%abc'", "true");
        assertOptimizedEquals("'0abc' LIKE '%abc'", "true");
        assertOptimizedEquals("'abc0' LIKE '%abc'", "false");

        assertOptimizedEquals("'abc' LIKE '%abc%'", "true");
        assertOptimizedEquals("'0abc' LIKE '%abc%'", "true");
        assertOptimizedEquals("'abc0' LIKE '%abc%'", "true");
        assertOptimizedEquals("'0abc0' LIKE '%abc%'", "true");
        assertOptimizedEquals("'xyzw' LIKE '%abc%'", "false");

        assertOptimizedEquals("'abc' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0abc' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'abc0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0abc0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'ab01c' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0ab01c' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'ab01c0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0ab01c0' LIKE '%ab%c%'", "true");

        assertOptimizedEquals("'xyzw' LIKE '%ab%c%'", "false");

        // ensure regex chars are escaped
        assertOptimizedEquals("'\' LIKE '\'", "true");
        assertOptimizedEquals("'.*' LIKE '.*'", "true");
        assertOptimizedEquals("'[' LIKE '['", "true");
        assertOptimizedEquals("']' LIKE ']'", "true");
        assertOptimizedEquals("'{' LIKE '{'", "true");
        assertOptimizedEquals("'}' LIKE '}'", "true");
        assertOptimizedEquals("'?' LIKE '?'", "true");
        assertOptimizedEquals("'+' LIKE '+'", "true");
        assertOptimizedEquals("'(' LIKE '('", "true");
        assertOptimizedEquals("')' LIKE ')'", "true");
        assertOptimizedEquals("'|' LIKE '|'", "true");
        assertOptimizedEquals("'^' LIKE '^'", "true");
        assertOptimizedEquals("'$' LIKE '$'", "true");

        assertOptimizedEquals("NULL LIKE '%'", "NULL");
        assertOptimizedEquals("'a' LIKE NULL", "NULL");
        assertOptimizedEquals("'a' LIKE '%' ESCAPE NULL", "NULL");

        assertOptimizedEquals("'%' LIKE 'z%' ESCAPE 'z'", "true");
    }

    @Test
    public void testLikeChar()
    {
        assertOptimizedEquals("CAST('abc' AS char(3)) LIKE 'abc'", "true");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE 'abc'", "false");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE 'abc '", "true");

        assertOptimizedEquals("CAST('abc' AS char(3)) LIKE '%abc'", "true");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%abc'", "false");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%abc '", "true");

        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%c'", "false");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%c '", "true");

        assertOptimizedEquals("CAST('abc' AS char(3)) LIKE '%a%b%c'", "true");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%a%b%c'", "false");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%a%b%c '", "true");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%a%b%c_'", "true");
        assertOptimizedEquals("CAST('abc' AS char(4)) LIKE '%a%b%c%'", "true");
    }

    @Test
    public void testLikeOptimization()
    {
        assertOptimizedEquals("unbound_string LIKE 'abc'", "unbound_string = VARCHAR 'abc'");

        assertOptimizedEquals("unbound_string LIKE '' ESCAPE '#'", "unbound_string LIKE '' ESCAPE '#'");
        assertOptimizedEquals("unbound_string LIKE 'abc' ESCAPE '#'", "unbound_string = VARCHAR 'abc'");
        assertOptimizedEquals("unbound_string LIKE 'a#_b' ESCAPE '#'", "unbound_string = VARCHAR 'a_b'");
        assertOptimizedEquals("unbound_string LIKE 'a#%b' ESCAPE '#'", "unbound_string = VARCHAR 'a%b'");
        assertOptimizedEquals("unbound_string LIKE 'a#_##b' ESCAPE '#'", "unbound_string = VARCHAR 'a_#b'");
        assertOptimizedEquals("unbound_string LIKE 'a#__b' ESCAPE '#'", "unbound_string LIKE 'a#__b' ESCAPE '#'");
        assertOptimizedEquals("unbound_string LIKE 'a##%b' ESCAPE '#'", "unbound_string LIKE 'a##%b' ESCAPE '#'");

        assertOptimizedEquals("bound_string LIKE bound_pattern", "true");
        assertOptimizedEquals("'abc' LIKE bound_pattern", "false");

        assertOptimizedEquals("unbound_string LIKE bound_pattern", "unbound_string LIKE bound_pattern");

        assertOptimizedEquals("unbound_string LIKE unbound_pattern ESCAPE unbound_string", "unbound_string LIKE unbound_pattern ESCAPE unbound_string");
    }

    @Test
    public void testLikeCharOptimization()
    {
        // constant literal pattern of length shorter than value length
        assertOptimizedEquals("unbound_char LIKE 'abc'", "false");
        assertOptimizedEquals("unbound_char LIKE 'abc' ESCAPE '#'", "false");
        assertOptimizedEquals("unbound_char LIKE 'ab#_' ESCAPE '#'", "false");
        assertOptimizedEquals("unbound_char LIKE 'ab#%' ESCAPE '#'", "false");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'abc'", "false");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'abc' ESCAPE '#'", "false");

        // constant non-literal pattern of length shorter than value length
        assertOptimizedEquals("unbound_char LIKE 'ab_'", "unbound_char LIKE 'ab_'");
        assertOptimizedEquals("unbound_char LIKE 'ab%'", "unbound_char LIKE 'ab%'");
        assertOptimizedEquals("unbound_char LIKE 'ab%' ESCAPE '#'", "unbound_char LIKE 'ab%' ESCAPE '#'");

        // constant literal pattern of length equal to value length
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'abcd'", "CAST(unbound_char AS char(4)) = CAST('abcd' AS char(4))");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'abcd' ESCAPE '#'", "CAST(unbound_char AS char(4)) = CAST('abcd' AS char(4))");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'jaźń'", "CAST(unbound_char AS char(4)) = CAST('jaźń' AS char(4))");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'ab#_' ESCAPE '#'", "false");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'ab#%' ESCAPE '#'", "false");

        // constant non-literal pattern of length equal to value length
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'ab#%' ESCAPE '\\'", "CAST(unbound_char AS char(4)) LIKE 'ab#%' ESCAPE '\\'");

        // constant pattern of length longer than value length
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'abcde'", "false");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE 'abcde' ESCAPE '#'", "false");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE '#%a#%b#%c#%d#%' ESCAPE '#'", "false");

        // constant non-literal pattern of length longer than value length
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE '%a%b%c%d%'", "CAST(unbound_char AS char(4)) LIKE '%a%b%c%d%'");
        assertOptimizedEquals("CAST(unbound_char AS char(4)) LIKE '%a%b%c%d%' ESCAPE '#'", "CAST(unbound_char AS char(4)) LIKE '%a%b%c%d%' ESCAPE '#'");

        // without explicit CAST on value, constant pattern of equal length
        assertOptimizedEquals(
                "unbound_char LIKE CAST(CAST('abc' AS char( " + TEST_CHAR_TYPE_LENGTH + ")) AS varchar(" + TEST_CHAR_TYPE_LENGTH + "))",
                "unbound_char = CAST('abc' AS char(17))");
        assertOptimizedEquals(
                "unbound_char LIKE CAST(CAST('abc' AS char( " + TEST_CHAR_TYPE_LENGTH + ")) AS varchar)",
                "unbound_char = CAST('abc' AS char(17))");

        assertOptimizedEquals(
                "unbound_char LIKE CAST(CAST('' AS char(" + TEST_CHAR_TYPE_LENGTH + ")) AS varchar(" + TEST_CHAR_TYPE_LENGTH + ")) ESCAPE '#'",
                "unbound_char LIKE CAST('                 ' AS varchar(17)) ESCAPE '#'");
        assertOptimizedEquals(
                "unbound_char LIKE CAST(CAST('' AS char(" + TEST_CHAR_TYPE_LENGTH + ")) AS varchar) ESCAPE '#'",
                "unbound_char LIKE CAST('                 ' AS varchar(17)) ESCAPE '#'");

        assertOptimizedEquals("unbound_char LIKE bound_pattern", "unbound_char LIKE VARCHAR '%el%'");
        assertOptimizedEquals("unbound_char LIKE unbound_pattern", "unbound_char LIKE unbound_pattern");
        assertOptimizedEquals("unbound_char LIKE unbound_pattern ESCAPE unbound_string", "unbound_char LIKE unbound_pattern ESCAPE unbound_string");
    }

    @Test
    public void testOptimizeInvalidLike()
    {
        assertOptimizedMatches("unbound_string LIKE 'abc' ESCAPE ''", "unbound_string LIKE 'abc' ESCAPE ''");
        assertOptimizedMatches("unbound_string LIKE 'abc' ESCAPE 'bc'", "unbound_string LIKE 'abc' ESCAPE 'bc'");
        assertOptimizedMatches("unbound_string LIKE '#' ESCAPE '#'", "unbound_string LIKE '#' ESCAPE '#'");
        assertOptimizedMatches("unbound_string LIKE '#abc' ESCAPE '#'", "unbound_string LIKE '#abc' ESCAPE '#'");
        assertOptimizedMatches("unbound_string LIKE 'ab#' ESCAPE '#'", "unbound_string LIKE 'ab#' ESCAPE '#'");
    }

    @Test
    public void testEvaluateInvalidLike()
    {
        // TODO This doesn't fail (https://github.com/trinodb/trino/issues/7273)
        /*assertTrinoExceptionThrownBy(() -> evaluate("'some_string' LIKE 'abc' ESCAPE ''"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Escape string must be a single character");*/

        assertTrinoExceptionThrownBy(() -> evaluate("unbound_string LIKE 'abc' ESCAPE 'bc'"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Escape string must be a single character");

        assertTrinoExceptionThrownBy(() -> evaluate("unbound_string LIKE '#' ESCAPE '#'"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");

        assertTrinoExceptionThrownBy(() -> evaluate("unbound_string LIKE '#abc' ESCAPE '#'"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");

        assertTrinoExceptionThrownBy(() -> evaluate("unbound_string LIKE 'ab#' ESCAPE '#'"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
    }

    @Test
    public void testFailedExpressionOptimization()
    {
        assertOptimizedEquals("IF(unbound_boolean, 1, 0 / 0)", "CASE WHEN unbound_boolean THEN 1 ELSE 0 / 0 END");
        assertOptimizedEquals("IF(unbound_boolean, 0 / 0, 1)", "CASE WHEN unbound_boolean THEN 0 / 0 ELSE 1 END");

        assertOptimizedMatches("CASE unbound_long WHEN 1 THEN 1 WHEN 0 / 0 THEN 2 END",
                "CASE unbound_long WHEN BIGINT '1' THEN 1 WHEN CAST((0 / 0) AS bigint) THEN 2 END");

        assertOptimizedMatches("CASE unbound_boolean WHEN true THEN 1 ELSE 0 / 0 END",
                "CASE unbound_boolean WHEN true THEN 1 ELSE 0 / 0 END");

        assertOptimizedMatches("CASE bound_long WHEN unbound_long THEN 1 WHEN 0 / 0 THEN 2 ELSE 1 END",
                "CASE BIGINT '1234' WHEN unbound_long THEN 1 WHEN CAST((0 / 0) AS bigint) THEN 2 ELSE 1 END");

        assertOptimizedMatches("CASE WHEN unbound_boolean THEN 1 WHEN 0 / 0 = 0 THEN 2 END",
                "CASE WHEN unbound_boolean THEN 1 WHEN 0 / 0 = 0 THEN 2 END");

        assertOptimizedMatches("CASE WHEN unbound_boolean THEN 1 ELSE 0 / 0 END",
                "CASE WHEN unbound_boolean THEN 1 ELSE 0 / 0 END");

        assertOptimizedMatches("CASE WHEN unbound_boolean THEN 0 / 0 ELSE 1 END",
                "CASE WHEN unbound_boolean THEN 0 / 0 ELSE 1 END");
    }

    @Test
    public void testOptimizeDivideByZero()
    {
        assertOptimizedEquals("0 / 0", "0 / 0");

        assertTrinoExceptionThrownBy(() -> evaluate("0 / 0"))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testMassiveArrayConstructor()
    {
        optimize(format("ARRAY[%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "(bound_long + " + i + ")").iterator())));
        optimize(format("ARRAY[%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "(bound_integer + " + i + ")").iterator())));
        optimize(format("ARRAY[%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "'" + i + "'").iterator())));
        optimize(format("ARRAY[%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "ARRAY['" + i + "']").iterator())));
    }

    @Test
    public void testArrayConstructor()
    {
        optimize("ARRAY[]");
        assertOptimizedEquals("ARRAY[(unbound_long + 0), (unbound_long + 1), (unbound_long + 2)]",
                "array_constructor((unbound_long + 0), (unbound_long + 1), (unbound_long + 2))");
        assertOptimizedEquals("ARRAY[(bound_long + 0), (unbound_long + 1), (bound_long + 2)]",
                "array_constructor((bound_long + 0), (unbound_long + 1), (bound_long + 2))");
        assertOptimizedEquals("ARRAY[(bound_long + 0), (unbound_long + 1), NULL]",
                "array_constructor((bound_long + 0), (unbound_long + 1), NULL)");
    }

    @Test
    public void testRowConstructor()
    {
        optimize("ROW(NULL)");
        optimize("ROW(1)");
        optimize("ROW(unbound_long + 0)");
        optimize("ROW(unbound_long + unbound_long2, unbound_string, unbound_double)");
        optimize("ROW(unbound_boolean, FALSE, ARRAY[unbound_long, unbound_long2], unbound_null_string, unbound_interval)");
        optimize("ARRAY[ROW(unbound_string, unbound_double), ROW(unbound_string, 0.0E0)]");
        optimize("ARRAY[ROW('string', unbound_double), ROW('string', bound_double)]");
        optimize("ROW(ROW(NULL), ROW(ROW(ROW(ROW('rowception')))))");
        optimize("ROW(unbound_string, bound_string)");

        optimize("ARRAY[ROW(unbound_string, unbound_double), ROW(CAST(bound_string AS varchar), 0.0E0)]");
        optimize("ARRAY[ROW(CAST(bound_string AS varchar), 0.0E0), ROW(unbound_string, unbound_double)]");

        optimize("ARRAY[ROW(unbound_string, unbound_double), CAST(NULL AS row(varchar, double))]");
        optimize("ARRAY[CAST(NULL AS row(varchar, double)), ROW(unbound_string, unbound_double)]");
    }

    @Test
    public void testRowSubscript()
    {
        assertOptimizedEquals("ROW(1, 'a', true)[3]", "true");
        assertOptimizedEquals("ROW(1, 'a', ROW(2, 'b', ROW(3, 'c')))[3][3][2]", "'c'");

        assertOptimizedEquals("ROW(1, null)[2]", "null");
        assertOptimizedEquals("ROW(0 / 0, 1)[1]", "ROW(0 / 0, 1)[1]");
        assertOptimizedEquals("ROW(0 / 0, 1)[2]", "ROW(0 / 0, 1)[2]");

        assertTrinoExceptionThrownBy(() -> evaluate("ROW(0 / 0, 1)[1]"))
                .hasErrorCode(DIVISION_BY_ZERO);
        assertTrinoExceptionThrownBy(() -> evaluate("ROW(0 / 0, 1)[2]"))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testArraySubscriptConstantNegativeIndex()
    {
        assertOptimizedEquals("ARRAY[1, 2, 3][-1]", "ARRAY[1,2,3][CAST(-1 AS bigint)]");

        assertTrinoExceptionThrownBy(() -> evaluate("ARRAY[1, 2, 3][-1]"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Array subscript is negative: -1");
    }

    @Test
    public void testArraySubscriptConstantZeroIndex()
    {
        assertOptimizedEquals("ARRAY[1, 2, 3][0]", "ARRAY[1,2,3][CAST(0 AS bigint)]");

        assertTrinoExceptionThrownBy(() -> evaluate("ARRAY[1, 2, 3][0]"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("SQL array indices start at 1");
    }

    @Test
    public void testMapSubscriptMissingKey()
    {
        assertOptimizedEquals("MAP(ARRAY[1, 2], ARRAY[3, 4])[-1]", "MAP(ARRAY[1, 2], ARRAY[3, 4])[-1]");

        assertTrinoExceptionThrownBy(() -> evaluate("MAP(ARRAY[1, 2], ARRAY[3, 4])[-1]"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Key not present in map: -1");
    }

    @Test
    public void testMapSubscriptConstantIndexes()
    {
        optimize("MAP(ARRAY[1, 2], ARRAY[3, 4])[1]");
        optimize("MAP(ARRAY[BIGINT '1', 2], ARRAY[3, 4])[1]");
        optimize("MAP(ARRAY[1, 2], ARRAY[3, 4])[2]");
        optimize("MAP(ARRAY[ARRAY[1,1]], ARRAY['a'])[ARRAY[1,1]]");
    }

    @Test(timeOut = 60000)
    public void testLikeInvalidUtf8()
    {
        assertLike(new byte[] {'a', 'b', 'c'}, "%b%", true);
        assertLike(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'}, "%b%", true);
    }

    @Test
    public void testLiterals()
    {
        optimize("DATE '2013-04-03' + unbound_interval");
        optimize("TIME '03:04:05.321' + unbound_interval");
        optimize("TIME '03:04:05.321+00:00' + unbound_interval");
        optimize("TIMESTAMP '2013-04-03 03:04:05.321' +     unbound_interval");
        optimize("TIMESTAMP '2013-04-03 03:04:05.321 UTC' + unbound_interval");

        optimize("INTERVAL '3' DAY * unbound_long");
        optimize("INTERVAL '3' YEAR * unbound_long");

        assertEquals(optimize("X'1234'"), Slices.wrappedBuffer((byte) 0x12, (byte) 0x34));
    }

    private static void assertLike(byte[] value, String pattern, boolean expected)
    {
        Expression predicate = new LikePredicate(
                rawStringLiteral(Slices.wrappedBuffer(value)),
                new StringLiteral(pattern),
                Optional.empty());
        assertEquals(evaluate(predicate), expected);
    }

    private static StringLiteral rawStringLiteral(Slice slice)
    {
        return new StringLiteral(slice.toStringUtf8())
        {
            @Override
            public Slice getSlice()
            {
                return slice;
            }
        };
    }

    private static void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(optimize(actual), optimize(expected));
    }

    private static void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        Expression actualOptimized = (Expression) optimize(actual);

        SymbolAliases.Builder aliases = SymbolAliases.builder()
                .putAll(SYMBOL_TYPES.allTypes().keySet().stream()
                        .map(Symbol::getName)
                        .collect(toImmutableMap(identity(), SymbolReference::new)));

        Expression rewrittenExpected = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected, new ParsingOptions()));
        assertExpressionEquals(actualOptimized, rewrittenExpected, aliases.build());
    }

    private static Object optimize(@Language("SQL") String expression)
    {
        assertRoundTrip(expression);

        return optimize(planExpression(expression));
    }

    static Object optimize(Expression parsedExpression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getTypes(TEST_SESSION, PLANNER_CONTEXT, SYMBOL_TYPES, parsedExpression);
        ExpressionInterpreter interpreter = new ExpressionInterpreter(parsedExpression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);
        return interpreter.optimize(INPUTS);
    }

    // TODO replace that method with io.trino.sql.ExpressionTestUtils.planExpression
    static Expression planExpression(@Language("SQL") String expression)
    {
        return TransactionBuilder.transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, transactionSession -> {
                    Expression parsedExpression = SQL_PARSER.createExpression(expression, createParsingOptions(transactionSession));
                    parsedExpression = rewriteIdentifiersToSymbolReferences(parsedExpression);
                    parsedExpression = resolveFunctionCalls(PLANNER_CONTEXT, transactionSession, SYMBOL_TYPES, parsedExpression);
                    parsedExpression = CanonicalizeExpressionRewriter.rewrite(
                            parsedExpression,
                            transactionSession,
                            PLANNER_CONTEXT.getMetadata(),
                            createTestingTypeAnalyzer(PLANNER_CONTEXT),
                            SYMBOL_TYPES);
                    return parsedExpression;
                });
    }

    private static void assertEvaluatedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(evaluate(actual), evaluate(expected));
    }

    private static Object evaluate(String expression)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = ExpressionTestUtils.createExpression(expression, PLANNER_CONTEXT, SYMBOL_TYPES);

        return evaluate(parsedExpression);
    }

    private static void assertRoundTrip(String expression)
    {
        ParsingOptions parsingOptions = createParsingOptions(TEST_SESSION);
        Expression parsed = SQL_PARSER.createExpression(expression, parsingOptions);
        String formatted = formatExpression(parsed);
        assertEquals(parsed, SQL_PARSER.createExpression(formatted, parsingOptions));
    }

    private static Object evaluate(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getTypes(TEST_SESSION, PLANNER_CONTEXT, SYMBOL_TYPES, expression);
        ExpressionInterpreter interpreter = new ExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);

        return interpreter.evaluate(INPUTS);
    }
}
