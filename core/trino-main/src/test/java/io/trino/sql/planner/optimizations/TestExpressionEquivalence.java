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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Decimals;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.type.DateTimes;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionEquivalence
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final ExpressionEquivalence EQUIVALENCE = new ExpressionEquivalence(
            PLANNER_CONTEXT.getMetadata(),
            PLANNER_CONTEXT.getFunctionManager(),
            PLANNER_CONTEXT.getTypeManager());

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MOD = FUNCTIONS.resolveFunction("mod", fromTypes(INTEGER, INTEGER));
    private static final ResolvedFunction LIKE = FUNCTIONS.resolveFunction("$like", fromTypes(createVarcharType(3), LIKE_PATTERN));

    @Test
    public void testEquivalent()
    {
        assertEquivalent(
                new Constant(BIGINT, null),
                new Constant(BIGINT, null));
        assertEquivalent(
                TRUE,
                TRUE);
        assertEquivalent(
                new Constant(INTEGER, 4L),
                new Constant(INTEGER, 4L));
        assertEquivalent(
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))),
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))));
        assertEquivalent(
                new Constant(VARCHAR, utf8Slice("foo")),
                new Constant(VARCHAR, utf8Slice("foo")));

        assertEquivalent(
                new Comparison(EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new Comparison(EQUAL, new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))), new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("5.5")))),
                new Comparison(EQUAL, new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("5.5"))), new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4")))));
        assertEquivalent(
                new Comparison(EQUAL, new Constant(VARCHAR, utf8Slice("foo")), new Constant(VARCHAR, utf8Slice("bar"))),
                new Comparison(EQUAL, new Constant(VARCHAR, utf8Slice("bar")), new Constant(VARCHAR, utf8Slice("foo"))));
        assertEquivalent(
                new Comparison(NOT_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(NOT_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new Comparison(LESS_THAN, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(GREATER_THAN, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new Comparison(EQUAL, new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-05-10 12:34:56.123456789")), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2021-05-10 12:34:56.123456789"))),
                new Comparison(EQUAL, new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2021-05-10 12:34:56.123456789")), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-05-10 12:34:56.123456789"))));
        assertEquivalent(
                new Comparison(EQUAL, new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 +8")), new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2021-05-10 12:34:56.123456789 +8"))),
                new Comparison(EQUAL, new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2021-05-10 12:34:56.123456789 +8")), new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 +8"))));

        assertEquivalent(
                new Call(MOD, ImmutableList.of(new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))),
                new Call(MOD, ImmutableList.of(new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))));

        assertEquivalent(
                new Reference(BIGINT, "a_bigint"),
                new Reference(BIGINT, "a_bigint"));
        assertEquivalent(
                new Comparison(EQUAL, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")),
                new Comparison(EQUAL, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "a_bigint")));
        assertEquivalent(
                new Comparison(LESS_THAN, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")),
                new Comparison(GREATER_THAN, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "a_bigint")));

        assertEquivalent(
                new Logical(AND, ImmutableList.of(TRUE, FALSE)),
                new Logical(AND, ImmutableList.of(FALSE, TRUE)));
        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")), new Comparison(LESS_THAN, new Reference(BIGINT, "c_bigint"), new Reference(BIGINT, "d_bigint")))),
                new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "d_bigint"), new Reference(BIGINT, "c_bigint")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "a_bigint")))));
        assertEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")), new Comparison(LESS_THAN, new Reference(BIGINT, "c_bigint"), new Reference(BIGINT, "d_bigint")))),
                new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "d_bigint"), new Reference(BIGINT, "c_bigint")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "a_bigint")))));

        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)))),
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)));
        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 2L), new Constant(INTEGER, 3L)), new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 3L), new Constant(INTEGER, 2L)))));

        assertEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)))),
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)));
        assertEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 2L), new Constant(INTEGER, 3L)), new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 3L), new Constant(INTEGER, 2L)))));

        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a_boolean"), new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "c_boolean"))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "c_boolean"), new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "a_boolean"))));
        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a_boolean"), new Reference(BOOLEAN, "b_boolean"))), new Reference(BOOLEAN, "c_boolean"))),
                new Logical(AND, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "c_boolean"), new Reference(BOOLEAN, "b_boolean"))), new Reference(BOOLEAN, "a_boolean"))));
        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a_boolean"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "c_boolean"))))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a_boolean"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "c_boolean"), new Reference(BOOLEAN, "b_boolean"))), new Reference(BOOLEAN, "a_boolean"))));

        assertEquivalent(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "a_boolean"), new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "c_boolean"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "d_boolean"), new Reference(BOOLEAN, "e_boolean"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "f_boolean"), new Reference(BOOLEAN, "g_boolean"), new Reference(BOOLEAN, "h_boolean"))))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "h_boolean"), new Reference(BOOLEAN, "g_boolean"), new Reference(BOOLEAN, "f_boolean"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "a_boolean"), new Reference(BOOLEAN, "c_boolean"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "e_boolean"), new Reference(BOOLEAN, "d_boolean"))))));

        assertEquivalent(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a_boolean"), new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "c_boolean"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "d_boolean"), new Reference(BOOLEAN, "e_boolean"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "f_boolean"), new Reference(BOOLEAN, "g_boolean"), new Reference(BOOLEAN, "h_boolean"))))),
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "h_boolean"), new Reference(BOOLEAN, "g_boolean"), new Reference(BOOLEAN, "f_boolean"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "b_boolean"), new Reference(BOOLEAN, "a_boolean"), new Reference(BOOLEAN, "c_boolean"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "e_boolean"), new Reference(BOOLEAN, "d_boolean"))))));
    }

    @Test
    public void testSpecialFormEquivalence()
    {
        assertEquivalent(
                new Logical(OR, ImmutableList.of(
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("a%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("b%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("c%", Optional.empty())))))),
                new Logical(OR, ImmutableList.of(
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("c%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("b%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("a%", Optional.empty())))))));

        assertNotEquivalent(
                new Logical(OR, ImmutableList.of(
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("a%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("b%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("c%", Optional.empty())))))),
                new Logical(OR, ImmutableList.of(
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("c%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("d%", Optional.empty())))),
                        new Call(LIKE, List.of(new Reference(createVarcharType(3), "col"), new Constant(LIKE_PATTERN, LikePattern.compile("a%", Optional.empty())))))));
    }

    private static void assertEquivalent(Expression leftExpression, Expression rightExpression)
    {
        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));

        assertThat(areExpressionEquivalent(leftExpression, rightExpression, symbols))
                .describedAs(format("Expected (%s) and (%s) to be equivalent", leftExpression, rightExpression))
                .isTrue();
        assertThat(areExpressionEquivalent(rightExpression, leftExpression, symbols))
                .describedAs(format("Expected (%s) and (%s) to be equivalent", rightExpression, leftExpression))
                .isTrue();
    }

    @Test
    public void testNotEquivalent()
    {
        assertNotEquivalent(
                new Constant(BOOLEAN, null),
                FALSE);
        assertNotEquivalent(
                FALSE,
                new Constant(BOOLEAN, null));
        assertNotEquivalent(
                TRUE,
                FALSE);
        assertNotEquivalent(
                new Constant(INTEGER, 4L),
                new Constant(INTEGER, 5L));
        assertNotEquivalent(
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))),
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("5.5"))));
        assertNotEquivalent(
                new Constant(VARCHAR, utf8Slice("'foo'")),
                new Constant(VARCHAR, utf8Slice("'bar'")));

        assertNotEquivalent(
                new Constant(LIKE_PATTERN, LikePattern.compile("foo%", Optional.empty())),
                new Constant(LIKE_PATTERN, LikePattern.compile("bar%", Optional.empty())));

        assertNotEquivalent(
                new Comparison(EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new Comparison(NOT_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(NOT_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new Comparison(LESS_THAN, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(GREATER_THAN, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));

        assertNotEquivalent(
                new Call(MOD, ImmutableList.of(new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))),
                new Call(MOD, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 4L))));

        assertNotEquivalent(
                new Reference(BIGINT, "a_bigint"),
                new Reference(BIGINT, "b_bigint"));
        assertNotEquivalent(
                new Comparison(EQUAL, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")),
                new Comparison(EQUAL, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "c_bigint")));
        assertNotEquivalent(
                new Comparison(LESS_THAN, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")),
                new Comparison(GREATER_THAN, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "c_bigint")));

        assertNotEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)))));
        assertNotEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new Comparison(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)))));
        assertNotEquivalent(
                new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")), new Comparison(LESS_THAN, new Reference(BIGINT, "c_bigint"), new Reference(BIGINT, "d_bigint")))),
                new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "d_bigint"), new Reference(BIGINT, "c_bigint")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "c_bigint")))));
        assertNotEquivalent(
                new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a_bigint"), new Reference(BIGINT, "b_bigint")), new Comparison(LESS_THAN, new Reference(BIGINT, "c_bigint"), new Reference(BIGINT, "d_bigint")))),
                new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "d_bigint"), new Reference(BIGINT, "c_bigint")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "b_bigint"), new Reference(BIGINT, "c_bigint")))));

        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(3), DateTimes.parseTimeWithTimeZone(3, "12:34:56.123 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(3), DateTimes.parseTimeWithTimeZone(3, "14:34:56.123 +02:00")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(6), DateTimes.parseTimeWithTimeZone(6, "12:34:56.123456 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(6), DateTimes.parseTimeWithTimeZone(6, "14:34:56.123456 +02:00")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(9), DateTimes.parseTimeWithTimeZone(9, "12:34:56.123456789 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(9), DateTimes.parseTimeWithTimeZone(9, "14:34:56.123456789 +02:00")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(12), DateTimes.parseTimeWithTimeZone(12, "12:34:56.123456789012 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(12), DateTimes.parseTimeWithTimeZone(12, "14:34:56.123456789012 +02:00")), VARCHAR));

        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(3), DateTimes.parseTimestampWithTimeZone(3, "2020-05-10 12:34:56.123 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(3), DateTimes.parseTimestampWithTimeZone(3, "2020-05-10 12:34:56.123 Europe/Paris")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(6), DateTimes.parseTimestampWithTimeZone(6, "2020-05-10 12:34:56.123456 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(6), DateTimes.parseTimestampWithTimeZone(6, "2020-05-10 12:34:56.123456 Europe/Paris")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 Europe/Paris")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(12), DateTimes.parseTimestampWithTimeZone(12, "2020-05-10 12:34:56.123456789012 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(12), DateTimes.parseTimestampWithTimeZone(12, "2020-05-10 12:34:56.123456789012 Europe/Paris")), VARCHAR));
    }

    private static void assertNotEquivalent(Expression leftExpression, Expression rightExpression)
    {
        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));

        assertThat(areExpressionEquivalent(leftExpression, rightExpression, symbols))
                .describedAs(format("Expected (%s) and (%s) to not be equivalent", leftExpression, rightExpression))
                .isFalse();
        assertThat(areExpressionEquivalent(rightExpression, leftExpression, symbols))
                .describedAs(format("Expected (%s) and (%s) to not be equivalent", rightExpression, leftExpression))
                .isFalse();
    }

    private static boolean areExpressionEquivalent(Expression leftExpression, Expression rightExpression, Set<Symbol> symbols)
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, transactionSession -> {
                    return EQUIVALENCE.areExpressionsEquivalent(transactionSession, leftExpression, rightExpression, symbols);
                });
    }
}
