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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Decimals;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.AggregationFunction;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.trino.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPreAggregateCaseAggregations
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_DECIMAL_10_0 = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(createDecimalType(10), createDecimalType(10)));

    private static final SchemaTableName TABLE = new SchemaTableName("default", "t");

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false") // remove hash computing projections for simplicity
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false") // remove partial aggregations for simplicity
                .setSystemProperty(TASK_CONCURRENCY, "1"); // these tests don't handle exchanges from local parallel

        PlanTester planTester = PlanTester.create(sessionBuilder.build());

        // create table with different column types
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withGetTableHandle((session, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withGetColumns(name -> {
                    if (!name.equals(TABLE)) {
                        throw new IllegalArgumentException();
                    }
                    return ImmutableList.of(
                            new ColumnMetadata("col_varchar", VARCHAR),
                            new ColumnMetadata("col_bigint", BIGINT),
                            new ColumnMetadata("col_tinyint", TINYINT),
                            new ColumnMetadata("col_decimal", createDecimalType(2, 1)),
                            new ColumnMetadata("col_long_decimal", createDecimalType(19, 18)),
                            new ColumnMetadata("col_double", DOUBLE));
                });
        planTester.createCatalog("local", builder.build(), ImmutableMap.of());

        return planTester;
    }

    @Test
    public void testPreAggregatesCaseAggregations()
    {
        assertPlan(
                "SELECT " +
                        "(col_varchar || 'a'), " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_bigint * 2 ELSE 0 END), " +
                        "CAST(sum(CASE WHEN col_bigint = 1 THEN CAST(col_bigint * 2 AS INTEGER) ELSE CAST(0 AS INTEGER) END) AS VARCHAR(10)), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_bigint * 2 ELSE null END), " +
                        "min(CASE WHEN col_bigint % 2 > 1.23 THEN col_bigint * 2 END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CAST(CASE WHEN col_bigint = 4 THEN col_decimal * 2 END AS BIGINT)) " +
                        "FROM t " +
                        "GROUP BY (col_varchar || 'a')",
                anyTree(
                        project(
                                ImmutableMap.of("SUM_2_CAST", expression(new Cast(new Reference(BIGINT, "SUM_2"), createVarcharType(10)))),
                                aggregation(
                                        singleGroupingSet("KEY"),
                                        ImmutableMap.<Optional<String>, ExpectedValueProvider<AggregationFunction>>builder()
                                                .put(Optional.of("SUM_1"), aggregationFunction("sum", ImmutableList.of("SUM_1_INPUT")))
                                                .put(Optional.of("SUM_2"), aggregationFunction("sum", ImmutableList.of("SUM_2_INPUT")))
                                                .put(Optional.of("SUM_3"), aggregationFunction("sum", ImmutableList.of("SUM_3_INPUT")))
                                                .put(Optional.of("MIN_1"), aggregationFunction("min", ImmutableList.of("MIN_1_INPUT")))
                                                .put(Optional.of("SUM_4"), aggregationFunction("sum", ImmutableList.of("SUM_4_INPUT")))
                                                .put(Optional.of("SUM_5"), aggregationFunction("sum", ImmutableList.of("SUM_5_INPUT")))
                                                .buildOrThrow(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("SUM_1_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Reference(BIGINT, "SUM_BIGINT"))), new Constant(BIGINT, 0L))))
                                                        .put("SUM_2_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Reference(BIGINT, "SUM_INT_CAST"))), new Constant(BIGINT, 0L))))
                                                        .put("SUM_3_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L)), new Reference(BIGINT, "SUM_BIGINT"))), new Constant(BIGINT, null))))
                                                        .put("MIN_1_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 1L)), new Reference(BIGINT, "MIN_BIGINT"))), new Constant(BIGINT, null))))
                                                        .put("SUM_4_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 3L)), new Reference(createDecimalType(38, 1), "SUM_DECIMAL"))), new Constant(createDecimalType(38, 1), null))))
                                                        .put("SUM_5_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 4L)), new Reference(BIGINT, "SUM_DECIMAL_CAST"))), new Constant(BIGINT, null))))
                                                        .buildOrThrow(),
                                                aggregation(
                                                        singleGroupingSet("KEY", "COL_BIGINT"),
                                                        ImmutableMap.of(
                                                                Optional.of("SUM_BIGINT"), aggregationFunction("sum", ImmutableList.of("VALUE_BIGINT")),
                                                                Optional.of("SUM_INT_CAST"), aggregationFunction("sum", ImmutableList.of("VALUE_INT_CAST")),
                                                                Optional.of("MIN_BIGINT"), aggregationFunction("min", ImmutableList.of("VALUE_2_BIGINT")),
                                                                Optional.of("SUM_DECIMAL"), aggregationFunction("sum", ImmutableList.of("COL_DECIMAL")),
                                                                Optional.of("SUM_DECIMAL_CAST"), aggregationFunction("sum", ImmutableList.of("VALUE_DECIMAL_CAST"))),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        exchange(
                                                                project(ImmutableMap.of(
                                                                                "KEY", expression(new Call(CONCAT, ImmutableList.of(new Reference(VARCHAR, "COL_VARCHAR"), new Constant(VARCHAR, Slices.utf8Slice("a"))))),
                                                                                "VALUE_BIGINT", expression(new Case(ImmutableList.of(new WhenClause(new In(new Reference(BIGINT, "COL_BIGINT"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))))), new Constant(BIGINT, null))),
                                                                                "VALUE_INT_CAST", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Cast(new Cast(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))), INTEGER), BIGINT))), new Constant(BIGINT, null))),
                                                                                "VALUE_2_BIGINT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 1L)), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))))), new Constant(BIGINT, null))),
                                                                                "VALUE_DECIMAL_CAST", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 4L)), new Cast(new Call(MULTIPLY_DECIMAL_10_0, ImmutableList.of(new Reference(createDecimalType(10, 0), "COL_DECIMAL"), new Constant(createDecimalType(10, 0), Decimals.valueOfShort(new BigDecimal("2"))))), BIGINT))), new Constant(BIGINT, null)))),
                                                                        tableScan(
                                                                                "t",
                                                                                ImmutableMap.of(
                                                                                        "COL_VARCHAR", "col_varchar",
                                                                                        "COL_BIGINT", "col_bigint",
                                                                                        "COL_DECIMAL", "col_decimal"))))))))));
    }

    @Test
    public void testGlobalPreAggregatesCaseAggregations()
    {
        assertPlan(
                "SELECT " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_bigint * 2 ELSE 0 END), " +
                        "CAST(sum(CASE WHEN col_bigint = 1 THEN CAST(col_bigint * 2 AS INTEGER) ELSE CAST(0 AS INTEGER) END) AS VARCHAR(10)), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_bigint * 2 ELSE null END), " +
                        "min(CASE WHEN col_bigint % 2 > 1.23 THEN col_bigint * 2 END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CAST(CASE WHEN col_bigint = 4 THEN col_decimal * 2 END AS BIGINT)) " +
                        "FROM t",
                anyTree(
                        project(
                                ImmutableMap.of("SUM_2_CAST", expression(new Cast(new Reference(BIGINT, "SUM_2"), createVarcharType(10)))),
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.<Optional<String>, ExpectedValueProvider<AggregationFunction>>builder()
                                                .put(Optional.of("SUM_1"), aggregationFunction("sum", ImmutableList.of("SUM_1_INPUT")))
                                                .put(Optional.of("SUM_2"), aggregationFunction("sum", ImmutableList.of("SUM_2_INPUT")))
                                                .put(Optional.of("SUM_3"), aggregationFunction("sum", ImmutableList.of("SUM_3_INPUT")))
                                                .put(Optional.of("MIN_1"), aggregationFunction("min", ImmutableList.of("MIN_1_INPUT")))
                                                .put(Optional.of("SUM_4"), aggregationFunction("sum", ImmutableList.of("SUM_4_INPUT")))
                                                .put(Optional.of("SUM_5"), aggregationFunction("sum", ImmutableList.of("SUM_5_INPUT")))
                                                .buildOrThrow(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("SUM_1_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Reference(BIGINT, "SUM_BIGINT"))), new Constant(BIGINT, 0L))))
                                                        .put("SUM_2_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Reference(BIGINT, "SUM_INT_CAST"))), new Constant(BIGINT, 0L))))
                                                        .put("SUM_3_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L)), new Reference(BIGINT, "SUM_BIGINT"))), new Constant(BIGINT, null))))
                                                        .put("MIN_1_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 1L)), new Reference(BIGINT, "MIN_BIGINT"))), new Constant(BIGINT, null))))
                                                        .put("SUM_4_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 3L)), new Reference(createDecimalType(38, 1), "SUM_DECIMAL"))), new Constant(createDecimalType(38, 1), null))))
                                                        .put("SUM_5_INPUT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 4L)), new Reference(BIGINT, "SUM_DECIMAL_CAST"))), new Constant(BIGINT, null))))
                                                        .buildOrThrow(),
                                                aggregation(
                                                        singleGroupingSet("COL_BIGINT"),
                                                        ImmutableMap.of(
                                                                Optional.of("SUM_BIGINT"), aggregationFunction("sum", ImmutableList.of("VALUE_BIGINT")),
                                                                Optional.of("SUM_INT_CAST"), aggregationFunction("sum", ImmutableList.of("VALUE_INT_CAST")),
                                                                Optional.of("MIN_BIGINT"), aggregationFunction("min", ImmutableList.of("VALUE_2_INT_CAST")),
                                                                Optional.of("SUM_DECIMAL"), aggregationFunction("sum", ImmutableList.of("COL_DECIMAL")),
                                                                Optional.of("SUM_DECIMAL_CAST"), aggregationFunction("sum", ImmutableList.of("VALUE_DECIMAL_CAST"))),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        exchange(
                                                                project(ImmutableMap.of(
                                                                                "VALUE_BIGINT", expression(new Case(ImmutableList.of(new WhenClause(new In(new Reference(BIGINT, "COL_BIGINT"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))))), new Constant(BIGINT, null))),
                                                                                "VALUE_INT_CAST", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Cast(new Cast(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))), INTEGER), BIGINT))), new Constant(BIGINT, null))),
                                                                                "VALUE_2_INT_CAST", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 1L)), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L))))), new Constant(BIGINT, null))),
                                                                                "VALUE_DECIMAL_CAST", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 4L)), new Cast(new Call(MULTIPLY_DECIMAL_10_0, ImmutableList.of(new Reference(createDecimalType(10, 0), "COL_DECIMAL"), new Constant(createDecimalType(10, 0), Decimals.valueOfShort(new BigDecimal("2"))))), BIGINT))), new Constant(BIGINT, null)))),
                                                                        tableScan(
                                                                                "t",
                                                                                ImmutableMap.of(
                                                                                        "COL_BIGINT", "col_bigint",
                                                                                        "COL_DECIMAL", "col_decimal"))))))))));
    }

    @Test
    public void testPreAggregatesWithDefaultValues()
    {
        assertPlan(
                "SELECT " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_bigint ELSE BIGINT '0' END), " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_bigint END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN CAST(col_bigint AS INTEGER) ELSE CAST(0 AS INTEGER) END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN CAST(col_bigint AS INTEGER) END) " +
                        "FROM t",
                anyTree(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<AggregationFunction>>builder()
                                        .put(Optional.of("SUM_1"), aggregationFunction("sum", ImmutableList.of("SUM_BIGINT_FINAL")))
                                        .put(Optional.of("SUM_1_DEFAULT"), aggregationFunction("sum", ImmutableList.of("SUM_BIGINT_FINAL_DEFAULT")))
                                        .put(Optional.of("SUM_2"), aggregationFunction("sum", ImmutableList.of("SUM_INT_CAST_FINAL")))
                                        .put(Optional.of("SUM_2_DEFAULT"), aggregationFunction("sum", ImmutableList.of("SUM_INT_CAST_FINAL_DEFAULT")))
                                        .buildOrThrow(),
                                Optional.empty(),
                                SINGLE,
                                project(ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("SUM_BIGINT_FINAL", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Reference(BIGINT, "SUM_BIGINT"))), new Constant(BIGINT, null))))
                                                .put("SUM_BIGINT_FINAL_DEFAULT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 1L)), new Reference(BIGINT, "SUM_BIGINT"))), new Constant(BIGINT, 0L))))
                                                .put("SUM_INT_CAST_FINAL", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L)), new Reference(BIGINT, "SUM_INT_CAST"))), new Constant(BIGINT, null))))
                                                .put("SUM_INT_CAST_FINAL_DEFAULT", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L)), new Reference(BIGINT, "SUM_INT_CAST"))), new Constant(BIGINT, 0L))))
                                                .buildOrThrow(),
                                        aggregation(
                                                singleGroupingSet("COL_BIGINT"),
                                                ImmutableMap.of(
                                                        Optional.of("SUM_BIGINT"), aggregationFunction("sum", ImmutableList.of("COL_BIGINT")),
                                                        Optional.of("SUM_INT_CAST"), aggregationFunction("sum", ImmutableList.of("VALUE_INT_CAST"))),
                                                Optional.empty(),
                                                SINGLE,
                                                exchange(
                                                        project(ImmutableMap.of(
                                                                        "VALUE_INT_CAST", expression(new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "COL_BIGINT"), new Constant(BIGINT, 2L)), new Cast(new Cast(new Reference(BIGINT, "COL_BIGINT"), INTEGER), BIGINT))), new Constant(BIGINT, null)))),
                                                                tableScan(
                                                                        "t",
                                                                        ImmutableMap.of(
                                                                                "COL_BIGINT", "col_bigint")))))))));
    }

    @Test
    public void testPreAggregatesSumAggregationsWithZeroDefault()
    {
        assertFires("" +
                "SELECT " +
                "col_varchar, " +
                "sum(CASE WHEN col_bigint = 1 THEN col_bigint ELSE BIGINT '0' END), " +
                "sum(CASE WHEN col_bigint = 2 THEN col_tinyint ELSE TINYINT '0' END), " +
                "sum(CASE WHEN col_bigint = 3 THEN col_double ELSE DOUBLE '0' END), " +
                "sum(CASE WHEN col_bigint = 4 THEN col_decimal ELSE DECIMAL '0.0' END), " +
                "sum(CASE WHEN col_bigint = 5 THEN col_long_decimal ELSE DECIMAL '0.000000000000000000' END) " +
                "FROM t " +
                "GROUP BY col_varchar");
    }

    @Test
    public void testPreAggregatesWithoutNewExtraGroupingKeys()
    {
        assertFires("" +
                "SELECT " +
                "col_bigint, " +
                "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                "sum(CASE WHEN col_bigint = 4 THEN col_decimal END) " +
                "FROM t " +
                "GROUP BY col_bigint");
    }

    @Test
    public void testDoesNotFireWithGroupingSets()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "col_bigint, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 4 THEN col_decimal END) " +
                        "FROM t " +
                        "GROUP BY GROUPING SETS ((col_varchar), (col_bigint))");
    }

    @Test
    public void testDoesNotFireWithoutEnoughAggregations()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    @Test
    public void testDoesNotFireWithMultipleExtraGroupingKeys()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CASE WHEN col_decimal = DECIMAL '4.1' THEN col_decimal END) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    @Test
    public void testDoesNotFireForSearchedCaseExpressionWithMultipleWithClauses()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 4 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 5 THEN col_decimal WHEN col_bigint = 6 THEN col_decimal * 2 END) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    @Test
    public void testDoesNotFireForNonCumulativeAggregation()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "count(CASE WHEN col_bigint = 4 THEN col_decimal END) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    @Test
    public void testDoesNotFireForSumAggregationWithNonZeroDefaultValue()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 4 THEN col_decimal ELSE 1 END) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    @Test
    public void testDoesNotFireForMinAggregationWithNonNullDefaultValue()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "min(CASE WHEN col_bigint = 4 THEN col_decimal ELSE 0 END) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    @Test
    public void testDoesNotFireForNonCaseAggregation()
    {
        assertThatDoesNotFire(
                "SELECT " +
                        "col_varchar, " +
                        "sum(CASE WHEN col_bigint = 1 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 2 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 4 THEN col_decimal END), " +
                        "sum(col_decimal) " +
                        "FROM t " +
                        "GROUP BY col_varchar");
    }

    private void assertFires(@Language("SQL") String query)
    {
        assertThat(countOfMatchingNodes(plan(query), AggregationNode.class::isInstance)).isEqualTo(2);
    }

    private void assertThatDoesNotFire(@Language("SQL") String query)
    {
        assertThat(countOfMatchingNodes(plan(query), AggregationNode.class::isInstance)).isEqualTo(1);
    }

    private static int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        return searchFrom(plan.getRoot()).where(predicate).count();
    }
}
