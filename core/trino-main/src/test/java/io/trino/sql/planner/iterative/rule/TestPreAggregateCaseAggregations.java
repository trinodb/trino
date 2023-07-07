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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.FunctionCall;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.trino.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
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
    private static final SchemaTableName TABLE = new SchemaTableName("default", "t");

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false") // remove hash computing projections for simplicity
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false") // remove partial aggregations for simplicity
                .setSystemProperty(TASK_CONCURRENCY, "1"); // these tests don't handle exchanges from local parallel

        LocalQueryRunner queryRunner = LocalQueryRunner.builder(sessionBuilder.build())
                .build();

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
                            new ColumnMetadata("col_decimal", DecimalType.createDecimalType(2, 1)),
                            new ColumnMetadata("col_long_decimal", DecimalType.createDecimalType(19, 18)),
                            new ColumnMetadata("col_double", DoubleType.DOUBLE));
                });
        queryRunner.createCatalog("local", builder.build(), ImmutableMap.of());

        return queryRunner;
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
                                ImmutableMap.of("SUM_2_CAST", expression("CAST(SUM_2 AS VARCHAR(10))")),
                                aggregation(
                                        singleGroupingSet("KEY"),
                                        ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                                .put(Optional.of("SUM_1"), functionCall("sum", ImmutableList.of("SUM_1_INPUT")))
                                                .put(Optional.of("SUM_2"), functionCall("sum", ImmutableList.of("SUM_2_INPUT")))
                                                .put(Optional.of("SUM_3"), functionCall("sum", ImmutableList.of("SUM_3_INPUT")))
                                                .put(Optional.of("MIN_1"), functionCall("min", ImmutableList.of("MIN_1_INPUT")))
                                                .put(Optional.of("SUM_4"), functionCall("sum", ImmutableList.of("SUM_4_INPUT")))
                                                .put(Optional.of("SUM_5"), functionCall("sum", ImmutableList.of("SUM_5_INPUT")))
                                                .buildOrThrow(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("SUM_1_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '1' THEN SUM_BIGINT ELSE BIGINT '0' END"))
                                                        .put("SUM_2_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '1' THEN SUM_INT_CAST ELSE BIGINT '0' END"))
                                                        .put("SUM_3_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '2' THEN SUM_BIGINT END"))
                                                        .put("MIN_1_INPUT", expression("CASE WHEN COL_BIGINT % BIGINT '2' > BIGINT '1' THEN MIN_BIGINT END"))
                                                        .put("SUM_4_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '3' THEN SUM_DECIMAL END"))
                                                        .put("SUM_5_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '4' THEN SUM_DECIMAL_CAST END"))
                                                        .buildOrThrow(),
                                                aggregation(
                                                        singleGroupingSet("KEY", "COL_BIGINT"),
                                                        ImmutableMap.of(
                                                                Optional.of("SUM_BIGINT"), functionCall("sum", ImmutableList.of("VALUE_BIGINT")),
                                                                Optional.of("SUM_INT_CAST"), functionCall("sum", ImmutableList.of("VALUE_INT_CAST")),
                                                                Optional.of("MIN_BIGINT"), functionCall("min", ImmutableList.of("VALUE_BIGINT")),
                                                                Optional.of("SUM_DECIMAL"), functionCall("sum", ImmutableList.of("COL_DECIMAL")),
                                                                Optional.of("SUM_DECIMAL_CAST"), functionCall("sum", ImmutableList.of("VALUE_DECIMAL_CAST"))),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        exchange(
                                                                project(ImmutableMap.of(
                                                                                "KEY", expression("CONCAT(COL_VARCHAR, VARCHAR 'a')"),
                                                                                "VALUE_BIGINT", expression("COL_BIGINT * BIGINT '2'"),
                                                                                "VALUE_INT_CAST", expression("CAST(CAST(COL_BIGINT * BIGINT '2' AS INTEGER) AS BIGINT)"),
                                                                                "VALUE_DECIMAL_CAST", expression("CAST(COL_DECIMAL * CAST(DECIMAL '2' AS DECIMAL(10, 0)) AS BIGINT)")),
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
                                ImmutableMap.of("SUM_2_CAST", expression("CAST(SUM_2 AS VARCHAR(10))")),
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                                .put(Optional.of("SUM_1"), functionCall("sum", ImmutableList.of("SUM_1_INPUT")))
                                                .put(Optional.of("SUM_2"), functionCall("sum", ImmutableList.of("SUM_2_INPUT")))
                                                .put(Optional.of("SUM_3"), functionCall("sum", ImmutableList.of("SUM_3_INPUT")))
                                                .put(Optional.of("MIN_1"), functionCall("min", ImmutableList.of("MIN_1_INPUT")))
                                                .put(Optional.of("SUM_4"), functionCall("sum", ImmutableList.of("SUM_4_INPUT")))
                                                .put(Optional.of("SUM_5"), functionCall("sum", ImmutableList.of("SUM_5_INPUT")))
                                                .buildOrThrow(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("SUM_1_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '1' THEN SUM_BIGINT ELSE BIGINT '0' END"))
                                                        .put("SUM_2_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '1' THEN SUM_INT_CAST ELSE BIGINT '0' END"))
                                                        .put("SUM_3_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '2' THEN SUM_BIGINT END"))
                                                        .put("MIN_1_INPUT", expression("CASE WHEN COL_BIGINT % BIGINT '2' > BIGINT '1' THEN MIN_BIGINT END"))
                                                        .put("SUM_4_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '3' THEN SUM_DECIMAL END"))
                                                        .put("SUM_5_INPUT", expression("CASE WHEN COL_BIGINT = BIGINT '4' THEN SUM_DECIMAL_CAST END"))
                                                        .buildOrThrow(),
                                                aggregation(
                                                        singleGroupingSet("COL_BIGINT"),
                                                        ImmutableMap.of(
                                                                Optional.of("SUM_BIGINT"), functionCall("sum", ImmutableList.of("VALUE_BIGINT")),
                                                                Optional.of("SUM_INT_CAST"), functionCall("sum", ImmutableList.of("VALUE_INT_CAST")),
                                                                Optional.of("MIN_BIGINT"), functionCall("min", ImmutableList.of("VALUE_BIGINT")),
                                                                Optional.of("SUM_DECIMAL"), functionCall("sum", ImmutableList.of("COL_DECIMAL")),
                                                                Optional.of("SUM_DECIMAL_CAST"), functionCall("sum", ImmutableList.of("VALUE_DECIMAL_CAST"))),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        exchange(
                                                                project(ImmutableMap.of(
                                                                                "VALUE_BIGINT", expression("COL_BIGINT * BIGINT '2'"),
                                                                                "VALUE_INT_CAST", expression("CAST(CAST(COL_BIGINT * BIGINT '2' AS INTEGER) AS BIGINT)"),
                                                                                "VALUE_DECIMAL_CAST", expression("CAST(COL_DECIMAL * CAST(DECIMAL '2' AS DECIMAL(10, 0)) AS BIGINT)")),
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
                        "sum(CASE WHEN col_bigint = 2 THEN CAST(col_bigint AS INTEGER) END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_tinyint ELSE TINYINT '0' END), " +
                        "sum(CASE WHEN col_bigint = 3 THEN col_tinyint END), " +
                        "sum(CASE WHEN col_bigint = 4 THEN col_decimal ELSE CAST(0 AS DECIMAL(2, 1)) END), " +
                        "sum(CASE WHEN col_bigint = 4 THEN col_decimal END), " +
                        "sum(CASE WHEN col_bigint = 5 THEN col_long_decimal ELSE CAST(0 AS DECIMAL(19, 18)) END), " +
                        "sum(CASE WHEN col_bigint = 5 THEN col_long_decimal END), " +
                        "sum(CASE WHEN col_bigint = 6 THEN col_double ELSE DOUBLE '0' END), " +
                        "sum(CASE WHEN col_bigint = 6 THEN col_double END) " +
                        "FROM t",
                anyTree(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                        .put(Optional.of("SUM_1"), functionCall("sum", ImmutableList.of("SUM_BIGINT_FINAL")))
                                        .put(Optional.of("SUM_1_DEFAULT"), functionCall("sum", ImmutableList.of("SUM_BIGINT_FINAL_DEFAULT")))
                                        .put(Optional.of("SUM_2"), functionCall("sum", ImmutableList.of("SUM_INT_CAST_FINAL")))
                                        .put(Optional.of("SUM_2_DEFAULT"), functionCall("sum", ImmutableList.of("SUM_INT_CAST_FINAL_DEFAULT")))
                                        .put(Optional.of("SUM_3"), functionCall("sum", ImmutableList.of("SUM_TINYINT_FINAL")))
                                        .put(Optional.of("SUM_3_DEFAULT"), functionCall("sum", ImmutableList.of("SUM_TINYINT_FINAL_DEFAULT")))
                                        .put(Optional.of("SUM_4"), functionCall("sum", ImmutableList.of("SUM_DECIMAL_FINAL")))
                                        .put(Optional.of("SUM_4_DEFAULT"), functionCall("sum", ImmutableList.of("SUM_DECIMAL_FINAL_DEFAULT")))
                                        .put(Optional.of("SUM_5"), functionCall("sum", ImmutableList.of("SUM_LONG_DECIMAL_FINAL")))
                                        .put(Optional.of("SUM_5_DEFAULT"), functionCall("sum", ImmutableList.of("SUM_LONG_DECIMAL_FINAL_DEFAULT")))
                                        .put(Optional.of("SUM_6"), functionCall("sum", ImmutableList.of("SUM_DOUBLE_FINAL")))
                                        .put(Optional.of("SUM_6_DEFAULT"), functionCall("sum", ImmutableList.of("SUM_DOUBLE_FINAL_DEFAULT")))
                                        .buildOrThrow(),
                                Optional.empty(),
                                SINGLE,
                                project(ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("SUM_BIGINT_FINAL", expression("CASE WHEN COL_BIGINT = BIGINT '1' THEN SUM_BIGINT END"))
                                                .put("SUM_BIGINT_FINAL_DEFAULT", expression("CASE WHEN COL_BIGINT = BIGINT '1' THEN SUM_BIGINT ELSE BIGINT '0' END"))
                                                .put("SUM_INT_CAST_FINAL", expression("CASE WHEN COL_BIGINT = BIGINT '2' THEN SUM_INT_CAST END"))
                                                .put("SUM_INT_CAST_FINAL_DEFAULT", expression("CASE WHEN COL_BIGINT = BIGINT '2' THEN SUM_INT_CAST ELSE BIGINT '0' END"))
                                                .put("SUM_TINYINT_FINAL", expression("CASE WHEN COL_BIGINT = BIGINT '3' THEN SUM_TINYINT END"))
                                                .put("SUM_TINYINT_FINAL_DEFAULT", expression("CASE WHEN COL_BIGINT = BIGINT '3' THEN SUM_TINYINT ELSE BIGINT '0' END"))
                                                .put("SUM_DECIMAL_FINAL", expression("CASE WHEN COL_BIGINT = BIGINT '4' THEN SUM_DECIMAL END"))
                                                .put("SUM_DECIMAL_FINAL_DEFAULT", expression("CASE WHEN COL_BIGINT = BIGINT '4' THEN SUM_DECIMAL ELSE CAST(DECIMAL '0.0' AS decimal(38, 1)) END"))
                                                .put("SUM_LONG_DECIMAL_FINAL", expression("CASE WHEN COL_BIGINT = BIGINT '5' THEN SUM_LONG_DECIMAL END"))
                                                .put("SUM_LONG_DECIMAL_FINAL_DEFAULT", expression("CASE WHEN COL_BIGINT = BIGINT '5' THEN SUM_LONG_DECIMAL ELSE CAST(DECIMAL '0.000000000000000000' AS decimal(38, 18)) END"))
                                                .put("SUM_DOUBLE_FINAL", expression("CASE WHEN COL_BIGINT = BIGINT '6' THEN SUM_DOUBLE END"))
                                                .put("SUM_DOUBLE_FINAL_DEFAULT", expression("CASE WHEN COL_BIGINT = BIGINT '6' THEN SUM_DOUBLE ELSE 0E0 END"))
                                                .buildOrThrow(),
                                        aggregation(
                                                singleGroupingSet("COL_BIGINT"),
                                                ImmutableMap.of(
                                                        Optional.of("SUM_BIGINT"), functionCall("sum", ImmutableList.of("COL_BIGINT")),
                                                        Optional.of("SUM_INT_CAST"), functionCall("sum", ImmutableList.of("VALUE_INT_CAST")),
                                                        Optional.of("SUM_TINYINT"), functionCall("sum", ImmutableList.of("VALUE_TINYINT_CAST")),
                                                        Optional.of("SUM_DECIMAL"), functionCall("sum", ImmutableList.of("COL_DECIMAL")),
                                                        Optional.of("SUM_LONG_DECIMAL"), functionCall("sum", ImmutableList.of("COL_LONG_DECIMAL")),
                                                        Optional.of("SUM_DOUBLE"), functionCall("sum", ImmutableList.of("COL_DOUBLE"))),
                                                Optional.empty(),
                                                SINGLE,
                                                exchange(
                                                        project(ImmutableMap.of(
                                                                        "VALUE_INT_CAST", expression("CAST(CAST(COL_BIGINT AS INTEGER) AS BIGINT)"),
                                                                        "VALUE_TINYINT_CAST", expression("CAST(COL_TINYINT AS BIGINT)")),
                                                                tableScan(
                                                                        "t",
                                                                        ImmutableMap.of(
                                                                                "COL_BIGINT", "col_bigint",
                                                                                "COL_TINYINT", "col_tinyint",
                                                                                "COL_DECIMAL", "col_decimal",
                                                                                "COL_LONG_DECIMAL", "col_long_decimal",
                                                                                "COL_DOUBLE", "col_double")))))))));
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
