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
package io.trino.plugin.hive.optimizer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.function.OperatorType;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.PlanTester;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.LikePatternType.LIKE_PATTERN;

public class TestHivePlans
        extends BasePlanTest
{
    private static final String HIVE_CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "test_schema";

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(HIVE_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction LIKE = FUNCTIONS.resolveFunction("$like", fromTypes(createVarcharType(5), LIKE_PATTERN));
    private static final ResolvedFunction SUBSTRING = FUNCTIONS.resolveFunction("substring", fromTypes(createVarcharType(5), BIGINT));
    private static final ResolvedFunction MODULUS_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(INTEGER, INTEGER));

    private File baseDir;

    @Override
    protected PlanTester createPlanTester()
    {
        try {
            baseDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        PlanTester planTester = PlanTester.create(HIVE_SESSION);
        planTester.createCatalog(HIVE_CATALOG_NAME, new TestingHiveConnectorFactory(baseDir.toPath()), Map.of("hive.max-partitions-for-eager-load", "5"));

        HiveMetastore metastore = getConnectorService(planTester, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        metastore.createDatabase(Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build());

        return planTester;
    }

    @BeforeAll
    public void setUp()
    {
        PlanTester planTester = getPlanTester();

        // Use common VALUES for setup so that types are the same and there are no coercions.
        String values = "VALUES ('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)";

        // partitioned on integer
        planTester.executeStatement("CREATE TABLE table_int_partitioned WITH (partitioned_by = ARRAY['int_part']) AS SELECT str_col, int_part FROM (" + values + ") t(str_col, int_part)");

        // partitioned on varchar
        planTester.executeStatement("CREATE TABLE table_str_partitioned WITH (partitioned_by = ARRAY['str_part']) AS SELECT int_col, str_part FROM (" + values + ") t(str_part, int_col)");

        // with too many partitions
        planTester.executeStatement("CREATE TABLE table_int_with_too_many_partitions WITH (partitioned_by = ARRAY['int_part']) AS SELECT str_col, int_part FROM (" + values + ", ('six', 6)) t(str_col, int_part)");

        // unpartitioned
        planTester.executeStatement("CREATE TABLE table_unpartitioned AS SELECT str_col, int_col FROM (" + values + ") t(str_col, int_col)");
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testPruneSimplePartitionLikeFilter()
    {
        assertDistributedPlan(
                "SELECT * FROM table_str_partitioned WHERE str_part LIKE 't%'",
                output(
                        filter(
                                new Call(
                                        FUNCTIONS.resolveFunction("$like", fromTypes(createVarcharType(55), LIKE_PATTERN)),
                                        ImmutableList.of(new Reference(createVarcharType(55), "STR_PART"), new Constant(LIKE_PATTERN, LikePattern.compile("t%", Optional.empty())))),
                                tableScan("table_str_partitioned", Map.of("INT_COL", "int_col", "STR_PART", "str_part")))));
    }

    @Test
    public void testPrunePartitionLikeFilter()
    {
        // LIKE predicate is partially convertible to a TupleDomain: (p LIKE 't%') implies (p BETWEEN 't' AND 'u').
        // Such filter is more likely to cause optimizer to loop, as the connector can try to enforce the predicate, but will never see the actual one.

        // Test that the partition filter is fully subsumed into the partitioned table, while also being propagated into the other Join side.
        // Join is important because it triggers PredicatePushDown logic (EffectivePredicateExtractor)
        assertDistributedPlan(
                "SELECT l.int_col, r.int_col FROM table_str_partitioned l JOIN table_unpartitioned r ON l.str_part = r.str_col " +
                        "WHERE l.str_part LIKE 't%'",
                noJoinReordering(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_STR_PART", "R_STR_COL")
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                filter(
                                                        new Call(LIKE, ImmutableList.of(new Reference(createVarcharType(5), "L_STR_PART"), new Constant(LIKE_PATTERN, LikePattern.compile("t%", Optional.empty())))),
                                                        tableScan("table_str_partitioned", Map.of("L_INT_COL", "int_col", "L_STR_PART", "str_part")))))
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPARTITION,
                                                filter(
                                                        new Logical(AND, ImmutableList.of(new In(new Reference(createVarcharType(5), "R_STR_COL"), ImmutableList.of(new Constant(createVarcharType(5), Slices.utf8Slice("three")), new Constant(createVarcharType(5), Slices.utf8Slice("two")))), new Call(LIKE, ImmutableList.of(new Reference(createVarcharType(5), "R_STR_COL"), new Constant(LIKE_PATTERN, LikePattern.compile("t%", Optional.empty())))))),
                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    @Test
    public void testSubsumePartitionFilter()
    {
        // Test that the partition filter is fully subsumed into the partitioned table, while also being propagated into the other Join side.
        // Join is important because it triggers PredicatePushDown logic (EffectivePredicateExtractor)
        assertDistributedPlan(
                "SELECT l.str_col, r.str_col FROM table_int_partitioned l JOIN table_unpartitioned r ON l.int_part = r.int_col " +
                        "WHERE l.int_part BETWEEN 2 AND 4",
                noJoinReordering(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_INT_PART", "R_INT_COL")
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                filter(
                                                        TRUE,
                                                        tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col")))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        filter(
                                                                new In(new Reference(INTEGER, "R_INT_COL"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L))),
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    @Test
    public void testSubsumePartitionPartOfAFilter()
    {
        // Test that the partition filter is fully subsumed into the partitioned table, while also being propagated into the other Join side, in the presence
        // of other pushdown-able filter.
        // Join is important because it triggers PredicatePushDown logic (EffectivePredicateExtractor)
        assertDistributedPlan(
                "SELECT l.str_col, r.str_col FROM table_int_partitioned l JOIN table_unpartitioned r ON l.int_part = r.int_col " +
                        "WHERE l.int_part BETWEEN 2 AND 4 AND l.str_col != 'three'",
                noJoinReordering(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_INT_PART", "R_INT_COL")
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                filter(
                                                        new Comparison(NOT_EQUAL, new Reference(createVarcharType(5), "L_STR_COL"), new Constant(createVarcharType(5), Slices.utf8Slice("three"))),
                                                        tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col")))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        filter(
                                                                new In(new Reference(INTEGER, "R_INT_COL"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L))),
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    @Test
    public void testSubsumePartitionPartWhenOtherFilterNotConvertibleToTupleDomain()
    {
        // Test that the partition filter is fully subsumed into the partitioned table, while also being propagated into the other Join side, in the presence
        // a non pushdown-able filter.
        // Join is important because it triggers PredicatePushDown logic (EffectivePredicateExtractor)
        assertDistributedPlan(
                "SELECT l.str_col, r.str_col FROM table_int_partitioned l JOIN table_unpartitioned r ON l.int_part = r.int_col " +
                        "WHERE l.int_part BETWEEN 2 AND 4 AND substring(l.str_col, 2) != 'hree'",
                noJoinReordering(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_INT_PART", "R_INT_COL")
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                filter(
                                                        new Comparison(NOT_EQUAL, new Call(SUBSTRING, ImmutableList.of(new Reference(createVarcharType(5), "L_STR_COL"), new Constant(BIGINT, 2L))), new Constant(createVarcharType(5), Slices.utf8Slice("hree"))),
                                                        tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col")))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        filter(
                                                                new In(new Reference(INTEGER, "R_INT_COL"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L))),
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    @Test
    public void testSubsumePartitionFilterNotConvertibleToTupleDomain()
    {
        // Test that the partition filter is fully subsumed into the partitioned table, while also being propagated into the other Join side, in the presence
        // of an enforceable partition filter that is not convertible to a TupleDomain
        // Join is important because it triggers PredicatePushDown logic (EffectivePredicateExtractor)
        assertDistributedPlan(
                "SELECT l.str_col, r.str_col FROM table_int_partitioned l JOIN table_unpartitioned r ON l.int_part = r.int_col " +
                        "WHERE l.int_part BETWEEN 2 AND 4 AND l.int_part % 2 = 0",
                noJoinReordering(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_INT_PART", "R_INT_COL")
                                .filter(new Comparison(EQUAL, new Call(MODULUS_INTEGER, ImmutableList.of(new Reference(INTEGER, "R_INT_COL"), new Constant(INTEGER, 2L))), new Constant(INTEGER, 0L)))
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                any(
                                                        tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col")))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        filter(
                                                                new In(new Reference(INTEGER, "R_INT_COL"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L))),
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    @Test
    public void testFilterDerivedFromTableProperties()
    {
        // Test that the filter is on build side table is derived from table properties
        assertDistributedPlan(
                "SELECT l.str_col, r.str_col FROM table_int_partitioned l JOIN table_unpartitioned r ON l.int_part = r.int_col",
                noJoinReordering(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_INT_PART", "R_INT_COL")
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                filter(
                                                        TRUE,
                                                        tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col")))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        filter(
                                                                new In(new Reference(INTEGER, "R_INT_COL"), ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))),
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    @Test
    public void testQueryScanningForTooManyPartitions()
    {
        String query = "SELECT l.str_col, r.str_col FROM table_int_with_too_many_partitions l JOIN table_unpartitioned r ON l.int_part = r.int_col";
        assertDistributedPlan(
                query,
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_INT_PART", "R_INT_COL")
                                .left(
                                        filter(
                                                TRUE,
                                                tableScan("table_int_with_too_many_partitions", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col"))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPLICATE,
                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))));
    }

    // Disable join ordering so that expected plans are well defined.
    private Session noJoinReordering()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }
}
