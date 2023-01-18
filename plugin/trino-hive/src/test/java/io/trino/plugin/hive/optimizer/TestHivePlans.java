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

import io.trino.Session;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestHivePlans
        extends BasePlanTest
{
    private static final String HIVE_CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "test_schema";

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(HIVE_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .build();

    private File baseDir;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        try {
            baseDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();

        metastore.createDatabase(database);

        return createQueryRunner(HIVE_SESSION, metastore);
    }

    protected LocalQueryRunner createQueryRunner(Session session, HiveMetastore metastore)
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);
        queryRunner.createCatalog(HIVE_CATALOG_NAME, new TestingHiveConnectorFactory(metastore), Map.of("hive.max-partitions-for-eager-load", "5"));
        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        QueryRunner queryRunner = getQueryRunner();

        // Use common VALUES for setup so that types are the same and there are no coercions.
        String values = "VALUES ('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)";

        // partitioned on integer
        queryRunner.execute("CREATE TABLE table_int_partitioned WITH (partitioned_by = ARRAY['int_part']) AS SELECT str_col, int_part FROM (" + values + ") t(str_col, int_part)");

        // partitioned on varchar
        queryRunner.execute("CREATE TABLE table_str_partitioned WITH (partitioned_by = ARRAY['str_part']) AS SELECT int_col, str_part FROM (" + values + ") t(str_part, int_col)");

        // with too many partitions
        queryRunner.execute("CREATE TABLE table_int_with_too_many_partitions WITH (partitioned_by = ARRAY['int_part']) AS SELECT str_col, int_part FROM (" + values + ", ('six', 6)) t(str_col, int_part)");

        // unpartitioned
        queryRunner.execute("CREATE TABLE table_unpartitioned AS SELECT str_col, int_col FROM (" + values + ") t(str_col, int_col)");
    }

    @AfterClass(alwaysRun = true)
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
                        filter("\"$like\"(STR_PART, \"$literal$\"(from_base64('DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAAHAAAAAAcAAAACAAAAdCUA')))",
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
                                                project(
                                                        filter("\"$like\"(L_STR_PART, \"$literal$\"(from_base64('DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAAHAAAAAAcAAAACAAAAdCUA')))",
                                                                tableScan("table_str_partitioned", Map.of("L_INT_COL", "int_col", "L_STR_PART", "str_part"))))))
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPARTITION,
                                                project(
                                                        filter("R_STR_COL IN ('three', CAST('two' AS varchar(5))) AND \"$like\"(R_STR_COL, \"$literal$\"(from_base64('DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAAHAAAAAAcAAAACAAAAdCUA')))",
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))))));
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
                                                project(
                                                        filter("true", // dynamic filter
                                                                tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col"))))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        project(
                                                                filter("R_INT_COL IN (2, 3, 4)",
                                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))))));
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
                                                project(
                                                        filter("L_STR_COL != 'three'",
                                                                tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col"))))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        project(
                                                                filter("R_INT_COL IN (2, 3, 4) AND R_INT_COL BETWEEN 2 AND 4", // TODO: R_INT_COL BETWEEN 2 AND 4 is redundant
                                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))))));
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
                                                project(
                                                        filter("substring(L_STR_COL, BIGINT '2') != CAST('hree' AS varchar(5))",
                                                                tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col"))))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        project(
                                                                filter("R_INT_COL IN (2, 3, 4) AND R_INT_COL BETWEEN 2 AND 4", // TODO: R_INT_COL BETWEEN 2 AND 4 is redundant
                                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))))));
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
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                project(
                                                        filter("L_INT_PART % 2 = 0",
                                                                tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col"))))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        project(
                                                                filter("R_INT_COL IN (2, 4) AND R_INT_COL % 2 = 0",
                                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))))));
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
                                                project(
                                                        filter("true", //dynamic filter
                                                                tableScan("table_int_partitioned", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col"))))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION,
                                                        project(
                                                                filter("R_INT_COL IN (1, 2, 3, 4, 5)",
                                                                        tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col"))))))))));
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
                                        project(
                                                filter("true", //dynamic filter
                                                        tableScan("table_int_with_too_many_partitions", Map.of("L_INT_PART", "int_part", "L_STR_COL", "str_col")))))
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPLICATE,
                                                        project(
                                                                tableScan("table_unpartitioned", Map.of("R_STR_COL", "str_col", "R_INT_COL", "int_col")))))))));
    }

    // Disable join ordering so that expected plans are well defined.
    private Session noJoinReordering()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }
}
