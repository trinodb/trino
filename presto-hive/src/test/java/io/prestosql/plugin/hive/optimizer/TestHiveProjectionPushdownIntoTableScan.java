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
package io.prestosql.plugin.hive.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.prestosql.plugin.hive.testing.TestingHiveConnectorFactory;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.sql.planner.assertions.BasePushdownPlanTest;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.any;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestHiveProjectionPushdownIntoTableScan
        extends BasePushdownPlanTest
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
        baseDir = Files.createTempDir();
        HdfsConfig config = new HdfsConfig();
        FileHiveMetastoreConfig metastoreConfig = new FileHiveMetastoreConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        HiveMetastore metastore = new FileHiveMetastore(environment, baseDir.toURI().toString(), "test", metastoreConfig.isAssumeCanonicalPartitionKeys());
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();

        metastore.createDatabase(new HiveIdentity(HIVE_SESSION.toConnectorSession()), database);

        LocalQueryRunner queryRunner = LocalQueryRunner.create(HIVE_SESSION);
        queryRunner.createCatalog(HIVE_CATALOG_NAME, new TestingHiveConnectorFactory(metastore), ImmutableMap.of());

        return queryRunner;
    }

    @Test
    public void testPushdownDisabled()
    {
        String testTable = "test_disabled_pushdown";

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG_NAME, "projection_pushdown_enabled", "false")
                .build();

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0) AS" +
                        " SELECT cast(row(5, 6) as row(a bigint, b bigint)) AS col0 WHERE false",
                testTable));

        assertPlan(
                format("SELECT col0.a expr_a, col0.b expr_b FROM %s", testTable),
                session,
                any(
                        project(
                                ImmutableMap.of("expr", expression("col0.a"), "expr_2", expression("col0.b")),
                                tableScan(testTable, ImmutableMap.of("col0", "col0")))));
    }

    @Test
    public void testDereferencePushdown()
    {
        String testTable = "test_simple_projection_pushdown";
        QualifiedObjectName completeTableName = new QualifiedObjectName(HIVE_CATALOG_NAME, SCHEMA_NAME, testTable);

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0, col1) AS" +
                        " SELECT cast(row(5, 6) as row(x bigint, y bigint)) AS col0, 5 AS col1 WHERE false",
                testTable));

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertTrue(tableHandle.isPresent(), "expected the table handle to be present");

        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        HiveColumnHandle column0Handle = (HiveColumnHandle) columns.get("col0");
        HiveColumnHandle column1Handle = (HiveColumnHandle) columns.get("col1");

        HiveColumnHandle columnX = createProjectedColumnHandle(column0Handle, ImmutableList.of(0));
        HiveColumnHandle columnY = createProjectedColumnHandle(column0Handle, ImmutableList.of(1));

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + testTable,
                any(tableScan(
                        equalTo(tableHandle.get().getConnectorHandle()),
                        TupleDomain.all(),
                        ImmutableMap.of("col0#x", equalTo(columnX), "col0#y", equalTo(columnY)))));

        // Projection and predicate pushdown
        assertPlan(
                format("SELECT col0.x FROM %s WHERE col0.x = col1 + 3 and col0.y = 2", testTable),
                anyTree(
                        filter(
                                "col0_y = BIGINT '2' AND (col0_x =  cast((col1 + 3) as BIGINT))",
                                tableScan(
                                        table -> ((HiveTableHandle) table).getCompactEffectivePredicate().getDomains().get()
                                                .equals(ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L))),
                                        TupleDomain.all(),
                                        ImmutableMap.of("col0_y", equalTo(columnY), "col0_x", equalTo(columnX), "col1", equalTo(column1Handle))))));

        // Projection and predicate pushdown with overlapping columns
        assertPlan(
                format("SELECT col0, col0.y expr_y FROM %s WHERE col0.x = 5", testTable),
                anyTree(
                        filter(
                                "col0_x = BIGINT '5'",
                                tableScan(
                                        table -> ((HiveTableHandle) table).getCompactEffectivePredicate().getDomains().get()
                                                .equals(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L))),
                                        TupleDomain.all(),
                                        ImmutableMap.of("col0", equalTo(column0Handle), "col0_x", equalTo(columnX))))));

        // Projection and predicate pushdown with joins
        assertPlan(
                format("SELECT T.col0.x, T.col0, T.col0.y FROM %s T join %s S on T.col1 = S.col1 WHERE (T.col0.x = 2)", testTable, testTable),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "expr_0_x", expression("expr_0.x"),
                                        "expr_0", expression("expr_0"),
                                        "expr_0_y", expression("expr_0.y")),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("t_expr_1", "s_expr_1")),
                                        anyTree(
                                                filter(
                                                        "expr_0_x = BIGINT '2'",
                                                        tableScan(
                                                                table -> ((HiveTableHandle) table).getCompactEffectivePredicate().getDomains().get()
                                                                        .equals(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L))),
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("expr_0_x", equalTo(columnX), "expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle))))),
                                        anyTree(
                                                tableScan(
                                                        equalTo(tableHandle.get().getConnectorHandle()),
                                                        TupleDomain.all(),
                                                        ImmutableMap.of("s_expr_1", equalTo(column1Handle))))))));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
