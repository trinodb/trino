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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestIcebergProjectionPushdown
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "schema";
    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(SCHEMA)
            .build();

    private File metastoreDir;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        this.metastoreDir = Files.createTempDir();
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("test_version"),
                environment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(metastoreDir.toURI().toString())
                        .setMetastoreUser("test"));
        Database database = Database.builder()
                .setDatabaseName(SCHEMA)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();

        metastore.createDatabase(new HiveIdentity(SESSION.toConnectorSession()), database);

        LocalQueryRunner queryRunner = LocalQueryRunner.create(SESSION);
        queryRunner.createCatalog(
                CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(metastore), false),
                ImmutableMap.of());

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (metastoreDir != null) {
            deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
        }
    }

    @DataProvider(name = "format")
    public static Object[][] formatProvider()
    {
        return new Object[][] {{"PARQUET"}, {"ORC"}};
    }

    @Test(dataProvider = "format")
    public void testPushdownDisabled(String format)
    {
        String testTable = "test_disabled_pushdown" + randomTableSuffix();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(CATALOG, "projection_pushdown_enabled", "false")
                .build();

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0) WITH (format = '%s') AS" +
                        " SELECT cast(row(5, 6) as row(a bigint, b bigint)) AS col0 WHERE false",
                testTable,
                format));

        assertPlan(
                format("SELECT col0.a expr_a, col0.b expr_b FROM %s", testTable),
                session,
                any(
                        project(
                                ImmutableMap.of("expr", expression("col0[1]"), "expr_2", expression("col0[2]")),
                                tableScan(testTable, ImmutableMap.of("col0", "col0")))));
    }

    @Test(dataProvider = "format")
    public void testDereferencePushdown(String format)
    {
        String testTable = "test_simple_projection_pushdown" + randomTableSuffix();
        QualifiedObjectName completeTableName = new QualifiedObjectName(CATALOG, SCHEMA, testTable);

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0, col1) with (partitioning = ARRAY['col1'], format = '" + format + "') AS" +
                        " SELECT cast(row(5, 6) as row(x bigint, y bigint)) AS col0, 5 AS col1 WHERE false",
                testTable));

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertTrue(tableHandle.isPresent(), "expected the table handle to be present");

        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        IcebergColumnHandle column0Handle = (IcebergColumnHandle) columns.get("col0");
        IcebergColumnHandle column1Handle = (IcebergColumnHandle) columns.get("col1");

        IcebergColumnHandle columnX = new IcebergColumnHandle(
                column0Handle.getColumnIdentity().getChildren().get(0),
                BIGINT,
                column0Handle.getColumnIdentity(),
                column0Handle.getType(),
                Optional.empty(), ImmutableList.of("col0"));
        IcebergColumnHandle columnY = new IcebergColumnHandle(
                column0Handle.getColumnIdentity().getChildren().get(1),
                BIGINT,
                column0Handle.getColumnIdentity(),
                column0Handle.getType(),
                Optional.empty(), ImmutableList.of("col0"));

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + testTable,
                any(tableScan(
                        equalTo(withProjectedColumns(((IcebergTableHandle) tableHandle.get().getConnectorHandle()), ImmutableSet.of(columnX, columnY))),
                        TupleDomain.all(),
                        ImmutableMap.of("col0#x", equalTo(columnX), "col0#y", equalTo(columnY)))));

        // Projection and predicate pushdown
        assertPlan(
                format("SELECT col0.x FROM %s WHERE col0.x = col1 + 3 and col0.y = 2", testTable),
                anyTree(
                        filter(
                                "y = BIGINT '2' AND (x =  cast((col1 + 3) as BIGINT))",
                                tableScan(
                                        table -> {
                                            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
                                            return icebergTableHandle.getProjectedColumns().equals(Optional.of(
                                                    ImmutableSet.of(column1Handle, columnX, columnY)));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of("y", equalTo(columnY), "x", equalTo(columnX), "col1", equalTo(column1Handle))))));

        // Projection and predicate pushdown with overlapping columns
        assertPlan(
                format("SELECT col0, col0.y expr_y FROM %s WHERE col0.x = 5", testTable),
                anyTree(
                        filter(
                                "x = BIGINT '5'",
                                tableScan(
                                        table -> {
                                            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
                                            return icebergTableHandle.getProjectedColumns().equals(Optional.of(
                                                    ImmutableSet.of(column0Handle, columnX)));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of("col0", equalTo(column0Handle), "x", equalTo(columnX))))));

        // Projection and predicate pushdown with joins
        assertPlan(
                format("SELECT T.col0.x, T.col0, T.col0.y FROM %s T join %s S on T.col1 = S.col1 WHERE (T.col0.x = 2)", testTable, testTable),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "expr_0_x", expression("expr_0[1]"),
                                        "expr_0", expression("expr_0"),
                                        "expr_0_y", expression("expr_0[2]")),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("t_expr_1", "s_expr_1")),
                                        anyTree(
                                                filter(
                                                        "x = BIGINT '2'",
                                                        tableScan(
                                                                table -> true,
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("x", equalTo(columnX), "expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle))))),
                                        anyTree(
                                                tableScan(
                                                        equalTo(withProjectedColumns((IcebergTableHandle) tableHandle.get().getConnectorHandle(), ImmutableSet.of(column1Handle))),
                                                        TupleDomain.all(),
                                                        ImmutableMap.of("s_expr_1", equalTo(column1Handle))))))));
    }

    private static IcebergTableHandle withProjectedColumns(IcebergTableHandle originalTableHandle, Set<ColumnHandle> projectedColumns)
    {
        return new IcebergTableHandle(
                originalTableHandle.getSchemaName(),
                originalTableHandle.getTableName(),
                originalTableHandle.getTableType(),
                originalTableHandle.getSnapshotId(),
                originalTableHandle.getUnenforcedPredicate(),
                originalTableHandle.getEnforcedPredicate(),
                Optional.of(projectedColumns));
    }
}
