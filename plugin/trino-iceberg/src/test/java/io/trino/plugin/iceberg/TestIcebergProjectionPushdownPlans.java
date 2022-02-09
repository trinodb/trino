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
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
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

public class TestIcebergProjectionPushdownPlans
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "schema";
    private File metastoreDir;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        metastoreDir = Files.createTempDir();
        HiveMetastore metastore = createTestingFileHiveMetastore(metastoreDir);
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        queryRunner.createCatalog(
                CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(metastore), Optional.empty()),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

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

    @Test
    public void testPushdownDisabled()
    {
        String testTable = "test_disabled_pushdown" + randomTableSuffix();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(CATALOG, "projection_pushdown_enabled", "false")
                .build();

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0) AS SELECT CAST(row(5, 6) AS row(a bigint, b bigint)) AS col0 WHERE false",
                testTable));

        assertPlan(
                format("SELECT col0.a expr_a, col0.b expr_b FROM %s", testTable),
                session,
                any(
                        project(
                                ImmutableMap.of("expr", expression("col0[1]"), "expr_2", expression("col0[2]")),
                                tableScan(testTable, ImmutableMap.of("col0", "col0")))));
    }

    @Test
    public void testDereferencePushdown()
    {
        String testTable = "test_simple_projection_pushdown" + randomTableSuffix();
        QualifiedObjectName completeTableName = new QualifiedObjectName(CATALOG, SCHEMA, testTable);

        getQueryRunner().execute(format(
                "CREATE TABLE %s (col0, col1) WITH (partitioning = ARRAY['col1']) AS" +
                        " SELECT CAST(row(5, 6) AS row(x bigint, y bigint)) AS col0, 5 AS col1 WHERE false",
                testTable));

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertTrue(tableHandle.isPresent(), "expected the table handle to be present");

        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        IcebergColumnHandle column0Handle = (IcebergColumnHandle) columns.get("col0");
        IcebergColumnHandle column1Handle = (IcebergColumnHandle) columns.get("col1");

        IcebergColumnHandle columnX = new IcebergColumnHandle(
                column0Handle.getColumnIdentity(),
                column0Handle.getType(),
                ImmutableList.of(column0Handle.getColumnIdentity().getChildren().get(0).getId()),
                BIGINT,
                Optional.empty());
        IcebergColumnHandle columnY = new IcebergColumnHandle(
                column0Handle.getColumnIdentity(),
                column0Handle.getType(),
                ImmutableList.of(column0Handle.getColumnIdentity().getChildren().get(1).getId()),
                BIGINT,
                Optional.empty());

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + testTable,
                any(tableScan(
                        equalTo(((IcebergTableHandle) tableHandle.get().getConnectorHandle()).withProjectedColumns(Set.of(columnX, columnY))),
                        TupleDomain.all(),
                        ImmutableMap.of("col0#x", equalTo(columnX), "col0#y", equalTo(columnY)))));

        // Projection and predicate pushdown
        assertPlan(
                format("SELECT col0.x FROM %s WHERE col0.x = col1 + 3 and col0.y = 2", testTable),
                anyTree(
                        filter(
                                "y = BIGINT '2' AND (x =  CAST((col1 + 3) AS BIGINT))",
                                tableScan(
                                        table -> {
                                            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
                                            TupleDomain<IcebergColumnHandle> unenforcedConstraint = icebergTableHandle.getUnenforcedPredicate();
                                            return icebergTableHandle.getProjectedColumns().equals(ImmutableSet.of(column1Handle, columnX, columnY)) &&
                                                    unenforcedConstraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L))));
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
                                            TupleDomain<IcebergColumnHandle> unenforcedConstraint = icebergTableHandle.getUnenforcedPredicate();
                                            return icebergTableHandle.getProjectedColumns().equals(ImmutableSet.of(column0Handle, columnX)) &&
                                                    unenforcedConstraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L))));
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
                                                                table -> {
                                                                    IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
                                                                    TupleDomain<IcebergColumnHandle> unenforcedConstraint = icebergTableHandle.getUnenforcedPredicate();
                                                                    Set<IcebergColumnHandle> expectedProjections = ImmutableSet.of(column0Handle, column1Handle, columnX);
                                                                    TupleDomain<IcebergColumnHandle> expectedUnenforcedConstraint = TupleDomain.withColumnDomains(
                                                                            ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L)));
                                                                    return icebergTableHandle.getProjectedColumns().equals(expectedProjections) &&
                                                                            unenforcedConstraint.equals(expectedUnenforcedConstraint);
                                                                },
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("x", equalTo(columnX), "expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle))))),
                                        anyTree(
                                                tableScan(
                                                        equalTo(((IcebergTableHandle) tableHandle.get().getConnectorHandle()).withProjectedColumns(Set.of(column1Handle))),
                                                        TupleDomain.all(),
                                                        ImmutableMap.of("s_expr_1", equalTo(column1Handle))))))));
    }
}
