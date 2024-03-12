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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeProjectionPushdownPlans
        extends BasePushdownPlanTest
{
    private static final String SCHEMA = "test_schema";

    private Path baseDir;

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(SCHEMA)
                .build();
        try {
            baseDir = Files.createTempDirectory("delta_lake_projection_pushdown");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        PlanTester planTester = PlanTester.create(session);
        planTester.installPlugin(new TestingDeltaLakePlugin(baseDir));
        planTester.createCatalog(DELTA_CATALOG, "delta_lake", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", baseDir.toString())
                .buildOrThrow());

        HiveMetastore metastore = TestingDeltaLakeUtils.getConnectorService(planTester, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        Database database = Database.builder()
                .setDatabaseName(SCHEMA)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();

        metastore.createDatabase(database);

        return planTester;
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir, ALLOW_INSECURE);
        }
    }

    @Test
    public void testPushdownDisabled()
    {
        String testTable = "test_pushdown_disabled_" + randomNameSuffix();

        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setCatalogSessionProperty(DELTA_CATALOG, "projection_pushdown_enabled", "false")
                .build();

        getPlanTester().executeStatement(format(
                "CREATE TABLE %s (col0) AS SELECT CAST(row(5, 6) AS row(a bigint, b bigint)) AS col0 WHERE false",
                testTable));

        assertPlan(
                format("SELECT col0.a expr_a, col0.b expr_b FROM %s", testTable),
                session,
                any(
                        project(
                                ImmutableMap.of("expr", expression(new SubscriptExpression(new SymbolReference("col0"), new io.trino.sql.tree.LongLiteral("1"))), "expr_2", expression(new SubscriptExpression(new SymbolReference("col0"), new io.trino.sql.tree.LongLiteral("2")))),
                                tableScan(testTable, ImmutableMap.of("col0", "col0")))));
    }

    @Test
    public void testDereferencePushdown()
    {
        String testTable = "test_simple_projection_pushdown" + randomNameSuffix();
        QualifiedObjectName completeTableName = new QualifiedObjectName(DELTA_CATALOG, SCHEMA, testTable);

        getPlanTester().executeStatement(format(
                "CREATE TABLE %s (col0, col1) WITH (partitioned_by = ARRAY['col1']) AS" +
                        " SELECT CAST(row(5, 6) AS row(x bigint, y bigint)) AS col0, 5 AS col1",
                testTable));

        Session session = getPlanTester().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertThat(tableHandle).as("expected the table handle to be present").isPresent();

        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        DeltaLakeColumnHandle column0Handle = (DeltaLakeColumnHandle) columns.get("col0");
        DeltaLakeColumnHandle column1Handle = (DeltaLakeColumnHandle) columns.get("col1");

        DeltaLakeColumnHandle columnX = createProjectedColumnHandle(column0Handle, ImmutableList.of(0), ImmutableList.of("x"));
        DeltaLakeColumnHandle columnY = createProjectedColumnHandle(column0Handle, ImmutableList.of(1), ImmutableList.of("y"));

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + testTable,
                any(tableScan(
                        equalTo(((DeltaLakeTableHandle) tableHandle.get().getConnectorHandle()).withProjectedColumns(Set.of(columnX, columnY))),
                        TupleDomain.all(),
                        ImmutableMap.of("col0.x", equalTo(columnX), "col0.y", equalTo(columnY)))));

        // Projection and predicate pushdown
        PlanMatchPattern source2 = tableScan(
                table -> {
                    DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) table;
                    TupleDomain<DeltaLakeColumnHandle> unenforcedConstraint = deltaLakeTableHandle.getNonPartitionConstraint();
                    return deltaLakeTableHandle.getProjectedColumns().orElseThrow().equals(ImmutableSet.of(column1Handle, columnX, columnY)) &&
                            unenforcedConstraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L))));
                },
                TupleDomain.all(),
                ImmutableMap.of("y", columnY::equals, "x", columnX::equals, "col1", column1Handle::equals));
        assertPlan(
                format("SELECT col0.x FROM %s WHERE col0.x = col1 + 3 and col0.y = 2", testTable),
                anyTree(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(
                                        new ComparisonExpression(EQUAL, new SymbolReference("y"), new GenericLiteral("BIGINT", "2")),
                                        new ComparisonExpression(EQUAL, new SymbolReference("x"), new Cast(new ArithmeticBinaryExpression(ADD, new SymbolReference("col1"), new LongLiteral("3")), dataType("bigint"))))),
                                source2)));

        // Projection and predicate pushdown with overlapping columns
        PlanMatchPattern source1 = tableScan(
                table -> {
                    DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) table;
                    TupleDomain<DeltaLakeColumnHandle> unenforcedConstraint = deltaLakeTableHandle.getNonPartitionConstraint();
                    return deltaLakeTableHandle.getProjectedColumns().orElseThrow().equals(ImmutableSet.of(column0Handle, columnX)) &&
                            unenforcedConstraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L))));
                },
                TupleDomain.all(),
                ImmutableMap.of("col0", equalTo(column0Handle), "x", equalTo(columnX)));
        assertPlan(
                format("SELECT col0, col0.y expr_y FROM %s WHERE col0.x = 5", testTable),
                anyTree(
                        filter(
                                new ComparisonExpression(EQUAL, new SymbolReference("x"), new GenericLiteral("BIGINT", "5")),
                                source1)));

        // Projection and predicate pushdown with joins
        assertPlan(
                format("SELECT T.col0.x, T.col0, T.col0.y FROM %s T join %s S on T.col1 = S.col1 WHERE (T.col0.x = 2)", testTable, testTable),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "expr_0_x", expression(new SubscriptExpression(new SymbolReference("expr_0"), new io.trino.sql.tree.LongLiteral("1"))),
                                        "expr_0", expression(new SymbolReference("expr_0")),
                                        "expr_0_y", expression(new SubscriptExpression(new SymbolReference("expr_0"), new io.trino.sql.tree.LongLiteral("2")))),
                                join(INNER, builder -> {
                                    PlanMatchPattern source = tableScan(
                                            table -> {
                                                DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) table;
                                                TupleDomain<DeltaLakeColumnHandle> unenforcedConstraint = deltaLakeTableHandle.getNonPartitionConstraint();
                                                Set<DeltaLakeColumnHandle> expectedProjections = ImmutableSet.of(column0Handle, column1Handle, columnX);
                                                TupleDomain<DeltaLakeColumnHandle> expectedUnenforcedConstraint = TupleDomain.withColumnDomains(
                                                        ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L)));
                                                return deltaLakeTableHandle.getProjectedColumns().orElseThrow().equals(expectedProjections) &&
                                                        unenforcedConstraint.equals(expectedUnenforcedConstraint);
                                            },
                                            TupleDomain.all(),
                                            ImmutableMap.of("x", equalTo(columnX), "expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle)));
                                    builder
                                            .equiCriteria("t_expr_1", "s_expr_1")
                                            .left(
                                                    anyTree(
                                                            filter(
                                                                    new ComparisonExpression(EQUAL, new SymbolReference("x"), new GenericLiteral("BIGINT", "2")),
                                                                    source)))
                                            .right(
                                                    anyTree(
                                                            tableScan(
                                                                    equalTo(((DeltaLakeTableHandle) tableHandle.get().getConnectorHandle()).withProjectedColumns(Set.of(column1Handle))),
                                                                    TupleDomain.all(),
                                                                    ImmutableMap.of("s_expr_1", equalTo(column1Handle)))));
                                }))));
    }

    private DeltaLakeColumnHandle createProjectedColumnHandle(
            DeltaLakeColumnHandle baseColumnHandle,
            List<Integer> dereferenceIndices,
            List<String> dereferenceNames)
    {
        return new DeltaLakeColumnHandle(
                baseColumnHandle.getBaseColumnName(),
                baseColumnHandle.getBaseType(),
                baseColumnHandle.getBaseFieldId(),
                baseColumnHandle.getBasePhysicalColumnName(),
                baseColumnHandle.getBasePhysicalType(),
                DeltaLakeColumnType.REGULAR,
                Optional.of(new DeltaLakeColumnProjectionInfo(
                        BIGINT,
                        dereferenceIndices,
                        dereferenceNames)));
    }
}
