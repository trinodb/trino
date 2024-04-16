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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveProjectionPushdownIntoTableScan
        extends BasePushdownPlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));

    private static final String HIVE_CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "test_schema";

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(HIVE_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .build();

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
        planTester.createCatalog(HIVE_CATALOG_NAME, new TestingHiveConnectorFactory(baseDir.toPath()), ImmutableMap.of());

        HiveMetastore metastore = getConnectorService(planTester, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        metastore.createDatabase(Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build());

        return planTester;
    }

    @Test
    public void testPushdownDisabled()
    {
        String testTable = "test_disabled_pushdown";

        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG_NAME, "projection_pushdown_enabled", "false")
                .build();

        getPlanTester().executeStatement(format(
                "CREATE TABLE %s (col0) AS" +
                        " SELECT cast(row(5, 6) as row(a bigint, b bigint)) AS col0 WHERE false",
                testTable));

        assertPlan(
                format("SELECT col0.a expr_a, col0.b expr_b FROM %s", testTable),
                session,
                any(
                        project(
                                ImmutableMap.of(
                                        "expr", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "col0"), 0)),
                                        "expr_2", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "col0"), 1))),
                                tableScan(testTable, ImmutableMap.of("col0", "col0")))));
    }

    @Test
    public void testDereferencePushdown()
    {
        String testTable = "test_simple_projection_pushdown";
        QualifiedObjectName completeTableName = new QualifiedObjectName(HIVE_CATALOG_NAME, SCHEMA_NAME, testTable);

        getPlanTester().executeStatement(format(
                "CREATE TABLE %s (col0, col1) AS" +
                        " SELECT cast(row(5, 6) as row(x bigint, y bigint)) AS col0, 5 AS col1 WHERE false",
                testTable));

        Session session = getPlanTester().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertThat(tableHandle.isPresent())
                .describedAs("expected the table handle to be present")
                .isTrue();

        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        HiveColumnHandle column0Handle = (HiveColumnHandle) columns.get("col0");
        HiveColumnHandle column1Handle = (HiveColumnHandle) columns.get("col1");

        HiveColumnHandle columnX = createProjectedColumnHandle(column0Handle, ImmutableList.of(0));
        HiveColumnHandle columnY = createProjectedColumnHandle(column0Handle, ImmutableList.of(1));

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + testTable,
                any(tableScan(
                        ((HiveTableHandle) tableHandle.get().connectorHandle())
                                .withProjectedColumns(ImmutableSet.of(columnX, columnY))::equals,
                        TupleDomain.all(),
                        ImmutableMap.of("col0#x", columnX::equals, "col0#y", columnY::equals))));

        // Projection and predicate pushdown
        assertPlan(
                format("SELECT col0.x FROM %s WHERE col0.x = col1 + 3 and col0.y = 2", testTable),
                anyTree(
                        filter(
                                new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(BIGINT, "col0_y"), new Constant(BIGINT, 2L)), new Comparison(EQUAL, new Reference(BIGINT, "col0_x"), new Cast(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "col1"), new Constant(INTEGER, 3L))), BIGINT)))),
                                tableScan(
                                        table -> {
                                            HiveTableHandle hiveTableHandle = (HiveTableHandle) table;
                                            return hiveTableHandle.getCompactEffectivePredicate().equals(TupleDomain.withColumnDomains(
                                                    ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L)))) &&
                                                    hiveTableHandle.getProjectedColumns().equals(
                                                            ImmutableSet.of(column1Handle, columnX, columnY));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of("col0_y", columnY::equals, "col0_x", columnX::equals, "col1", column1Handle::equals)))));

        // Projection and predicate pushdown with overlapping columns
        assertPlan(
                format("SELECT col0, col0.y expr_y FROM %s WHERE col0.x = 5", testTable),
                anyTree(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "col0_x"), new Constant(BIGINT, 5L)),
                                tableScan(
                                        table -> {
                                            HiveTableHandle hiveTableHandle = (HiveTableHandle) table;
                                            return hiveTableHandle.getCompactEffectivePredicate().equals(TupleDomain.withColumnDomains(
                                                    ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L)))) &&
                                                    hiveTableHandle.getProjectedColumns().equals(
                                                            ImmutableSet.of(column0Handle, columnX));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of("col0", column0Handle::equals, "col0_x", columnX::equals)))));

        // Projection and predicate pushdown with joins
        assertPlan(
                format("SELECT T.col0.x, T.col0, T.col0.y FROM %s T join %s S on T.col1 = S.col1 WHERE (T.col0.x = 2)", testTable, testTable),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "expr_0_x", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "expr_0"), 0)),
                                        "expr_0", expression(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "expr_0")),
                                        "expr_0_y", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "expr_0"), 1))),
                                join(INNER, builder -> builder
                                        .equiCriteria("t_expr_1", "s_expr_1")
                                        .left(
                                                anyTree(
                                                        filter(
                                                                new Comparison(EQUAL, new Reference(BIGINT, "expr_0_x"), new Constant(BIGINT, 2L)),
                                                                tableScan(
                                                                        table -> ((HiveTableHandle) table).getCompactEffectivePredicate().getDomains().get()
                                                                                .equals(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L))),
                                                                        TupleDomain.all(),
                                                                        ImmutableMap.of("expr_0_x", columnX::equals, "expr_0", column0Handle::equals, "t_expr_1", column1Handle::equals)))))
                                        .right(
                                                anyTree(
                                                        tableScan(
                                                                ((HiveTableHandle) tableHandle.get().connectorHandle())
                                                                        .withProjectedColumns(ImmutableSet.of(column1Handle))::equals,
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("s_expr_1", column1Handle::equals))))))));
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
