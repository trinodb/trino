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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.mongodb.client.MongoClient;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.equalTo;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoClient;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoProjectionPushdownPlans
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "mongodb";
    private static final String SCHEMA = "test";

    private Closer closer;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        closer = Closer.create();
        MongoServer server = closer.register(new MongoServer());
        MongoClient client = closer.register(createMongoClient(server));

        try {
            queryRunner.installPlugin(new MongoPlugin());
            queryRunner.createCatalog(
                    CATALOG,
                    "mongodb",
                    ImmutableMap.of("mongodb.connection-url", server.getConnectionString().toString()));
            // Put an dummy schema collection because MongoDB doesn't support a database without collections
            client.getDatabase(SCHEMA).createCollection("dummy");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws Exception
    {
        closer.close();
        closer = null;
    }

    @Test
    public void testPushdownDisabled()
    {
        String tableName = "test_pushdown_disabled_" + randomNameSuffix();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(CATALOG, "projection_pushdown_enabled", "false")
                .build();

        getQueryRunner().execute("CREATE TABLE " + tableName + " (col0) AS SELECT CAST(row(5, 6) AS row(a bigint, b bigint)) AS col0 WHERE false");

        assertPlan(
                "SELECT col0.a expr_a, col0.b expr_b FROM " + tableName,
                session,
                any(
                        project(
                                ImmutableMap.of("expr_1", expression("col0[1]"), "expr_2", expression("col0[2]")),
                                tableScan(tableName, ImmutableMap.of("col0", "col0")))));
    }

    @Test
    public void testDereferencePushdown()
    {
        String tableName = "test_simple_projection_pushdown" + randomNameSuffix();
        QualifiedObjectName completeTableName = new QualifiedObjectName(CATALOG, SCHEMA, tableName);

        getQueryRunner().execute("CREATE TABLE " + tableName + " (col0, col1)" +
                " AS SELECT CAST(row(5, 6) AS row(x BIGINT, y BIGINT)) AS col0, BIGINT '5' AS col1");

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertThat(tableHandle).as("expected the table handle to be present").isPresent();

        MongoTableHandle mongoTableHandle = (MongoTableHandle) tableHandle.get().getConnectorHandle();
        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        MongoColumnHandle column0Handle = (MongoColumnHandle) columns.get("col0");
        MongoColumnHandle column1Handle = (MongoColumnHandle) columns.get("col1");

        MongoColumnHandle columnX = createProjectedColumnHandle(column0Handle, ImmutableList.of("x"), BIGINT);
        MongoColumnHandle columnY = createProjectedColumnHandle(column0Handle, ImmutableList.of("y"), BIGINT);

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + tableName,
                any(
                        tableScan(
                                equalTo(mongoTableHandle.withProjectedColumns(Set.of(columnX, columnY))),
                                TupleDomain.all(),
                                ImmutableMap.of("col0.x", equalTo(columnX), "col0.y", equalTo(columnY)))));

        // Projection and predicate pushdown
        assertPlan(
                "SELECT col0.x FROM " + tableName + " WHERE col0.x = col1 + 3 and col0.y = 2",
                anyTree(
                        filter(
                                "x = col1 + BIGINT '3'",
                                tableScan(
                                        table -> {
                                            MongoTableHandle actualTableHandle = (MongoTableHandle) table;
                                            TupleDomain<ColumnHandle> constraint = actualTableHandle.getConstraint();
                                            return actualTableHandle.getProjectedColumns().equals(ImmutableSet.of(column1Handle, columnX))
                                                    && constraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L))));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of("col1", equalTo(column1Handle), "x", equalTo(columnX))))));

        // Projection and predicate pushdown with overlapping columns
        assertPlan(
                "SELECT col0, col0.y expr_y FROM " + tableName + " WHERE col0.x = 5",
                anyTree(
                        tableScan(
                                table -> {
                                    MongoTableHandle actualTableHandle = (MongoTableHandle) table;
                                    TupleDomain<ColumnHandle> constraint = actualTableHandle.getConstraint();
                                    return actualTableHandle.getProjectedColumns().equals(ImmutableSet.of(column0Handle, columnY))
                                            && constraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L))));
                                },
                                TupleDomain.all(),
                                ImmutableMap.of("col0", equalTo(column0Handle), "y", equalTo(columnY)))));

        // Projection and predicate pushdown with joins
        assertPlan(
                "SELECT T.col0.x, T.col0, T.col0.y FROM " + tableName + " T join " + tableName + " S on T.col1 = S.col1 WHERE T.col0.x = 2",
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "expr_0_x", expression("expr_0[1]"),
                                        "expr_0", expression("expr_0"),
                                        "expr_0_y", expression("expr_0[2]")),
                                PlanMatchPattern.join(INNER, builder -> builder
                                        .equiCriteria("t_expr_1", "s_expr_1")
                                        .left(
                                                anyTree(
                                                        tableScan(
                                                                table -> {
                                                                    MongoTableHandle actualTableHandle = (MongoTableHandle) table;
                                                                    TupleDomain<ColumnHandle> constraint = actualTableHandle.getConstraint();
                                                                    Set<MongoColumnHandle> expectedProjections = ImmutableSet.of(column0Handle, column1Handle);
                                                                    TupleDomain<MongoColumnHandle> expectedConstraint = TupleDomain.withColumnDomains(
                                                                            ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L)));
                                                                    return actualTableHandle.getProjectedColumns().equals(expectedProjections)
                                                                            && constraint.equals(expectedConstraint);
                                                                },
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle)))))
                                        .right(
                                                anyTree(
                                                        tableScan(
                                                                equalTo(mongoTableHandle.withProjectedColumns(Set.of(column1Handle))),
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("s_expr_1", equalTo(column1Handle)))))))));
    }

    @Test
    public void testDereferencePushdownWithDotAndDollarContainingField()
    {
        String tableName = "test_dereference_pushdown_with_dot_and_dollar_containing_field_" + randomNameSuffix();
        QualifiedObjectName completeTableName = new QualifiedObjectName(CATALOG, SCHEMA, tableName);

        getQueryRunner().execute(
                "CREATE TABLE " + tableName + " (id, root1) AS" +
                        " SELECT BIGINT '1', CAST(ROW(11, ROW(111, ROW(1111, varchar 'foo', varchar 'bar'))) AS" +
                        " ROW(id BIGINT, root2 ROW(id BIGINT, root3 ROW(id BIGINT, \"dotted.field\" VARCHAR, \"$name\" VARCHAR))))");

        Session session = getQueryRunner().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertThat(tableHandle).as("expected the table handle to be present").isPresent();

        MongoTableHandle mongoTableHandle = (MongoTableHandle) tableHandle.get().getConnectorHandle();
        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        RowType rowType = RowType.rowType(
                RowType.field("id", BIGINT),
                RowType.field("dotted.field", VARCHAR),
                RowType.field("$name", VARCHAR));

        MongoColumnHandle columnRoot1 = (MongoColumnHandle) columns.get("root1");
        MongoColumnHandle columnRoot3 = createProjectedColumnHandle(columnRoot1, ImmutableList.of("root2", "root3"), rowType);

        //  Dotted field will not get pushdown, But it's parent filed 'root1.root2.root3' will get pushdown
        assertPlan(
                "SELECT root1.root2.root3.\"dotted.field\" FROM " + tableName,
                anyTree(
                        tableScan(
                                equalTo(mongoTableHandle.withProjectedColumns(Set.of(columnRoot3))),
                                TupleDomain.all(),
                                ImmutableMap.of("root1.root2.root3", equalTo(columnRoot3)))));

        //  Dollar containing field will not get pushdown, But it's parent filed 'root1.root2.root3' will get pushdown
        assertPlan(
                "SELECT root1.root2.root3.\"$name\" FROM " + tableName,
                anyTree(
                        tableScan(
                                equalTo(mongoTableHandle.withProjectedColumns(Set.of(columnRoot3))),
                                TupleDomain.all(),
                                ImmutableMap.of("root1.root2.root3", equalTo(columnRoot3)))));

        assertPlan(
                "SELECT 1 FROM " + tableName + " WHERE root1.root2.root3.\"dotted.field\" = 'foo'",
                anyTree(
                        tableScan(
                                table -> {
                                    MongoTableHandle actualTableHandle = (MongoTableHandle) table;
                                    TupleDomain<ColumnHandle> constraint = actualTableHandle.getConstraint();
                                    return actualTableHandle.getProjectedColumns().equals(ImmutableSet.of(columnRoot3))
                                            && constraint.equals(TupleDomain.all()); // Predicate will not get pushdown for dollar containing field
                                },
                                TupleDomain.all(),
                                ImmutableMap.of("root1.root2.root3", equalTo(columnRoot3)))));

        assertPlan(
                "SELECT 1 FROM " + tableName + " WHERE root1.root2.root3.\"$name\" = 'bar'",
                anyTree(
                        tableScan(
                                table -> {
                                    MongoTableHandle actualTableHandle = (MongoTableHandle) table;
                                    TupleDomain<ColumnHandle> constraint = actualTableHandle.getConstraint();
                                    return actualTableHandle.getProjectedColumns().equals(ImmutableSet.of(columnRoot3))
                                            && constraint.equals(TupleDomain.all()); // Predicate will not get pushdown for dollar containing field
                                },
                                TupleDomain.all(),
                                ImmutableMap.of("root1.root2.root3", equalTo(columnRoot3)))));
    }

    private MongoColumnHandle createProjectedColumnHandle(
            MongoColumnHandle baseColumnHandle,
            List<String> dereferenceNames,
            Type type)
    {
        return new MongoColumnHandle(
                baseColumnHandle.getBaseName(),
                dereferenceNames,
                type,
                baseColumnHandle.isHidden(),
                baseColumnHandle.isDbRefField(),
                baseColumnHandle.getComment());
    }
}
