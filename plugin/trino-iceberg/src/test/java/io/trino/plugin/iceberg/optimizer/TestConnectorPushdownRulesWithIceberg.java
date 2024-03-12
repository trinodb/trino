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
package io.trino.plugin.iceberg.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TestingIcebergConnectorFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class TestConnectorPushdownRulesWithIceberg
        extends BaseRuleTest
{
    private static final String SCHEMA_NAME = "test_schema";

    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private File baseDir;
    private HiveMetastore metastore;
    private CatalogHandle catalogHandle;

    private static final Session ICEBERG_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .build();

    @Override
    protected Optional<PlanTester> createPlanTester()
    {
        try {
            baseDir = Files.createTempDirectory("metastore").toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        PlanTester planTester = PlanTester.create(ICEBERG_SESSION);

        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        planTester.addFunctions(functions.build());

        planTester.createCatalog(
                TEST_CATALOG_NAME,
                new TestingIcebergConnectorFactory(baseDir.toPath()),
                ImmutableMap.of());
        catalogHandle = planTester.getCatalogHandle(TEST_CATALOG_NAME);

        metastore = ((IcebergConnector) planTester.getConnector(TEST_CATALOG_NAME)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();

        metastore.createDatabase(database);

        return Optional.of(planTester);
    }

    @Test
    public void testProjectionPushdown()
    {
        String tableName = "projection_test";
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        tester().getPlanTester().executeStatement(format(
                "CREATE TABLE  %s (struct_of_int) AS " +
                        "SELECT cast(row(5, 6) as row(a bigint, b bigint)) as struct_of_int where false",
                tableName));

        Type baseType = ROW_TYPE;

        IcebergColumnHandle partialColumn = new IcebergColumnHandle(
                new ColumnIdentity(3, "struct_of_int", STRUCT, ImmutableList.of(primitiveColumnIdentity(1, "a"), primitiveColumnIdentity(2, "b"))),
                baseType,
                ImmutableList.of(1),
                BIGINT,
                true,
                Optional.empty());

        IcebergTableHandle icebergTable = new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                SCHEMA_NAME,
                tableName,
                DATA,
                Optional.of(1L),
                "",
                Optional.of(""),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "",
                ImmutableMap.of(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
        TableHandle table = new TableHandle(catalogHandle, icebergTable, new HiveTransactionHandle(false));

        IcebergColumnHandle fullColumn = partialColumn.getBaseColumn();

        // Test projected columns pushdown to IcebergTableHandle in case of full column references
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("struct_of_int", baseType), p.symbol("struct_of_int", baseType).toSymbolReference()),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("struct_of_int", baseType)),
                                        ImmutableMap.of(p.symbol("struct_of_int", baseType), fullColumn))))
                .matches(
                        project(
                                ImmutableMap.of("expr", expression(new SymbolReference("col"))),
                                tableScan(
                                        icebergTable.withProjectedColumns(ImmutableSet.of(fullColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("col", fullColumn::equals))));

        // Rule should return Optional.empty after projected ColumnHandles have been added to IcebergTableHandle
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("struct_of_int", baseType), p.symbol("struct_of_int", baseType).toSymbolReference()),
                                p.tableScan(
                                        new TableHandle(
                                                catalogHandle,
                                                icebergTable.withProjectedColumns(ImmutableSet.of(fullColumn)),
                                                new HiveTransactionHandle(false)),
                                        ImmutableList.of(p.symbol("struct_of_int", baseType)),
                                        ImmutableMap.of(p.symbol("struct_of_int", baseType), fullColumn))))
                .doesNotFire();

        // Test Dereference pushdown
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_deref", BIGINT), new SubscriptExpression(p.symbol("struct_of_int", baseType).toSymbolReference(), new LongLiteral("1"))),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("struct_of_int", baseType)),
                                        ImmutableMap.of(p.symbol("struct_of_int", baseType), fullColumn))))
                .matches(project(
                        ImmutableMap.of("expr_deref", expression(new SymbolReference("struct_of_int#a"))),
                        tableScan(
                                icebergTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("struct_of_int#a", partialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testPredicatePushdown()
    {
        String tableName = "predicate_test";
        tester().getPlanTester().executeStatement(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));
        long snapshotId = ((IcebergTableHandle) tester().getPlanTester().getTableHandle(TEST_CATALOG_NAME, SCHEMA_NAME, tableName).getConnectorHandle()).getSnapshotId().orElseThrow();

        PushPredicateIntoTableScan pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getPlannerContext(), tester().getTypeAnalyzer(), false);

        IcebergTableHandle icebergTable = new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                SCHEMA_NAME,
                tableName,
                DATA,
                Optional.of(snapshotId),
                "",
                Optional.of(""),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "",
                ImmutableMap.of(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
        TableHandle table = new TableHandle(catalogHandle, icebergTable, new HiveTransactionHandle(false));

        IcebergColumnHandle column = new IcebergColumnHandle(primitiveColumnIdentity(1, "a"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());

        tester().assertThat(pushPredicateIntoTableScan)
                .on(p ->
                        p.filter(
                                new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("5")),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("a", INTEGER)),
                                        ImmutableMap.of(p.symbol("a", INTEGER), column))))
                .matches(filter(
                        new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("5")),
                        tableScan(
                                tableHandle -> ((IcebergTableHandle) tableHandle).getUnenforcedPredicate().getDomains().get()
                                        .equals(ImmutableMap.of(column, Domain.singleValue(INTEGER, 5L))),
                                TupleDomain.all(),
                                ImmutableMap.of("a", column::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testColumnPruningProjectionPushdown()
    {
        String tableName = "column_pruning_projection_test";
        tester().getPlanTester().executeStatement(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

        PruneTableScanColumns pruneTableScanColumns = new PruneTableScanColumns(tester().getMetadata());

        IcebergTableHandle icebergTable = new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                SCHEMA_NAME,
                tableName,
                DATA,
                Optional.empty(),
                "",
                Optional.of(""),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "",
                ImmutableMap.of(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
        TableHandle table = new TableHandle(catalogHandle, icebergTable, new HiveTransactionHandle(false));

        IcebergColumnHandle columnA = new IcebergColumnHandle(primitiveColumnIdentity(0, "a"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
        IcebergColumnHandle columnB = new IcebergColumnHandle(primitiveColumnIdentity(1, "b"), INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());

        tester().assertThat(pruneTableScanColumns)
                .on(p -> {
                    Symbol symbolA = p.symbol("a", INTEGER);
                    Symbol symbolB = p.symbol("b", INTEGER);
                    return p.project(
                            Assignments.of(p.symbol("x"), symbolA.toSymbolReference()),
                            p.tableScan(
                                    table,
                                    ImmutableList.of(symbolA, symbolB),
                                    ImmutableMap.of(
                                            symbolA, columnA,
                                            symbolB, columnB)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", expression(new SymbolReference("COLA"))),
                                tableScan(
                                        icebergTable.withProjectedColumns(ImmutableSet.of(columnA))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("COLA", columnA::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testPushdownWithDuplicateExpressions()
    {
        String tableName = "duplicate_expressions";
        tester().getPlanTester().executeStatement(format(
                "CREATE TABLE  %s (struct_of_bigint, just_bigint) AS SELECT cast(row(5, 6) AS row(a bigint, b bigint)) AS struct_of_int, 5 AS just_bigint WHERE false",
                tableName));

        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        IcebergTableHandle icebergTable = new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                SCHEMA_NAME,
                tableName,
                DATA,
                Optional.of(1L),
                "",
                Optional.of(""),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "",
                ImmutableMap.of(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
        TableHandle table = new TableHandle(catalogHandle, icebergTable, new HiveTransactionHandle(false));

        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(primitiveColumnIdentity(1, "just_bigint"), BIGINT, ImmutableList.of(), BIGINT, true, Optional.empty());
        IcebergColumnHandle partialColumn = new IcebergColumnHandle(
                new ColumnIdentity(3, "struct_of_bigint", STRUCT, ImmutableList.of(primitiveColumnIdentity(1, "a"), primitiveColumnIdentity(2, "b"))),
                ROW_TYPE,
                ImmutableList.of(1),
                BIGINT,
                true,
                Optional.empty());

        // Test projection pushdown with duplicate column references
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p -> {
                    SymbolReference column = p.symbol("just_bigint", BIGINT).toSymbolReference();
                    Expression negation = new ArithmeticUnaryExpression(MINUS, column);
                    return p.project(
                            Assignments.of(
                                    // The column reference is part of both the assignments
                                    p.symbol("column_ref", BIGINT), column,
                                    p.symbol("negated_column_ref", BIGINT), negation),
                            p.tableScan(
                                    table,
                                    ImmutableList.of(p.symbol("just_bigint", BIGINT)),
                                    ImmutableMap.of(p.symbol("just_bigint", BIGINT), bigintColumn)));
                })
                .matches(project(
                        ImmutableMap.of(
                                "column_ref", expression(new SymbolReference("just_bigint_0")),
                                "negated_column_ref", expression(new ArithmeticUnaryExpression(MINUS, new SymbolReference("just_bigint_0")))),
                        tableScan(
                                icebergTable.withProjectedColumns(ImmutableSet.of(bigintColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("just_bigint_0", bigintColumn::equals))));

        // Test Dereference pushdown
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p -> {
                    SubscriptExpression subscript = new SubscriptExpression(p.symbol("struct_of_bigint", ROW_TYPE).toSymbolReference(), new LongLiteral("1"));
                    Expression sum = new ArithmeticBinaryExpression(ADD, subscript, new LongLiteral("2"));
                    return p.project(
                            Assignments.of(
                                    // The subscript expression instance is part of both the assignments
                                    p.symbol("expr_deref", BIGINT), subscript,
                                    p.symbol("expr_deref_2", BIGINT), sum),
                            p.tableScan(
                                    table,
                                    ImmutableList.of(p.symbol("struct_of_bigint", ROW_TYPE)),
                                    ImmutableMap.of(p.symbol("struct_of_bigint", ROW_TYPE), partialColumn.getBaseColumn())));
                })
                .matches(project(
                        ImmutableMap.of(
                                "expr_deref", expression(new SymbolReference("struct_of_bigint#a")),
                                "expr_deref_2", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("struct_of_bigint#a"), new LongLiteral("2")))),
                        tableScan(
                                icebergTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("struct_of_bigint#a", partialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
