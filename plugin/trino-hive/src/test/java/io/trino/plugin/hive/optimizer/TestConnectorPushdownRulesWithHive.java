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
import io.trino.cost.ScalarStatsCalculator;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.subfield.Subfield;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushSubscriptLambdaIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushSubscriptLambdaThroughFilterIntoTableScan;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.LocalQueryRunner;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.ENABLE_PUSH_SUBSCRIPT_LAMBDA_INTO_SCAN;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class TestConnectorPushdownRulesWithHive
        extends BaseRuleTest
{
    private static final String SCHEMA_NAME = "test_schema";

    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private File baseDir;
    private HiveMetastore metastore;
    private CatalogHandle catalogHandle;

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .setSystemProperty(ENABLE_PUSH_SUBSCRIPT_LAMBDA_INTO_SCAN, "true")
            .build();

    private static final Type PRUNED_ROW_TYPE = RowType.from(asList(field("a", BIGINT)));
    private static final Type ARRAY_PRUNED_ROW_TYPE = new ArrayType(PRUNED_ROW_TYPE);
    private static final Type ARRAY_ROW_TYPE = new ArrayType(ROW_TYPE);
    private static final Type ROW_ARRAY_ROW_TYPE = RowType.from(asList(field("array", ARRAY_ROW_TYPE)));
    private static final FunctionCall subscriptfunctionCall = FunctionCallBuilder.resolve(HIVE_SESSION, PLANNER_CONTEXT.getMetadata())
            .setName(QualifiedName.of("transform"))
            .addArgument(ARRAY_ROW_TYPE, new SymbolReference("array_of_struct"))
            .addArgument(new FunctionType(singletonList(ROW_TYPE), PRUNED_ROW_TYPE),
                    new LambdaExpression(ImmutableList.of(
                            new LambdaArgumentDeclaration(
                                    new Identifier("transformarray$element"))),
                            new Row(ImmutableList.of(new SubscriptExpression(
                                    new SymbolReference("transformarray$element"),
                                    new LongLiteral("1"))))))
            .build();

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        try {
            baseDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        metastore = createTestingFileHiveMetastore(baseDir);
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();

        metastore.createDatabase(database);

        LocalQueryRunner queryRunner = LocalQueryRunner.create(HIVE_SESSION);
        queryRunner.createCatalog(TEST_CATALOG_NAME, new TestingHiveConnectorFactory(metastore), ImmutableMap.of());
        catalogHandle = queryRunner.getCatalogHandle(TEST_CATALOG_NAME);

        return Optional.of(queryRunner);
    }

    @Test
    public void testProjectionPushdown()
    {
        String tableName = "projection_test";
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        tester().getQueryRunner().execute(format(
                "CREATE TABLE  %s (struct_of_int) AS " +
                        "SELECT cast(row(5, 6) as row(a bigint, b bigint)) as struct_of_int where false",
                tableName));

        Type baseType = ROW_TYPE;

        HiveColumnHandle partialColumn = new HiveColumnHandle(
                "struct_of_int",
                0,
                toHiveType(baseType),
                baseType,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("a"),
                        toHiveType(BIGINT),
                        BIGINT,
                        ImmutableList.of())),
                REGULAR,
                Optional.empty());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle fullColumn = partialColumn.getBaseColumn();

        // Test projected columns pushdown to HiveTableHandle in case of full column references
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
                                ImmutableMap.of("expr", expression("col")),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(fullColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("col", fullColumn::equals))));

        // Rule should return Optional.empty after projected ColumnHandles have been added to HiveTableHandle
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("struct_of_int", baseType), p.symbol("struct_of_int", baseType).toSymbolReference()),
                                p.tableScan(
                                        new TableHandle(
                                                catalogHandle,
                                                hiveTable.withProjectedColumns(ImmutableSet.of(fullColumn)),
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
                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("struct_of_int#a", partialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testPredicatePushdown()
    {
        String tableName = "predicate_test";
        tester().getQueryRunner().execute(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

        PushPredicateIntoTableScan pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getPlannerContext(), tester().getTypeAnalyzer(), false);

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle column = createBaseColumn("a", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());

        tester().assertThat(pushPredicateIntoTableScan)
                .on(p ->
                        p.filter(
                                PlanBuilder.expression("a = 5"),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("a", INTEGER)),
                                        ImmutableMap.of(p.symbol("a", INTEGER), column))))
                .matches(filter(
                        "a = 5",
                        tableScan(
                                tableHandle -> ((HiveTableHandle) tableHandle).getCompactEffectivePredicate().getDomains().get()
                                        .equals(ImmutableMap.of(column, Domain.singleValue(INTEGER, 5L))),
                                TupleDomain.all(),
                                ImmutableMap.of("a", column::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testColumnPruningProjectionPushdown()
    {
        String tableName = "column_pruning_projection_test";
        tester().getQueryRunner().execute(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

        PruneTableScanColumns pruneTableScanColumns = new PruneTableScanColumns(tester().getMetadata());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle columnA = createBaseColumn("a", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());
        HiveColumnHandle columnB = createBaseColumn("b", 1, HIVE_INT, INTEGER, REGULAR, Optional.empty());

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
                                ImmutableMap.of("expr", expression("COLA")),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(columnA))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("COLA", columnA::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testPushdownWithDuplicateExpressions()
    {
        String tableName = "duplicate_expressions";
        tester().getQueryRunner().execute(format(
                "CREATE TABLE  %s (struct_of_bigint, just_bigint) AS SELECT cast(row(5, 6) AS row(a bigint, b bigint)) AS struct_of_int, 5 AS just_bigint WHERE false",
                tableName));

        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle bigintColumn = createBaseColumn("just_bigint", 1, toHiveType(BIGINT), BIGINT, REGULAR, Optional.empty());
        HiveColumnHandle partialColumn = new HiveColumnHandle(
                "struct_of_bigint",
                0,
                toHiveType(ROW_TYPE),
                ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("a"),
                        toHiveType(BIGINT),
                        BIGINT,
                        ImmutableList.of())),
                REGULAR,
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
                                "column_ref", expression("just_bigint_0"),
                                "negated_column_ref", expression("- just_bigint_0")),
                        tableScan(
                                hiveTable.withProjectedColumns(ImmutableSet.of(bigintColumn))::equals,
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
                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("struct_of_bigint#a", partialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testDereferenceInSubscriptLambdaPushdown()
    {
        String tableName = "array_projection_test";
        PushSubscriptLambdaIntoTableScan pushSubscriptLambdaIntoTableScan =
                new PushSubscriptLambdaIntoTableScan(
                        tester().getPlannerContext(),
                        tester().getTypeAnalyzer(),
                        new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        tester().getQueryRunner().execute(format(
                "CREATE TABLE  %s (array_of_struct) AS " +
                        "SELECT cast(ARRAY[ROW(1, 2), ROW(3, 4)] as ARRAY(ROW(a bigint, b bigint))) as array_of_struct",
                tableName));

        HiveColumnHandle partialColumn = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(ARRAY_ROW_TYPE),
                ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of(new Subfield("array_of_struct",
                                ImmutableList.of(Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("a")))))),
                REGULAR,
                Optional.empty());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName,
                ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle fullColumn = partialColumn.getBaseColumn();

        // Base symbol referenced by other assignments, skip the optimization
        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                        subscriptfunctionCall,
                                        p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                        p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn))))
                .doesNotFire();

        // No subscript lambda exists, skip the optimization
        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                        p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn))))
                .doesNotFire();

        // Transform input argument is not symbol reference, skip the optimization
        FunctionCall nestedSubscriptfunctionCall = FunctionCallBuilder.resolve(HIVE_SESSION, PLANNER_CONTEXT.getMetadata())
                .setName(QualifiedName.of("transform"))
                .addArgument(ARRAY_ROW_TYPE, new SubscriptExpression(new SymbolReference("struct_of_array_of_struct"),
                        new LongLiteral("1")))
                .addArgument(new FunctionType(singletonList(ROW_TYPE), PRUNED_ROW_TYPE),
                        new LambdaExpression(ImmutableList.of(
                                new LambdaArgumentDeclaration(
                                        new Identifier("transformarray$element"))),
                                new Row(ImmutableList.of(new SubscriptExpression(
                                        new SymbolReference("transformarray$element"),
                                        new LongLiteral("1"))))))
                .build();

        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                        nestedSubscriptfunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("struct_of_array_of_struct", ROW_ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("struct_of_array_of_struct", ROW_ARRAY_ROW_TYPE), fullColumn))))
                .doesNotFire();

        // Functions other than transform() will not trigger the optimization
        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                        PlanBuilder.expression("trim_array(array_of_struct, 1)")),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn))))
                .doesNotFire();

        // If already applied and same subfields generated, will not re-apply
        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), partialColumn))))
                .doesNotFire();

        // Overwrite the existing subfields with latest
        HiveColumnHandle previousColumnHandle = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(ARRAY_ROW_TYPE),
                ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of(new Subfield("array_of_struct",
                                ImmutableList.of(Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("previous")))))),
                REGULAR,
                Optional.empty());

        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), previousColumnHandle))))
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression(subscriptfunctionCall)),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("array_of_struct", partialColumn::equals))));

        // Subfields are added based on the subscript lambda
        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn))))
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression(subscriptfunctionCall)),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("array_of_struct", partialColumn::equals))));

        // Subfields are added based on the subscript lambda, and extends the existing prefix
        HiveColumnHandle nestedColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(ROW_ARRAY_ROW_TYPE),
                ROW_ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of())),
                REGULAR,
                Optional.empty());

        HiveColumnHandle nestedPartialColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(ROW_ARRAY_ROW_TYPE),
                ROW_ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of(new Subfield("struct_of_array_of_struct",
                                ImmutableList.of(new Subfield.NestedField("array"),
                                        Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("a")))))),
                REGULAR,
                Optional.empty());

        FunctionCall subscriptfunctionCallOnStructField = FunctionCallBuilder.resolve(HIVE_SESSION, PLANNER_CONTEXT.getMetadata())
                .setName(QualifiedName.of("transform"))
                .addArgument(ARRAY_ROW_TYPE, new SymbolReference("array_of_struct"))
                .addArgument(new FunctionType(singletonList(ROW_TYPE), PRUNED_ROW_TYPE),
                        new LambdaExpression(ImmutableList.of(
                                new LambdaArgumentDeclaration(
                                        new Identifier("transformarray$element"))),
                                new Row(ImmutableList.of(new SubscriptExpression(
                                        new SymbolReference("transformarray$element"),
                                        new LongLiteral("1"))))))
                .build();

        tester().assertThat(pushSubscriptLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCallOnStructField),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), nestedColumn))))
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression(subscriptfunctionCallOnStructField)),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(nestedPartialColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("array_of_struct", nestedPartialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testDereferenceInSubscriptLambdaPushdownThroughFilter()
    {
        String tableName = "array_projection_test";
        PushSubscriptLambdaThroughFilterIntoTableScan pushSubscriptLambdaThroughFilterIntoTableScan =
                new PushSubscriptLambdaThroughFilterIntoTableScan(
                        tester().getPlannerContext(),
                        tester().getTypeAnalyzer(),
                        new ScalarStatsCalculator(tester().getPlannerContext(), tester().getTypeAnalyzer()));

        tester().getQueryRunner().execute(format(
                "CREATE TABLE  %s (array_of_struct) AS " +
                        "SELECT cast(ARRAY[ROW(1, 2), ROW(3, 4)] as ARRAY(ROW(a bigint, b bigint))) as array_of_struct",
                tableName));

        HiveColumnHandle partialColumn = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(ARRAY_ROW_TYPE),
                ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of(new Subfield("array_of_struct",
                                ImmutableList.of(Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("a")))))),
                REGULAR,
                Optional.empty());

        HiveColumnHandle bigIntColumn = new HiveColumnHandle(
                "e",
                0,
                toHiveType(BIGINT),
                BIGINT,
                Optional.empty(),
                REGULAR,
                Optional.empty());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName,
                ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle fullColumn = partialColumn.getBaseColumn();

        // Base symbol referenced by other assignments, skip the optimization
        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    subscriptfunctionCall,
                                    p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                    p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // No subscript lambda exists, skip the optimization
        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                    p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // Transform input argument is not symbol reference, skip the optimization
        FunctionCall nestedSubscriptfunctionCall = FunctionCallBuilder.resolve(HIVE_SESSION, PLANNER_CONTEXT.getMetadata())
                .setName(QualifiedName.of("transform"))
                .addArgument(ARRAY_ROW_TYPE, new SubscriptExpression(new SymbolReference("struct_of_array_of_struct"),
                        new LongLiteral("1")))
                .addArgument(new FunctionType(singletonList(ROW_TYPE), PRUNED_ROW_TYPE),
                        new LambdaExpression(ImmutableList.of(
                                new LambdaArgumentDeclaration(
                                        new Identifier("transformarray$element"))),
                                new Row(ImmutableList.of(new SubscriptExpression(
                                        new SymbolReference("transformarray$element"),
                                        new LongLiteral("1"))))))
                .build();

        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    nestedSubscriptfunctionCall),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // Functions other than transform() will not trigger the optimization
        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE),
                                    PlanBuilder.expression("trim_array(array_of_struct, 1)")),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // If already applied and same subfields generated, will not re-apply
        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCall),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), partialColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // Overwrite the existing subfields with latest
        HiveColumnHandle previousColumnHandle = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(ARRAY_ROW_TYPE),
                ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of(new Subfield("array_of_struct",
                                ImmutableList.of(Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("previous")))))),
                REGULAR,
                Optional.empty());

        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCall),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), previousColumnHandle,
                                                    e, bigIntColumn))));
                })
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression(subscriptfunctionCall)),
                                filter("e > 0",
                                        tableScan(
                                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn, bigIntColumn))::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of("array_of_struct", partialColumn::equals, "e", bigIntColumn::equals)))));

        // Subfields are added based on the subscript lambda
        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCall),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression(subscriptfunctionCall)),
                                filter("e > 0",
                                        tableScan(
                                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn, bigIntColumn))::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of("array_of_struct", partialColumn::equals, "e", bigIntColumn::equals)))));

        // Subfields are added based on the subscript lambda, and extends the existing prefix
        HiveColumnHandle nestedColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(ROW_ARRAY_ROW_TYPE),
                ROW_ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of())),
                REGULAR,
                Optional.empty());

        HiveColumnHandle nestedPartialColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(ROW_ARRAY_ROW_TYPE),
                ROW_ARRAY_ROW_TYPE,
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(ARRAY_ROW_TYPE),
                        ARRAY_ROW_TYPE,
                        ImmutableList.of(new Subfield("struct_of_array_of_struct",
                                ImmutableList.of(new Subfield.NestedField("array"),
                                        Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("a")))))),
                REGULAR,
                Optional.empty());

        FunctionCall subscriptfunctionCallOnStructField = FunctionCallBuilder.resolve(HIVE_SESSION, PLANNER_CONTEXT.getMetadata())
                .setName(QualifiedName.of("transform"))
                .addArgument(ARRAY_ROW_TYPE, new SymbolReference("array_of_struct"))
                .addArgument(new FunctionType(singletonList(ROW_TYPE), PRUNED_ROW_TYPE),
                        new LambdaExpression(ImmutableList.of(
                                new LambdaArgumentDeclaration(
                                        new Identifier("transformarray$element"))),
                                new Row(ImmutableList.of(new SubscriptExpression(
                                        new SymbolReference("transformarray$element"),
                                        new LongLiteral("1"))))))
                .build();

        tester().assertThat(pushSubscriptLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("nested_array_transformed", ARRAY_PRUNED_ROW_TYPE), subscriptfunctionCallOnStructField),
                            p.filter(PlanBuilder.expression("e > 0"),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), nestedColumn,
                                                    e, bigIntColumn))));
                })
                .matches(
                        project(
                                ImmutableMap.of("nested_array_transformed", expression(subscriptfunctionCallOnStructField)),
                                filter("e > 0",
                                        tableScan(
                                                hiveTable.withProjectedColumns(ImmutableSet.of(nestedPartialColumn, bigIntColumn))::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of("array_of_struct", nestedPartialColumn::equals, "e", bigIntColumn::equals)))));

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
