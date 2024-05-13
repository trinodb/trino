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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.subfield.Subfield;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushFieldReferenceLambdaIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushFieldReferenceLambdaThroughFilterIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.testing.PlanTester;
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
import static io.trino.SystemSessionProperties.ENABLE_PUSH_FIELD_DEREFERENCE_LAMBDA_INTO_SCAN;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class TestConnectorPushdownRulesWithHive
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));

    private static final String SCHEMA_NAME = "test_schema";

    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private File baseDir;
    private HiveMetastore metastore;
    private CatalogHandle catalogHandle;

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(HIVE_CATALOG)
            .setSchema(SCHEMA_NAME)
            .setSystemProperty(ENABLE_PUSH_FIELD_DEREFERENCE_LAMBDA_INTO_SCAN, "true")
            .build();

    private static final Type ANONYMOUS_ROW_TYPE = RowType.anonymous(ImmutableList.of(BIGINT, BIGINT));
    private static final Type PRUNED_ROW_TYPE = RowType.anonymous(ImmutableList.of(BIGINT));
    private static final Type PRUNED_ARRAY_ROW_TYPE = new ArrayType(PRUNED_ROW_TYPE);
    private static final Type ARRAY_ROW_TYPE = new ArrayType(ANONYMOUS_ROW_TYPE);
    private static final Type ROW_ARRAY_ROW_TYPE = RowType.anonymous(ImmutableList.of(ARRAY_ROW_TYPE));
    private static final Reference LAMBDA_ELEMENT_REFERENCE = new Reference(ROW_TYPE, "transformarray$element");
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(ARRAY_ROW_TYPE, new FunctionType(ImmutableList.of(ANONYMOUS_ROW_TYPE), PRUNED_ROW_TYPE)));
    private static final Call dereferenceFunctionCall = new Call(TRANSFORM, ImmutableList.of(new Reference(ARRAY_ROW_TYPE, "array_of_struct"),
            new Lambda(ImmutableList.of(new Symbol(ANONYMOUS_ROW_TYPE, "transformarray$element")),
                    new Row(ImmutableList.of(new FieldReference(
                            LAMBDA_ELEMENT_REFERENCE,
                            0))))));

    @Override
    protected Optional<PlanTester> createPlanTester()
    {
        try {
            baseDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        PlanTester planTester = PlanTester.create(HIVE_SESSION);
        planTester.createCatalog(HIVE_CATALOG, new TestingHiveConnectorFactory(baseDir.toPath()), ImmutableMap.of());
        catalogHandle = planTester.getCatalogHandle(HIVE_CATALOG);

        metastore = getConnectorService(planTester, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        metastore.createDatabase(Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build());

        return Optional.of(planTester);
    }

    @Test
    public void testProjectionPushdown()
    {
        String tableName = "projection_test";
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                new ScalarStatsCalculator(tester().getPlannerContext()));

        tester().getPlanTester().executeStatement(format(
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
                                ImmutableMap.of("expr", expression(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "col"))),
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
                                        p.symbol("expr_deref", BIGINT), new FieldReference(p.symbol("struct_of_int", baseType).toSymbolReference(), 0)),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("struct_of_int", baseType)),
                                        ImmutableMap.of(p.symbol("struct_of_int", baseType), fullColumn))))
                .matches(project(
                        ImmutableMap.of("expr_deref", expression(new Reference(BIGINT, "struct_of_int#a"))),
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
        tester().getPlanTester().executeStatement(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

        PushPredicateIntoTableScan pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getPlannerContext(), false);

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(catalogHandle, hiveTable, new HiveTransactionHandle(false));

        HiveColumnHandle column = createBaseColumn("a", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());

        tester().assertThat(pushPredicateIntoTableScan)
                .on(p ->
                        p.filter(
                                new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 5L)),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("a", INTEGER)),
                                        ImmutableMap.of(p.symbol("a", INTEGER), column))))
                .matches(filter(
                        new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 5L)),
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
        tester().getPlanTester().executeStatement(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

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
                            Assignments.of(p.symbol("x", INTEGER), symbolA.toSymbolReference()),
                            p.tableScan(
                                    table,
                                    ImmutableList.of(symbolA, symbolB),
                                    ImmutableMap.of(
                                            symbolA, columnA,
                                            symbolB, columnB)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("expr", expression(new Reference(INTEGER, "COLA"))),
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
        tester().getPlanTester().executeStatement(format(
                "CREATE TABLE  %s (struct_of_bigint, just_bigint) AS SELECT cast(row(5, 6) AS row(a bigint, b bigint)) AS struct_of_int, 5 AS just_bigint WHERE false",
                tableName));

        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                new ScalarStatsCalculator(tester().getPlannerContext()));

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
                    Reference column = p.symbol("just_bigint", BIGINT).toSymbolReference();
                    Expression negation = new Call(NEGATION_BIGINT, ImmutableList.of(column));
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
                                "column_ref", expression(new Reference(BIGINT, "just_bigint_0")),
                                "negated_column_ref", expression(new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "just_bigint_0"))))),
                        tableScan(
                                hiveTable.withProjectedColumns(ImmutableSet.of(bigintColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("just_bigint_0", bigintColumn::equals))));

        // Test Dereference pushdown
        tester().assertThat(pushProjectionIntoTableScan)
                .on(p -> {
                    FieldReference fieldReference = new FieldReference(p.symbol("struct_of_bigint", ROW_TYPE).toSymbolReference(), 0);
                    Expression sum = new Call(ADD_BIGINT, ImmutableList.of(fieldReference, new Constant(BIGINT, 2L)));
                    return p.project(
                            Assignments.of(
                                    // The subscript expression instance is part of both the assignments
                                    p.symbol("expr_deref", BIGINT), fieldReference,
                                    p.symbol("expr_deref_2", BIGINT), sum),
                            p.tableScan(
                                    table,
                                    ImmutableList.of(p.symbol("struct_of_bigint", ROW_TYPE)),
                                    ImmutableMap.of(p.symbol("struct_of_bigint", ROW_TYPE), partialColumn.getBaseColumn())));
                })
                .matches(project(
                        ImmutableMap.of(
                                "expr_deref", expression(new Reference(BIGINT, "struct_of_bigint#a")),
                                "expr_deref_2", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "struct_of_bigint#a"), new Constant(BIGINT, 2L))))),
                        tableScan(
                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("struct_of_bigint#a", partialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testDereferenceInFieldReferenceLambdaPushdown()
    {
        String tableName = "array_filter_dereference_projection_test";
        PushFieldReferenceLambdaIntoTableScan pushFieldReferenceLambdaIntoTableScan =
                new PushFieldReferenceLambdaIntoTableScan(
                        tester().getPlannerContext());

        tester().getPlanTester().executeStatement(format(
                "CREATE TABLE  %s (array_of_struct) AS " +
                        "SELECT cast(ARRAY[ROW(1, 2), ROW(3, 4)] as ARRAY(ROW(a bigint, b bigint))) as array_of_struct",
                tableName));

        HiveColumnHandle partialColumn = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(new ArrayType(ROW_TYPE)),
                new ArrayType(ROW_TYPE),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
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
        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        dereferenceFunctionCall,
                                        p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                        p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn))))
                .doesNotFire();

        // No subscript lambda exists, skip the optimization
        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
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
        Call nestedSubscriptfunctionCall = new Call(FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(ARRAY_ROW_TYPE, new FunctionType(ImmutableList.of(ANONYMOUS_ROW_TYPE), PRUNED_ROW_TYPE))), ImmutableList.of(new FieldReference(new Reference(ROW_ARRAY_ROW_TYPE, "struct_of_array_of_struct"), 0),
                new Lambda(ImmutableList.of(new Symbol(ANONYMOUS_ROW_TYPE, "transformarray$element")),
                        new Row(ImmutableList.of(new FieldReference(
                                LAMBDA_ELEMENT_REFERENCE,
                                0))))));

        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                        nestedSubscriptfunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("struct_of_array_of_struct", ROW_ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("struct_of_array_of_struct", ROW_ARRAY_ROW_TYPE), fullColumn))))
                .doesNotFire();

        // If already applied and same subfields generated, will not re-apply
        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), partialColumn))))
                .doesNotFire();

        // Overwrite the existing subfields with latest
        HiveColumnHandle previousColumnHandle = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(new ArrayType(ROW_TYPE)),
                new ArrayType(ROW_TYPE),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
                        ImmutableList.of(new Subfield("array_of_struct",
                                ImmutableList.of(Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("previous")))))),
                REGULAR,
                Optional.empty());

        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), previousColumnHandle))))
                .matches(
                        project(
                                ImmutableMap.of("pruned_nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("array_of_struct", partialColumn::equals))));

        // Subfields are added based on the subscript lambda
        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn))))
                .matches(
                        project(
                                ImmutableMap.of("pruned_nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("array_of_struct", partialColumn::equals))));

        // Subfields are added based on the subscript lambda, and extends the existing prefix
        HiveColumnHandle nestedColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(RowType.from(asList(field("array", new ArrayType(ROW_TYPE))))),
                RowType.from(asList(field("array", new ArrayType(ROW_TYPE)))),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
                        ImmutableList.of())),
                REGULAR,
                Optional.empty());

        HiveColumnHandle nestedPartialColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(RowType.from(asList(field("array", new ArrayType(ROW_TYPE))))),
                RowType.from(asList(field("array", new ArrayType(ROW_TYPE)))),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
                        ImmutableList.of(new Subfield("struct_of_array_of_struct",
                                ImmutableList.of(new Subfield.NestedField("array"),
                                        Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("a")))))),
                REGULAR,
                Optional.empty());

        tester().assertThat(pushFieldReferenceLambdaIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), nestedColumn))))
                .matches(
                        project(
                                ImmutableMap.of("pruned_nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(nestedPartialColumn))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("array_of_struct", nestedPartialColumn::equals))));

        metastore.dropTable(SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testDereferenceInSubscriptLambdaPushdownThroughFilter()
    {
        String tableName = "array_filter_dereference_with_filter_projection_test";
        PushFieldReferenceLambdaThroughFilterIntoTableScan pushFieldReferenceLambdaThroughFilterIntoTableScan =
                new PushFieldReferenceLambdaThroughFilterIntoTableScan(
                        tester().getPlannerContext());

        tester().getPlanTester().executeStatement(format(
                "CREATE TABLE  %s (array_of_struct) AS " +
                        "SELECT cast(ARRAY[ROW(1, 2), ROW(3, 4)] as ARRAY(ROW(a bigint, b bigint))) as array_of_struct",
                tableName));

        HiveColumnHandle partialColumn = new HiveColumnHandle(
                "array_of_struct",
                0,
                toHiveType(new ArrayType(ROW_TYPE)),
                new ArrayType(ROW_TYPE),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
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
        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                    dereferenceFunctionCall,
                                    p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                    p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // No subscript lambda exists, skip the optimization
        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE),
                                    p.symbol("array_of_struct", ARRAY_ROW_TYPE).toSymbolReference()),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // Transform input argument is not symbol reference, skip the optimization
        Call nestedSubscriptfunctionCall = new Call(FUNCTIONS.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(ARRAY_ROW_TYPE, new FunctionType(ImmutableList.of(ANONYMOUS_ROW_TYPE), PRUNED_ROW_TYPE))), ImmutableList.of(new FieldReference(new Reference(ROW_ARRAY_ROW_TYPE, "struct_of_array_of_struct"), 0),
                new Lambda(ImmutableList.of(new Symbol(ANONYMOUS_ROW_TYPE, "transformarray$element")),
                        new Row(ImmutableList.of(new FieldReference(
                                LAMBDA_ELEMENT_REFERENCE,
                                0))))));

        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE),
                                    nestedSubscriptfunctionCall),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .doesNotFire();

        // If already applied and same subfields generated, will not re-apply
        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
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
                toHiveType(new ArrayType(ROW_TYPE)),
                new ArrayType(ROW_TYPE),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
                        ImmutableList.of(new Subfield("array_of_struct",
                                ImmutableList.of(Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("previous")))))),
                REGULAR,
                Optional.empty());

        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), previousColumnHandle,
                                                    e, bigIntColumn))));
                })
                .matches(
                        project(
                                ImmutableMap.of("pruned_nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                        tableScan(
                                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn, bigIntColumn))::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of("array_of_struct", partialColumn::equals, "e", bigIntColumn::equals)))));

        // Subfields are added based on the subscript lambda
        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), fullColumn,
                                                    e, bigIntColumn))));
                })
                .matches(
                        project(
                                ImmutableMap.of("pruned_nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                        tableScan(
                                                hiveTable.withProjectedColumns(ImmutableSet.of(partialColumn, bigIntColumn))::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of("array_of_struct", partialColumn::equals, "e", bigIntColumn::equals)))));

        // Subfields are added based on the subscript lambda, and extends the existing prefix
        HiveColumnHandle nestedColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(RowType.from(asList(field("array", new ArrayType(ROW_TYPE))))),
                RowType.from(asList(field("array", new ArrayType(ROW_TYPE)))),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
                        ImmutableList.of())),
                REGULAR,
                Optional.empty());

        HiveColumnHandle nestedPartialColumn = new HiveColumnHandle(
                "struct_of_array_of_struct",
                0,
                toHiveType(RowType.from(asList(field("array", new ArrayType(ROW_TYPE))))),
                RowType.from(asList(field("array", new ArrayType(ROW_TYPE)))),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0),
                        ImmutableList.of("array"),
                        toHiveType(new ArrayType(ROW_TYPE)),
                        new ArrayType(ROW_TYPE),
                        ImmutableList.of(new Subfield("struct_of_array_of_struct",
                                ImmutableList.of(new Subfield.NestedField("array"),
                                        Subfield.AllSubscripts.getInstance(),
                                        new Subfield.NestedField("a")))))),
                REGULAR,
                Optional.empty());

        tester().assertThat(pushFieldReferenceLambdaThroughFilterIntoTableScan)
                .on(p -> {
                    Symbol e = p.symbol("e", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("pruned_nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                            p.filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                    p.tableScan(
                                            table,
                                            ImmutableList.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), e),
                                            ImmutableMap.of(p.symbol("array_of_struct", ARRAY_ROW_TYPE), nestedColumn,
                                                    e, bigIntColumn))));
                })
                .matches(
                        project(
                                ImmutableMap.of("pruned_nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                                filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "e"), new Constant(BIGINT, 1L)),
                                        tableScan(
                                                hiveTable.withProjectedColumns(ImmutableSet.of(nestedPartialColumn, bigIntColumn))::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of("array_of_struct", nestedPartialColumn::equals, "e", bigIntColumn::equals)))));

        // PushProjectionIntoTableScan will not be impacted and will not remove any array subscript column
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getPlannerContext(),
                new ScalarStatsCalculator(tester().getPlannerContext()));

        HiveColumnHandle structPartialColumn = new HiveColumnHandle(
                "struct_of_int",
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

        tester().assertThat(pushProjectionIntoTableScan)
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_deref", BIGINT), new FieldReference(p.symbol("struct_of_int", ROW_TYPE).toSymbolReference(), 0),
                                        p.symbol("nested_array", PRUNED_ARRAY_ROW_TYPE), dereferenceFunctionCall),
                                p.tableScan(
                                        table,
                                        ImmutableList.of(p.symbol("struct_of_int", ROW_TYPE), p.symbol("array_of_struct", ARRAY_ROW_TYPE)),
                                        ImmutableMap.of(p.symbol("struct_of_int", ROW_TYPE), structPartialColumn.getBaseColumn(),
                                                p.symbol("array_of_struct", ARRAY_ROW_TYPE), partialColumn.getBaseColumn()))))
                .matches(project(
                        ImmutableMap.of("expr_deref", expression(new Reference(BIGINT, "struct_of_int#a")),
                                "nested_array", expression(dereferenceFunctionCall, Optional.of(SymbolAliases.builder().put("transformarray$element", LAMBDA_ELEMENT_REFERENCE).build()))),
                        tableScan(
                                hiveTable.withProjectedColumns(ImmutableSet.of(structPartialColumn, partialColumn.getBaseColumn()))::equals,
                                TupleDomain.all(),
                                ImmutableMap.of("struct_of_int#a", structPartialColumn::equals, "array_of_struct", partialColumn.getBaseColumn()::equals))));

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
