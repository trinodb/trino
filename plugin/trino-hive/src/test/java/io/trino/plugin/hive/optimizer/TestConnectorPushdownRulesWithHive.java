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
import com.google.common.io.Files;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.metadata.TableHandle;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class TestConnectorPushdownRulesWithHive
        extends BaseRuleTest
{
    private static final String HIVE_CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "test_schema";

    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private File baseDir;
    private HiveMetastore metastore;

    private static final Session HIVE_SESSION = testSessionBuilder()
            .setCatalog(HIVE_CATALOG_NAME)
            .setSchema(SCHEMA_NAME)
            .build();

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        baseDir = Files.createTempDir();
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        metastore = new FileHiveMetastore(
                new NodeVersion("test_version"),
                environment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();

        metastore.createDatabase(new HiveIdentity(SESSION), database);

        LocalQueryRunner queryRunner = LocalQueryRunner.create(HIVE_SESSION);
        queryRunner.createCatalog(HIVE_CATALOG_NAME, new TestingHiveConnectorFactory(metastore), ImmutableMap.of());

        return Optional.of(queryRunner);
    }

    @Test
    public void testProjectionPushdown()
    {
        String tableName = "projection_test";
        PushProjectionIntoTableScan pushProjectionIntoTableScan = new PushProjectionIntoTableScan(
                tester().getMetadata(),
                tester().getTypeAnalyzer(),
                new ScalarStatsCalculator(tester().getMetadata(), tester().getTypeAnalyzer()));

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
                        BIGINT)),
                REGULAR,
                Optional.empty());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(new CatalogName(HIVE_CATALOG_NAME), hiveTable, new HiveTransactionHandle(), Optional.empty());

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
                                                new CatalogName(HIVE_CATALOG_NAME),
                                                hiveTable.withProjectedColumns(ImmutableSet.of(fullColumn)),
                                                new HiveTransactionHandle(),
                                                Optional.empty()),
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

        metastore.dropTable(new HiveIdentity(SESSION), SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testPredicatePushdown()
    {
        String tableName = "predicate_test";
        tester().getQueryRunner().execute(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

        PushPredicateIntoTableScan pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getMetadata(), new TypeOperators(), tester().getTypeAnalyzer());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(new CatalogName(HIVE_CATALOG_NAME), hiveTable, new HiveTransactionHandle(), Optional.empty());

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

        metastore.dropTable(new HiveIdentity(SESSION), SCHEMA_NAME, tableName, true);
    }

    @Test
    public void testColumnPruningProjectionPushdown()
    {
        String tableName = "column_pruning_projection_test";
        tester().getQueryRunner().execute(format("CREATE TABLE %s (a, b) AS SELECT 5, 6", tableName));

        PruneTableScanColumns pruneTableScanColumns = new PruneTableScanColumns(tester().getMetadata());

        HiveTableHandle hiveTable = new HiveTableHandle(SCHEMA_NAME, tableName, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        TableHandle table = new TableHandle(new CatalogName(HIVE_CATALOG_NAME), hiveTable, new HiveTransactionHandle(), Optional.empty());

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
                                ImmutableMap.of("expr", PlanMatchPattern.expression("COLA")),
                                tableScan(
                                        hiveTable.withProjectedColumns(ImmutableSet.of(columnA))::equals,
                                        TupleDomain.all(),
                                        ImmutableMap.of("COLA", columnA::equals))));

        metastore.dropTable(new HiveIdentity(SESSION), SCHEMA_NAME, tableName, true);
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup()
            throws IOException
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
