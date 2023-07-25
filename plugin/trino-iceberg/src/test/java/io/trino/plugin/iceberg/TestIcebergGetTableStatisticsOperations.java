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

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.MetadataManager;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tracing.TracingMetadata;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;

// Cost-based optimizers' behaviors are affected by the statistics returned by the Connectors. Here is to count the getTableStatistics calls
// when CBOs work with Iceberg Connector.
@Test(singleThreaded = true) // counting metadata is a shared mutable state
public class TestIcebergGetTableStatisticsOperations
        extends AbstractTestQueryFramework
{
    private LocalQueryRunner localQueryRunner;
    private InMemorySpanExporter spanExporter;
    private File metastoreDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        spanExporter = closeAfterClass(InMemorySpanExporter.create());

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        localQueryRunner = LocalQueryRunner.builder(testSessionBuilder().build())
                .withMetadataProvider((systemSecurityMetadata, transactionManager, globalFunctionCatalog, typeManager)
                        -> new TracingMetadata(tracerProvider.get("test"), new MetadataManager(systemSecurityMetadata, transactionManager, globalFunctionCatalog, typeManager)))
                .build();
        localQueryRunner.installPlugin(new TpchPlugin());
        localQueryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        localQueryRunner.addFunctions(functions.build());

        metastoreDir = Files.createTempDirectory("test_iceberg_get_table_statistics_operations").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(metastoreDir);
        localQueryRunner.createCatalog(
                "iceberg",
                new TestingIcebergConnectorFactory(Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)), Optional.empty(), EMPTY_MODULE),
                ImmutableMap.of());
        Database database = Database.builder()
                .setDatabaseName("tiny")
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        localQueryRunner.execute("CREATE TABLE iceberg.tiny.orders AS SELECT * FROM tpch.tiny.orders");
        localQueryRunner.execute("CREATE TABLE iceberg.tiny.lineitem AS SELECT * FROM tpch.tiny.lineitem");
        localQueryRunner.execute("CREATE TABLE iceberg.tiny.customer AS SELECT * FROM tpch.tiny.customer");

        return localQueryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
        localQueryRunner.close();
        localQueryRunner = null;
        spanExporter = null;
    }

    @BeforeMethod
    public void resetCounters()
    {
        spanExporter.reset();
    }

    @Test
    public void testTwoWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM iceberg.tiny.orders o, iceberg.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(2);
    }

    @Test
    public void testThreeWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM iceberg.tiny.customer c, iceberg.tiny.orders o, iceberg.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey AND c.custkey = o.custkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(3);
    }

    private void planDistributedQuery(@Language("SQL") String sql)
    {
        transaction(localQueryRunner.getTransactionManager(), localQueryRunner.getAccessControl())
                .execute(localQueryRunner.getDefaultSession(), session -> {
                    localQueryRunner.createPlan(session, sql, OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
                });
    }

    private long getTableStatisticsMethodInvocations()
    {
        return spanExporter.getFinishedSpanItems().stream()
                .map(SpanData::getName)
                .filter(name -> name.equals("Metadata.getTableStatistics"))
                .count();
    }
}
