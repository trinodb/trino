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

package io.trino.sql.planner;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.metastore.recording.HiveMetastoreRecording;
import io.trino.plugin.hive.metastore.recording.RecordingHiveMetastore;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static io.trino.Session.SessionBuilder;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.hive.metastore.recording.TestRecordingHiveMetastore.createJsonCodec;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

public abstract class AbstractHiveCostBasedPlanTest
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        String catalog = "local";
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(getSchema())
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name());
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(sessionBuilder.build())
                .withNodeCountForStats(8)
                .build();
        queryRunner.createCatalog(
                catalog,
                createConnectorFactory(),
                ImmutableMap.of());
        return queryRunner;
    }

    protected ConnectorFactory createConnectorFactory()
    {
        RecordingMetastoreConfig recordingConfig = new RecordingMetastoreConfig()
                .setRecordingPath(getRecordingPath())
                .setReplay(true);
        try {
            // The RecordingHiveMetastore loads the metadata files generated through HiveMetadataRecorder
            // which essentially helps to generate the optimal query plans for validation purposes. These files
            // contains all the metadata including statistics.
            RecordingHiveMetastore metastore = new RecordingHiveMetastore(
                    new UnimplementedHiveMetastore(),
                    new HiveMetastoreRecording(recordingConfig, createJsonCodec()));
            return new TestingHiveConnectorFactory(metastore);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getSchema()
    {
        String fileName = Paths.get(getRecordingPath()).getFileName().toString();
        return fileName.split("\\.")[0];
    }

    private String getRecordingPath()
    {
        URL resource = getClass().getResource(getMetadataDir());
        if (resource == null) {
            throw new RuntimeException("Hive metadata directory doesn't exist: " + getMetadataDir());
        }

        File[] files = new File(resource.getPath()).listFiles();
        if (files == null) {
            throw new RuntimeException("Hive metadata recording file doesn't exist in directory: " + getMetadataDir());
        }

        return Arrays.stream(files)
                .filter(f -> !f.isDirectory())
                .collect(onlyElement())
                .getPath();
    }

    protected abstract String getMetadataDir();

    protected abstract Stream<String> getQueryResourcePaths();

    @DataProvider
    public Object[][] getQueriesDataProvider()
    {
        return getQueryResourcePaths()
                .collect(toDataProvider());
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void test(String queryResourcePath)
    {
        assertEquals(generateQueryPlan(readQuery(queryResourcePath)), read(getQueryPlanResourcePath(queryResourcePath)));
    }

    private String getQueryPlanResourcePath(String queryResourcePath)
    {
        String subDir = isPartitioned() ? "partitioned" : "unpartitioned";
        java.nio.file.Path tempPath = Paths.get(queryResourcePath.replaceAll("\\.sql$", ".plan.txt"));
        return Paths.get(tempPath.getParent().toString(), subDir, tempPath.getFileName().toString()).toString();
    }

    protected abstract boolean isPartitioned();

    protected void generate()
    {
        initPlanTest();
        try {
            getQueryResourcePaths()
                    .parallel()
                    .forEach(queryResourcePath -> {
                        try {
                            Path queryPlanWritePath = Paths.get(
                                    getSourcePath().toString(),
                                    "src/test/resources",
                                    getQueryPlanResourcePath(queryResourcePath));
                            createParentDirs(queryPlanWritePath.toFile());
                            write(generateQueryPlan(readQuery(queryResourcePath)).getBytes(UTF_8), queryPlanWritePath.toFile());
                            System.out.println("Generated expected plan for query: " + queryResourcePath);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        finally {
            destroyPlanTest();
        }
    }

    public static String readQuery(String resource)
    {
        return read(resource).replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"")
                .replace("${scale}", "1");
    }

    private static String read(String resource)
    {
        try {
            return Resources.toString(getResource(AbstractHiveCostBasedPlanTest.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String generateQueryPlan(String query)
    {
        Plan plan = plan(query, OPTIMIZED_AND_VALIDATED, false);

        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    protected Path getSourcePath()
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        verify(isDirectory(workingDir), "Working directory is not a directory");
        String topDirectoryName = workingDir.getFileName().toString();
        switch (topDirectoryName) {
            case "trino-benchto-benchmarks":
                return workingDir;
            case "trino":
                return workingDir.resolve("testing/trino-benchto-benchmarks");
            default:
                throw new IllegalStateException("This class must be executed from trino-benchto-benchmarks or Trino source directory");
        }
    }

    private class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder result = new StringBuilder();

        public String result()
        {
            return result.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinNode.DistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new VerifyException("Expected distribution type to be set"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS JOIN to be INNER REPLICATED");
                if (node.isMaySkipOutputDuplicates()) {
                    output(indent, "cross join (can skip output duplicates):");
                }
                else {
                    output(indent, "cross join:");
                }
            }
            else {
                if (node.isMaySkipOutputDuplicates()) {
                    output(indent, "join (%s, %s, can skip output duplicates):", node.getType(), distributionType);
                }
                else {
                    output(indent, "join (%s, %s):", node.getType(), distributionType);
                }
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            Partitioning partitioning = node.getPartitioningScheme().getPartitioning();
            output(
                    indent,
                    "%s exchange (%s, %s, %s)",
                    node.getScope().name().toLowerCase(ENGLISH),
                    node.getType(),
                    partitioning.getHandle(),
                    partitioning.getArguments().stream()
                            .map(Object::toString)
                            .sorted() // Currently, order of hash columns is not deterministic
                            .collect(joining(", ", "[", "]")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            output(
                    indent,
                    "%s aggregation over (%s)",
                    node.getStep().name().toLowerCase(ENGLISH),
                    node.getGroupingKeys().stream()
                            .map(Object::toString)
                            .sorted()
                            .collect(joining(", ")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TableMetadata tableMetadata = getTableMetadata(node.getTable());
            output(indent, "scan %s", tableMetadata.getTable().getTableName());

            return null;
        }

        private TableMetadata getTableMetadata(TableHandle tableHandle)
        {
            QueryRunner queryRunner = getQueryRunner();
            return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                    .singleStatement()
                    .execute(queryRunner.getDefaultSession(), transactionSession -> {
                        // metadata.getCatalogHandle() registers the catalog for the transaction
                        queryRunner.getMetadata().getCatalogHandle(transactionSession, tableHandle.getCatalogName().getCatalogName());
                        return queryRunner.getMetadata().getTableMetadata(transactionSession, tableHandle);
                    });
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            output(indent, "semijoin (%s):", node.getDistributionType().get());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            output(indent, "values (%s rows)", node.getRowCount());

            return null;
        }

        private void output(int indent, String message, Object... args)
        {
            String formattedMessage = format(message, args);
            result.append(format("%s%s\n", "    ".repeat(indent), formattedMessage));
        }
    }
}
