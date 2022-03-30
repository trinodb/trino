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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.plugin.deltalake.util.TestingHadoop;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_ACCESS_KEY;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_SECRET_KEY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.util.Strings.isNullOrEmpty;

public final class DeltaLakeQueryRunner
{
    private static final Logger log = Logger.get(DeltaLakeQueryRunner.class);
    public static final String DELTA_CATALOG = "delta_lake";
    public static final String TPCH_SCHEMA = "tpch";

    private DeltaLakeQueryRunner() {}

    public static DistributedQueryRunner createDeltaLakeQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(ImmutableMap.of(), ImmutableMap.of());
    }

    public static DistributedQueryRunner createDeltaLakeQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        return createDeltaLakeQueryRunner(ImmutableMap.of(), connectorProperties);
    }

    public static DistributedQueryRunner createDeltaLakeQueryRunner(Map<String, String> extraProperties, Map<String, String> connectorProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(session);
        extraProperties.forEach(builder::addExtraProperty);
        DistributedQueryRunner queryRunner = builder.build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve(DELTA_CATALOG);

        connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
        connectorProperties.putIfAbsent("hive.metastore", "file");
        connectorProperties.putIfAbsent("hive.metastore.catalog.dir", dataDir.toString());

        queryRunner.installPlugin(new TestingDeltaLakePlugin());
        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, connectorProperties);

        queryRunner.execute("CREATE SCHEMA tpch");

        return queryRunner;
    }

    public static DistributedQueryRunner createS3DeltaLakeQueryRunner(String catalogName, String schemaName, Map<String, String> connectorProperties, String minioAddress, TestingHadoop testingHadoop)
            throws Exception
    {
        return createS3DeltaLakeQueryRunner(catalogName, schemaName, ImmutableMap.of(), ImmutableMap.of(), connectorProperties, minioAddress, testingHadoop, queryRunner -> {});
    }

    public static DistributedQueryRunner createS3DeltaLakeQueryRunner(
            String catalogName,
            String schemaName,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            String minioAddress,
            TestingHadoop testingHadoop,
            Consumer<QueryRunner> additionalSetup)
            throws Exception
    {
        return createDockerizedDeltaLakeQueryRunner(
                catalogName,
                schemaName,
                coordinatorProperties,
                extraProperties,
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", minioAddress)
                        .put("hive.s3.path-style-access", "true")
                        .putAll(connectorProperties)
                        .buildOrThrow(),
                testingHadoop,
                additionalSetup);
    }

    public static QueryRunner createAbfsDeltaLakeQueryRunner(
            String catalogName,
            String schemaName,
            TestingHadoop testingHadoop)
            throws Exception
    {
        return createAbfsDeltaLakeQueryRunner(catalogName, schemaName, ImmutableMap.of(), ImmutableMap.of(), testingHadoop);
    }

    public static QueryRunner createAbfsDeltaLakeQueryRunner(
            String catalogName,
            String schemaName,
            Map<String, String> connectorProperties,
            TestingHadoop testingHadoop)
            throws Exception
    {
        return createAbfsDeltaLakeQueryRunner(catalogName, schemaName, ImmutableMap.of(), connectorProperties, testingHadoop);
    }

    public static QueryRunner createAbfsDeltaLakeQueryRunner(
            String catalogName,
            String schemaName,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            TestingHadoop testingHadoop)
            throws Exception
    {
        return createDockerizedDeltaLakeQueryRunner(
                catalogName,
                schemaName,
                extraProperties,
                ImmutableMap.<String, String>builder()
                        .put("hive.azure.abfs-storage-account", requiredNonEmptySystemProperty("hive.hadoop2.azure-abfs-account"))
                        .put("hive.azure.abfs-access-key", requiredNonEmptySystemProperty("hive.hadoop2.azure-abfs-access-key"))
                        .putAll(connectorProperties)
                        .buildOrThrow(),
                testingHadoop);
    }

    public static DistributedQueryRunner createDockerizedDeltaLakeQueryRunner(
            String catalogName,
            String schemaName,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            TestingHadoop testingHadoop)
            throws Exception
    {
        return createDockerizedDeltaLakeQueryRunner(
                catalogName,
                schemaName,
                ImmutableMap.of(),
                extraProperties,
                connectorProperties,
                testingHadoop,
                queryRunner -> {});
    }

    public static DistributedQueryRunner createDockerizedDeltaLakeQueryRunner(
            String catalogName,
            String schemaName,
            Map<String, String> coordinatorProperties,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            TestingHadoop testingHadoop,
            Consumer<QueryRunner> additionalSetup)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .build();

        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(session);
        extraProperties.forEach(builder::addExtraProperty);
        coordinatorProperties.forEach(builder::setSingleCoordinatorProperty);
        builder.setAdditionalSetup(additionalSetup);
        DistributedQueryRunner queryRunner = builder.build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new TestingDeltaLakePlugin());
        Map<String, String> deltaLakeProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", testingHadoop.getMetastoreAddress())
                .put("hive.s3.streaming.part-size", "5MB") //must be at least 5MB according to annotations on io.trino.plugin.hive.s3.HiveS3Config.getS3StreamingPartSize
                .putAll(connectorProperties)
                .buildOrThrow();

        queryRunner.createCatalog(catalogName, CONNECTOR_NAME, deltaLakeProperties);
        return queryRunner;
    }

    private static String requiredNonEmptySystemProperty(String propertyName)
    {
        String val = System.getProperty(propertyName);
        checkArgument(!isNullOrEmpty(val), format("System property %s must be non-empty", propertyName));
        return val;
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            TableLocationSupplier locationSupplier,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), locationSupplier, session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, TableLocationSupplier locationSupplier, Session session)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        copyTable(queryRunner, table, locationSupplier, session);
    }

    private static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, TableLocationSupplier locationSupplier, Session session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        String location = locationSupplier.getTableLocation(table.getSchemaName(), table.getObjectName());
        @Language("SQL") String sql = format("CREATE TABLE IF NOT EXISTS %s WITH (location='%s') AS SELECT * FROM %s", table.getObjectName(), location, table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());

        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + table).getOnlyValue())
                .as("Table is not loaded properly: %s", table)
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + table.getObjectName()).getOnlyValue());
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    @FunctionalInterface
    private interface TableLocationSupplier
    {
        String getTableLocation(String schemaName, String tableName);
    }

    public static class DefaultDeltaLakeQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            DistributedQueryRunner queryRunner = createDeltaLakeQueryRunner(
                    ImmutableMap.of("http-server.http.port", "8080"),
                    ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));

            Path baseDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve(DELTA_CATALOG);
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, (schemaName, tableName) -> format("file://%s/%s/%s", baseDirectory, schemaName, tableName), createSession(), TpchTable.getTables());
            log.info("Data directory is: %s", baseDirectory);

            Thread.sleep(10);
            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static class S3DeltaLakeQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            String bucketName = "test-bucket";

            DockerizedMinioDataLake dockerizedMinioDataLake = createDockerizedMinioDataLakeForDeltaLake(bucketName);
            DistributedQueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                    DELTA_CATALOG,
                    TPCH_SCHEMA,
                    ImmutableMap.of("http-server.http.port", "8080"),
                    ImmutableMap.of(),
                    ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                    dockerizedMinioDataLake.getMinioAddress(),
                    dockerizedMinioDataLake.getTestingHadoop(),
                    runner -> {});

            queryRunner.execute("CREATE SCHEMA tpch");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, (schemaName, tableName) -> format("s3://%s/%s/%s", bucketName, schemaName, tableName), createSession(), TpchTable.getTables());

            Thread.sleep(10);
            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
