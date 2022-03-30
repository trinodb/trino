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
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.plugin.deltalake.util.TestingHadoop;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_ACCESS_KEY;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_SECRET_KEY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.util.Strings.isNullOrEmpty;

public final class DeltaLakeQueryRunner
{
    private static final Logger log = Logger.get(DeltaLakeQueryRunner.class);
    public static final String DELTA_CATALOG = "delta_lake";

    private DeltaLakeQueryRunner() {}

    public static DistributedQueryRunner createDeltaLakeQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(ImmutableMap.of(), ImmutableMap.of());
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
                ImmutableMap.<String, String>builder()
                        .put("hive.azure.abfs-storage-account", requiredNonEmptySystemProperty("hive.hadoop2.azure-abfs-account"))
                        .put("hive.azure.abfs-access-key", requiredNonEmptySystemProperty("hive.hadoop2.azure-abfs-access-key"))
                        .putAll(connectorProperties)
                        .buildOrThrow(),
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

    public static class DefaultDeltaLakeQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            DistributedQueryRunner queryRunner = null;
            try {
                queryRunner = createDeltaLakeQueryRunner(
                        ImmutableMap.of("http-server.http.port", "8080"),
                        ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));
            }
            catch (Throwable t) {
                log.error(t);
                System.exit(1);
            }
            Thread.sleep(10);
            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

            System.out.println("Data directory is: " + queryRunner.getCoordinator().getBaseDataDir().resolve(DELTA_CATALOG));
            String dataPath = DeltaLakeQueryRunner.class.getClassLoader().getResource("databricks/person").toExternalForm();
            queryRunner.execute(
                    format("CREATE TABLE person (name VARCHAR(256), age INTEGER) WITH (location = '%s')", dataPath));
            System.out.println("Record count for person table: " + queryRunner.execute("select name from person").getRowCount());
        }
    }

    public static class S3DeltaLakeQueryRunnerMain
    {
        public static void main(String[] args)
        {
            Logging.initialize();
            DistributedQueryRunner queryRunner;
            String schema = "default";
            String bucketName = "test-bucket";

            try {
                DockerizedMinioDataLake dockerizedMinioDataLake = createDockerizedMinioDataLakeForDeltaLake(bucketName);
                queryRunner = DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                        DELTA_CATALOG,
                        schema,
                        ImmutableMap.of("http-server.http.port", "8080"), ImmutableMap.of(),
                        ImmutableMap.of(),
                        dockerizedMinioDataLake.getMinioAddress(),
                        dockerizedMinioDataLake.getTestingHadoop(),
                        runner -> {});

                Thread.sleep(10);
                Logger log = Logger.get(DeltaLakeQueryRunner.class);
                log.info("======== SERVER STARTED ========");
                log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

                System.out.println("Data directory is: " + queryRunner.getCoordinator().getBaseDataDir().resolve(DELTA_CATALOG));
            }
            catch (Throwable t) {
                log.error(t);
            }
        }
    }
}
