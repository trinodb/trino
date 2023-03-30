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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.util.Strings.isNullOrEmpty;

public final class DeltaLakeQueryRunner
{
    private static final Logger log = Logger.get(DeltaLakeQueryRunner.class);
    public static final String DELTA_CATALOG = "delta_lake";
    public static final String TPCH_SCHEMA = "tpch";

    private DeltaLakeQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Session defaultSession)
    {
        return new Builder(defaultSession);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private String catalogName;
        private ImmutableMap.Builder<String, String> deltaProperties = ImmutableMap.builder();

        protected Builder()
        {
            super(createSession());
        }

        protected Builder(Session defaultSession)
        {
            super(defaultSession);
        }

        @CanIgnoreReturnValue
        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder setDeltaProperties(Map<String, String> deltaProperties)
        {
            this.deltaProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(deltaProperties, "deltaProperties is null"));
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new TpcdsPlugin());
                queryRunner.createCatalog("tpcds", "tpcds");

                queryRunner.installPlugin(new TestingDeltaLakePlugin());
                Map<String, String> deltaProperties = new HashMap<>(this.deltaProperties.buildOrThrow());
                if (!deltaProperties.containsKey("hive.metastore") && !deltaProperties.containsKey("hive.metastore.uri")) {
                    Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve(DELTA_CATALOG);
                    deltaProperties.put("hive.metastore", "file");
                    deltaProperties.put("hive.metastore.catalog.dir", dataDir.toUri().toString());
                }

                queryRunner.createCatalog(catalogName, CONNECTOR_NAME, deltaProperties);

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static DistributedQueryRunner createDeltaLakeQueryRunner(String catalogName)
            throws Exception
    {
        return createDeltaLakeQueryRunner(catalogName, ImmutableMap.of(), ImmutableMap.of());
    }

    public static DistributedQueryRunner createDeltaLakeQueryRunner(String catalogName, Map<String, String> extraProperties, Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = builder(createSession())
                .setCatalogName(catalogName)
                .setExtraProperties(extraProperties)
                .setDeltaProperties(connectorProperties)
                .build();

        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS tpch");

        return queryRunner;
    }

    public static DistributedQueryRunner createS3DeltaLakeQueryRunner(String catalogName, String schemaName, Map<String, String> connectorProperties, String minioAddress, HiveHadoop testingHadoop)
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
            HiveHadoop testingHadoop,
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
                        .put("hive.metastore-timeout", "1m") // read timed out sometimes happens with the default timeout
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
            HiveHadoop testingHadoop)
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
            HiveHadoop hiveHadoop,
            Consumer<QueryRunner> additionalSetup)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .build();

        Builder builder = builder(session);
        extraProperties.forEach(builder::addExtraProperty);
        coordinatorProperties.forEach(builder::setSingleCoordinatorProperty);
        return builder
                .setCatalogName(catalogName)
                .setAdditionalSetup(additionalSetup)
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://" + hiveHadoop.getHiveMetastoreEndpoint())
                        .put("hive.s3.streaming.part-size", "5MB") //must be at least 5MB according to annotations on io.trino.plugin.hive.s3.HiveS3Config.getS3StreamingPartSize
                        .putAll(connectorProperties)
                        .buildOrThrow())
                .build();
    }

    private static String requiredNonEmptySystemProperty(String propertyName)
    {
        String val = System.getProperty(propertyName);
        checkArgument(!isNullOrEmpty(val), format("System property %s must be non-empty", propertyName));
        return val;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static class DefaultDeltaLakeQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            DistributedQueryRunner queryRunner = createDeltaLakeQueryRunner(
                    DELTA_CATALOG,
                    ImmutableMap.of("http-server.http.port", "8080"),
                    ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));

            Path baseDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve(DELTA_CATALOG);
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), TpchTable.getTables());
            log.info("Data directory is: %s", baseDirectory);

            Thread.sleep(10);
            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static class DeltaLakeGlueQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            // Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
            // See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
            DistributedQueryRunner queryRunner = createDeltaLakeQueryRunner(
                    DELTA_CATALOG,
                    ImmutableMap.of("http-server.http.port", "8080"),
                    ImmutableMap.of("hive.metastore", "glue"));

            Logger log = Logger.get(DeltaLakeGlueQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static class S3DeltaLakeQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            String bucketName = "test-bucket";

            HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
            hiveMinioDataLake.start();
            DistributedQueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                    DELTA_CATALOG,
                    TPCH_SCHEMA,
                    ImmutableMap.of("http-server.http.port", "8080"),
                    ImmutableMap.of(),
                    ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                    hiveMinioDataLake.getMinio().getMinioAddress(),
                    hiveMinioDataLake.getHiveHadoop(),
                    runner -> {});

            queryRunner.execute("CREATE SCHEMA tpch WITH (location='s3://" + bucketName + "/tpch')");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), TpchTable.getTables());

            Thread.sleep(10);
            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
