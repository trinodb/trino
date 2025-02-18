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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer;
import io.trino.plugin.iceberg.catalog.rest.TestingPolarisCatalog;
import io.trino.plugin.iceberg.containers.NessieContainer;
import io.trino.plugin.iceberg.containers.UnityCatalogContainer;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

public final class IcebergQueryRunner
{
    private IcebergQueryRunner() {}

    public static final String ICEBERG_CATALOG = "iceberg";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.iceberg", Level.OFF);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(String schema)
    {
        return new Builder(schema);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Optional<File> metastoreDirectory = Optional.empty();
        private ImmutableMap.Builder<String, String> icebergProperties = ImmutableMap.builder();
        private Optional<SchemaInitializer> schemaInitializer = Optional.of(SchemaInitializer.builder().build());
        private boolean tpcdsCatalogEnabled;

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(ICEBERG_CATALOG)
                    .setSchema("tpch")
                    .build());
        }

        protected Builder(String schema)
        {
            super(testSessionBuilder()
                    .setCatalog(ICEBERG_CATALOG)
                    .setSchema(schema)
                    .build());
        }

        public Builder setMetastoreDirectory(File metastoreDirectory)
        {
            this.metastoreDirectory = Optional.of(metastoreDirectory);
            return self();
        }

        public Builder setIcebergProperties(Map<String, String> icebergProperties)
        {
            this.icebergProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(icebergProperties, "icebergProperties is null"));
            return self();
        }

        public Builder addIcebergProperty(String key, String value)
        {
            this.icebergProperties.put(key, value);
            return self();
        }

        public Builder setInitialTables(TpchTable<?>... initialTables)
        {
            return setInitialTables(ImmutableList.copyOf(initialTables));
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            setSchemaInitializer(SchemaInitializer.builder().withClonedTpchTables(initialTables).build());
            return self();
        }

        public Builder setSchemaInitializer(SchemaInitializer schemaInitializer)
        {
            this.schemaInitializer = Optional.of(requireNonNull(schemaInitializer, "schemaInitializer is null"));
            amendSession(sessionBuilder -> sessionBuilder.setSchema(schemaInitializer.getSchemaName()));
            return self();
        }

        public Builder disableSchemaInitializer()
        {
            schemaInitializer = Optional.empty();
            return self();
        }

        public Builder setTpcdsCatalogEnabled(boolean tpcdsCatalogEnabled)
        {
            this.tpcdsCatalogEnabled = tpcdsCatalogEnabled;
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

                if (tpcdsCatalogEnabled) {
                    queryRunner.installPlugin(new TpcdsPlugin());
                    queryRunner.createCatalog("tpcds", "tpcds");
                }

                if (!icebergProperties.buildOrThrow().containsKey("fs.hadoop.enabled")) {
                    icebergProperties.put("fs.hadoop.enabled", "true");
                }

                Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));
                queryRunner.installPlugin(new TestingIcebergPlugin(dataDir));
                queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties.buildOrThrow());
                schemaInitializer.ifPresent(initializer -> initializer.accept(queryRunner));

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static Builder icebergQueryRunnerMainBuilder()
    {
        return IcebergQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setTpcdsCatalogEnabled(true);
    }

    public static final class IcebergRestQueryRunnerMain
    {
        private IcebergRestQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Path warehouseLocation = Files.createTempDirectory(null);
            warehouseLocation.toFile().deleteOnExit();

            Catalog backend = backendCatalog(warehouseLocation);

            DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                    .delegate(backend)
                    .build();

            TestingHttpServer testServer = delegatingCatalog.testServer();
            testServer.start();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setBaseDataDir(Optional.of(warehouseLocation))
                    .setIcebergProperties(ImmutableMap.of(
                            "iceberg.catalog.type", "rest",
                            "iceberg.rest-catalog.uri", testServer.getBaseUrl().toString()))
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(IcebergRestQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergPolarisQueryRunnerMain
    {
        private IcebergPolarisQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Path warehouseLocation = Files.createTempDirectory(null);
            warehouseLocation.toFile().deleteOnExit();

            @SuppressWarnings("resource")
            TestingPolarisCatalog polarisCatalog = new TestingPolarisCatalog(warehouseLocation.toString());

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setBaseDataDir(Optional.of(warehouseLocation))
                    .addIcebergProperty("iceberg.catalog.type", "rest")
                    .addIcebergProperty("iceberg.rest-catalog.uri", polarisCatalog.restUri() + "/api/catalog")
                    .addIcebergProperty("iceberg.rest-catalog.warehouse", TestingPolarisCatalog.WAREHOUSE)
                    .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                    .addIcebergProperty("iceberg.rest-catalog.oauth2.credential", polarisCatalog.oauth2Credentials())
                    .addIcebergProperty("iceberg.rest-catalog.oauth2.scope", "PRINCIPAL_ROLE:ALL")
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(IcebergPolarisQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergUnityQueryRunnerMain
    {
        private IcebergUnityQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Path warehouseLocation = Files.createTempDirectory(null);
            warehouseLocation.toFile().deleteOnExit();

            @SuppressWarnings("resource")
            UnityCatalogContainer unityCatalog = new UnityCatalogContainer("unity", "tpch");

            @SuppressWarnings("resource")
            QueryRunner queryRunner = IcebergQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setBaseDataDir(Optional.of(warehouseLocation))
                    .addIcebergProperty("iceberg.security", "read_only")
                    .addIcebergProperty("iceberg.catalog.type", "rest")
                    .addIcebergProperty("iceberg.rest-catalog.uri", unityCatalog.uri() + "/iceberg")
                    .addIcebergProperty("iceberg.rest-catalog.warehouse", "unity")
                    .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                    .disableSchemaInitializer()
                    .build();

            unityCatalog.copyTpchTables(TpchTable.getTables());

            Logger log = Logger.get(IcebergUnityQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergExternalQueryRunnerMain
    {
        private IcebergExternalQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            // Please set Iceberg connector properties via VM options. e.g. -Diceberg.catalog.type=glue -D..
            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setIcebergProperties(ImmutableMap.of("iceberg.catalog.type", System.getProperty("iceberg.catalog.type")))
                    .build();

            Logger log = Logger.get(IcebergExternalQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergMinioHiveMetastoreQueryRunnerMain
    {
        private IcebergMinioHiveMetastoreQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            String bucketName = "test-bucket";
            @SuppressWarnings("resource")
            Hive3MinioDataLake hiveMinioDataLake = new Hive3MinioDataLake(bucketName);
            hiveMinioDataLake.start();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = IcebergQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setIcebergProperties(Map.of(
                            "iceberg.catalog.type", "HIVE_METASTORE",
                            "hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString(),
                            "fs.hadoop.enabled", "false",
                            "fs.native-s3.enabled", "true",
                            "s3.aws-access-key", MINIO_ACCESS_KEY,
                            "s3.aws-secret-key", MINIO_SECRET_KEY,
                            "s3.region", MINIO_REGION,
                            "s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress(),
                            "s3.path-style-access", "true",
                            "s3.streaming.part-size", "5MB"))
                    .setSchemaInitializer(
                            SchemaInitializer.builder()
                                    .withSchemaName("tpch")
                                    .withClonedTpchTables(TpchTable.getTables())
                                    .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/tpch'"))
                                    .build())
                    .build();

            Logger log = Logger.get(IcebergMinioHiveMetastoreQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergMinioQueryRunnerMain
    {
        private IcebergMinioQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();

            String bucketName = "test-bucket";
            @SuppressWarnings("resource")
            Minio minio = Minio.builder().build();
            minio.start();
            minio.createBucket(bucketName);

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setIcebergProperties(Map.of(
                            "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                            "hive.metastore.catalog.dir", "s3://%s/".formatted(bucketName),
                            "fs.hadoop.enabled", "false",
                            "fs.native-s3.enabled", "true",
                            "s3.aws-access-key", MINIO_ACCESS_KEY,
                            "s3.aws-secret-key", MINIO_SECRET_KEY,
                            "s3.region", MINIO_REGION,
                            "s3.endpoint", "http://" + minio.getMinioApiEndpoint(),
                            "s3.path-style-access", "true",
                            "s3.streaming.part-size", "5MB"))
                    .setSchemaInitializer(
                            SchemaInitializer.builder()
                                    .withSchemaName("tpch")
                                    .withClonedTpchTables(TpchTable.getTables())
                                    .build())
                    .build();

            Logger log = Logger.get(IcebergMinioQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergAzureQueryRunnerMain
    {
        private IcebergAzureQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            String azureContainer = requiredNonEmptySystemProperty("testing.azure-abfs-container");
            String azureAccount = requiredNonEmptySystemProperty("testing.azure-abfs-account");
            String azureAccessKey = requiredNonEmptySystemProperty("testing.azure-abfs-access-key");

            String abfsSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("hdp3.1-core-site.xml.abfs-template"), UTF_8)
                    .replace("%ABFS_ACCESS_KEY%", azureAccessKey)
                    .replace("%ABFS_ACCOUNT%", azureAccount);

            FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
            Path hadoopCoreSiteXmlTempFile = java.nio.file.Files.createTempFile("core-site", ".xml", posixFilePermissions);
            hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
            java.nio.file.Files.writeString(hadoopCoreSiteXmlTempFile, abfsSpecificCoreSiteXmlContent);

            @SuppressWarnings("resource")
            HiveHadoop hiveHadoop = HiveHadoop.builder()
                    .withImage(HiveHadoop.HIVE3_IMAGE)
                    .withFilesToMount(ImmutableMap.of("/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString()))
                    .build();
            hiveHadoop.start();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setIcebergProperties(Map.of(
                            "iceberg.catalog.type", "HIVE_METASTORE",
                            "hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString(),
                            "fs.hadoop.enabled", "false",
                            "fs.native-azure.enabled", "true",
                            "azure.auth-type", "ACCESS_KEY",
                            "azure.access-key", azureAccessKey))
                    .setSchemaInitializer(
                            SchemaInitializer.builder()
                                    .withSchemaName("tpch")
                                    .withClonedTpchTables(TpchTable.getTables())
                                    .withSchemaProperties(Map.of("location", "'abfs://%s@%s.dfs.core.windows.net/test-bucket/'".formatted(azureContainer, azureAccount)))
                                    .build())
                    .build();

            Logger log = Logger.get(IcebergAzureQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergJdbcQueryRunnerMain
    {
        private IcebergJdbcQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Path warehouseLocation = Files.createTempDirectory(null);
            warehouseLocation.toFile().deleteOnExit();

            TestingIcebergJdbcServer server = new TestingIcebergJdbcServer();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setIcebergProperties(ImmutableMap.<String, String>builder()
                            .put("iceberg.catalog.type", "jdbc")
                            .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                            .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                            .put("iceberg.jdbc-catalog.connection-user", USER)
                            .put("iceberg.jdbc-catalog.connection-password", PASSWORD)
                            .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                            .put("iceberg.jdbc-catalog.default-warehouse-dir", warehouseLocation.toAbsolutePath().toString())
                            .buildOrThrow())
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(IcebergJdbcQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergSnowflakeQueryRunnerMain
    {
        private IcebergSnowflakeQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .setIcebergProperties(ImmutableMap.<String, String>builder()
                            .put("iceberg.catalog.type", "snowflake")
                            .put("fs.native-s3.enabled", "true")
                            .put("s3.aws-access-key", requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.access-key"))
                            .put("s3.aws-secret-key", requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.secret-key"))
                            .put("s3.region", requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.region"))
                            .put("iceberg.file-format", "PARQUET")
                            .put("iceberg.snowflake-catalog.account-uri", requiredNonEmptySystemProperty("testing.snowflake.catalog.account-url"))
                            .put("iceberg.snowflake-catalog.user", requiredNonEmptySystemProperty("testing.snowflake.catalog.user"))
                            .put("iceberg.snowflake-catalog.password", requiredNonEmptySystemProperty("testing.snowflake.catalog.password"))
                            .put("iceberg.snowflake-catalog.database", requiredNonEmptySystemProperty("testing.snowflake.catalog.database"))
                            .buildOrThrow())
                    .setSchemaInitializer(
                            SchemaInitializer.builder()
                                    .withSchemaName("tpch") // Requires schema to pre-exist as Iceberg Snowflake catalog doesn't support creating schemas
                                    .build())
                    .build();

            Logger log = Logger.get(IcebergSnowflakeQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergNessieQueryRunnerMain
    {
        private IcebergNessieQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            NessieContainer nessieContainer = NessieContainer.builder().build();
            nessieContainer.start();

            Path tempDir = createTempDirectory("trino_nessie_catalog");

            @SuppressWarnings("resource")
            QueryRunner queryRunner = IcebergQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setBaseDataDir(Optional.of(tempDir))
                    .setIcebergProperties(ImmutableMap.<String, String>builder()
                            .put("iceberg.catalog.type", "nessie")
                            .put("iceberg.nessie-catalog.uri", nessieContainer.getRestApiUri())
                            .put("iceberg.nessie-catalog.default-warehouse-dir", tempDir.toString())
                            .buildOrThrow())
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(IcebergNessieQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class DefaultIcebergQueryRunnerMain
    {
        private DefaultIcebergQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logger log = Logger.get(DefaultIcebergQueryRunnerMain.class);
            File metastoreDir = createTempDirectory("iceberg_query_runner").toFile();
            metastoreDir.deleteOnExit();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .addIcebergProperty("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                    .setInitialTables(TpchTable.getTables())
                    .build();
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class IcebergQueryRunnerWithTaskRetries
    {
        private IcebergQueryRunnerWithTaskRetries() {}

        public static void main(String[] args)
                throws Exception
        {
            Logger log = Logger.get(IcebergQueryRunnerWithTaskRetries.class);

            File exchangeManagerDirectory = createTempDirectory("exchange_manager").toFile();
            Map<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("exchange.base-directories", exchangeManagerDirectory.getAbsolutePath())
                    .buildOrThrow();
            exchangeManagerDirectory.deleteOnExit();

            File metastoreDir = createTempDirectory("iceberg_query_runner").toFile();
            metastoreDir.deleteOnExit();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = icebergQueryRunnerMainBuilder()
                    .addIcebergProperty("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                    .setExtraProperties(ImmutableMap.<String, String>builder()
                            .put("retry-policy", "TASK")
                            .put("fault-tolerant-execution-task-memory", "1GB")
                            .buildOrThrow())
                    .setInitialTables(TpchTable.getTables())
                    .setAdditionalSetup(runner -> {
                        runner.installPlugin(new FileSystemExchangePlugin());
                        runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                    })
                    .build();

            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
