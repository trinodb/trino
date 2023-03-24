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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.assertj.core.util.Files;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class IcebergQueryRunner
{
    public static final String ICEBERG_CATALOG = "iceberg";

    private IcebergQueryRunner() {}

    public static DistributedQueryRunner createIcebergQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return builder()
                .setInitialTables(tables)
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Optional<File> metastoreDirectory = Optional.empty();
        private ImmutableMap.Builder<String, String> icebergProperties = ImmutableMap.builder();
        private Optional<SchemaInitializer> schemaInitializer = Optional.empty();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(ICEBERG_CATALOG)
                    .setSchema("tpch")
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
            checkState(this.schemaInitializer.isEmpty(), "schemaInitializer is already set");
            this.schemaInitializer = Optional.of(requireNonNull(schemaInitializer, "schemaInitializer is null"));
            amendSession(sessionBuilder -> sessionBuilder.setSchema(schemaInitializer.getSchemaName()));
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

                queryRunner.installPlugin(new IcebergPlugin());
                Map<String, String> icebergProperties = new HashMap<>(this.icebergProperties.buildOrThrow());
                String catalogType = icebergProperties.get("iceberg.catalog.type");
                Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));
                if (catalogType == null) {
                    icebergProperties.put("iceberg.catalog.type", "TESTING_FILE_METASTORE");
                    icebergProperties.put("hive.metastore.catalog.dir", dataDir.toString());
                }

                queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);
                schemaInitializer.orElseGet(() -> SchemaInitializer.builder().build()).accept(queryRunner);

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static final class IcebergRestQueryRunnerMain
    {
        private IcebergRestQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            File warehouseLocation = Files.newTemporaryFolder();
            warehouseLocation.deleteOnExit();

            Catalog backend = backendCatalog(warehouseLocation);

            DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                    .delegate(backend)
                    .build();

            TestingHttpServer testServer = delegatingCatalog.testServer();
            testServer.start();

            @SuppressWarnings("resource")
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
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

    public static final class IcebergGlueQueryRunnerMain
    {
        private IcebergGlueQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            // Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
            // See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
            @SuppressWarnings("resource")
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .setIcebergProperties(ImmutableMap.of("iceberg.catalog.type", "glue"))
                    .build();

            Logger log = Logger.get(IcebergGlueQueryRunnerMain.class);
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
            HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
            hiveMinioDataLake.start();

            @SuppressWarnings("resource")
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setCoordinatorProperties(Map.of(
                            "http-server.http.port", "8080"))
                    .setIcebergProperties(Map.of(
                            "iceberg.catalog.type", "HIVE_METASTORE",
                            "hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint(),
                            "hive.s3.aws-access-key", MINIO_ACCESS_KEY,
                            "hive.s3.aws-secret-key", MINIO_SECRET_KEY,
                            "hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress(),
                            "hive.s3.path-style-access", "true",
                            "hive.s3.streaming.part-size", "5MB"))
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
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setCoordinatorProperties(Map.of(
                            "http-server.http.port", "8080"))
                    .setIcebergProperties(Map.of(
                            "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                            "hive.metastore.catalog.dir", "s3://%s/".formatted(bucketName),
                            "hive.s3.aws-access-key", MINIO_ACCESS_KEY,
                            "hive.s3.aws-secret-key", MINIO_SECRET_KEY,
                            "hive.s3.endpoint", "http://" + minio.getMinioApiEndpoint(),
                            "hive.s3.path-style-access", "true",
                            "hive.s3.streaming.part-size", "5MB"))
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
            String azureContainer = requireNonNull(
                    System.getProperty("hive.hadoop2.azure-abfs-container"),
                    "System property hive.hadoop2.azure-abfs-container must be provided");
            String azureAccount = requireNonNull(
                    System.getProperty("hive.hadoop2.azure-abfs-account"),
                    "System property hive.hadoop2.azure-abfs-account must be provided");
            String azureAccessKey = requireNonNull(
                    System.getProperty("hive.hadoop2.azure-abfs-access-key"),
                    "System property hive.hadoop2.azure-abfs-access-key must be provided");

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
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setCoordinatorProperties(Map.of(
                            "http-server.http.port", "8080"))
                    .setIcebergProperties(Map.of(
                            "iceberg.catalog.type", "HIVE_METASTORE",
                            "hive.metastore.uri", "thrift://" + hiveHadoop.getHiveMetastoreEndpoint(),
                            "hive.azure.abfs-storage-account", azureAccount,
                            "hive.azure.abfs-access-key", azureAccessKey))
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
            File warehouseLocation = Files.newTemporaryFolder();
            warehouseLocation.deleteOnExit();

            TestingIcebergJdbcServer server = new TestingIcebergJdbcServer();

            @SuppressWarnings("resource")
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .setIcebergProperties(ImmutableMap.<String, String>builder()
                            .put("iceberg.catalog.type", "jdbc")
                            .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                            .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                            .put("iceberg.jdbc-catalog.connection-user", USER)
                            .put("iceberg.jdbc-catalog.connection-password", PASSWORD)
                            .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                            .put("iceberg.jdbc-catalog.default-warehouse-dir", warehouseLocation.getAbsolutePath())
                            .buildOrThrow())
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(IcebergJdbcQueryRunnerMain.class);
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
            @SuppressWarnings("resource")
            DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                    .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .setInitialTables(TpchTable.getTables())
                    .build();
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
