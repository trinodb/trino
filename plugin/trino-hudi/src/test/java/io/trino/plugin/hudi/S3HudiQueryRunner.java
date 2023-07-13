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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.SOCKS_PROXY;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

public final class S3HudiQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private S3HudiQueryRunner() {}

    public static DistributedQueryRunner create(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            HudiTablesInitializer dataLoader,
            HiveMinioDataLake hiveMinioDataLake)
            throws Exception
    {
        String basePath = "s3a://" + hiveMinioDataLake.getBucketName() + "/" + TPCH_SCHEMA;
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(hiveMinioDataLake);

        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .hdfsEnvironment(hdfsEnvironment)
                        .build());
        Database database = Database.builder()
                .setDatabaseName(TPCH_SCHEMA)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        try {
            metastore.createDatabase(database);
        }
        catch (SchemaAlreadyExistsException e) {
            // do nothing if database already exists
        }

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new TestingHudiPlugin(Optional.of(metastore)));
        queryRunner.createCatalog(
                "hudi",
                "hudi",
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                        .put("hive.s3.path-style-access", "true")
                        .putAll(connectorProperties)
                        .buildOrThrow());

        dataLoader.initializeTables(queryRunner, metastore, TPCH_SCHEMA, basePath, hdfsEnvironment);
        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("hudi")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    private static HdfsEnvironment getHdfsEnvironment(HiveMinioDataLake hiveMinioDataLake)
    {
        DynamicHdfsConfiguration dynamicHdfsConfiguration = new DynamicHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig()
                                .setSocksProxy(SOCKS_PROXY.orElse(null)),
                        ImmutableSet.of(
                                new TrinoS3ConfigurationInitializer(new HiveS3Config()
                                        .setS3AwsAccessKey(MINIO_ACCESS_KEY)
                                        .setS3AwsSecretKey(MINIO_SECRET_KEY)
                                        .setS3Endpoint(hiveMinioDataLake.getMinio().getMinioAddress())
                                        .setS3PathStyleAccess(true)))),
                ImmutableSet.of());

        return new HdfsEnvironment(
                dynamicHdfsConfiguration,
                new HdfsConfig(),
                new NoHdfsAuthentication());
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Logger log = Logger.get(S3HudiQueryRunner.class);

        String bucketName = "test-bucket";
        HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
        hiveMinioDataLake.start();
        DistributedQueryRunner queryRunner = create(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                new TpchHudiTablesInitializer(COPY_ON_WRITE, TpchTable.getTables()),
                hiveMinioDataLake);

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
