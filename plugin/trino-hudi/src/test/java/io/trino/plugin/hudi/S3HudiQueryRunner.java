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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

public final class S3HudiQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private S3HudiQueryRunner() {}

    public static QueryRunner create(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            HudiTablesInitializer dataLoader,
            HiveMinioDataLake hiveMinioDataLake)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new TestingHudiPlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hudi_data")));
        queryRunner.createCatalog(
                "hudi",
                "hudi",
                ImmutableMap.<String, String>builder()
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .putAll(connectorProperties)
                        .buildOrThrow());

        // Hudi connector does not support creating schema or any other write operations
        ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty())
                .createDatabase(Database.builder()
                        .setDatabaseName(TPCH_SCHEMA)
                        .setOwnerName(Optional.of("public"))
                        .setOwnerType(Optional.of(PrincipalType.ROLE))
                        .build());

        dataLoader.initializeTables(queryRunner, Location.of("s3://" + hiveMinioDataLake.getBucketName() + "/"), TPCH_SCHEMA);

        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("hudi")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Logger log = Logger.get(S3HudiQueryRunner.class);

        String bucketName = "test-bucket";
        HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
        hiveMinioDataLake.start();
        QueryRunner queryRunner = create(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                new TpchHudiTablesInitializer(TpchTable.getTables()),
                hiveMinioDataLake);

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
