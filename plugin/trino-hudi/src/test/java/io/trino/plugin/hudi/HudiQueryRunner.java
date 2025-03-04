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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.filesystem.Location;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public final class HudiQueryRunner
{
    private HudiQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.hudi", Level.OFF);
    }

    private static final String SCHEMA_NAME = "tests";

    public static Builder builder()
    {
        return new Builder("local:///");
    }

    public static Builder builder(Hive3MinioDataLake hiveMinioDataLake)
    {
        return new Builder("s3://" + hiveMinioDataLake.getBucketName() + "/")
                .addConnectorProperty("fs.native-s3.enabled", "true")
                .addConnectorProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addConnectorProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .addConnectorProperty("s3.region", MINIO_REGION)
                .addConnectorProperty("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .addConnectorProperty("s3.path-style-access", "true");
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final String schemaLocation;
        private HudiTablesInitializer dataLoader;
        private final Map<String, String> connectorProperties = new HashMap<>();

        protected Builder(String schemaLocation)
        {
            super(testSessionBuilder()
                    .setCatalog("hudi")
                    .setSchema(SCHEMA_NAME)
                    .build());
            this.schemaLocation = requireNonNull(schemaLocation, "schemaLocation is null");
        }

        @CanIgnoreReturnValue
        public Builder setDataLoader(HudiTablesInitializer dataLoader)
        {
            this.dataLoader = dataLoader;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TestingHudiPlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hudi_data")));
                queryRunner.createCatalog("hudi", "hudi", connectorProperties);

                // Hudi connector does not support creating schema or any other write operations
                ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                        .getInstance(HiveMetastoreFactory.class)
                        .createMetastore(Optional.empty())
                        .createDatabase(Database.builder()
                                .setDatabaseName(SCHEMA_NAME)
                                .setOwnerName(Optional.of("public"))
                                .setOwnerType(Optional.of(PrincipalType.ROLE))
                                .build());

                dataLoader.initializeTables(queryRunner, Location.of(schemaLocation), SCHEMA_NAME);
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static final class DefaultHudiQueryRunnerMain
    {
        private DefaultHudiQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            Logger log = Logger.get(DefaultHudiQueryRunnerMain.class);

            QueryRunner queryRunner = builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setDataLoader(new ResourceHudiTablesInitializer())
                    .build();

            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class HudiMinioQueryRunnerMain
    {
        private HudiMinioQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            Logger log = Logger.get(HudiMinioQueryRunnerMain.class);

            Hive3MinioDataLake hiveMinioDataLake = new Hive3MinioDataLake("test-bucket");
            hiveMinioDataLake.start();
            QueryRunner queryRunner = builder(hiveMinioDataLake)
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setDataLoader(new TpchHudiTablesInitializer(TpchTable.getTables()))
                    .build();

            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
