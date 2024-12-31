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
package io.trino.plugin.paimon;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.Session;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.paimon.testing.PaimonTablesInitializer;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

/**
 * The query runner of trino.
 */
final class PaimonQueryRunner
{
    private static final String PAIMON_CATALOG = "paimon";
    private static final String SCHEMA_NAME = "paimon";

    private PaimonQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Hive3MinioDataLake hiveMinioDataLake)
    {
        return new Builder()
                .setWarehouse("s3://" + hiveMinioDataLake.getBucketName() + "/")
                .addConnectorProperty("fs.native-s3.enabled", "true")
                .addConnectorProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addConnectorProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .addConnectorProperty("s3.region", MINIO_REGION)
                .addConnectorProperty("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .addConnectorProperty("s3.path-style-access", "true")
                .addConnectorProperty("paimon.catalog.type", "hive")
                .addConnectorProperty("hive.metastore", "file");
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private String warehouse;
        private PaimonTablesInitializer dataLoader;

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("paimon")
                    .setSchema(SCHEMA_NAME)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder setWarehouse(String warehosue)
        {
            this.warehouse = warehosue;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setDataLoader(PaimonTablesInitializer dataLoader)
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
            Session session = testSessionBuilder().setCatalog(PAIMON_CATALOG).setSchema(SCHEMA_NAME).build();
            DistributedQueryRunner queryRunner =
                    DistributedQueryRunner.builder(session).build();
            queryRunner.installPlugin(new TestingHivePaimonPlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("paimon_data")));
            if (warehouse == null) {
                Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("paimon_data");
                Path catalogDir = dataDir.getParent().resolve("catalog");
                warehouse = catalogDir.toFile().toURI().toString();
            }
            connectorProperties.put("paimon.warehouse", warehouse);
            queryRunner.createCatalog(PAIMON_CATALOG, PAIMON_CATALOG, connectorProperties);

            dataLoader.initializeTables(session, queryRunner, SCHEMA_NAME);
            return queryRunner;
        }
    }
}
