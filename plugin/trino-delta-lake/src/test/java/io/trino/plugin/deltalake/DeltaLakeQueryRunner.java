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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public final class DeltaLakeQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.parquet.filter2.compat.FilterCompat", Level.OFF);
        logging.setLevel("com.amazonaws.util.Base64", Level.OFF);
        logging.setLevel("com.google.cloud", Level.OFF);
    }

    public static final String DELTA_CATALOG = "delta";
    public static final String TPCH_SCHEMA = "tpch";

    private DeltaLakeQueryRunner() {}

    public static Builder builder()
    {
        return new Builder(TPCH_SCHEMA);
    }

    public static Builder builder(String schemaName)
    {
        return new Builder(schemaName);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final String schemaName;
        private ImmutableMap.Builder<String, String> deltaProperties = ImmutableMap.builder();
        private Optional<String> schemaLocation = Optional.empty();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        protected Builder(String schemaName)
        {
            super(testSessionBuilder()
                    .setCatalog(DELTA_CATALOG)
                    .setSchema(schemaName)
                    .build());
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
        }

        @CanIgnoreReturnValue
        public Builder setDeltaProperties(Map<String, String> deltaProperties)
        {
            this.deltaProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(deltaProperties, "deltaProperties is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addDeltaProperties(Map<String, String> deltaProperties)
        {
            this.deltaProperties.putAll(deltaProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addDeltaProperty(String key, String value)
        {
            return addDeltaProperties(Map.of(key, value));
        }

        @CanIgnoreReturnValue
        public Builder addMetastoreProperties(HiveHadoop hiveHadoop)
        {
            return addDeltaProperties(ImmutableMap.<String, String>builder()
                    .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                    .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                    .buildOrThrow());
        }

        @CanIgnoreReturnValue
        public Builder addS3Properties(Minio minio, String bucketName)
        {
            addDeltaProperties(ImmutableMap.<String, String>builder()
                    .put("fs.hadoop.enabled", "false")
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                    .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", minio.getMinioAddress())
                    .put("s3.path-style-access", "true")
                    .put("s3.streaming.part-size", "5MB") // minimize memory usage
                    .buildOrThrow());
            setSchemaLocation("s3://%s/%s".formatted(bucketName, schemaName));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setSchemaLocation(String schemaLocation)
        {
            this.schemaLocation = Optional.of(schemaLocation);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new TestingDeltaLakePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data")));

                Map<String, String> deltaProperties = new HashMap<>(this.deltaProperties.buildOrThrow());
                if (!deltaProperties.containsKey("hive.metastore") && !deltaProperties.containsKey("hive.metastore.uri")) {
                    deltaProperties.put("hive.metastore", "file");
                }
                queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, deltaProperties);

                String schemaName = queryRunner.getDefaultSession().getSchema().orElseThrow();
                String createSchema = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
                if (schemaLocation.isPresent()) {
                    createSchema += " WITH (location = '" + schemaLocation.get() + "')";
                }
                queryRunner.execute(createSchema);

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static final class DefaultDeltaLakeQueryRunnerMain
    {
        private DefaultDeltaLakeQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            QueryRunner queryRunner = builder()
                    .addExtraProperty("http-server.http.port", "8080")
                    .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class DeltaLakeExternalQueryRunnerMain
    {
        private DeltaLakeExternalQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            // Please set Delta Lake connector properties via VM options. e.g. -Dhive.metastore=glue -D..
            QueryRunner queryRunner = builder()
                    .addExtraProperty("http-server.http.port", "8080")
                    .build();

            Logger log = Logger.get(DeltaLakeExternalQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class S3DeltaLakeQueryRunnerMain
    {
        private S3DeltaLakeQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            String bucketName = "test-bucket";

            HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
            hiveMinioDataLake.start();

            QueryRunner queryRunner = builder()
                    .addExtraProperty("http-server.http.port", "8080")
                    .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                    .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                    .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                    .setInitialTables(TpchTable.getTables())
                    .build();

            Logger log = Logger.get(DeltaLakeQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
