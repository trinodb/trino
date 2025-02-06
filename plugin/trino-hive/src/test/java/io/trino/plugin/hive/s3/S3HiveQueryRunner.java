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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.Hive4MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.net.URI;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public final class S3HiveQueryRunner
{
    static {
        Logging.initialize();
    }

    private S3HiveQueryRunner() {}

    public static QueryRunner create(
            Hive3MinioDataLake hiveMinioDataLake,
            Map<String, String> additionalHiveProperties)
            throws Exception
    {
        return builder(hiveMinioDataLake)
                .setHiveProperties(additionalHiveProperties)
                .build();
    }

    public static Builder builder(HiveMinioDataLake hiveMinioDataLake)
    {
        return builder()
                .setHiveMetastoreEndpoint(hiveMinioDataLake.getHiveMetastoreEndpoint())
                .setS3Endpoint("http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                .setS3Region(MINIO_REGION)
                .setS3AccessKey(MINIO_ACCESS_KEY)
                .setS3SecretKey(MINIO_SECRET_KEY)
                .setBucketName(hiveMinioDataLake.getBucketName());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends HiveQueryRunner.Builder<Builder>
    {
        private URI hiveMetastoreEndpoint;
        private Duration thriftMetastoreTimeout = TestingTokenAwareMetastoreClientFactory.TIMEOUT;
        private ThriftMetastoreConfig thriftMetastoreConfig = new ThriftMetastoreConfig();
        private String s3Region;
        private String s3Endpoint;
        private String s3AccessKey;
        private String s3SecretKey;
        private String bucketName;

        @CanIgnoreReturnValue
        public Builder setHiveMetastoreEndpoint(URI hiveMetastoreEndpoint)
        {
            this.hiveMetastoreEndpoint = requireNonNull(hiveMetastoreEndpoint, "hiveMetastoreEndpoint is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setThriftMetastoreTimeout(Duration thriftMetastoreTimeout)
        {
            this.thriftMetastoreTimeout = requireNonNull(thriftMetastoreTimeout, "thriftMetastoreTimeout is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setThriftMetastoreConfig(ThriftMetastoreConfig thriftMetastoreConfig)
        {
            this.thriftMetastoreConfig = requireNonNull(thriftMetastoreConfig, "thriftMetastoreConfig is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3Region(String s3Region)
        {
            this.s3Region = requireNonNull(s3Region, "s3Region is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3Endpoint(String s3Endpoint)
        {
            this.s3Endpoint = requireNonNull(s3Endpoint, "s3Endpoint is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3AccessKey(String s3AccessKey)
        {
            this.s3AccessKey = requireNonNull(s3AccessKey, "s3AccessKey is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3SecretKey(String s3SecretKey)
        {
            this.s3SecretKey = requireNonNull(s3SecretKey, "s3SecretKey is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setBucketName(String bucketName)
        {
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            requireNonNull(hiveMetastoreEndpoint, "hiveMetastoreEndpoint is null");
            requireNonNull(s3Region, "s3Region is null");
            requireNonNull(s3Endpoint, "s3Endpoint is null");
            requireNonNull(s3AccessKey, "s3AccessKey is null");
            requireNonNull(s3SecretKey, "s3SecretKey is null");
            requireNonNull(bucketName, "bucketName is null");
            String lowerCaseS3Endpoint = s3Endpoint.toLowerCase(Locale.ENGLISH);
            checkArgument(lowerCaseS3Endpoint.startsWith("http://") || lowerCaseS3Endpoint.startsWith("https://"), "Expected http URI for S3 endpoint; got %s", s3Endpoint);

            addHiveProperty("fs.hadoop.enabled", "false");
            addHiveProperty("fs.native-s3.enabled", "true");
            addHiveProperty("s3.region", s3Region);
            addHiveProperty("s3.endpoint", s3Endpoint);
            addHiveProperty("s3.aws-access-key", s3AccessKey);
            addHiveProperty("s3.aws-secret-key", s3SecretKey);
            addHiveProperty("s3.path-style-access", "true");
            setMetastore(distributedQueryRunner -> new BridgingHiveMetastore(
                    testingThriftHiveMetastoreBuilder()
                            .metastoreClient(hiveMetastoreEndpoint, thriftMetastoreTimeout)
                            .thriftMetastoreConfig(thriftMetastoreConfig)
                            .build(distributedQueryRunner::registerResource)));
            setInitialSchemasLocationBase("s3a://" + bucketName); // cannot use s3:// as Hive metastore is not configured to accept it
            return super.build();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Hive3MinioDataLake hiveMinioDataLake = new Hive3MinioDataLake("tpch");
        hiveMinioDataLake.start();

        QueryRunner queryRunner = S3HiveQueryRunner.builder(hiveMinioDataLake)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                .setSkipTimezoneSetup(true)
                .setInitialTables(TpchTable.getTables())
                .build();
        Logger log = Logger.get(S3HiveQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static class S3Hive4QueryRunner
    {
        public static void main(String[] args)
                throws Exception
        {
            Hive4MinioDataLake hiveMinioDataLake = new Hive4MinioDataLake("tpch");
            hiveMinioDataLake.start();

            QueryRunner queryRunner = S3HiveQueryRunner.builder(hiveMinioDataLake)
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                    .setSkipTimezoneSetup(true)
                    .setInitialTables(TpchTable.getTables())
                    .build();
            Logger log = Logger.get(S3Hive4QueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
