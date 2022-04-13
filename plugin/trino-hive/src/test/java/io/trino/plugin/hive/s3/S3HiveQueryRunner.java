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

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingMetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.DistributedQueryRunner;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class S3HiveQueryRunner
{
    private S3HiveQueryRunner() {}

    public static DistributedQueryRunner create(
            HiveMinioDataLake hiveMinioDataLake,
            Map<String, String> additionalHiveProperties)
            throws Exception
    {
        return builder(hiveMinioDataLake)
                .setHiveProperties(additionalHiveProperties)
                .build();
    }

    public static DistributedQueryRunner create(
            HostAndPort hiveMetastoreEndpoint,
            String s3Endpoint,
            String s3AccessKey,
            String s3SecretKey,
            String bucketName,
            Map<String, String> additionalHiveProperties)
            throws Exception
    {
        return builder()
                .setHiveMetastoreEndpoint(hiveMetastoreEndpoint)
                .setS3Endpoint(s3Endpoint)
                .setS3AccessKey(s3AccessKey)
                .setS3SecretKey(s3SecretKey)
                .setBucketName(bucketName)
                .setHiveProperties(additionalHiveProperties)
                .build();
    }

    public static Builder builder(HiveMinioDataLake hiveMinioDataLake)
    {
        return builder()
                .setHiveMetastoreEndpoint(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                .setS3Endpoint("http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                .setS3AccessKey(HiveMinioDataLake.ACCESS_KEY)
                .setS3SecretKey(HiveMinioDataLake.SECRET_KEY)
                .setBucketName(hiveMinioDataLake.getBucketName());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends HiveQueryRunner.Builder<Builder>
    {
        private HostAndPort hiveMetastoreEndpoint;
        private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
        private String s3Endpoint;
        private String s3AccessKey;
        private String s3SecretKey;
        private String bucketName;

        public Builder setHiveMetastoreEndpoint(HostAndPort hiveMetastoreEndpoint)
        {
            this.hiveMetastoreEndpoint = requireNonNull(hiveMetastoreEndpoint, "hiveMetastoreEndpoint is null");
            return this;
        }

        public Builder setMetastoreTimeout(Duration metastoreTimeout)
        {
            this.metastoreTimeout = metastoreTimeout;
            return this;
        }

        public Builder setS3Endpoint(String s3Endpoint)
        {
            this.s3Endpoint = requireNonNull(s3Endpoint, "s3Endpoint is null");
            return this;
        }

        public Builder setS3AccessKey(String s3AccessKey)
        {
            this.s3AccessKey = requireNonNull(s3AccessKey, "s3AccessKey is null");
            return this;
        }

        public Builder setS3SecretKey(String s3SecretKey)
        {
            this.s3SecretKey = requireNonNull(s3SecretKey, "s3SecretKey is null");
            return this;
        }

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
            requireNonNull(s3Endpoint, "s3Endpoint is null");
            requireNonNull(s3AccessKey, "s3AccessKey is null");
            requireNonNull(s3SecretKey, "s3SecretKey is null");
            requireNonNull(bucketName, "bucketName is null");
            String lowerCaseS3Endpoint = s3Endpoint.toLowerCase(Locale.ENGLISH);
            checkArgument(lowerCaseS3Endpoint.startsWith("http://") || lowerCaseS3Endpoint.startsWith("https://"), "Expected http URI for S3 endpoint; got %s", s3Endpoint);

            addHiveProperty("hive.s3.endpoint", s3Endpoint);
            addHiveProperty("hive.s3.aws-access-key", s3AccessKey);
            addHiveProperty("hive.s3.aws-secret-key", s3SecretKey);
            addHiveProperty("hive.s3.path-style-access", "true");
            setMetastore(distributedQueryRunner -> new BridgingHiveMetastore(
                            new ThriftHiveMetastore(
                                    new TestingMetastoreLocator(
                                            Optional.empty(),
                                            hiveMetastoreEndpoint,
                                            metastoreTimeout),
                                    new HiveConfig(),
                                    new MetastoreConfig(),
                                    new ThriftMetastoreConfig(),
                                    new HdfsEnvironment(new HiveHdfsConfiguration(
                                            new HdfsConfigurationInitializer(
                                                    new HdfsConfig(),
                                                    ImmutableSet.of()),
                                            ImmutableSet.of()),
                                            new HdfsConfig(),
                                            new NoHdfsAuthentication()),
                                    false),
                            new HiveIdentity(distributedQueryRunner.getDefaultSession().getIdentity().toConnectorIdentity())));
            setInitialSchemasLocationBase("s3a://" + bucketName); // cannot use s3:// as Hive metastore is not configured to accept it
            return super.build();
        }
    }
}
