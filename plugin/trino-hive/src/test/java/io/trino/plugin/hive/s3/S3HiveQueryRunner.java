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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingMetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;
import java.util.Optional;

public final class S3HiveQueryRunner
{
    private S3HiveQueryRunner() {}

    public static DistributedQueryRunner create(
            HostAndPort hiveEndpoint,
            HostAndPort s3Endpoint,
            String s3AccessKey,
            String s3SecretKey,
            Map<String, String> additionalHiveProperties)
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setMetastore(distributedQueryRunner -> new BridgingHiveMetastore(
                        new ThriftHiveMetastore(
                                new TestingMetastoreLocator(
                                        Optional.empty(),
                                        hiveEndpoint),
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
                                false)))
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.s3.endpoint", "http://" + s3Endpoint)
                        .put("hive.s3.aws-access-key", s3AccessKey)
                        .put("hive.s3.aws-secret-key", s3SecretKey)
                        .putAll(additionalHiveProperties)
                        .build())
                .setInitialTables(ImmutableList.of())
                .build();
    }
}
