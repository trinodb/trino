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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.azure.HiveAzureConfig;
import io.trino.plugin.hive.azure.TrinoAzureConfigurationInitializer;
import io.trino.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.MetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.TestingMetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.TokenDelegationThriftMetastoreFactory;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class TestingThriftHiveMetastoreBuilder
{
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment(
            new HiveHdfsConfiguration(
                    new HdfsConfigurationInitializer(
                            new HdfsConfig()
                                    .setSocksProxy(HiveTestUtils.SOCKS_PROXY.orElse(null)),
                            ImmutableSet.of(
                                    new TrinoS3ConfigurationInitializer(new HiveS3Config()),
                                    new GoogleGcsConfigurationInitializer(new HiveGcsConfig()),
                                    new TrinoAzureConfigurationInitializer(new HiveAzureConfig()))),
                    ImmutableSet.of()),
            new HdfsConfig(),
            new NoHdfsAuthentication());

    private MetastoreLocator metastoreLocator;
    private HiveConfig hiveConfig = new HiveConfig();
    private ThriftMetastoreConfig thriftMetastoreConfig = new ThriftMetastoreConfig();
    private HdfsEnvironment hdfsEnvironment = HDFS_ENVIRONMENT;

    public static TestingThriftHiveMetastoreBuilder testingThriftHiveMetastoreBuilder()
    {
        return new TestingThriftHiveMetastoreBuilder();
    }

    private TestingThriftHiveMetastoreBuilder() {}

    public TestingThriftHiveMetastoreBuilder metastoreClient(HostAndPort address)
    {
        requireNonNull(address, "address is null");
        checkState(metastoreLocator == null, "Metastore client already set");
        metastoreLocator = new TestingMetastoreLocator(HiveTestUtils.SOCKS_PROXY, address);
        return this;
    }

    public TestingThriftHiveMetastoreBuilder metastoreClient(ThriftMetastoreClient client)
    {
        requireNonNull(client, "client is null");
        checkState(metastoreLocator == null, "Metastore client already set");
        metastoreLocator = token -> client;
        return this;
    }

    public TestingThriftHiveMetastoreBuilder hiveConfig(HiveConfig hiveConfig)
    {
        this.hiveConfig = requireNonNull(hiveConfig, "hiveConfig is null");
        return this;
    }

    public TestingThriftHiveMetastoreBuilder thriftMetastoreConfig(ThriftMetastoreConfig thriftMetastoreConfig)
    {
        this.thriftMetastoreConfig = requireNonNull(thriftMetastoreConfig, "thriftMetastoreConfig is null");
        return this;
    }

    public TestingThriftHiveMetastoreBuilder hdfsEnvironment(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        return this;
    }

    public ThriftMetastore build()
    {
        checkState(metastoreLocator != null, "metastore client not set");
        ThriftHiveMetastoreFactory metastoreFactory = new ThriftHiveMetastoreFactory(
                new TokenDelegationThriftMetastoreFactory(
                        metastoreLocator,
                        thriftMetastoreConfig,
                        new ThriftMetastoreAuthenticationConfig(),
                        hdfsEnvironment),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                hiveConfig.isTranslateHiveViews(),
                thriftMetastoreConfig,
                hdfsEnvironment);
        return metastoreFactory.createMetastore(Optional.empty());
    }
}
