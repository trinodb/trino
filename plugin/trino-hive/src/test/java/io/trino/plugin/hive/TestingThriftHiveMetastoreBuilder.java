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

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.TokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.UgiBasedMetastoreClientFactory;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.security.UserNameProvider.SIMPLE_USER_NAME_PROVIDER;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public final class TestingThriftHiveMetastoreBuilder
{
    private TokenAwareMetastoreClientFactory tokenAwareMetastoreClientFactory;
    private HiveConfig hiveConfig = new HiveConfig();
    private ThriftMetastoreConfig thriftMetastoreConfig = new ThriftMetastoreConfig();
    private HdfsEnvironment hdfsEnvironment = HDFS_ENVIRONMENT;

    public static TestingThriftHiveMetastoreBuilder testingThriftHiveMetastoreBuilder()
    {
        return new TestingThriftHiveMetastoreBuilder();
    }

    private TestingThriftHiveMetastoreBuilder() {}

    public TestingThriftHiveMetastoreBuilder metastoreClient(HostAndPort address, Duration timeout)
    {
        requireNonNull(address, "address is null");
        requireNonNull(timeout, "timeout is null");
        checkState(tokenAwareMetastoreClientFactory == null, "Metastore client already set");
        tokenAwareMetastoreClientFactory = new TestingTokenAwareMetastoreClientFactory(HiveTestUtils.SOCKS_PROXY, address, timeout);
        return this;
    }

    public TestingThriftHiveMetastoreBuilder metastoreClient(HostAndPort address)
    {
        requireNonNull(address, "address is null");
        checkState(tokenAwareMetastoreClientFactory == null, "Metastore client already set");
        tokenAwareMetastoreClientFactory = new TestingTokenAwareMetastoreClientFactory(HiveTestUtils.SOCKS_PROXY, address);
        return this;
    }

    public TestingThriftHiveMetastoreBuilder metastoreClient(ThriftMetastoreClient client)
    {
        requireNonNull(client, "client is null");
        checkState(tokenAwareMetastoreClientFactory == null, "Metastore client already set");
        tokenAwareMetastoreClientFactory = token -> client;
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
        checkState(tokenAwareMetastoreClientFactory != null, "metastore client not set");
        ThriftHiveMetastoreFactory metastoreFactory = new ThriftHiveMetastoreFactory(
                new UgiBasedMetastoreClientFactory(tokenAwareMetastoreClientFactory, SIMPLE_USER_NAME_PROVIDER, thriftMetastoreConfig),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                hiveConfig.isTranslateHiveViews(),
                thriftMetastoreConfig,
                new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS),
                newFixedThreadPool(thriftMetastoreConfig.getWriteStatisticsThreads()));
        return metastoreFactory.createMetastore(Optional.empty());
    }
}
