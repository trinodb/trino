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

import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.MetastoreClientAdapterProvider;
import io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.TokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.UgiBasedMetastoreClientFactory;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.security.UserNameProvider.SIMPLE_USER_NAME_PROVIDER;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory.TIMEOUT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public final class TestingThriftHiveMetastoreBuilder
{
    private TokenAwareMetastoreClientFactory tokenAwareMetastoreClientFactory;
    private HiveConfig hiveConfig = new HiveConfig();
    private ThriftMetastoreConfig thriftMetastoreConfig = new ThriftMetastoreConfig();
    private TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);

    public static TestingThriftHiveMetastoreBuilder testingThriftHiveMetastoreBuilder()
    {
        return new TestingThriftHiveMetastoreBuilder();
    }

    private TestingThriftHiveMetastoreBuilder() {}

    public TestingThriftHiveMetastoreBuilder metastoreClient(URI metastoreUri)
    {
        requireNonNull(metastoreUri, "metastoreUri is null");
        checkState(tokenAwareMetastoreClientFactory == null, "Metastore client already set");
        tokenAwareMetastoreClientFactory = new TestingTokenAwareMetastoreClientFactory(HiveTestUtils.SOCKS_PROXY, metastoreUri);
        return this;
    }

    public TestingThriftHiveMetastoreBuilder metastoreClient(URI address, Duration timeout)
    {
        requireNonNull(address, "address is null");
        requireNonNull(timeout, "timeout is null");
        checkState(tokenAwareMetastoreClientFactory == null, "Metastore client already set");
        tokenAwareMetastoreClientFactory = new TestingTokenAwareMetastoreClientFactory(HiveTestUtils.SOCKS_PROXY, address, timeout);
        return this;
    }

    public TestingThriftHiveMetastoreBuilder metastoreClient(URI uri, MetastoreClientAdapterProvider metastoreClientAdapterProvider)
    {
        requireNonNull(uri, "uri is null");
        checkState(tokenAwareMetastoreClientFactory == null, "Metastore client already set");
        tokenAwareMetastoreClientFactory = new TestingTokenAwareMetastoreClientFactory(HiveTestUtils.SOCKS_PROXY, uri, TIMEOUT, metastoreClientAdapterProvider);
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

    public TestingThriftHiveMetastoreBuilder fileSystemFactory(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
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
                fileSystemFactory,
                newFixedThreadPool(thriftMetastoreConfig.getWriteStatisticsThreads()));
        return metastoreFactory.createMetastore(Optional.empty());
    }
}
