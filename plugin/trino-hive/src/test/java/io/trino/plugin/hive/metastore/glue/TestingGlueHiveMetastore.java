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
package io.trino.plugin.hive.metastore.glue;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreFactory.DEFAULT_METASTORE_USER;

public final class TestingGlueHiveMetastore
{
    private TestingGlueHiveMetastore() {}

    public static GlueHiveMetastore createTestingGlueHiveMetastore(java.nio.file.Path defaultWarehouseDir)
    {
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        GlueMetastoreStats stats = new GlueMetastoreStats();
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(defaultWarehouseDir.toUri().toString());
        AWSGlueAsync glueClient = createTestingAsyncGlueClient(glueConfig, stats);
        return new GlueHiveMetastore(
                Optional.empty(),
                new HdfsFileSystemFactory(hdfsEnvironment, new TrinoHdfsFileSystemStats()).create(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER)),
                glueClient,
                glueConfig.getDefaultWarehouseDir(),
                glueConfig.getPartitionSegments(),
                directExecutor(),
                glueConfig.isAssumeCanonicalPartitionKeys(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()).createGlueColumnStatisticsProvider(glueClient, stats),
                stats,
                table -> true);
    }

    public static AWSGlueAsync createTestingAsyncGlueClient(GlueHiveMetastoreConfig glueConfig, GlueMetastoreStats stats)
    {
        return createAsyncGlueClient(
                glueConfig,
                DefaultAWSCredentialsProviderChain.getInstance(),
                ImmutableSet.of(),
                stats.newRequestMetricsCollector());
    }
}
