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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;

public final class TestingGlueHiveMetastore
{
    private TestingGlueHiveMetastore() {}

    public static GlueHiveMetastore createTestingGlueHiveMetastore(Path defaultWarehouseDir)
    {
        if (!exists(defaultWarehouseDir)) {
            try {
                createDirectories(defaultWarehouseDir);
            }
            catch (IOException e) {
                throw new RuntimeException("Could not create directory: %s".formatted(defaultWarehouseDir), e);
            }
        }
        verify(isDirectory(defaultWarehouseDir), "%s is not a directory", defaultWarehouseDir);
        return createTestingGlueHiveMetastore(defaultWarehouseDir.toUri());
    }

    public static GlueHiveMetastore createTestingGlueHiveMetastore(URI warehouseUri)
    {
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(warehouseUri.toString());
        GlueMetastoreStats stats = new GlueMetastoreStats();
        return new GlueHiveMetastore(
                HDFS_FILE_SYSTEM_FACTORY,
                glueConfig,
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()),
                createTestingAsyncGlueClient(glueConfig, stats),
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
