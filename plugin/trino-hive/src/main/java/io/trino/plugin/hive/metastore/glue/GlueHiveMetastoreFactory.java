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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.Table;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class GlueHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    public static final String DEFAULT_METASTORE_USER = "presto";
    private final TrinoFileSystem fileSystem;
    private final AWSGlueAsync glueClient;
    private final Optional<String> defaultDir;
    private final int partitionSegments;
    private final Executor partitionsReadExecutor;
    private final GlueMetastoreStats stats;
    private final GlueColumnStatisticsProvider columnStatisticsProvider;
    private final boolean assumeCanonicalPartitionKeys;
    private final Predicate<com.amazonaws.services.glue.model.Table> tableFilter;
    private final boolean impersonationEnabled;
    private final Tracer tracer;

    public GlueHiveMetastoreFactory(
            TrinoFileSystemFactory fileSystemFactory,
            GlueHiveMetastoreConfig glueConfig,
            @ForGlueHiveMetastore Executor partitionsReadExecutor,
            GlueColumnStatisticsProviderFactory columnStatisticsProviderFactory,
            AWSGlueAsync glueClient,
            @ForGlueHiveMetastore GlueMetastoreStats stats,
            @ForGlueHiveMetastore Predicate<Table> tableFilter,
            Tracer tracer)
    {
        this.fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER));
        this.glueClient = glueClient;
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.partitionsReadExecutor = partitionsReadExecutor;
        this.assumeCanonicalPartitionKeys = glueConfig.isAssumeCanonicalPartitionKeys();
        this.tableFilter = tableFilter;
        this.stats = stats;
        this.columnStatisticsProvider = columnStatisticsProviderFactory.createGlueColumnStatisticsProvider(glueClient, stats);
        this.impersonationEnabled = glueConfig.isImpersonationEnabled();
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new TracingHiveMetastore(tracer, new GlueHiveMetastore(identity,
                fileSystem,
                glueClient,
                defaultDir,
                partitionSegments,
                partitionsReadExecutor,
                assumeCanonicalPartitionKeys,
                columnStatisticsProvider,
                stats,
                tableFilter));
    }
}
