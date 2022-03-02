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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class GlueIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final FileIoProvider fileIoProvider;
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats = new GlueMetastoreStats();

    @Inject
    public GlueIcebergTableOperationsProvider(FileIoProvider fileIoProvider, GlueHiveMetastoreConfig glueConfig)
    {
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        requireNonNull(glueConfig, "glueConfig is null");
        this.glueClient = createAsyncGlueClient(glueConfig, Optional.empty(), stats.newRequestMetricsCollector());
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new GlueIcebergTableOperations(
                glueClient,
                stats,
                fileIoProvider.createFileIo(new HdfsContext(session), session.getQueryId()),
                session,
                database,
                table,
                owner,
                location);
    }
}
