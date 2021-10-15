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
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class GlueTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final FileIoProvider fileIoProvider;
    private final GlueMetastoreStats stats = new GlueMetastoreStats();

    @Inject
    public GlueTableOperationsProvider(
            GlueHiveMetastoreConfig glueConfig,
            FileIoProvider fileIoProvider)
    {
        this.fileIoProvider = fileIoProvider;
        requireNonNull(glueConfig, "glueConfig is null");
        this.glueClient = createAsyncGlueClient(glueConfig, Optional.empty(), stats.newRequestMetricsCollector());
        this.catalogId = glueConfig.getCatalogId().orElse(null);
    }

    @Override
    public IcebergTableOperations createTableOperations(
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new GlueTableOperations(
                glueClient,
                stats,
                catalogId,
                fileIoProvider.createFileIo(new HdfsEnvironment.HdfsContext(session), session.getQueryId()),
                session,
                database,
                table,
                owner,
                location);
    }
}
