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
package io.trino.plugin.deltalake.metastore.glue;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.hive.metastore.glue.GlueCache;
import io.trino.plugin.hive.metastore.glue.GlueContext;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.connector.ConnectorSession;
import software.amazon.awssdk.services.glue.GlueClient;

import static java.util.Objects.requireNonNull;

public class DeltaLakeGlueMetastoreTableOperationsProvider
        implements DeltaLakeTableOperationsProvider
{
    private final GlueClient glueClient;
    private final GlueContext glueContext;
    private final GlueCache glueCache;
    private final GlueMetastoreStats stats;

    @Inject
    public DeltaLakeGlueMetastoreTableOperationsProvider(
            GlueClient glueClient,
            GlueContext glueContext,
            GlueCache glueCache,
            GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.glueContext = requireNonNull(glueContext, "glueContext is null");
        this.glueCache = requireNonNull(glueCache, "glueCache is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public DeltaLakeTableOperations createTableOperations(ConnectorSession session)
    {
        return new DeltaLakeGlueMetastoreTableOperations(glueClient, glueContext, glueCache, stats);
    }
}
