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

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlueHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final Tracer tracer;
    private final GlueClientFactory glueClientFactory;
    private final GlueCache glueCache;
    private final GlueMetastoreStats glueStats;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final GlueHiveMetastoreConfig config;
    private final CatalogName catalogName;
    private final Set<GlueHiveMetastore.TableKind> visibleTableKinds;

    @Inject
    public GlueHiveMetastoreFactory(
            Tracer tracer,
            GlueClientFactory glueClientFactory,
            GlueCache glueCache,
            GlueMetastoreStats glueStats,
            TrinoFileSystemFactory fileSystemFactory,
            GlueHiveMetastoreConfig config,
            CatalogName catalogName,
            Set<GlueHiveMetastore.TableKind> visibleTableKinds)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.glueClientFactory = requireNonNull(glueClientFactory, "glueClientFactory is null");
        this.glueCache = requireNonNull(glueCache, "glueCache is null");
        this.glueStats = requireNonNull(glueStats, "glueStats is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.config = requireNonNull(config, "config is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.visibleTableKinds = requireNonNull(visibleTableKinds, "visibleTableKinds is null");
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        if (identity.isEmpty()) {
            throw new IllegalStateException("ConnectorIdentity must be provided");
        }

        GlueClient glueClient = glueClientFactory.create(identity.get());
        GlueHiveMetastore metastore = new GlueHiveMetastore(
                glueClient,
                glueCache,
                glueStats,
                fileSystemFactory,
                config,
                catalogName,
                visibleTableKinds);
        return new TracingHiveMetastore(tracer, metastore);
    }

    @Override
    public boolean hasBuiltInCaching()
    {
        return true;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
