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
package io.trino.plugin.base;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class ConnectorContextModule
        implements Module
{
    private final String catalogName;
    private final ConnectorContext context;

    public ConnectorContextModule(String catalogName, ConnectorContext context)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));

        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
        binder.bind(Tracer.class).toInstance(context.getTracer());
        binder.bind(Node.class).toInstance(context.getCurrentNode());
        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getCurrentNode().getVersion()));
        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
        binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
        binder.bind(MetadataProvider.class).toInstance(context.getMetadataProvider());
        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
    }
}
