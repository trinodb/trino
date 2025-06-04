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
package io.trino.plugin.postgresql;

import io.airlift.tracing.Tracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.FeaturesConfig;
import io.trino.connector.ConnectorAwareNodeManager;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.geospatial.GeometryType;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.util.EmbedVersion;

import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;

public class TestingPostgreSqlConnectorContext
        implements ConnectorContext
{
    private final NodeManager nodeManager;
    private final VersionEmbedder versionEmbedder = new EmbedVersion("testversion");
    private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
    private final PageIndexerFactory pageIndexerFactory;
    private final TypeManager typeManager;

    public TestingPostgreSqlConnectorContext()
    {
        pageIndexerFactory = new GroupByHashPageIndexerFactory(new FlatHashStrategyCompiler(new TypeOperators()));
        nodeManager = new ConnectorAwareNodeManager(new InMemoryNodeManager(), "testenv", TEST_CATALOG_HANDLE, true);
        TypeRegistry typeRegistry = new TypeRegistry(new TypeOperators(), new FeaturesConfig());
        typeRegistry.addType(GeometryType.GEOMETRY);
        typeManager = new InternalTypeManager(typeRegistry);
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return TEST_CATALOG_HANDLE;
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return OpenTelemetry.noop();
    }

    @Override
    public Tracer getTracer()
    {
        return Tracing.noopTracer();
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public VersionEmbedder getVersionEmbedder()
    {
        return versionEmbedder;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public MetadataProvider getMetadataProvider()
    {
        return NOOP_METADATA_PROVIDER;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }
}
