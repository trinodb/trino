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
package io.trino.connector;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.trino.connector.ConnectorManager.ConnectorServices;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.CatalogProcedures;
import io.trino.metadata.CatalogTableFunctions;
import io.trino.metadata.CatalogTableProcedures;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import javax.inject.Singleton;

public class CatalogServiceProviderModule
        implements Module
{
    @Override
    public void configure(Binder binder) {}

    @Provides
    @Singleton
    public static CatalogServiceProvider<ConnectorSplitManager> createSplitManagerProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("split manager", connectorServicesProvider, connector -> connector.getSplitManager().orElse(null));
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<ConnectorPageSourceProvider> createPageSourceProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("page source provider", connectorServicesProvider, connector -> connector.getPageSourceProvider().orElse(null));
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<ConnectorPageSinkProvider> createPageSinkProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("page sink provider", connectorServicesProvider, connector -> connector.getPageSinkProvider().orElse(null));
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<ConnectorIndexProvider> createIndexProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("index provider", connectorServicesProvider, connector -> connector.getIndexProvider().orElse(null));
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<ConnectorNodePartitioningProvider> createNodePartitioningProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("node partitioning provider", connectorServicesProvider, connector -> connector.getPartitioningProvider().orElse(null));
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<CatalogProcedures> createProceduresProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("procedures", connectorServicesProvider, ConnectorServices::getProcedures);
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<CatalogTableProcedures> createTableProceduresProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("table procedures", connectorServicesProvider, ConnectorServices::getTableProcedures);
    }

    @Provides
    @Singleton
    public static CatalogServiceProvider<CatalogTableFunctions> createTableFunctionProvider(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ConnectorCatalogServiceProvider<>("table functions", connectorServicesProvider, ConnectorServices::getTableFunctions);
    }

    public static SchemaPropertyManager createSchemaPropertyManager(ConnectorServicesProvider connectorServicesProvider)
    {
        return new SchemaPropertyManager(new ConnectorCatalogServiceProvider<>("schema properties", connectorServicesProvider, ConnectorServices::getSchemaProperties));
    }

    @Provides
    @Singleton
    public static ColumnPropertyManager createColumnPropertyManager(ConnectorServicesProvider connectorServicesProvider)
    {
        return new ColumnPropertyManager(new ConnectorCatalogServiceProvider<>("column properties", connectorServicesProvider, ConnectorServices::getColumnProperties));
    }

    @Provides
    @Singleton
    public static TablePropertyManager createTablePropertyManager(ConnectorServicesProvider connectorServicesProvider)
    {
        return new TablePropertyManager(new ConnectorCatalogServiceProvider<>("table properties", connectorServicesProvider, ConnectorServices::getTableProperties));
    }

    @Provides
    @Singleton
    public static MaterializedViewPropertyManager createMaterializedViewPropertyManager(ConnectorServicesProvider connectorServicesProvider)
    {
        return new MaterializedViewPropertyManager(new ConnectorCatalogServiceProvider<>("materialized view properties", connectorServicesProvider, ConnectorServices::getMaterializedViewProperties));
    }

    @Provides
    @Singleton
    public static AnalyzePropertyManager createAnalyzePropertyManager(ConnectorServicesProvider connectorServicesProvider)
    {
        return new AnalyzePropertyManager(new ConnectorCatalogServiceProvider<>("analyze properties", connectorServicesProvider, ConnectorServices::getAnalyzeProperties));
    }
}
