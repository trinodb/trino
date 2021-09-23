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
package io.trino.plugin.raptor.legacy;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.raptor.legacy.metadata.Distribution;
import io.trino.plugin.raptor.legacy.metadata.ForMetadata;
import io.trino.plugin.raptor.legacy.metadata.TableColumn;
import io.trino.plugin.raptor.legacy.systemtables.ShardMetadataSystemTable;
import io.trino.plugin.raptor.legacy.systemtables.TableMetadataSystemTable;
import io.trino.plugin.raptor.legacy.systemtables.TableStatsSystemTable;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.type.TypeManager;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;

public class RaptorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(RaptorConnector.class).in(Scopes.SINGLETON);
        binder.bind(RaptorMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(RaptorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(RaptorPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(RaptorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(RaptorHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(RaptorNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(RaptorSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(RaptorTableProperties.class).in(Scopes.SINGLETON);

        Multibinder<SystemTable> tableBinder = newSetBinder(binder, SystemTable.class);
        tableBinder.addBinding().to(ShardMetadataSystemTable.class).in(Scopes.SINGLETON);
        tableBinder.addBinding().to(TableMetadataSystemTable.class).in(Scopes.SINGLETON);
        tableBinder.addBinding().to(TableStatsSystemTable.class).in(Scopes.SINGLETON);
    }

    @ForMetadata
    @Singleton
    @Provides
    public static Jdbi createJdbi(@ForMetadata ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        Jdbi dbi = Jdbi.create(connectionFactory)
                .installPlugin(new SqlObjectPlugin())
                .registerRowMapper(new TableColumn.Mapper(typeManager))
                .registerRowMapper(new Distribution.Mapper(typeManager));
        createTablesWithRetry(dbi);
        return dbi;
    }

    @Provides
    @Singleton
    public static NodeSupplier createNodeSupplier(NodeManager nodeManager)
    {
        return nodeManager::getWorkerNodes;
    }
}
