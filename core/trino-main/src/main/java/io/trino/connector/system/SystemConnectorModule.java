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
package io.trino.connector.system;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.trino.connector.ConnectorManager;
import io.trino.connector.system.jdbc.AttributeJdbcTable;
import io.trino.connector.system.jdbc.CatalogJdbcTable;
import io.trino.connector.system.jdbc.ColumnJdbcTable;
import io.trino.connector.system.jdbc.ProcedureColumnJdbcTable;
import io.trino.connector.system.jdbc.ProcedureJdbcTable;
import io.trino.connector.system.jdbc.PseudoColumnJdbcTable;
import io.trino.connector.system.jdbc.SchemaJdbcTable;
import io.trino.connector.system.jdbc.SuperTableJdbcTable;
import io.trino.connector.system.jdbc.SuperTypeJdbcTable;
import io.trino.connector.system.jdbc.TableJdbcTable;
import io.trino.connector.system.jdbc.TableTypeJdbcTable;
import io.trino.connector.system.jdbc.TypesJdbcTable;
import io.trino.connector.system.jdbc.UdtJdbcTable;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;

public class SystemConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Multibinder<SystemTable> globalTableBinder = Multibinder.newSetBinder(binder, SystemTable.class);
        globalTableBinder.addBinding().to(NodeSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(QuerySystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TaskSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(CatalogSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TableCommentSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SchemaPropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TablePropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(MaterializedViewPropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ColumnPropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(AnalyzePropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TransactionsSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(RuleStatsSystemTable.class).in(Scopes.SINGLETON);

        globalTableBinder.addBinding().to(AttributeJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(CatalogJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ColumnJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TypesJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ProcedureColumnJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ProcedureJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(PseudoColumnJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SchemaJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SuperTableJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SuperTypeJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TableJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TableTypeJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(UdtJdbcTable.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, Procedure.class);

        binder.bind(KillQueryProcedure.class).in(Scopes.SINGLETON);

        binder.bind(GlobalSystemConnectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(SystemConnectorRegistrar.class).asEagerSingleton();
    }

    @ProvidesIntoSet
    public static Procedure getKillQueryProcedure(KillQueryProcedure procedure)
    {
        return procedure.getProcedure();
    }

    private static class SystemConnectorRegistrar
    {
        @Inject
        public SystemConnectorRegistrar(ConnectorManager manager, GlobalSystemConnectorFactory globalSystemConnectorFactory)
        {
            manager.addConnectorFactory(globalSystemConnectorFactory, globalSystemConnectorFactory.getClass()::getClassLoader);
            manager.createCatalog(GlobalSystemConnector.NAME, GlobalSystemConnector.NAME, ImmutableMap.of());
        }
    }
}
