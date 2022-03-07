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
package io.trino.plugin.kudu;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.kudu.procedures.RangePartitionProcedures;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class KuduModule
        extends AbstractConfigurationAwareModule
{
    private final TypeManager typeManager;

    public KuduModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(KuduConnector.class).in(Scopes.SINGLETON);
        binder.bind(KuduMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KuduTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(KuduSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(KuduPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(KuduPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(KuduSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).annotatedWith(ForClassLoaderSafe.class).to(KuduNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(ClassLoaderSafeNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(KuduRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KuduClientConfig.class);

        binder.bind(RangePartitionProcedures.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder, Procedure.class);

        install(new KuduSecurityModule());
    }

    @ProvidesIntoSet
    Procedure getAddRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getAddPartitionProcedure();
    }

    @ProvidesIntoSet
    Procedure getDropRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getDropPartitionProcedure();
    }
}
