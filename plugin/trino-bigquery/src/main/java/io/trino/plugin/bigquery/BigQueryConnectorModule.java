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
package io.trino.plugin.bigquery;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.spi.NodeManager;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class BigQueryConnectorModule
        implements Module
{
    @Provides
    @Singleton
    public static HeaderProvider createHeaderProvider(NodeManager nodeManager)
    {
        return FixedHeaderProvider.create("user-agent", "Trino/" + nodeManager.getCurrentNode().getVersion());
    }

    @Override
    public void configure(Binder binder)
    {
        // BigQuery related
        binder.bind(BigQueryReadClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryClientFactory.class).in(Scopes.SINGLETON);

        // SingletonIdentityCacheMapping is safe to use with StaticBigQueryCredentialsSupplier
        // as credentials do not depend on actual connector session.
        newOptionalBinder(binder, IdentityCacheMapping.class)
                .setDefault()
                .to(IdentityCacheMapping.SingletonIdentityCacheMapping.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, BigQueryCredentialsSupplier.class)
                .setDefault()
                .to(StaticBigQueryCredentialsSupplier.class)
                .in(Scopes.SINGLETON);

        // Connector implementation
        binder.bind(BigQueryConnector.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryMetadata.class).in(Scopes.SINGLETON);
        binder.bind(BigQuerySplitManager.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ViewMaterializationCache.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BigQueryConfig.class);
    }
}
