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
package io.prestosql.plugin.bigquery;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.prestosql.spi.NodeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class BigQueryConnectorModule
        implements Module
{
    private final NodeManager nodeManager;

    public BigQueryConnectorModule(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Provides
    @Singleton
    public static HeaderProvider createHeaderProvider(NodeManager nodeManager)
    {
        return FixedHeaderProvider.create("prestosql/" + nodeManager.getCurrentNode().getVersion());
    }

    @Override
    public void configure(Binder binder)
    {
        // BigQuery related
        binder.bind(BigQueryStorageClientFactory.class).in(Scopes.SINGLETON);

        // Connector implementation
        binder.bind(BigQueryConnector.class).in(Scopes.SINGLETON);

        binder.bind(BigQueryMetadata.class).in(Scopes.SINGLETON);
        binder.bind(BigQuerySplitManager.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BigQueryConfig.class);
    }

    @Provides
    @Singleton
    public BigQuery provideBigQuery(BigQueryConfig config, HeaderProvider headerProvider)
    {
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder()
                .setHeaderProvider(headerProvider)
                .setProjectId(config.getParentProject());
        // set credentials of provided
        config.createCredentials().ifPresent(options::setCredentials);
        return options.build().getService();
    }
}
