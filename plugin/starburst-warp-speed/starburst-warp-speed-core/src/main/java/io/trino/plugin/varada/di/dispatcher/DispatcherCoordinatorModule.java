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
package io.trino.plugin.varada.di.dispatcher;

import com.google.inject.Binder;
import io.trino.plugin.varada.di.ExtraModule;
import io.trino.plugin.varada.di.VaradaBaseModule;
import io.trino.plugin.varada.dispatcher.DispatcherCacheMetadata;
import io.trino.plugin.varada.dispatcher.DispatcherMetadataFactory;
import io.trino.plugin.varada.dispatcher.DispatcherSplitManager;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTransactionManager;
import io.trino.plugin.varada.expression.rewrite.ExpressionService;
import io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions;
import io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.NativeExpressionRulesHandler;
import io.trino.plugin.varada.storage.splits.ConnectorSplitConsistentHashNodeDistributor;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * this module will install dependencies which are required in the coordinator regardless if it is single or not
 */
public class DispatcherCoordinatorModule
        implements ExtraModule
{
    private ConnectorContext context;

    public DispatcherCoordinatorModule(Map<String, String> config, ConnectorContext context)
    {
        withConfig(config).withContext(context);
    }

    @Override
    public void configure(Binder binder)
    {
        if (VaradaBaseModule.isCoordinator(context)) {
            binder.bind(DispatcherTransactionManager.class);
            binder.bind(DispatcherStatisticsProvider.class);
            binder.bind(DispatcherMetadataFactory.class);
            binder.bind(DispatcherCacheMetadata.class);
            binder.bind(ExpressionService.class);
            binder.bind(ConnectorSplitNodeDistributor.class).to(ConnectorSplitConsistentHashNodeDistributor.class);
            binder.bind(SupportedFunctions.class);
            binder.bind(NativeExpressionRulesHandler.class);
            binder.bind(DispatcherSplitManager.class);
        }
    }

    @Override
    public boolean shouldInstall()
    {
        return VaradaBaseModule.isCoordinator(context);
    }

    @Override
    public DispatcherCoordinatorModule withConfig(Map<String, String> config)
    {
        return this;
    }

    @Override
    public DispatcherCoordinatorModule withContext(ConnectorContext context)
    {
        this.context = requireNonNull(context);
        return this;
    }
}
