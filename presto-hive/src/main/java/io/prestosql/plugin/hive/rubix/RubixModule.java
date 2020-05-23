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
package io.prestosql.plugin.hive.rubix;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.spi.NodeManager;

import javax.inject.Singleton;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class RubixModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RubixConfig.class);
        binder.bind(RubixConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(RubixConfigurationInitializer.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public RubixInitializer createRubixInitializer(
            RubixConfig rubixConfig,
            NodeManager nodeManager,
            CatalogName catalogName,
            Set<DynamicConfigurationProvider> configProviders,
            HdfsConfigurationInitializer hdfsConfigurationInitializer)
    {
        checkArgument(configProviders.size() == 1, "Rubix cache does not work with dynamic configuration providers");
        RubixConfigurationInitializer configProvider = (RubixConfigurationInitializer) getOnlyElement(configProviders);
        return new RubixInitializer(rubixConfig, nodeManager, catalogName, configProvider, hdfsConfigurationInitializer);
    }
}
