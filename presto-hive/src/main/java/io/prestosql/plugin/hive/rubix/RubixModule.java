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
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.plugin.hive.ConfigurationInitializer;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class RubixModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RubixConfig.class);
        binder.bind(RubixConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(RubixInitializer.class).in(Scopes.SINGLETON);
        // Make initialization of Rubix happen just once.
        // Alternative initialization via @PostConstruct in RubixInitializer
        // would be called multiple times by Guice (RubixInitializer is transient
        // dependency for many objects) whenever initialization error happens
        // (Guice doesn't fail-fast)
        binder.bind(RubixStarter.class).asEagerSingleton();
        newOptionalBinder(binder, Key.get(ConfigurationInitializer.class, ForRubix.class));
        newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(RubixConfigurationInitializer.class).in(Scopes.SINGLETON);
    }

    private static class RubixStarter
    {
        @Inject
        private RubixStarter(RubixInitializer rubixInitializer, Set<DynamicConfigurationProvider> configProviders)
        {
            checkArgument(configProviders.size() == 1, "Rubix cache does not work with dynamic configuration providers");
            rubixInitializer.initializeRubix();
        }
    }
}
