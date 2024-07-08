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
package io.trino.configuration.keystore;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.configuration.ConfigurationValueResolver;
import io.trino.spi.configuration.ConfigurationValueResolverFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class KeystoreConfigurationResolverFactory
        implements ConfigurationValueResolverFactory
{
    @Override
    public String getName()
    {
        return "keystore";
    }

    @Override
    public ConfigurationValueResolver createConfigurationValueResolver(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        configBinder(binder).bindConfig(KeystoreConfigurationResolverConfig.class);
                        binder.bind(ConfigurationValueResolver.class).to(KeystoreConfigurationResolver.class).in(Scopes.SINGLETON);
                    }
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ConfigurationValueResolver.class);
    }
}
