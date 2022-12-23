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
package io.trino.plugin.base.security;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.spi.connector.ConnectorAccessControl;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedAccessControlModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(FileBasedAccessControlModule.class);

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FileBasedAccessControlConfig.class);
        install(conditionalModule(
                FileBasedAccessControlConfig.class,
                FileBasedAccessControlConfig::isHttp,
                new HttpAccessControlModule(),
                new LocalAccessControlModule()));
    }

    @Inject
    @Provides
    @Singleton
    public ConnectorAccessControl getConnectorAccessControl(
            CatalogName catalogName,
            FileBasedAccessControlConfig config,
            Supplier<AccessControlRules> rulesProvider)
    {
        if (config.getRefreshPeriod() != null) {
            return ForwardingConnectorAccessControl.of(memoizeWithExpiration(
                    () -> {
                        log.info("Refreshing access control for catalog '%s' from: %s", catalogName, config.getConfigFile());
                        return new FileBasedAccessControl(catalogName, rulesProvider.get());
                    },
                    config.getRefreshPeriod().toMillis(),
                    MILLISECONDS));
        }
        return new FileBasedAccessControl(catalogName, rulesProvider.get());
    }

    private static class LocalAccessControlModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Inject
        @Provides
        @Singleton
        public Supplier<AccessControlRules> getAccessControlRules(
                FileBasedAccessControlConfig config)
        {
            File configFile = new File(config.getConfigFile());
            Supplier<AccessControlRules> accessControlRulesProvider = () -> parseJson(configFile.toPath(), config.getJsonPointer(), AccessControlRules.class);
            return accessControlRulesProvider;
        }
    }

    private static class HttpAccessControlModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            httpClientBinder(binder).bindHttpClient("access-control", ForAccessControlRules.class)
                    .withConfigDefaults(config -> config
                            .setRequestTimeout(Duration.succinctDuration(10, TimeUnit.SECONDS))
                            .setSelectorCount(1)
                            .setMinThreads(1));
            binder.bind(HttpBasedAccessControlRulesProvider.class).in(Scopes.SINGLETON);
        }

        @Inject
        @Provides
        @Singleton
        public Supplier<AccessControlRules> getAccessControlRules(
                HttpBasedAccessControlRulesProvider rulesProvider)
        {
            return () -> rulesProvider.extract(AccessControlRules.class);
        }
    }
}
