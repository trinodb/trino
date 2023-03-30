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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.security.SystemAccessControl;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.plugin.base.security.CatalogAccessControlRule.AccessMode.ALL;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedSystemAccessControlModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(FileBasedSystemAccessControlModule.class);

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FileBasedAccessControlConfig.class);
        install(conditionalModule(
                FileBasedAccessControlConfig.class,
                FileBasedAccessControlConfig::isHttp,
                new HttpSystemAccessControlModule(),
                new LocalSystemAccessControlModule()));
    }

    @Inject
    @Provides
    @Singleton
    public SystemAccessControl getSystemAccessControl(
            FileBasedAccessControlConfig config,
            Supplier<FileBasedSystemAccessControlRules> rulesProvider)
    {
        Duration refreshPeriod = config.getRefreshPeriod();
        if (refreshPeriod != null) {
            return ForwardingSystemAccessControl.of(memoizeWithExpiration(
                    () -> {
                        log.info("Refreshing system access control from %s", config.getConfigFile());
                        return create(rulesProvider.get());
                    },
                    refreshPeriod.toMillis(),
                    MILLISECONDS));
        }
        return create(rulesProvider.get());
    }

    private SystemAccessControl create(FileBasedSystemAccessControlRules rules)
    {
        List<CatalogAccessControlRule> catalogAccessControlRules;
        if (rules.getCatalogRules().isPresent()) {
            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules().get());

            // Hack to allow Trino Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Trino Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    ALL,
                    Optional.of(Pattern.compile(".*")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(Pattern.compile("system"))));
            catalogAccessControlRules = catalogRulesBuilder.build();
        }
        else {
            // if no rules are defined then all access is allowed
            catalogAccessControlRules = ImmutableList.of(CatalogAccessControlRule.ALLOW_ALL);
        }
        return FileBasedSystemAccessControl.builder()
                .setCatalogRules(catalogAccessControlRules)
                .setQueryAccessRules(rules.getQueryAccessRules())
                .setImpersonationRules(rules.getImpersonationRules())
                .setPrincipalUserMatchRules(rules.getPrincipalUserMatchRules())
                .setSystemInformationRules(rules.getSystemInformationRules())
                .setSchemaRules(rules.getSchemaRules().orElse(ImmutableList.of(CatalogSchemaAccessControlRule.ALLOW_ALL)))
                .setTableRules(rules.getTableRules().orElse(ImmutableList.of(CatalogTableAccessControlRule.ALLOW_ALL)))
                .setSessionPropertyRules(rules.getSessionPropertyRules().orElse(ImmutableList.of(SessionPropertyAccessControlRule.ALLOW_ALL)))
                .setCatalogSessionPropertyRules(rules.getCatalogSessionPropertyRules().orElse(ImmutableList.of(CatalogSessionPropertyAccessControlRule.ALLOW_ALL)))
                .setFunctionRules(rules.getFunctionRules().orElse(ImmutableList.of(CatalogFunctionAccessControlRule.ALLOW_ALL)))
                .build();
    }

    private static class LocalSystemAccessControlModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Inject
        @Provides
        @Singleton
        public Supplier<FileBasedSystemAccessControlRules> getSystemAccessControlRules(
                FileBasedAccessControlConfig config)
        {
            File configFile = new File(config.getConfigFile());
            return () -> parseJson(configFile.toPath(), config.getJsonPointer(), FileBasedSystemAccessControlRules.class);
        }
    }

    private static class HttpSystemAccessControlModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            httpClientBinder(binder).bindHttpClient("system-access-control", ForAccessControlRules.class)
                    .withConfigDefaults(config -> config
                            .setRequestTimeout(Duration.succinctDuration(10, TimeUnit.SECONDS))
                            .setSelectorCount(1)
                            .setMinThreads(1));
            binder.bind(HttpBasedAccessControlRulesProvider.class).in(Scopes.SINGLETON);
        }

        @Inject
        @Provides
        @Singleton
        public Supplier<FileBasedSystemAccessControlRules> getSystemAccessControlRules(
                HttpBasedAccessControlRulesProvider rulesProvider)
        {
            return () -> rulesProvider.extract(FileBasedSystemAccessControlRules.class);
        }
    }
}
