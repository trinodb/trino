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
package io.trino.plugin.jdbc.mapping;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;

import javax.inject.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class IdentifierMappingModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(IdentifierMappingModule.class);

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MappingConfig.class);
        binder.bind(DefaultIdentifierMapping.class).in(SINGLETON);

        MappingConfig config = buildConfigObject(MappingConfig.class);
        if (config.isCaseInsensitiveNameMatching()) {
            Provider<JdbcClient> baseJdbcClientProvider = binder.getProvider(Key.get(JdbcClient.class, ForBaseJdbc.class));
            binder.bind(BaseJdbcClient.class).toProvider(() -> (BaseJdbcClient) baseJdbcClientProvider.get());
            binder.bind(IdentifierMapping.class)
                    .annotatedWith(ForCachingIdentifierMapping.class)
                    .to(DefaultIdentifierMapping.class)
                    .in(SINGLETON);
            binder.bind(IdentifierMapping.class)
                    .annotatedWith(ForRuleBasedIdentifierMapping.class)
                    .to(CachingIdentifierMapping.class)
                    .in(SINGLETON);
        }
        else {
            binder.bind(IdentifierMapping.class)
                    .annotatedWith(ForRuleBasedIdentifierMapping.class)
                    .to(DefaultIdentifierMapping.class)
                    .in(SINGLETON);
        }

        if (config.getCaseInsensitiveNameMatchingConfigFile().isPresent()) {
            install(new RuleBasedIdentifierMappingModule());
        }
        else {
            binder.bind(IdentifierMapping.class)
                    .to(Key.get(IdentifierMapping.class, ForRuleBasedIdentifierMapping.class))
                    .in(SINGLETON);
        }
    }

    private static final class RuleBasedIdentifierMappingModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder) {}

        @Singleton
        @Provides
        public IdentifierMapping getIdentifierMapping(
                CatalogName catalogName,
                @ForRuleBasedIdentifierMapping IdentifierMapping identifierMapping,
                MappingConfig config)
        {
            String configFile = config.getCaseInsensitiveNameMatchingConfigFile()
                    .orElseThrow(() -> new IllegalStateException("Missing case insensitive matching config file"));
            Optional<Duration> refreshPeriod = config.getCaseInsensitiveNameMatchingConfigFileRefreshPeriod();
            if (refreshPeriod.isPresent()) {
                return ForwardingIdentifierMapping.of(memoizeWithExpiration(
                        () -> {
                            log.info("Refreshing identifier mapping for %s from %s", catalogName, configFile);
                            return new RuleBasedIdentifierMapping(createRules(configFile), identifierMapping);
                        },
                        refreshPeriod.get().toMillis(),
                        MILLISECONDS));
            }
            return new RuleBasedIdentifierMapping(createRules(configFile), identifierMapping);
        }

        private static IdentifierMappingRules createRules(String configFile)
        {
            return parseJson(Paths.get(configFile), IdentifierMappingRules.class);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForCachingIdentifierMapping {}

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier
    public @interface ForRuleBasedIdentifierMapping {}
}
