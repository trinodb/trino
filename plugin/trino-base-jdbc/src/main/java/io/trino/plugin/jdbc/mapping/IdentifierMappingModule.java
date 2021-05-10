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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;

import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class IdentifierMappingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MappingConfig.class);
        binder.bind(DefaultIdentifierMapping.class).in(SINGLETON);
        install(installModuleIf(
                MappingConfig.class,
                MappingConfig::isCaseInsensitiveNameMatching,
                new CachingIdentifierMappingModule(),
                new DefaultIdentifierMappingModule()));
    }

    private static final class CachingIdentifierMappingModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            Provider<JdbcClient> baseJdbcClientProvider = binder.getProvider(Key.get(JdbcClient.class, ForBaseJdbc.class));
            binder.bind(BaseJdbcClient.class).toProvider(() -> (BaseJdbcClient) baseJdbcClientProvider.get());
            binder.bind(IdentifierMapping.class)
                    .annotatedWith(ForCachingIdentifierMapping.class)
                    .to(DefaultIdentifierMapping.class)
                    .in(SINGLETON);
            binder.bind(IdentifierMapping.class).to(CachingIdentifierMapping.class).in(SINGLETON);
        }
    }

    private static final class DefaultIdentifierMappingModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(IdentifierMapping.class).to(DefaultIdentifierMapping.class).in(SINGLETON);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForCachingIdentifierMapping {}
}
