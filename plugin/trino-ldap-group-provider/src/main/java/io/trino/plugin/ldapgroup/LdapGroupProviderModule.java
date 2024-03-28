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
package io.trino.plugin.ldapgroup;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.group.CachingGroupProviderModule;
import io.trino.spi.security.GroupProvider;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class LdapGroupProviderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LdapGroupProviderConfig.class);
        binder.bind(GroupProvider.class).annotatedWith(CachingGroupProviderModule.ForCachingGroupProvider.class).to(LdapGroupProvider.class).in(Scopes.SINGLETON);
        install(conditionalModule(
                LdapGroupProviderConfig.class,
                config -> config.getLdapUserMemberOfAttribute() != null,
                innerBinder -> innerBinder.bind(LdapGroupProviderClient.class).to(LdapGroupProviderSimpleClient.class).in(Scopes.SINGLETON),
                innerBinder -> {
                    configBinder(innerBinder).bindConfig(LdapGroupProviderFilteringClientConfig.class);
                    innerBinder.bind(LdapGroupProviderClient.class).annotatedWith(ForLdapUserSearchDelegate.class).to(LdapGroupProviderSimpleClient.class).in(Scopes.SINGLETON);
                    innerBinder.bind(LdapGroupProviderClient.class).to(LdapGroupProviderFilteringClient.class).in(Scopes.SINGLETON);
                }));
    }
}
