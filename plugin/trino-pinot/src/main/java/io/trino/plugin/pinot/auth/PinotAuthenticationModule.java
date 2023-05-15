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
package io.trino.plugin.pinot.auth;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.pinot.auth.none.PinotEmptyAuthenticationProvider;
import io.trino.plugin.pinot.auth.password.PinotPasswordAuthenticationProvider;
import io.trino.plugin.pinot.auth.password.inline.PinotPasswordBrokerAuthenticationConfig;
import io.trino.plugin.pinot.auth.password.inline.PinotPasswordControllerAuthenticationConfig;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.pinot.auth.PinotAuthenticationType.NONE;
import static io.trino.plugin.pinot.auth.PinotAuthenticationType.PASSWORD;

public class PinotAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindAuthenticationProviderModule(
                NONE,
                new PinotNoneControllerAuthenticationProviderModule(),
                new PinotNoneBrokerAuthenticationProviderModule());
        bindAuthenticationProviderModule(
                PASSWORD,
                new PinotPasswordControllerAuthenticationProviderModule(),
                new PinotPasswordBrokerAuthenticationProviderModule());
    }

    private void bindAuthenticationProviderModule(PinotAuthenticationType authType, Module controllerModule, Module brokerModule)
    {
        install(conditionalModule(
                PinotAuthenticationTypeConfig.class,
                config -> authType == config.getControllerAuthenticationType(),
                controllerModule));
        install(conditionalModule(
                PinotAuthenticationTypeConfig.class,
                config -> authType == config.getBrokerAuthenticationType(),
                brokerModule));
    }

    private static class PinotNoneControllerAuthenticationProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
        }

        @Provides
        @Singleton
        public PinotControllerAuthenticationProvider getAuthenticationProvider()
        {
            return PinotControllerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance());
        }
    }

    private static class PinotNoneBrokerAuthenticationProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
        }

        @Provides
        @Singleton
        public PinotBrokerAuthenticationProvider getAuthenticationProvider()
        {
            return PinotBrokerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance());
        }
    }

    private static class PinotPasswordControllerAuthenticationProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(PinotPasswordControllerAuthenticationConfig.class);
        }

        @Provides
        @Singleton
        public PinotControllerAuthenticationProvider getAuthenticationProvider(
                PinotPasswordControllerAuthenticationConfig config)
        {
            return PinotControllerAuthenticationProvider.create(new PinotPasswordAuthenticationProvider(config.getUser(), config.getPassword()));
        }
    }

    private static class PinotPasswordBrokerAuthenticationProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(PinotPasswordBrokerAuthenticationConfig.class);
        }

        @Provides
        @Singleton
        public PinotBrokerAuthenticationProvider getAuthenticationProvider(
                PinotPasswordBrokerAuthenticationConfig config)
        {
            return PinotBrokerAuthenticationProvider.create(new PinotPasswordAuthenticationProvider(config.getUser(), config.getPassword()));
        }
    }
}
