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
package io.trino.plugin.password.salesforce;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class SalesforceAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "salesforce";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                binder -> {
                    configBinder(binder).bindConfig(SalesforceConfig.class);
                    binder.bind(SalesforceBasicAuthenticator.class).in(Scopes.SINGLETON);
                    httpClientBinder(binder)
                            .bindHttpClient("salesforce-authenticator", SalesforceAuthenticationClient.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(SalesforceBasicAuthenticator.class);
    }
}
