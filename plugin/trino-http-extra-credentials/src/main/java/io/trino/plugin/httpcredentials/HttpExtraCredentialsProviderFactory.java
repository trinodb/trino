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
package io.trino.plugin.httpcredentials;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.units.Duration;
import io.trino.spi.security.ExtraCredentialsProvider;
import io.trino.spi.security.ExtraCredentialsProviderFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HttpExtraCredentialsProviderFactory
        implements ExtraCredentialsProviderFactory
{
    @Override
    public String getName()
    {
        return "http";
    }

    @Override
    public ExtraCredentialsProvider create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.extra-credentials." + getName(),
                binder -> {
                    configBinder(binder).bindConfig(HttpExtraCredentialsConfig.class);
                    binder.bind(HttpExtraCredentialsProvider.class).in(Scopes.SINGLETON);
                    httpClientBinder(binder)
                            .bindHttpClient("http-extra-credentials", ForHttpExtraCredentials.class)
                            .withConfigDefaults(clientConfig -> {
                                clientConfig.setConnectTimeout(new Duration(2, SECONDS));
                                clientConfig.setRequestTimeout(new Duration(2, SECONDS));
                            });
                });

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(HttpExtraCredentialsProvider.class);
    }
}
