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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClientBinder.HttpClientBindingBuilder;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

public class InternalCommunicationHttpClientModule
        extends AbstractConfigurationAwareModule
{
    private final String clientName;
    private final Class<? extends Annotation> annotation;
    private final boolean withTracing;
    private final Consumer<HttpClientConfig> configDefaults;
    private final List<Class<? extends HttpRequestFilter>> filters;

    private InternalCommunicationHttpClientModule(
            String clientName,
            Class<? extends Annotation> annotation,
            boolean withTracing,
            Consumer<HttpClientConfig> configDefaults,
            List<Class<? extends HttpRequestFilter>> filters)
    {
        this.clientName = requireNonNull(clientName, "clientName is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
        this.withTracing = withTracing;
        this.configDefaults = requireNonNull(configDefaults, "configDefaults is null");
        this.filters = ImmutableList.copyOf(requireNonNull(filters, "filters is null"));
    }

    @Override
    protected void setup(Binder binder)
    {
        HttpClientBindingBuilder httpClientBindingBuilder = httpClientBinder(binder).bindHttpClient(clientName, annotation);
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        httpClientBindingBuilder.withConfigDefaults(httpConfig -> {
            configureClient(httpConfig, internalCommunicationConfig);
            configDefaults.accept(httpConfig);
        });

        httpClientBindingBuilder.addFilterBinding().to(InternalAuthenticationManager.class);

        if (withTracing) {
            httpClientBindingBuilder.withTracing();
        }

        filters.forEach(httpClientBindingBuilder::withFilter);
    }

    static void configureClient(HttpClientConfig httpConfig, InternalCommunicationConfig internalCommunicationConfig)
    {
        httpConfig.setHttp2Enabled(internalCommunicationConfig.isHttp2Enabled());
        if (internalCommunicationConfig.isHttpsRequired() && internalCommunicationConfig.getKeyStorePath() == null && internalCommunicationConfig.getTrustStorePath() == null) {
            configureClientForAutomaticHttps(httpConfig, internalCommunicationConfig);
        }
        else {
            configureClientForManualHttps(httpConfig, internalCommunicationConfig);
        }
    }

    private static void configureClientForAutomaticHttps(HttpClientConfig httpConfig, InternalCommunicationConfig internalCommunicationConfig)
    {
        String sharedSecret = internalCommunicationConfig.getSharedSecret()
                .orElseThrow(() -> new IllegalArgumentException("Internal shared secret must be set when internal HTTPS is enabled"));
        httpConfig.setAutomaticHttpsSharedSecret(sharedSecret);
    }

    private static void configureClientForManualHttps(HttpClientConfig httpConfig, InternalCommunicationConfig internalCommunicationConfig)
    {
        httpConfig.setKeyStorePath(internalCommunicationConfig.getKeyStorePath());
        httpConfig.setKeyStorePassword(internalCommunicationConfig.getKeyStorePassword());
        httpConfig.setTrustStorePath(internalCommunicationConfig.getTrustStorePath());
        httpConfig.setTrustStorePassword(internalCommunicationConfig.getTrustStorePassword());
        httpConfig.setAutomaticHttpsSharedSecret(null);
    }

    public static class Builder
    {
        private final String clientName;
        private final Class<? extends Annotation> annotation;
        private boolean withTracing;
        private Consumer<HttpClientConfig> configDefaults = config -> {};
        private final List<Class<? extends HttpRequestFilter>> filters = new ArrayList<>();

        private Builder(String clientName, Class<? extends Annotation> annotation)
        {
            this.clientName = requireNonNull(clientName, "clientName is null");
            this.annotation = requireNonNull(annotation, "annotation is null");
        }

        public Builder withTracing()
        {
            this.withTracing = true;
            return this;
        }

        public Builder withConfigDefaults(Consumer<HttpClientConfig> configDefaults)
        {
            this.configDefaults = requireNonNull(configDefaults, "configDefaults is null");
            return this;
        }

        public Builder withFilter(Class<? extends HttpRequestFilter> requestFilter)
        {
            this.filters.add(requestFilter);
            return this;
        }

        public InternalCommunicationHttpClientModule build()
        {
            return new InternalCommunicationHttpClientModule(clientName, annotation, withTracing, configDefaults, filters);
        }
    }

    public static InternalCommunicationHttpClientModule.Builder internalHttpClientModule(String clientName, Class<? extends Annotation> annotation)
    {
        return new Builder(clientName, annotation);
    }
}
