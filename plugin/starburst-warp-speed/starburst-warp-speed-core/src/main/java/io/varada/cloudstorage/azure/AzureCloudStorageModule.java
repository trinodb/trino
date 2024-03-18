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
package io.varada.cloudstorage.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracingOptions;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigurationFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureAuthAccessKeyConfig;
import io.trino.filesystem.azure.AzureAuthOAuthConfig;
import io.trino.filesystem.azure.AzureAuthOauth;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorContext;

import java.lang.annotation.Annotation;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class AzureCloudStorageModule
        implements Module
{
    private final ConnectorContext context;
    private final ConfigurationFactory configurationFactory;
    private final Class<? extends Annotation> annotation;

    public AzureCloudStorageModule(ConnectorContext context,
            ConfigurationFactory configurationFactory,
            Class<? extends Annotation> annotation)
    {
        this.context = requireNonNull(context, "context is null");
        this.configurationFactory = requireNonNull(configurationFactory, "configurationFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigurationFactory.class).toInstance(configurationFactory);

        configBinder(binder).bindConfig(AzureFileSystemConfig.class);

        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
        binder.bind(AzureFileSystemFactory.class);

        AzureFileSystemConfig config = configurationFactory.build(AzureFileSystemConfig.class);
        switch (config.getAuthType()) {
            case ACCESS_KEY -> {
                configBinder(binder).bindConfig(AzureAuthAccessKeyConfig.class);
                binder.bind(AzureAuth.class).to(AzureAuthAccessKey.class);
            }
            case OAUTH -> {
                configBinder(binder).bindConfig(AzureAuthOAuthConfig.class);
                binder.bind(AzureAuth.class).to(AzureAuthOauth.class);
            }
            case DEFAULT -> {}
        }

        binder.bind(AzureCloudStorage.class).annotatedWith(annotation).to(AzureCloudStorage.class);
    }

    @Provides
    @Singleton
    public AzureCloudStorage provideAzureCloudStorage(AzureFileSystemFactory fileSystemFactory,
            OpenTelemetry openTelemetry,
            AzureAuth azureAuth)
    {
        AzureFileSystemFactory azureFileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        TracingOptions tracingOptions = new OpenTelemetryTracingOptions().setOpenTelemetry(openTelemetry);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        HttpClient httpClient = HttpClient.createDefault((HttpClientOptions) new HttpClientOptions()
                .setConfiguration(configurationBuilder.build())
                .setTracingOptions(tracingOptions));

        return new AzureCloudStorage(azureFileSystemFactory, httpClient, tracingOptions, azureAuth);
    }
}
