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
package io.trino.metastore.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

/**
 * Guice module for configuring Polaris metastore dependencies.
 * This module sets up all the necessary bindings for the Polaris metastore
 * to work within the trino-metastore library.
 */
public class PolarisMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        PolarisMetastoreConfig polarisConfig = buildConfigObject(PolarisMetastoreConfig.class);
        requireNonNull(polarisConfig.getUri(), "hive.metastore.polaris.uri is required");

        configBinder(binder).bindConfig(PolarisMetastoreConfig.class);

        switch (polarisConfig.getSecurity()) {
            case OAUTH2 -> {
                configBinder(binder).bindConfig(OAuth2SecurityConfig.class);
                install(new OAuth2SecurityModule());
            }
            case NONE -> install(new NoneSecurityModule());
        }

        binder.bind(AwsProperties.class).to(DefaultAwsProperties.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("polaris", ForPolarisClient.class);
        binder.bind(PolarisRestClient.class).in(Scopes.SINGLETON);
        binder.bind(PolarisMetastoreStats.class).in(Scopes.SINGLETON);
        binder.bind(PolarisMetastoreFactory.class).in(Scopes.SINGLETON);
        binder.bind(PolarisMetastore.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public RESTSessionCatalog createRESTSessionCatalog(
                PolarisMetastoreConfig config,
                SecurityProperties securityProperties,
                AwsProperties awsProperties,
                TrinoFileSystemFactory fileSystemFactory)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("uri", config.getUri().toString())
                .put("prefix", config.getPrefix() != null ? config.getPrefix() : "polaris")
                .put("warehouse", config.getWarehouse().orElse("polaris"));

        properties.putAll(securityProperties.get());
        properties.putAll(awsProperties.get());

        Map<String, String> allProperties = properties.buildOrThrow();

        RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog(
                httpConfig -> HTTPClient.builder(httpConfig)
                        .uri(httpConfig.get("uri"))
                        .withHeaders(Map.of())
                        .build(),
                (context, ioConfig) -> {
                    return new ForwardingFileIo(
                            fileSystemFactory.create(ConnectorIdentity.ofUser("polaris-metastore")),
                            ioConfig);
                });
        restSessionCatalog.initialize("polaris", allProperties);
        return restSessionCatalog;
    }
}
