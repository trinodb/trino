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
package io.trino.plugin.hive.metastore.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

public class PolarisMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // 1. Load configuration for immediate validation and conditional module installation
        PolarisMetastoreConfig polarisConfig = buildConfigObject(PolarisMetastoreConfig.class);
        requireNonNull(polarisConfig.getUri(), "polaris.uri is required");

        // 2. Bind configuration for runtime injection
        configBinder(binder).bindConfig(PolarisMetastoreConfig.class);

        // 3. Install security module based on configuration
        switch (polarisConfig.getSecurity()) {
            case OAUTH2 -> {
                configBinder(binder).bindConfig(OAuth2SecurityConfig.class);
                install(new OAuth2SecurityModule());
            }
            case NONE -> install(new NoneSecurityModule());
        }

        // 4. Bind AWS properties (currently none, but ready for future SigV4 support)
        binder.bind(AwsProperties.class).to(DefaultAwsProperties.class).in(Scopes.SINGLETON);

        // 5. Bind HTTP client for Polaris API calls
        httpClientBinder(binder).bindHttpClient("polaris", ForPolarisClient.class);

        // 6. Bind core components
        binder.bind(PolarisRestClient.class).in(Scopes.SINGLETON);

        binder.bind(HiveMetastoreFactory.class)
                .annotatedWith(RawHiveMetastoreFactory.class)
                .to(PolarisHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        // 7. Bind standard Hive settings
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(true);
    }

    @Provides
    @Singleton
    public RESTSessionCatalog createRESTSessionCatalog(
            PolarisMetastoreConfig config,
            SecurityProperties securityProperties,
            AwsProperties awsProperties,
            TrinoFileSystemFactory fileSystemFactory)
    {
        // Build properties map following Iceberg REST catalog pattern
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder();
        properties.put(CatalogProperties.URI, config.getUri().toString());
        properties.put("prefix", config.getPrefix());

        // Add warehouse if specified
        config.getWarehouse().ifPresent(warehouse -> properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse));

        // Add security properties from the injected SecurityProperties
        properties.putAll(securityProperties.get());

        // Add AWS properties (empty for now, but ready for future SigV4 support)
        properties.putAll(awsProperties.get());

        // Create RESTSessionCatalog with HTTP client and FileIO factories to avoid Hadoop dependencies
        RESTSessionCatalog catalog = new RESTSessionCatalog(
                httpConfig -> HTTPClient.builder(httpConfig)
                        .uri(httpConfig.get(CatalogProperties.URI))
                        .withHeaders(RESTUtil.configHeaders(httpConfig))
                        .build(),
                (context, ioConfig) -> {
                    ConnectorIdentity currentIdentity = (context.wrappedIdentity() != null)
                            ? ((ConnectorIdentity) context.wrappedIdentity())
                            : ConnectorIdentity.ofUser("fake");
                    return new ForwardingFileIo(fileSystemFactory.create(currentIdentity), ioConfig);
                });

        catalog.initialize("polaris", properties.buildOrThrow());
        return catalog;
    }
}
