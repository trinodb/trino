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
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PolarisHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final PolarisRestClient polarisClient;
    private final SecurityProperties securityProperties;
    private final PolarisMetastoreStats stats;
    private final PolarisMetastoreConfig config;
    private final AwsProperties awsProperties;
    private final TrinoFileSystemFactory fileSystemFactory;

    private volatile RESTSessionCatalog restSessionCatalog;

    @Inject
    public PolarisHiveMetastoreFactory(
            PolarisRestClient polarisClient,
            SecurityProperties securityProperties,
            PolarisMetastoreStats stats,
            PolarisMetastoreConfig config,
            AwsProperties awsProperties,
            TrinoFileSystemFactory fileSystemFactory)
    {
        this.polarisClient = requireNonNull(polarisClient, "polarisClient is null");
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.config = requireNonNull(config, "config is null");
        this.awsProperties = requireNonNull(awsProperties, "awsProperties is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public boolean hasBuiltInCaching()
    {
        return true;
    }

    @Override
    public synchronized HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        // Lazy initialization of RESTSessionCatalog - only when metastore is actually needed
        if (restSessionCatalog == null) {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder();
            properties.put(CatalogProperties.URI, config.getUri().toString());
            properties.put("prefix", config.getPrefix());

            config.getWarehouse().ifPresent(warehouse -> properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse));
            properties.putAll(securityProperties.get());
            properties.putAll(awsProperties.get());

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
            restSessionCatalog = catalog;
        }

        return new PolarisHiveMetastore(polarisClient, restSessionCatalog, securityProperties, stats);
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
