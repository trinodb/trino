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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergFileSystemFactory fileSystemFactory;
    private final CatalogName catalogName;
    private final String trinoVersion;
    private final URI serverUri;
    private final Optional<String> prefix;
    private final Optional<String> warehouse;
    private final SessionType sessionType;
    private final boolean vendedCredentialsEnabled;
    private final SecurityProperties securityProperties;
    private final boolean uniqueTableLocation;
    private final TypeManager typeManager;

    @GuardedBy("this")
    private RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            IcebergFileSystemFactory fileSystemFactory,
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            IcebergConfig icebergConfig,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.serverUri = restConfig.getBaseUri();
        this.prefix = restConfig.getPrefix();
        this.warehouse = restConfig.getWarehouse();
        this.sessionType = restConfig.getSessionType();
        this.vendedCredentialsEnabled = restConfig.isVendedCredentialsEnabled();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.uniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        // Creation of the RESTSessionCatalog is lazy due to required network calls
        // for authorization and config route
        if (icebergCatalog == null) {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            properties.put(CatalogProperties.URI, serverUri.toString());
            warehouse.ifPresent(location -> properties.put(CatalogProperties.WAREHOUSE_LOCATION, location));
            prefix.ifPresent(prefix -> properties.put("prefix", prefix));
            properties.put("trino-version", trinoVersion);
            properties.putAll(securityProperties.get());

            if (vendedCredentialsEnabled) {
                properties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
            }

            RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog(
                    config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build(),
                    (context, config) -> {
                        ConnectorIdentity currentIdentity = (context.wrappedIdentity() != null)
                                ? ((ConnectorIdentity) context.wrappedIdentity())
                                : ConnectorIdentity.ofUser("fake");
                        return new ForwardingFileIo(fileSystemFactory.create(currentIdentity, config), config);
                    });
            icebergCatalogInstance.initialize(catalogName.toString(), properties.buildOrThrow());

            icebergCatalog = icebergCatalogInstance;
        }

        // `OAuth2Properties.SCOPE` is not set as scope passed through credentials is unused in
        // https://github.com/apache/iceberg/blob/229d8f6fcd109e6c8943ea7cbb41dab746c6d0ed/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L714-L721
        Map<String, String> credentials = Maps.filterKeys(securityProperties.get(), key -> Set.of(TOKEN, CREDENTIAL).contains(key));

        return new TrinoRestCatalog(
                icebergCatalog,
                catalogName,
                sessionType,
                credentials,
                trinoVersion,
                typeManager,
                uniqueTableLocation);
    }
}
