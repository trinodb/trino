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

import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final CatalogName catalogName;
    private final String trinoVersion;
    private final boolean nestedNamespaceEnabled;
    private final Security security;
    private final SessionType sessionType;
    private final boolean viewEndpointsEnabled;
    private final SecurityProperties securityProperties;
    private final IcebergRestCatalogPropertiesProvider catalogPropertiesProvider;
    private final boolean uniqueTableLocation;
    private final TypeManager typeManager;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<Namespace, Namespace> remoteNamespaceMappingCache;
    private final Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache;

    @GuardedBy("this")
    private RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            IcebergFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            IcebergRestCatalogPropertiesProvider catalogPropertiesProvider,
            IcebergConfig icebergConfig,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.nestedNamespaceEnabled = restConfig.isNestedNamespaceEnabled();
        this.security = restConfig.getSecurity();
        this.sessionType = restConfig.getSessionType();
        this.viewEndpointsEnabled = restConfig.isViewEndpointsEnabled();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.catalogPropertiesProvider = requireNonNull(catalogPropertiesProvider, "catalogPropertiesProvider is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.uniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.caseInsensitiveNameMatching = restConfig.isCaseInsensitiveNameMatching();
        this.remoteNamespaceMappingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
        this.remoteTableMappingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        // Creation of the RESTSessionCatalog is lazy due to required network calls
        // for authorization and config route
        if (icebergCatalog == null) {
            RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog(
                    config -> HTTPClient.builder(config)
                            .uri(config.get(CatalogProperties.URI))
                            .withHeaders(RESTUtil.configHeaders(config))
                            .build(),
                    (context, config) -> {
                        ConnectorIdentity currentIdentity = (context.wrappedIdentity() != null)
                                ? ((ConnectorIdentity) context.wrappedIdentity())
                                : ConnectorIdentity.ofUser("fake");
                        return fileIoFactory.create(fileSystemFactory.create(currentIdentity, config), true, config);
                    });
            icebergCatalogInstance.initialize(catalogName.toString(), catalogPropertiesProvider.catalogProperties());

            icebergCatalog = icebergCatalogInstance;
        }

        // `OAuth2Properties.SCOPE` is not set as scope passed through credentials is unused in
        // https://github.com/apache/iceberg/blob/229d8f6fcd109e6c8943ea7cbb41dab746c6d0ed/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L714-L721
        Map<String, String> credentials = Maps.filterKeys(securityProperties.get(), key -> Set.of(TOKEN, CREDENTIAL).contains(key));

        return new TrinoRestCatalog(
                fileSystemFactory,
                icebergCatalog,
                catalogName,
                security,
                sessionType,
                credentials,
                nestedNamespaceEnabled,
                trinoVersion,
                typeManager,
                uniqueTableLocation,
                caseInsensitiveNameMatching,
                remoteNamespaceMappingCache,
                remoteTableMappingCache,
                viewEndpointsEnabled);
    }
}
