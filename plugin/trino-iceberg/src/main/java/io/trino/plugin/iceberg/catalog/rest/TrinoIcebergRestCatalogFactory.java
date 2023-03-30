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
import io.trino.hdfs.ConfigurationUtils;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTSessionCatalog;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final String trinoVersion;
    private final URI serverUri;
    private final Optional<String> warehouse;
    private final SessionType sessionType;
    private final SecurityProperties securityProperties;
    private final boolean uniqueTableLocation;

    @GuardedBy("this")
    private RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            IcebergConfig icebergConfig,
            NodeVersion nodeVersion)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.serverUri = restConfig.getBaseUri();
        this.warehouse = restConfig.getWarehouse();
        this.sessionType = restConfig.getSessionType();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.uniqueTableLocation = icebergConfig.isUniqueTableLocation();
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
            properties.put("trino-version", trinoVersion);
            properties.putAll(securityProperties.get());
            RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog();
            icebergCatalogInstance.setConf(ConfigurationUtils.getInitialConfiguration());
            icebergCatalogInstance.initialize(catalogName.toString(), properties.buildOrThrow());

            icebergCatalog = icebergCatalogInstance;
        }

        return new TrinoRestCatalog(icebergCatalog, catalogName, sessionType, trinoVersion, uniqueTableLocation);
    }
}
