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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class IcebergRestCatalogConfig
{
    public enum Security
    {
        NONE,
        OAUTH2,
    }

    public enum SessionType
    {
        NONE,
        USER
    }

    private URI restUri;
    private Optional<String> warehouse = Optional.empty();
    private Security security = Security.NONE;
    private SessionType sessionType = SessionType.NONE;

    @NotNull
    public URI getBaseUri()
    {
        return this.restUri;
    }

    @Config("iceberg.rest-catalog.uri")
    @ConfigDescription("The URI to the REST server")
    public IcebergRestCatalogConfig setBaseUri(String uri)
    {
        if (uri != null) {
            this.restUri = URI.create(uri);
        }
        return this;
    }

    @NotNull
    public Security getSecurity()
    {
        return security;
    }

    @Config("iceberg.rest-catalog.security")
    @ConfigDescription("Authorization protocol to use when communicating with the REST catalog server")
    public IcebergRestCatalogConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }

    @NotNull
    public IcebergRestCatalogConfig.SessionType getSessionType()
    {
        return sessionType;
    }

    @Config("iceberg.rest-catalog.session")
    @ConfigDescription("Type of REST catalog sessionType to use when communicating with REST catalog Server")
    public IcebergRestCatalogConfig setSessionType(SessionType sessionType)
    {
        this.sessionType = sessionType;
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return warehouse;
    }

    @Config("iceberg.rest-catalog.warehouse")
    @ConfigDescription("The warehouse location/identifier to use with the REST catalog server")
    public IcebergRestCatalogConfig setWarehouse(String warehouse)
    {
        this.warehouse = Optional.ofNullable(warehouse);
        return this;
    }
}
