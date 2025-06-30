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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

/**
 * Configuration for Apache Polaris metastore backend.
 */
public class PolarisMetastoreConfig
{
    public enum Security
    {
        NONE,
        OAUTH2
    }

    private URI uri;
    private String prefix = "";
    private String warehouse;
    private Security security = Security.OAUTH2;

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("hive.metastore.polaris.uri")
    @ConfigDescription("URI of the Polaris catalog server")
    public PolarisMetastoreConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    public String getPrefix()
    {
        return prefix;
    }

    @Config("hive.metastore.polaris.prefix")
    @ConfigDescription("Optional prefix for all API requests")
    public PolarisMetastoreConfig setPrefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return Optional.ofNullable(warehouse);
    }

    @Config("hive.metastore.polaris.warehouse")
    @ConfigDescription("Default warehouse location for tables")
    public PolarisMetastoreConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public Security getSecurity()
    {
        return security;
    }

    @Config("hive.metastore.polaris.security")
    @ConfigDescription("Security type: NONE or OAUTH2")
    public PolarisMetastoreConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }
}
