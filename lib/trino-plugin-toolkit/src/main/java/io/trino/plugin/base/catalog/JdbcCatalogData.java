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
package io.trino.plugin.base.catalog;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Data Structure carrying catalog row data pulled from the backing store.
 */
public class JdbcCatalogData
{
    public final String catalogName;
    public final String versionIdentifier;
    public final String connectorName;
    public final Map<String, String> properties;

    public JdbcCatalogData(String catalogName, String versionIdentifier, String connectorName, Map<String, String> properties)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.versionIdentifier = requireNonNull(versionIdentifier, "versionIdentifier is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.properties = requireNonNull(properties, "properties is null");
    }
}
