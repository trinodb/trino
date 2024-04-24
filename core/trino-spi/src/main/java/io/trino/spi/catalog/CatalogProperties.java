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
package io.trino.spi.catalog;

import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public record CatalogProperties(CatalogHandle catalogHandle, ConnectorName connectorName, Map<String, String> properties)
{
    public CatalogProperties
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorName, "connectorName is null");
        properties = Map.copyOf(requireNonNull(properties, "properties is null"));
    }
}
