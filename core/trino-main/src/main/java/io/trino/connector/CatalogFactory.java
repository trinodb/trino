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
package io.trino.connector;

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;

import java.util.Set;

@ThreadSafe
public interface CatalogFactory
{
    void addConnectorFactory(ConnectorFactory connectorFactory);

    CatalogConnector createCatalog(CatalogProperties catalogProperties);

    CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector);

    Set<String> getSecuritySensitivePropertyNames(CatalogProperties catalogProperties);
}
