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

import io.trino.spi.connector.CatalogHandle;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnectorCatalogServiceProvider<T>
        implements CatalogServiceProvider<T>
{
    private final String name;
    private final ConnectorServicesProvider connectorServicesProvider;
    private final Function<ConnectorServices, T> serviceGetter;

    public ConnectorCatalogServiceProvider(String name, ConnectorServicesProvider connectorServicesProvider, Function<ConnectorServices, T> serviceGetter)
    {
        this.name = requireNonNull(name, "name is null");
        this.connectorServicesProvider = requireNonNull(connectorServicesProvider, "connectorServicesProvider is null");
        this.serviceGetter = requireNonNull(serviceGetter, "serviceGetter is null");
    }

    @Override
    public T getService(CatalogHandle catalogHandle)
    {
        ConnectorServices connectorServices = connectorServicesProvider.getConnectorServices(catalogHandle);
        T result = serviceGetter.apply(connectorServices);
        checkArgument(result != null, "Catalog '%s' does not have a %s", catalogHandle, name);
        return result;
    }
}
