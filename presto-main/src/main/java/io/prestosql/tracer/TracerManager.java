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
package io.prestosql.tracer;

import io.prestosql.connector.CatalogName;
import io.prestosql.spi.tracer.ConnectorEventEmitter;
import io.prestosql.spi.tracer.ConnectorTracer;
import io.prestosql.spi.tracer.ConnectorTracerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TracerManager
{
    private final ConcurrentMap<CatalogName, ConnectorTracerFactory> tracerFactories = new ConcurrentHashMap<>();

    public void addConnectorTracerFactory(CatalogName catalogName, ConnectorTracerFactory connectorTracerFactory)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorTracerFactory, "connectorTracerFactory is null");
        checkState(tracerFactories.putIfAbsent(catalogName, connectorTracerFactory) == null, "TracerFactory for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorTracerFactory(CatalogName catalogName)
    {
        tracerFactories.remove(catalogName);
    }

    public Optional<ConnectorTracer> getConnectorTracer(CatalogName catalogName, ConnectorEventEmitter emitter)
    {
        ConnectorTracerFactory tracerFactory = tracerFactories.get(catalogName);
        if (tracerFactory == null) {
            return Optional.empty();
        }
        return Optional.of(tracerFactory.createConnectorTracer(emitter));
    }
}
