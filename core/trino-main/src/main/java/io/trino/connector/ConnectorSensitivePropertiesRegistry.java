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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorName;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnectorSensitivePropertiesRegistry
{
    private final ConcurrentMap<ConnectorName, Set<String>> connectorSensitiveProperties = new ConcurrentHashMap<>();

    public void addSensitivePropertyNames(String connectorName, Set<String> propertyNames)
    {
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(propertyNames, "propertyNames is null");
        Set<String> existingProperties = connectorSensitiveProperties.putIfAbsent(new ConnectorName(connectorName), ImmutableSet.copyOf(propertyNames));
        checkArgument(existingProperties == null, "Sensitive property names for connector '%s' are already registered", connectorName);
    }

    public Optional<Set<String>> getSensitivePropertyNames(ConnectorName connectorName)
    {
        return Optional.ofNullable(connectorSensitiveProperties.get(connectorName));
    }
}
