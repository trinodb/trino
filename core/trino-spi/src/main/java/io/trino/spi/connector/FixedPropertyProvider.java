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
package io.trino.spi.connector;

import io.trino.spi.session.PropertyMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class FixedPropertyProvider
        implements PropertyProvider
{
    private final Map<String, PropertyMetadata<?>> properties;

    public FixedPropertyProvider(Collection<PropertyMetadata<?>> properties)
    {
        requireNonNull(properties, "properties is null");
        this.properties = properties.stream()
                .collect(Collectors.toUnmodifiableMap(PropertyMetadata::getName, identity()));
    }

    @Override
    public Set<String> getKnownPropertyNames()
    {
        return properties.keySet();
    }

    @Override
    public Optional<PropertyMetadata<?>> getProperty(String name)
    {
        return Optional.ofNullable(properties.get(name));
    }
}
