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

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class TableObjectProperties
{
    private static final TableObjectProperties EMPTY = new TableObjectProperties(Map.of(), Map.of());

    private final Map<String, Object> properties;
    private final Map<String, Map<String, Object>> columnProperties;

    public TableObjectProperties(Map<String, Object> properties, Map<String, Map<String, Object>> columnProperties)
    {
        requireNonNull(properties, "properties is null");
        requireNonNull(columnProperties, "columnProperties is null");

        this.properties = Map.copyOf(properties);
        this.columnProperties = columnProperties.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), Map.copyOf(entry.getValue())))
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    public Map<String, Map<String, Object>> getColumnProperties()
    {
        return columnProperties;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TableProperties{");
        sb.append("properties=").append(properties);
        sb.append(", columnProperties=").append(columnProperties);
        sb.append('}');
        return sb.toString();
    }

    public static TableObjectProperties empty()
    {
        return EMPTY;
    }
}
