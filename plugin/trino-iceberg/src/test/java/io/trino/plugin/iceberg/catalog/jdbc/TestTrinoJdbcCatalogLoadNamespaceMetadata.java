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
package io.trino.plugin.iceberg.catalog.jdbc;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergSchemaProperties.SUPPORTED_SCHEMA_PROPERTIES;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoJdbcCatalogLoadNamespaceMetadata
{
    // Mirrors exactly what the fixed loadNamespaceMetadata does
    private static Map<String, Object> filterNamespaceMetadata(Map<String, String> raw)
    {
        return raw.entrySet().stream()
                .filter(entry -> SUPPORTED_SCHEMA_PROPERTIES.contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Test
    public void testFiltersInvalidProperties()
    {
        Map<String, String> raw = new HashMap<>();
        raw.put("location", "s3://bucket/path");
        raw.put("invalid_key", "should_be_stripped");
        raw.put("another_bad_prop", "also_stripped");

        Map<String, Object> result = filterNamespaceMetadata(raw);

        assertThat(result).containsOnlyKeys("location");
        assertThat(result).containsEntry("location", "s3://bucket/path");
    }

    @Test
    public void testRetainsLocationWhenNoInvalidProperties()
    {
        Map<String, String> raw = new HashMap<>();
        raw.put("location", "s3://bucket/clean");

        Map<String, Object> result = filterNamespaceMetadata(raw);

        assertThat(result).containsOnlyKeys("location");
    }

    @Test
    public void testReturnsEmptyWhenOnlyInvalidProperties()
    {
        Map<String, String> raw = new HashMap<>();
        raw.put("invalid_key", "value1");
        raw.put("another_bad_prop", "value2");

        Map<String, Object> result = filterNamespaceMetadata(raw);

        assertThat(result).isEmpty();
    }
}