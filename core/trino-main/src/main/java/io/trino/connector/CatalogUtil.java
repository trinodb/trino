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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.stream.Collectors;

public class CatalogUtil
{
    private static final String METADATA_MAPPING = "connector.metadata-mapping";

    private CatalogUtil() {}

    public static Map<String, String> getMetadataMapping(Map<String, String> properties)
    {
        String mapping = properties.get(METADATA_MAPPING);
        return mapping != null ? getMetadataMapping(mapping) : ImmutableMap.of();
    }

    public static Map<String, String> getPropertyMapping(Map<String, String> properties)
    {
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (!METADATA_MAPPING.equals(property.getKey())) {
                propertiesBuilder.put(property.getKey(), property.getValue());
            }
        }
        return propertiesBuilder.buildOrThrow();
    }

    private static Map<String, String> getMetadataMapping(String mapping)
    {
        return Splitter.on(",").withKeyValueSeparator("=").split(mapping).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
