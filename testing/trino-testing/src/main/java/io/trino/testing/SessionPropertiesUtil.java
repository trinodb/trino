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
package io.trino.testing;

import io.trino.spi.session.PropertyMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotEquals;

public final class SessionPropertiesUtil
{
    private SessionPropertiesUtil() {}

    public static void assertDefaultProperties(List<PropertyMetadata<?>> propertiesMetadata, Map<String, Object> properties)
    {
        assertPropertyNames(propertiesMetadata, properties);

        for (PropertyMetadata<?> metadata : propertiesMetadata) {
            assertThat(properties.get(metadata.getName()))
                    .withFailMessage(format("Expected '%s' but got '%s' as '%s' default value", metadata.getDefaultValue(), properties.get(metadata.getName()), metadata.getName()))
                    .isEqualTo(metadata.getDefaultValue());
        }
    }

    public static void assertExplicitProperties(List<PropertyMetadata<?>> propertiesMetadata, Map<String, Object> properties)
    {
        assertPropertyNames(propertiesMetadata, properties);

        Map<String, Object> defaultValues = defaultNameAndValues(propertiesMetadata);

        for (Map.Entry<String, Object> property : properties.entrySet()) {
            assertNotEquals(
                    property.getValue(),
                    defaultValues.get(property.getKey()),
                    format("The value '%s' should be different from the default '%s' for '%s' property", property.getValue(), defaultValues.get(property.getKey()), property.getKey()));
        }
    }

    private static Map<String, Object> defaultNameAndValues(List<PropertyMetadata<?>> propertiesMetadata)
    {
        Map<String, Object> defaultValues = new HashMap<>();
        for (PropertyMetadata<?> metadata : propertiesMetadata) {
            defaultValues.put(metadata.getName(), metadata.getDefaultValue());
        }
        return defaultValues;
    }

    private static void assertPropertyNames(List<PropertyMetadata<?>> propertiesMetadata, Map<String, Object> properties)
    {
        Set<String> availableNames = propertiesMetadata.stream()
                .map(PropertyMetadata::getName)
                .collect(toImmutableSet());
        assertThat(properties.keySet()).hasSameElementsAs(availableNames);
    }
}
