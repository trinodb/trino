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

package io.trino.plugin.base.config;

import io.airlift.configuration.ConfigurationMetadata;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigurationMetadata.getConfigurationMetadata;
import static java.util.Objects.requireNonNull;

public record ConfigPropertyMetadata(String name, boolean sensitive)
{
    public ConfigPropertyMetadata
    {
        requireNonNull(name, "name is null");
    }

    public static Set<ConfigPropertyMetadata> getConfigProperties(Class<?> configClass)
    {
        ConfigurationMetadata<?> configurationMetadata = getConfigurationMetadata(configClass);
        return configurationMetadata.getAttributes().values().stream()
                .map(attribute -> new ConfigPropertyMetadata(attribute.getInjectionPoint().getProperty(), attribute.isSecuritySensitive()))
                .collect(toImmutableSet());
    }
}
