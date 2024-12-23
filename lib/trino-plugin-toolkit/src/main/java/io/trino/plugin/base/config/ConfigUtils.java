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

import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.ConfigPropertyMetadata;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ConfigUtils
{
    private ConfigUtils() {}

    public static Set<String> getSecuritySensitivePropertyNames(Map<String, String> config, Set<ConfigPropertyMetadata> usedProperties)
    {
        Set<String> sensitivePropertyNames = new HashSet<>(config.keySet());

        for (ConfigPropertyMetadata propertyMetadata : usedProperties) {
            if (!propertyMetadata.securitySensitive()) {
                sensitivePropertyNames.remove(propertyMetadata.name());
            }
        }

        return ImmutableSet.copyOf(sensitivePropertyNames);
    }
}
