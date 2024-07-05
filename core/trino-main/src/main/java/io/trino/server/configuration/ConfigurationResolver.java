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
package io.trino.server.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.spi.Message;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.configuration.ConfigurationValueResolver;
import io.trino.spi.configuration.InvalidConfigurationException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ConfigurationResolver
{
    private static final Pattern PATTERN = Pattern.compile("\\$\\{([a-zA-Z][a-zA-Z0-9_-]*):(?<key>[^}]+?)}");

    private final Map<String, ConfigurationValueResolver> configurationValueResolvers;

    public ConfigurationResolver(Map<String, ConfigurationValueResolver> configurationValueResolvers)
    {
        this.configurationValueResolvers = ImmutableMap.copyOf(requireNonNull(configurationValueResolvers, "configurationValueResolvers is null"));
    }

    public Map<String, String> getResolvedConfiguration(Map<String, String> properties)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builderWithExpectedSize(properties.size());
        ImmutableList.Builder<Message> errorsBuilder = ImmutableList.builder();
        properties.forEach((propertyKey, propertyValue) -> {
            builder.put(propertyKey, resolveConfiguration(propertyValue).orElse(propertyValue));
        });
        List<Message> errors = errorsBuilder.build();
        if (!errors.isEmpty()) {
            throw new ApplicationConfigurationException(errors, List.of());
        }
        return builder.buildOrThrow();
    }

    private Optional<String> resolveConfiguration(String configurationValue)
            throws InvalidConfigurationException
    {
        Matcher matcher = PATTERN.matcher(configurationValue);
        if (matcher.find()) {
            String configProviderName = matcher.group(1).toLowerCase(ENGLISH);
            ConfigurationValueResolver configProvider = configurationValueResolvers.get(configProviderName);
            if (configProvider != null) {
                String keyName = matcher.group("key");
                return Optional.of(configProvider.resolveConfigurationValue(keyName));
            }
        }
        return Optional.empty();
    }
}
