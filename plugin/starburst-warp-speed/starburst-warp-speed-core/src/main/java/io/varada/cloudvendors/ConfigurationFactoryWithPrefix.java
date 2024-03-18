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
package io.varada.cloudvendors;

import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.WarningsMonitor;

import java.util.Map;
import java.util.stream.Collectors;

public class ConfigurationFactoryWithPrefix
        extends ConfigurationFactory
{
    public ConfigurationFactoryWithPrefix(Map<String, String> properties, String prefix, WarningsMonitor warningsMonitor)
    {
        super(properties.entrySet()
                        .stream()
                        .filter(entry -> prefix == null || entry.getKey().startsWith(prefix))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                warningsMonitor);
    }
}
