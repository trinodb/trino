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
package io.trino.tests.product.launcher.env;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tests.product.launcher.Configurations.canonicalConfigName;

public class EnvironmentConfigFactory
{
    private final Map<String, EnvironmentConfig> configurations;

    @Inject
    public EnvironmentConfigFactory(Map<String, EnvironmentConfig> configurations)
    {
        this.configurations = ImmutableMap.copyOf(configurations);
    }

    public EnvironmentConfig getConfig(String configName)
    {
        configName = canonicalConfigName(configName);
        checkArgument(configurations.containsKey(configName), "No environment config with name '%s'. Those do exist, however: %s", configName, listConfigs());
        return configurations.get(configName);
    }

    public List<String> listConfigs()
    {
        return Ordering.natural().sortedCopy(configurations.keySet());
    }
}
