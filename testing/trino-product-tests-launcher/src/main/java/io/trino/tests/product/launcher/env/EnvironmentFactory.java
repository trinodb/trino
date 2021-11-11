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

import com.google.common.collect.Ordering;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tests.product.launcher.Configurations.canonicalEnvironmentName;
import static java.util.Objects.requireNonNull;

public final class EnvironmentFactory
{
    private final Map<String, EnvironmentProvider> environmentProviders;

    @Inject
    public EnvironmentFactory(Map<String, EnvironmentProvider> environmentProviders)
    {
        this.environmentProviders = requireNonNull(environmentProviders, "environmentProviders is null");
    }

    public Environment.Builder get(String environmentName, EnvironmentConfig config, Map<String, String> extraOptions)
    {
        environmentName = canonicalEnvironmentName(environmentName);
        checkArgument(environmentProviders.containsKey(environmentName), "No environment with name '%s'. Those do exist, however: %s", environmentName, list());
        return environmentProviders.get(environmentName)
                .createEnvironment(environmentName, config, extraOptions);
    }

    public List<String> list()
    {
        return Ordering.natural().sortedCopy(environmentProviders.keySet());
    }
}
