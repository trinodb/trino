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
package io.prestosql.tests.product.launcher.env;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public final class SelectedEnvironmentProvider
{
    private final EnvironmentFactory environmentFactory;
    private final String environmentName;

    @Inject
    public SelectedEnvironmentProvider(EnvironmentFactory environmentFactory, EnvironmentOptions environmentOptions)
    {
        this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
        this.environmentName = requireNonNull(environmentOptions, "environmentOptions is null").environment;
    }

    public String getEnvironmentName()
    {
        return environmentName;
    }

    public Environment.Builder getEnvironment()
    {
        return environmentFactory.get(environmentName);
    }
}
