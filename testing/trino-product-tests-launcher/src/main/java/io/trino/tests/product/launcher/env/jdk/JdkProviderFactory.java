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
package io.trino.tests.product.launcher.env.jdk;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JdkProviderFactory
{
    private final Map<String, JdkProvider> providers;

    @Inject
    public JdkProviderFactory(Map<String, JdkProvider> providers)
    {
        this.providers = ImmutableMap.copyOf(requireNonNull(providers, "providers is null"));
    }

    public JdkProvider get(String name)
    {
        checkArgument(providers.containsKey(name), "No JDK provider with name '%s'. Those do exist, however: %s", name, list());
        return providers.get(name);
    }

    public List<String> list()
    {
        return Ordering.natural().sortedCopy(providers.keySet());
    }
}
