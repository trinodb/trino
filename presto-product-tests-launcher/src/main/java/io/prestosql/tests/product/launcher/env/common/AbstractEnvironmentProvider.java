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
package io.prestosql.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentProvider;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class AbstractEnvironmentProvider
        implements EnvironmentProvider
{
    private final List<EnvironmentExtender> bases;

    protected AbstractEnvironmentProvider(List<EnvironmentExtender> bases)
    {
        this.bases = ImmutableList.copyOf(requireNonNull(bases, "bases is null"));
    }

    @Override
    public final Environment.Builder createEnvironment(String name)
    {
        Environment.Builder builder = Environment.builder(name);
        bases.forEach(base -> base.extendEnvironment(builder));
        extendEnvironment(builder);
        return builder;
    }

    protected abstract void extendEnvironment(Environment.Builder builder);
}
