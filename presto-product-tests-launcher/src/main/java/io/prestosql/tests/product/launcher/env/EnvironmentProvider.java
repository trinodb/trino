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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.env.common.EnvironmentExtender;

import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public abstract class EnvironmentProvider
        implements EnvironmentExtender
{
    private static final Logger log = Logger.get(EnvironmentExtender.class);
    private final List<EnvironmentExtender> bases;

    protected EnvironmentProvider(List<EnvironmentExtender> bases)
    {
        this.bases = requireNonNull(bases, "bases is null");
    }

    public final Environment.Builder createEnvironment(String name, EnvironmentConfig environmentConfig)
    {
        requireNonNull(environmentConfig, "environmentConfig is null");
        Environment.Builder builder = Environment.builder(name);

        // Environment is created by applying bases, environment definition and environment config to builder
        ImmutableList<EnvironmentExtender> extenders = ImmutableList.<EnvironmentExtender>builder()
                .addAll(bases)
                .add(this)
                .add(environmentConfig)
                .build();

        compose(extenders).extendEnvironment(builder);
        return builder;
    }

    static EnvironmentExtender compose(Iterable<EnvironmentExtender> extenders)
    {
        String extendersNames = StreamSupport.stream(extenders.spliterator(), false)
                .map(Object::getClass)
                .map(Class::getSimpleName)
                .collect(joining(", "));

        return builder -> {
            log.info("Building environment %s with extenders: %s", builder.getEnvironmentName(), extendersNames);
            extenders.forEach(extender -> extender.extendEnvironment(builder));
        };
    }
}
