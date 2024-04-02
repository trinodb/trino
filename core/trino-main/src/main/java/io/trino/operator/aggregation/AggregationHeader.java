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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.function.WindowAccumulator;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record AggregationHeader(
        String name,
        Set<String> aliases,
        Optional<String> description,
        boolean decomposable,
        boolean orderSensitive,
        boolean hidden,
        boolean deprecated,
        Optional<Class<? extends WindowAccumulator>> windowAccumulator)
{
    public AggregationHeader
    {
        requireNonNull(name, "name cannot be null");
        aliases = ImmutableSet.copyOf(aliases);
        requireNonNull(description, "description cannot be null");
        requireNonNull(windowAccumulator, "windowAccumulator is null");
    }
}
