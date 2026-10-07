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
package io.trino.spi.function;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A scalar implementation that consumes constant SQL arguments while creating
 * its per-expression instance. Consumed arguments are omitted when invoking the
 * implementation method handle.
 */
public record ConstantSpecializedImplementation(
        ScalarFunctionImplementation implementation,
        Set<Integer> consumedArguments)
{
    public ConstantSpecializedImplementation
    {
        requireNonNull(implementation, "implementation is null");
        consumedArguments = Set.copyOf(requireNonNull(consumedArguments, "consumedArguments is null"));
        if (consumedArguments.isEmpty()) {
            throw new IllegalArgumentException("consumedArguments is empty");
        }
        if (implementation.getInstanceFactory().isEmpty()) {
            throw new IllegalArgumentException("constant specialized implementation does not have an instance factory");
        }
    }
}
