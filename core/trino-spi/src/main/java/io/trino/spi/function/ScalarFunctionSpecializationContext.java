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

import io.trino.spi.expression.Constant;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Describes constants available while compiling a scalar function call site.
 * The constant argument list is aligned with the function's SQL arguments. An
 * empty entry denotes a runtime argument, while a present {@link Constant}
 * denotes a compile-time constant, including a constant whose value is null.
 */
public record ScalarFunctionSpecializationContext(
        InvocationConvention invocationConvention,
        List<Optional<Constant>> constantArguments)
{
    public ScalarFunctionSpecializationContext
    {
        requireNonNull(invocationConvention, "invocationConvention is null");
        constantArguments = List.copyOf(requireNonNull(constantArguments, "constantArguments is null"));
        if (invocationConvention.getArgumentConventions().size() != constantArguments.size()) {
            throw new IllegalArgumentException("Expected %s constant arguments, but got %s".formatted(
                    invocationConvention.getArgumentConventions().size(),
                    constantArguments.size()));
        }
    }
}
