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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record CanonicalAggregation(
        ResolvedFunction resolvedFunction,
        Optional<Symbol> mask,
        List<Expression> arguments)
{
    public CanonicalAggregation
    {
        requireNonNull(resolvedFunction, "resolvedFunction is null");
        requireNonNull(mask, "mask is null");
        arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }
}
