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
package io.trino.metadata;

import com.google.common.collect.ImmutableSortedMap;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;

import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;

/// The values bound to a signature's declared variables by a successful bind. A name resolves to
/// exactly one [Binding], whose kind is structural — a variable cannot be bound as both a type and
/// a numeric value.
@Immutable
public final class VariableBindings
{
    /// The value bound to one signature variable: a [Type] for a type variable, a numeric value for
    /// a numeric variable.
    public sealed interface Binding
            permits TypeBinding, NumericBinding {}

    public record TypeBinding(Type type)
            implements Binding
    {
        public TypeBinding
        {
            requireNonNull(type, "type is null");
        }
    }

    public record NumericBinding(long value)
            implements Binding {}

    private final Map<String, Binding> bindings;

    public VariableBindings(Map<String, Binding> bindings)
    {
        // Use CASE_INSENSITIVE_ORDER for consistency with other places. TODO: revisit whether this is even needed
        this.bindings = ImmutableSortedMap.copyOf(bindings, CASE_INSENSITIVE_ORDER);
    }

    public Map<String, Type> getTypeVariables()
    {
        return bindings.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof TypeBinding)
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, Map.Entry::getKey, entry -> ((TypeBinding) entry.getValue()).type()));
    }

    /// The type variables' bindings in their ground descriptor form — the shape
    /// [io.trino.spi.type.TypeTemplates#bind] consumes.
    public Map<String, TypeDescriptor> getTypeDescriptors()
    {
        return bindings.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof TypeBinding)
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, Map.Entry::getKey, entry -> ((TypeBinding) entry.getValue()).type().getTypeDescriptor()));
    }

    public Map<String, Long> getNumericVariables()
    {
        return bindings.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof NumericBinding)
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, Map.Entry::getKey, entry -> ((NumericBinding) entry.getValue()).value()));
    }

    public Type getTypeVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        if (!(bindings.get(variableName) instanceof TypeBinding(Type type))) {
            throw new IllegalStateException("variable '%s' is not bound to a type".formatted(variableName));
        }
        return type;
    }

    public boolean containsTypeVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        return bindings.get(variableName) instanceof TypeBinding;
    }

    public long getNumericVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        if (!(bindings.get(variableName) instanceof NumericBinding(long value))) {
            throw new IllegalStateException("variable '%s' is not bound to a numeric value".formatted(variableName));
        }
        return value;
    }

    public boolean containsNumericVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        return bindings.get(variableName) instanceof NumericBinding;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof VariableBindings that)) {
            return false;
        }
        return Objects.equals(bindings, that.bindings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bindings);
    }
}
