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

import io.trino.metadata.VariableBindings.Binding;
import io.trino.metadata.VariableBindings.NumericBinding;
import io.trino.metadata.VariableBindings.TypeBinding;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;

class BindingsBuilder
{
    private final Map<String, Binding> bindings = new TreeMap<>(CASE_INSENSITIVE_ORDER);

    public VariableBindings build()
    {
        return new VariableBindings(bindings);
    }

    public Type getTypeVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        checkState(bindings.get(variableName) instanceof TypeBinding, "variable '%s' is not bound to a type", variableName);
        return ((TypeBinding) bindings.get(variableName)).type();
    }

    public BindingsBuilder setTypeVariable(String variableName, Type variableValue)
    {
        set(variableName, new TypeBinding(requireNonNull(variableValue, "variableValue is null")));
        return this;
    }

    public boolean containsTypeVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        return bindings.get(variableName) instanceof TypeBinding;
    }

    public Map<String, Type> getTypeVariables()
    {
        return bindings.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof TypeBinding)
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, Map.Entry::getKey, entry -> ((TypeBinding) entry.getValue()).type()));
    }

    public long getNumericVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        checkState(bindings.get(variableName) instanceof NumericBinding, "variable '%s' is not bound to a numeric value", variableName);
        return ((NumericBinding) bindings.get(variableName)).value();
    }

    public BindingsBuilder setNumericVariable(String variableName, long variableValue)
    {
        set(variableName, new NumericBinding(variableValue));
        return this;
    }

    public boolean containsNumericVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        return bindings.get(variableName) instanceof NumericBinding;
    }

    public Map<String, Long> getNumericVariables()
    {
        return bindings.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof NumericBinding)
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, Map.Entry::getKey, entry -> ((NumericBinding) entry.getValue()).value()));
    }

    private void set(String variableName, Binding binding)
    {
        requireNonNull(variableName, "variableName is null");
        Binding existing = bindings.get(variableName);
        // A solver may refine a binding across iterations, but the kind of a variable never changes
        checkState(existing == null || existing.getClass() == binding.getClass(), "variable '%s' is already bound as a %s", variableName, existing == null ? null : existing.getClass().getSimpleName());
        bindings.put(variableName, binding);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BindingsBuilder that = (BindingsBuilder) o;
        return Objects.equals(bindings, that.bindings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bindings);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bindings", bindings)
                .toString();
    }
}
