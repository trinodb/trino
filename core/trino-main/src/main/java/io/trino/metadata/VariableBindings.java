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

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;

@Immutable
public final class VariableBindings
{
    private final Map<String, Type> typeVariables;
    private final Map<String, Long> longVariables;

    public VariableBindings(Map<String, Type> typeVariables, Map<String, Long> longVariables)
    {
        // Use CASE_INSENSITIVE_ORDER for consistency with other places. TODO: revisit whether this is even needed
        this.typeVariables = ImmutableSortedMap.copyOf(typeVariables, CASE_INSENSITIVE_ORDER);
        this.longVariables = ImmutableSortedMap.copyOf(longVariables, CASE_INSENSITIVE_ORDER);
    }

    public Type getTypeVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        Type value = typeVariables.get(variableName);
        checkState(value != null, "value for variable '%s' is null", variableName);
        return value;
    }

    public boolean containsTypeVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        return typeVariables.containsKey(variableName);
    }

    public Long getLongVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        Long value = longVariables.get(variableName);
        checkState(value != null, "value for variable '%s' is null", variableName);
        return value;
    }

    public boolean containsLongVariable(String variableName)
    {
        requireNonNull(variableName, "variableName is null");
        return longVariables.containsKey(variableName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof VariableBindings that)) {
            return false;
        }
        return Objects.equals(typeVariables, that.typeVariables) && Objects.equals(longVariables, that.longVariables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeVariables, longVariables);
    }
}
