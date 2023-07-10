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
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;

public class FunctionBinding
{
    private final FunctionId functionId;
    private final BoundSignature boundSignature;
    private final Map<String, Type> typeVariables;
    private final Map<String, Long> longVariables;

    public FunctionBinding(FunctionId functionId, BoundSignature boundSignature, Map<String, Type> typeVariables, Map<String, Long> longVariables)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
        this.typeVariables = ImmutableSortedMap.copyOf(requireNonNull(typeVariables, "typeVariables is null"), CASE_INSENSITIVE_ORDER);
        this.longVariables = ImmutableSortedMap.copyOf(requireNonNull(longVariables, "longVariables is null"), CASE_INSENSITIVE_ORDER);
    }

    public FunctionId getFunctionId()
    {
        return functionId;
    }

    public BoundSignature getBoundSignature()
    {
        return boundSignature;
    }

    public int getArity()
    {
        return boundSignature.getArgumentTypes().size();
    }

    public Type getTypeVariable(String variableName)
    {
        return getValue(typeVariables, variableName);
    }

    public boolean containsTypeVariable(String variableName)
    {
        return containsValue(typeVariables, variableName);
    }

    public Long getLongVariable(String variableName)
    {
        return getValue(longVariables, variableName);
    }

    public boolean containsLongVariable(String variableName)
    {
        return containsValue(longVariables, variableName);
    }

    private static <T> T getValue(Map<String, T> map, String variableName)
    {
        checkState(variableName != null, "variableName is null");
        T value = map.get(variableName);
        checkState(value != null, "value for variable '%s' is null", variableName);
        return value;
    }

    private static boolean containsValue(Map<String, ?> map, String variableName)
    {
        checkState(variableName != null, "variableName is null");
        return map.containsKey(variableName);
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
        FunctionBinding that = (FunctionBinding) o;
        return Objects.equals(functionId, that.functionId) &&
                Objects.equals(boundSignature, that.boundSignature) &&
                Objects.equals(typeVariables, that.typeVariables) &&
                Objects.equals(longVariables, that.longVariables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionId, boundSignature, typeVariables, longVariables);
    }
}
