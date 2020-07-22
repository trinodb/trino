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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableSortedMap;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Objects;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;

public class FunctionBinding
{
    private final FunctionId functionId;
    private final Signature boundSignature;
    private final Map<String, Type> typeVariables;
    private final Map<String, Long> longVariables;

    public FunctionBinding(FunctionId functionId, Signature boundSignature, Map<String, Type> typeVariables, Map<String, Long> longVariables)
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

    public Signature getBoundSignature()
    {
        return boundSignature;
    }

    public Map<String, Type> getTypeVariables()
    {
        return typeVariables;
    }

    public Map<String, Long> getLongVariables()
    {
        return longVariables;
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
