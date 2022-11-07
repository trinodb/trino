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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Experimental;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

@Experimental(eta = "2022-10-31")
public class BoundSignature
{
    private final String name;
    private final Type returnType;
    private final List<Type> argumentTypes;

    @JsonCreator
    public BoundSignature(
            @JsonProperty("name") String name,
            @JsonProperty("returnType") Type returnType,
            @JsonProperty("argumentTypes") List<Type> argumentTypes)
    {
        this.name = requireNonNull(name, "name is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = List.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getReturnType()
    {
        return returnType;
    }

    public int getArity()
    {
        return argumentTypes.size();
    }

    public Type getArgumentType(int index)
    {
        return argumentTypes.get(index);
    }

    @JsonProperty
    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    public Signature toSignature()
    {
        return Signature.builder()
                .name(name)
                .returnType(returnType)
                .argumentTypes(argumentTypes.stream()
                        .map(Type::getTypeSignature)
                        .collect(toUnmodifiableList()))
                .build();
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
        BoundSignature that = (BoundSignature) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(argumentTypes, that.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, returnType, argumentTypes);
    }

    @Override
    public String toString()
    {
        return name +
                argumentTypes.stream()
                        .map(Type::toString)
                        .collect(joining(", ", "(", "):")) +
                returnType;
    }
}
