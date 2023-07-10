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
import io.trino.spi.type.TypeSignature;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Experimental(eta = "2022-10-31")
public class TypeVariableConstraint
{
    private final String name;
    private final boolean comparableRequired;
    private final boolean orderableRequired;
    private final Optional<String> variadicBound;
    private final Set<TypeSignature> castableTo;
    private final Set<TypeSignature> castableFrom;

    private TypeVariableConstraint(
            String name,
            boolean comparableRequired,
            boolean orderableRequired,
            Optional<String> variadicBound,
            Set<TypeSignature> castableTo,
            Set<TypeSignature> castableFrom)
    {
        this.name = requireNonNull(name, "name is null");
        this.comparableRequired = comparableRequired;
        this.orderableRequired = orderableRequired;
        this.variadicBound = requireNonNull(variadicBound, "variadicBound is null");
        if (variadicBound.map(bound -> !bound.equalsIgnoreCase("row")).orElse(false)) {
            throw new IllegalArgumentException("variadicBound must be row but is " + variadicBound.get());
        }
        this.castableTo = Set.copyOf(requireNonNull(castableTo, "castableTo is null"));
        this.castableFrom = Set.copyOf(requireNonNull(castableFrom, "castableFrom is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isComparableRequired()
    {
        return comparableRequired;
    }

    @JsonProperty
    public boolean isOrderableRequired()
    {
        return orderableRequired;
    }

    @JsonProperty
    public Optional<String> getVariadicBound()
    {
        return variadicBound;
    }

    @JsonProperty
    public Set<TypeSignature> getCastableTo()
    {
        return castableTo;
    }

    @JsonProperty
    public Set<TypeSignature> getCastableFrom()
    {
        return castableFrom;
    }

    @Override
    public String toString()
    {
        String value = name;
        if (comparableRequired) {
            value += ":comparable";
        }
        if (orderableRequired) {
            value += ":orderable";
        }
        if (variadicBound.isPresent()) {
            value += ":" + variadicBound + "<*>";
        }
        if (!castableTo.isEmpty()) {
            value += castableTo.stream().map(Object::toString).collect(joining(", ", ":castableTo(", ")"));
        }
        if (!castableFrom.isEmpty()) {
            value += castableFrom.stream().map(Object::toString).collect(joining(", ", ":castableFrom(", ")"));
        }
        return value;
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
        TypeVariableConstraint that = (TypeVariableConstraint) o;
        return comparableRequired == that.comparableRequired &&
                orderableRequired == that.orderableRequired &&
                Objects.equals(name, that.name) &&
                Objects.equals(variadicBound, that.variadicBound) &&
                Objects.equals(castableTo, that.castableTo) &&
                Objects.equals(castableFrom, that.castableFrom);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, comparableRequired, orderableRequired, variadicBound, castableTo, castableFrom);
    }

    public static TypeVariableConstraint typeVariable(String name)
    {
        return builder(name).build();
    }

    public static TypeVariableConstraintBuilder builder(String name)
    {
        return new TypeVariableConstraintBuilder(name);
    }

    public static class TypeVariableConstraintBuilder
    {
        private final String name;
        private boolean comparableRequired;
        private boolean orderableRequired;
        private String variadicBound;
        private final Set<TypeSignature> castableTo = new HashSet<>();
        private final Set<TypeSignature> castableFrom = new HashSet<>();

        private TypeVariableConstraintBuilder(String name)
        {
            this.name = name;
        }

        public TypeVariableConstraintBuilder comparableRequired()
        {
            this.comparableRequired = true;
            return this;
        }

        public TypeVariableConstraintBuilder orderableRequired()
        {
            this.orderableRequired = true;
            return this;
        }

        public TypeVariableConstraintBuilder variadicBound(String variadicBound)
        {
            this.variadicBound = variadicBound;
            return this;
        }

        public TypeVariableConstraintBuilder castableTo(Type type)
        {
            return castableTo(type.getTypeSignature());
        }

        public TypeVariableConstraintBuilder castableTo(TypeSignature type)
        {
            this.castableTo.add(type);
            return this;
        }

        public TypeVariableConstraintBuilder castableFrom(Type type)
        {
            return castableFrom(type.getTypeSignature());
        }

        public TypeVariableConstraintBuilder castableFrom(TypeSignature type)
        {
            this.castableFrom.add(type);
            return this;
        }

        public TypeVariableConstraint build()
        {
            return new TypeVariableConstraint(name, comparableRequired, orderableRequired, Optional.ofNullable(variadicBound), castableTo, castableFrom);
        }
    }

    /**
     * This method is only visible for JSON deserialization.
     *
     * @deprecated use builder
     */
    @Deprecated
    @JsonCreator
    public static TypeVariableConstraint fromJson(
            @JsonProperty("name") String name,
            @JsonProperty("comparableRequired") boolean comparableRequired,
            @JsonProperty("orderableRequired") boolean orderableRequired,
            @JsonProperty("variadicBound") Optional<String> variadicBound,
            @JsonProperty("castableTo") Set<TypeSignature> castableTo,
            @JsonProperty("castableFrom") Set<TypeSignature> castableFrom)
    {
        return new TypeVariableConstraint(name, comparableRequired, orderableRequired, variadicBound, castableTo, castableFrom);
    }
}
