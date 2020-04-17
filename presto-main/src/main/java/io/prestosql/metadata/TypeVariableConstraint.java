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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.TypeSignature;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TypeVariableConstraint
{
    private final String name;
    private final boolean comparableRequired;
    private final boolean orderableRequired;
    private final String variadicBound;
    private final Set<TypeSignature> castableTo;
    private final Set<TypeSignature> castableFrom;

    @JsonCreator
    public TypeVariableConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("comparableRequired") boolean comparableRequired,
            @JsonProperty("orderableRequired") boolean orderableRequired,
            @JsonProperty("variadicBound") @Nullable String variadicBound,
            @JsonProperty("castableTo") Set<TypeSignature> castableTo,
            @JsonProperty("castableFrom") Set<TypeSignature> castableFrom)
    {
        this.name = name;
        this.comparableRequired = comparableRequired;
        this.orderableRequired = orderableRequired;
        this.variadicBound = variadicBound;
        if (variadicBound != null && !variadicBound.equalsIgnoreCase("row")) {
            throw new IllegalArgumentException("variadicBound must be row but is " + variadicBound);
        }
        this.castableTo = ImmutableSet.copyOf(requireNonNull(castableTo, "castableTo is null"));
        this.castableFrom = ImmutableSet.copyOf(requireNonNull(castableFrom, "castableFrom is null"));
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
    public String getVariadicBound()
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
        if (variadicBound != null) {
            value += ":" + variadicBound + "<*>";
        }
        if (!castableTo.isEmpty()) {
            value += ":castableTo(" + Joiner.on(", ").join(castableTo) + ")";
        }
        if (!castableFrom.isEmpty()) {
            value += ":castableFrom(" + Joiner.on(", ").join(castableFrom) + ")";
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
}
