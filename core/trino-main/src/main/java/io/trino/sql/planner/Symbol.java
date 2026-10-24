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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@JsonSerialize(keyUsing = SymbolKeySerializer.class)
public final class Symbol
{
    private final Type type;
    private final String name;
    // Symbols are pervasive map and set keys, so the hash is computed once and kept
    private final int hash;

    public static Symbol from(Expression expression)
    {
        if (!(expression instanceof Reference reference)) {
            throw new IllegalArgumentException("Unexpected expression: " + expression);
        }
        return from(reference);
    }

    public static Symbol from(Reference reference)
    {
        return new Symbol(reference.type(), reference.name());
    }

    @JsonCreator
    public Symbol(@JsonProperty("type") Type type, @JsonProperty("name") String name)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        this.type = requireNonNull(type, "type is null");
        this.hash = 31 * type.hashCode() + name.hashCode();
    }

    @JsonProperty("type")
    public Type type()
    {
        return type;
    }

    @JsonProperty("name")
    public String name()
    {
        return name;
    }

    public Reference toSymbolReference()
    {
        return new Reference(type, name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Symbol other)) {
            return false;
        }
        return hash == other.hash && name.equals(other.name) && type.equals(other.type);
    }

    @Override
    public int hashCode()
    {
        return hash;
    }

    @Override
    public String toString()
    {
        return name + "::[" + type + "]";
    }
}
