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
package io.prestosql.spi.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents an opaque identifier for a Type than can be used
 * for serialization or storage in external systems.
 */
public class TypeId
{
    private final String id;

    private TypeId(String id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @JsonCreator
    public static TypeId of(String id)
    {
        return new TypeId(id);
    }

    @JsonValue
    public String getId()
    {
        return id;
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
        TypeId typeId = (TypeId) o;
        return id.equals(typeId.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return "type:[" + getId() + "]";
    }
}
