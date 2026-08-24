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
package io.trino.spi.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TrinoPrincipal
{
    private final PrincipalType type;
    private final String name;

    @JsonCreator
    public TrinoPrincipal(@JsonProperty("type") PrincipalType type, @JsonProperty("name") String name)
    {
        this.type = requireNonNull(type, "type is null");
        requireNonNull(name, "name is null");
        this.name = type == PrincipalType.USER ? name : name.toLowerCase(ENGLISH);
    }

    @JsonProperty
    public PrincipalType getType()
    {
        return type;
    }

    @JsonProperty("name")
    public String getPrincipalName()
    {
        return name;
    }

    /**
     * @deprecated Use {@link #getPrincipalName()} which preserves the original case of the principal name.
     *         This method lowercases the name, which causes identity mismatches when the principal
     *         was created with a mixed-case name.
     */
    @Deprecated
    @JsonIgnore
    public String getName()
    {
        return name.toLowerCase(ENGLISH);
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
        TrinoPrincipal trinoPrincipal = (TrinoPrincipal) o;
        return type == trinoPrincipal.type &&
                Objects.equals(name, trinoPrincipal.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
    }

    @Override
    public String toString()
    {
        return type + " " + name;
    }
}
