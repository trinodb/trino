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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * TODO: This class is a duplicate of SelectedRole class from SPI. Think about better way
 * of implementing it without duplicating code.
 */
public class ClientSelectedRole
{
    public enum Type
    {
        ROLE, ALL, NONE
    }

    private static final Pattern PATTERN = Pattern.compile("(ROLE|ALL|NONE)(\\{(.+?)\\})?");

    private final Type type;
    private final Optional<String> role;

    @JsonCreator
    public ClientSelectedRole(@JsonProperty("type") Type type, @JsonProperty("role") Optional<String> role)
    {
        this.type = requireNonNull(type, "type is null");
        this.role = requireNonNull(role, "role is null");
        if (type == Type.ROLE && !role.isPresent()) {
            throw new IllegalArgumentException("Role must be present for the selected role type: " + type);
        }
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getRole()
    {
        return role;
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
        ClientSelectedRole that = (ClientSelectedRole) o;
        return type == that.type &&
                Objects.equals(role, that.role);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, role);
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        result.append(type);
        role.ifPresent(s -> result.append("{").append(s).append("}"));
        return result.toString();
    }

    public static ClientSelectedRole valueOf(String value)
    {
        Matcher m = PATTERN.matcher(value);
        if (m.matches()) {
            Type type = Type.valueOf(m.group(1));
            Optional<String> role = Optional.ofNullable(m.group(3));
            return new ClientSelectedRole(type, role);
        }
        throw new IllegalArgumentException("Could not parse selected role: " + value);
    }
}
