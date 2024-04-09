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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.regex.Pattern;

public final class ConnectorName
{
    private static final Pattern VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*");

    private final String name;

    @JsonCreator
    public ConnectorName(String name)
    {
        if (!VALID_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid connector name: " + name);
        }
        this.name = name;
    }

    @JsonValue
    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof ConnectorName other &&
                name.equals(other.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
