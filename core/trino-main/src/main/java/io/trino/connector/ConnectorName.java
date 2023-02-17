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
package io.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnectorName
{
    private static final Pattern VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*");

    private final String name;

    @JsonCreator
    public ConnectorName(String name)
    {
        requireNonNull(name, "name is null");
        checkArgument(VALID_NAME.matcher(name).matches(), "Invalid connector name: %s", name);
        this.name = requireNonNull(name, "name is null");
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectorName that = (ConnectorName) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
