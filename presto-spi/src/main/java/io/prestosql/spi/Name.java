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
package io.prestosql.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class Name
        implements Comparable<Name>
{
    private final String name;
    private final boolean isDelimited;

    public static Name createNonDelimitedName(String name)
    {
        return new Name(name, false);
    }

    public static Name createDelimitedName(String name)
    {
        return new Name(name, true);
    }

    @JsonCreator
    public Name(@JsonProperty("name") String name, @JsonProperty("isDelimited") boolean isDelimited)
    {
        this.name = requireNonNull(name, "Name is null");
        this.isDelimited = isDelimited;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    public String getNormalizedName()
    {
        return isDelimited ? this.name : this.name.toUpperCase(ENGLISH);
    }

    @Deprecated
    public String getLegacyName()
    {
        return name.toLowerCase(ENGLISH);
    }

    @JsonProperty("isDelimited")
    public boolean isDelimited()
    {
        return isDelimited;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.name, this.isDelimited);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Name other = (Name) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.isDelimited, other.isDelimited);
    }

    @Override
    public String toString()
    {
        String delimiter = isDelimited ? "\"" : "";
        return delimiter + name.replace("\"", "\"\"") + delimiter;
    }

    public static boolean equivalentNames(Name leftName, Name rightName)
    {
        String left = leftName.name;
        if (!leftName.isDelimited) {
            left = left.toUpperCase(ENGLISH);
        }

        String right = rightName.name;
        if (!rightName.isDelimited) {
            right = right.toUpperCase(ENGLISH);
        }

        return left.equals(right);
    }

    @Override
    public int compareTo(Name name)
    {
        return this.name.compareTo(name.name);
    }
}
