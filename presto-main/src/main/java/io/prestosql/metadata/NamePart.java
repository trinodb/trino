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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class NamePart
{
    private final String value;
    private final boolean delimited;

    public static NamePart createDelimitedNamePart(String delimitedValue)
    {
        return new NamePart(delimitedValue, true);
    }

    public static NamePart createDefaultNamePart(String delimitedValue)
    {
        return new NamePart(delimitedValue.toLowerCase(ENGLISH), false);
    }

    @JsonCreator
    public NamePart(@JsonProperty("value") String value, @JsonProperty("delimited") boolean delimited)
    {
        checkArgument(!value.isEmpty(), "value is empty");
        this.value = requireNonNull(value, "value is null");
        this.delimited = delimited;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @Deprecated
    public String getLegacyName()
    {
        return value.toLowerCase(ENGLISH);
    }

    @JsonProperty
    public boolean isDelimited()
    {
        return delimited;
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

        NamePart that = (NamePart) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
