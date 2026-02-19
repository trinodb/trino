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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ConnectorIdentifier
{
    private final String value;
    private final boolean delimited;

    @JsonCreator
    public ConnectorIdentifier(String value, boolean delimited)
    {
        this.value = requireNonNull(value, "value is null");
        this.delimited = delimited;
    }

    @JsonProperty
    public String getValue()
    {
        return delimited ? value : canonicalize();
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
        ConnectorIdentifier that = (ConnectorIdentifier) o;
        return Objects.equals(this.getValue(), that.getValue());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, delimited);
    }

    @Override
    public String toString()
    {
        return value;
    }

    private String canonicalize()
    {
        return value.toLowerCase(ENGLISH);
    }
}
