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
package io.trino.spi.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class ExchangeId
{
    private static final Pattern ID_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private final String id;

    public static ExchangeId createRandomExchangeId()
    {
        return new ExchangeId(randomUUID().toString());
    }

    @JsonCreator
    public ExchangeId(String id)
    {
        requireNonNull(id, "id is null");
        if (!ID_PATTERN.matcher(id).matches()) {
            throw new IllegalArgumentException("Invalid id: " + id);
        }
        this.id = id;
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
        ExchangeId that = (ExchangeId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return id;
    }
}
