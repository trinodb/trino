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
package io.trino.client;

import jakarta.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RowField
{
    private final int ordinal;
    private final Optional<String> name;
    @Nullable
    private final Object value;

    RowField(int ordinal, Optional<String> name, @Nullable Object value)
    {
        this.ordinal = ordinal;
        this.name = requireNonNull(name, "name is null");
        this.value = value;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public Optional<String> getName()
    {
        return name;
    }

    @Nullable
    public Object getValue()
    {
        return value;
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
        RowField rowField = (RowField) o;
        return ordinal == rowField.ordinal &&
                Objects.equals(name, rowField.name) &&
                Objects.equals(value, rowField.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ordinal, name, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("ordinal", ordinal)
                .add("name", name)
                .add("value", value)
                .toString();
    }
}
