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
package io.trino.jdbc;

import jakarta.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

// A public facade for RowField from trino-client
public final class RowField
{
    private final io.trino.client.RowField rowField;

    RowField(io.trino.client.RowField rowField)
    {
        this.rowField = requireNonNull(rowField, "rowField is null");
    }

    public int getOrdinal()
    {
        return rowField.getOrdinal();
    }

    public Optional<String> getName()
    {
        return rowField.getName();
    }

    @Nullable
    public Object getValue()
    {
        return rowField.getValue();
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
        RowField other = (RowField) o;
        return Objects.equals(rowField, other.rowField);
    }

    @Override
    public int hashCode()
    {
        return rowField.hashCode();
    }

    @Override
    public String toString()
    {
        return rowField.toString();
    }
}
