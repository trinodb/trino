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
package io.prestosql.pinot.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class OrderByExpression
{
    private final String column;
    private final boolean asc;

    @JsonCreator
    public OrderByExpression(
            @JsonProperty("column") String column,
            @JsonProperty("asc") boolean asc)
    {
        this.column = requireNonNull(column, "column is null");
        this.asc = asc;
    }

    @JsonProperty
    public String getColumn()
    {
        return column;
    }

    @JsonProperty
    public boolean isAsc()
    {
        return asc;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OrderByExpression)) {
            return false;
        }
        OrderByExpression that = (OrderByExpression) other;
        return column.equals(that.column) &&
                asc == that.asc;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, asc);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("asc", asc)
                .toString();
    }
}
