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

public final class PinotQuery
{
    private final String table;
    private final String query;
    private final int groupByClauses;

    @JsonCreator
    public PinotQuery(
            @JsonProperty("table") String table,
            @JsonProperty("query") String query,
            @JsonProperty("groupByClauses") int groupByClauses)
    {
        this.table = table;
        this.query = query;
        this.groupByClauses = groupByClauses;
    }

    @JsonProperty("query")
    public String getQuery()
    {
        return query;
    }

    @JsonProperty("groupByClauses")
    public int getGroupByClauses()
    {
        return groupByClauses;
    }

    @JsonProperty("table")
    public String getTable()
    {
        return table;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PinotQuery)) {
            return false;
        }
        PinotQuery that = (PinotQuery) other;
        return table.equals(that.table) &&
                query.equals(that.query) &&
                groupByClauses == that.groupByClauses;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, query, groupByClauses);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .add("table", table)
                .add("groupByClauses", groupByClauses)
                .toString();
    }
}
