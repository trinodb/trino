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
package io.trino.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.lang.String.format;

public class JdbcPassThroughQueryHandle
        extends JdbcRelationHandle
{
    private final PreparedQuery preparedQuery;

    @JsonCreator
    public JdbcPassThroughQueryHandle(PreparedQuery preparedQuery)
    {
        this.preparedQuery = preparedQuery;
    }

    @JsonProperty
    public PreparedQuery getPreparedQuery()
    {
        return preparedQuery;
    }

    @Override
    public String toString()
    {
        return format("PassThroughQuery[%s]", preparedQuery.getQuery());
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
        JdbcPassThroughQueryHandle that = (JdbcPassThroughQueryHandle) o;
        return preparedQuery.equals(that.preparedQuery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(preparedQuery);
    }

    public static JdbcTableHandle tableHandleForPassThroughQuery(PreparedQuery preparedQuery, List<JdbcColumnHandle> columns)
    {
        return new JdbcTableHandle(
                new JdbcPassThroughQueryHandle(preparedQuery),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(columns),
                // The query is opaque, so we don't know referenced tables
                Optional.empty(),
                0);
    }
}
