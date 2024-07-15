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

import java.util.Objects;

import static java.lang.String.format;

public class JdbcQueryRelationHandle
        extends JdbcRelationHandle
{
    private final PreparedQuery preparedQuery;

    @JsonCreator
    public JdbcQueryRelationHandle(PreparedQuery preparedQuery)
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
        return format("Query[%s]", preparedQuery.query());
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
        JdbcQueryRelationHandle that = (JdbcQueryRelationHandle) o;
        return preparedQuery.equals(that.preparedQuery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(preparedQuery);
    }
}
