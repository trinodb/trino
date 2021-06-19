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

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class PreparedQuery
{
    private final String query;
    private final List<QueryParameter> parameters;

    @JsonCreator
    public PreparedQuery(String query, List<QueryParameter> parameters)
    {
        this.query = requireNonNull(query, "query is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public List<QueryParameter> getParameters()
    {
        return parameters;
    }

    public PreparedQuery transformQuery(Function<String, String> sqlFunction)
    {
        return new PreparedQuery(
                sqlFunction.apply(query),
                parameters);
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
        PreparedQuery that = (PreparedQuery) o;
        return query.equals(that.query)
                && parameters.equals(that.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, parameters);
    }
}
