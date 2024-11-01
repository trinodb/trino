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
package io.trino.plugin.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryQueryRelationHandle
        extends BigQueryRelationHandle
{
    private final String query;
    private final RemoteTableName destinationTableName;
    private final boolean useStorageApi;

    @JsonCreator
    public BigQueryQueryRelationHandle(String query, RemoteTableName destinationTableName, boolean useStorageApi)
    {
        this.query = query;
        this.destinationTableName = requireNonNull(destinationTableName, "destinationTableName is null");
        this.useStorageApi = useStorageApi;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public RemoteTableName getDestinationTableName()
    {
        return destinationTableName;
    }

    @JsonProperty
    @Override
    public boolean isUseStorageApi()
    {
        return useStorageApi;
    }

    @Override
    public String toString()
    {
        return format("Query[%s], Destination table[%s], Api[%s]", query, destinationTableName, useStorageApi ? "Storage" : "Rest");
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
        BigQueryQueryRelationHandle that = (BigQueryQueryRelationHandle) o;
        return query.equals(that.query)
                && destinationTableName.equals(that.destinationTableName)
                && useStorageApi == that.useStorageApi;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, destinationTableName, useStorageApi);
    }
}
