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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static io.trino.client.FixJsonDataUtils.fixData;
import static java.util.Objects.requireNonNull;

@QueryResultsFormat(formatName = "json")
public class JsonQueryResults
        extends QueryResults
{
    private final Iterable<List<Object>> data;

    @JsonCreator
    public JsonQueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error,
            @JsonProperty("warnings") List<Warning> warnings,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount,
            @Nullable @JsonProperty("data") List<List<Object>> data)
    {
        super(id, infoUri, partialCancelUri, nextUri, columns, stats, error, warnings, updateType, updateCount);
        this.data = data == null ? null : fixData(columns, data);
    }

    public JsonQueryResults(QueryResults base, Iterable<List<Object>> data)
    {
        super(base);
        this.data = unmodifiableIterable(requireNonNull(data, "data is null"));
        checkArgument(getColumns() != null, "data present without columns");
    }

    @Nullable
    @JsonProperty
    @Override
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @Override
    public boolean hasData()
    {
        return true;
    }
}
