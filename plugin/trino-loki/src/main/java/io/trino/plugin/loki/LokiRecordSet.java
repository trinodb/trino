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
package io.trino.plugin.loki;

import com.google.common.collect.ImmutableList;
import io.github.jeschkies.loki.client.LokiClient;
import io.github.jeschkies.loki.client.LokiClientException;
import io.github.jeschkies.loki.client.model.Matrix;
import io.github.jeschkies.loki.client.model.QueryResult;
import io.github.jeschkies.loki.client.model.Streams;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.loki.LokiErrorCode.LOKI_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class LokiRecordSet
        implements RecordSet
{
    private final List<LokiColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final QueryResult result;

    public LokiRecordSet(LokiClient lokiClient, LokiSplit split, List<LokiColumnHandle> columnHandles)
    {
        requireNonNull(lokiClient, "lokiClient is null");
        requireNonNull(split, "split is null");
        requireNonNull(columnHandles, "columnHandles is null");

        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.columnTypes = columnHandles.stream().map(LokiColumnHandle::type).collect(toImmutableList());

        // Execute the query
        try {
            this.result = lokiClient.rangeQuery(split.query(), split.start(), split.end(), split.step());
        }
        catch (LokiClientException e) {
            throw new TrinoException(LOKI_CLIENT_ERROR, e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return switch (result.getData().getResult()) {
            case Streams streams -> new LokiRecordCursor(columnHandles, new LokiStreamEntriesIterator(streams));
            case Matrix matrix -> new LokiRecordCursor(columnHandles, new LokiMatrixValuePairsIterator(matrix));
        };
    }
}
