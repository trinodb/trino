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
package io.trino.loki;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LokiRecordSet implements RecordSet {

    private final List<LokiColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final QueryResult result;

    public LokiRecordSet(LokiClient lokiClient, LokiSplit split, List<LokiColumnHandle> columnHandles)
    {
        requireNonNull(lokiClient, "lokiClient is null");
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LokiColumnHandle column : columnHandles) {
            types.add(column.columnType());
        }
        this.columnTypes = types.build();

        // TODO: get query and start from table
        final String query = "{source=\"stderr\"}";
        final Long start = 1722852098L - (15 * 60 * 1000); // Start and end should be part of split
        final Long end = 1722852098L;
        // Actually execute the query
        // TODO: lazily parse
        this.result = lokiClient.doQuery(query, start, end); // TODO: pass split
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new LokiRecordCursor(columnHandles, result);
    }
}
