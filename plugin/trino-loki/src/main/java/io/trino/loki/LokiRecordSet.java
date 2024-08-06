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
import io.trino.loki.model.Matrix;
import io.trino.loki.model.QueryResult;
import io.trino.loki.model.Streams;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

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

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LokiColumnHandle column : columnHandles) {
            types.add(column.type());
        }
        this.columnTypes = types.build();

        Long end = now();
        Long start = end - ONE_HOUR;
        if (split.end().getEpochSecond() != 0) {
            end = nanosFromInstant(split.end());
        }
        if (split.start().getEpochSecond() != 0) {
            start = nanosFromInstant(split.start());
        }

        final String query = split.query();
        // Actually execute the query
        // TODO: lazily parse
        this.result = lokiClient.rangeQuery(query, start, end);
    }

    static long ONE_HOUR = Duration.ofHours(1).toNanos();

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return switch (result.getData().getResult()) {
            case Streams s -> new LokiStreamsRecordCursor(columnHandles, s);
            case Matrix m -> new LokiMatrixRecordCursor(columnHandles, m);
        };
    }

    private Long now()
    {
        // This precision is fine for us.
        var now = Instant.now();
        return nanosFromInstant(now);
    }

    private Long nanosFromInstant(Instant i)
    {
        return i.getEpochSecond() * 1000000000L + i.getNano(); // as nanoseconds
    }
}
