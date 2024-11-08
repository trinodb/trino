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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.bigquery.BigQuerySplit.Mode.QUERY;
import static io.trino.plugin.bigquery.BigQuerySplit.Mode.STORAGE;
import static java.util.Objects.requireNonNull;

public record BigQuerySplit(
        Mode mode,
        String streamName,
        String schemaString,
        List<BigQueryColumnHandle> columns,
        long emptyRowsToGenerate,
        Optional<String> filter,
        OptionalInt dataSize)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(BigQuerySplit.class);

    private static final int NO_ROWS_TO_GENERATE = -1;

    // do not use directly, it is public only for Jackson
    public BigQuerySplit
    {
        requireNonNull(mode, "mode is null");
        requireNonNull(streamName, "streamName cannot be null");
        requireNonNull(schemaString, "schemaString cannot be null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns cannot be null"));
        requireNonNull(filter, "filter is null");
        requireNonNull(dataSize, "dataSize is null");
    }

    static BigQuerySplit forStream(String streamName, String schemaString, List<BigQueryColumnHandle> columns, OptionalInt dataSize)
    {
        return new BigQuerySplit(STORAGE, streamName, schemaString, columns, NO_ROWS_TO_GENERATE, Optional.empty(), dataSize);
    }

    static BigQuerySplit forViewStream(List<BigQueryColumnHandle> columns, Optional<String> filter)
    {
        return new BigQuerySplit(QUERY, "", "", columns, NO_ROWS_TO_GENERATE, filter, OptionalInt.empty());
    }

    static BigQuerySplit emptyProjection(long numberOfRows)
    {
        return new BigQuerySplit(STORAGE, "", "", ImmutableList.of(), numberOfRows, Optional.empty(), OptionalInt.of(0));
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of(
                "mode", mode.name(),
                "filter", filter.orElse(""),
                "streamName", streamName,
                "emptyRowsToGenerate", String.valueOf(emptyRowsToGenerate));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(streamName)
                + estimatedSizeOf(schemaString)
                + estimatedSizeOf(columns, BigQueryColumnHandle::getRetainedSizeInBytes);
    }

    boolean representsEmptyProjection()
    {
        return emptyRowsToGenerate != NO_ROWS_TO_GENERATE;
    }

    public enum Mode
    {
        STORAGE,
        QUERY,
        /**/;
    }
}
