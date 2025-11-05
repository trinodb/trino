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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.VarcharType;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

public class QueryRangeTableFunction
        extends AbstractConnectorTableFunction
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query_range";

    private final LokiMetadata metadata;

    public QueryRangeTableFunction(LokiMetadata metadata)
    {
        super(
                SCHEMA_NAME,
                NAME,
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name("QUERY")
                                .type(VarcharType.VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("START")
                                .type(TIMESTAMP_TZ_NANOS)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("END")
                                .type(TIMESTAMP_TZ_NANOS)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("STEP")
                                .type(INTEGER)
                                .defaultValue(0L)
                                .build()),
                GENERIC_TABLE);

        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        ScalarArgument argument = (ScalarArgument) arguments.get("QUERY");
        String query = ((Slice) argument.getValue()).toStringUtf8();

        LongTimestampWithTimeZone startArgument = (LongTimestampWithTimeZone) ((ScalarArgument) arguments.get("START")).getValue();
        LongTimestampWithTimeZone endArgument = (LongTimestampWithTimeZone) ((ScalarArgument) arguments.get("END")).getValue();

        Long step = (Long) ((ScalarArgument) arguments.get("STEP")).getValue();
        if (step == null || step < 0L) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "step must be positive");
        }

        if (Strings.isNullOrEmpty(query)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, query);
        }
        requireNonNull(startArgument, "startArgument is null");
        requireNonNull(endArgument, "endArgument is null");

        // determine the returned row type
        List<ColumnHandle> columnHandles;
        try {
            columnHandles = metadata.getColumnHandles(query);
        }
        catch (UndeclaredThrowableException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot get column definition", Throwables.getRootCause(e));
        }

        checkArgument(!columnHandles.isEmpty(), "Cannot get column definition");

        Descriptor returnedType = new Descriptor(columnHandles.stream()
                .map(LokiColumnHandle.class::cast)
                .map(column -> new Descriptor.Field(column.name(), Optional.of(column.type())))
                .collect(toImmutableList()));

        Instant end = toInstant(endArgument);
        Instant start = toInstant(startArgument);

        final LokiTableHandle tableHandle = new LokiTableHandle(
                query,
                start,
                end,
                step.intValue(),
                columnHandles);

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(new QueryHandle(tableHandle))
                .build();
    }

    /**
     * Convert LongTimestampWithTimeZone to Instant. See BigQuery code for an example
     */
    private static Instant toInstant(LongTimestampWithTimeZone timestamp)
    {
        long epochSeconds = floorDiv(timestamp.getEpochMillis(), MILLISECONDS_PER_SECOND);
        long nanosOfSecond = (long) floorMod(timestamp.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
        return Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
    }

    public record QueryHandle(LokiTableHandle tableHandle)
            implements ConnectorTableFunctionHandle
    {
        public QueryHandle
        {
            requireNonNull(tableHandle, "tableHandle is null");
        }
    }
}
