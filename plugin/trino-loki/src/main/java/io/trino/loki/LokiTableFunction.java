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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.*;
import io.trino.spi.type.*;

import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LokiTableFunction
        extends AbstractConnectorTableFunction
{
    private Type varcharMapType;
    private LokiMetadata metadata;

    public LokiTableFunction(LokiMetadata metadata)
    {
        super(
                "default",
                "loki",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name("START")
                                .type(TIMESTAMP_TZ_NANOS)
                                .defaultValue(LongTimestampWithTimeZone.fromEpochSecondsAndFraction(0, 0, TimeZoneKey.UTC_KEY))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("END")
                                .type(TIMESTAMP_TZ_NANOS)
                                .defaultValue(LongTimestampWithTimeZone.fromEpochSecondsAndFraction(0, 0, TimeZoneKey.UTC_KEY))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("QUERY")
                                .type(VarcharType.VARCHAR)
                                .build()),
                GENERIC_TABLE);

        this.metadata = metadata;
        this.varcharMapType = metadata.getVarcharMapType();
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        var argument = (ScalarArgument) arguments.get("QUERY");
        String query = ((io.airlift.slice.Slice) argument.getValue()).toStringUtf8();

        var start = (LongTimestampWithTimeZone) ((ScalarArgument) arguments.get("START")).getValue();
        var end = (LongTimestampWithTimeZone) ((ScalarArgument) arguments.get("END")).getValue();

        if (Strings.isNullOrEmpty(query)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, query);
        }

        // determine the returned row type
        List<ColumnHandle> columnHandles;
        try {
            columnHandles = metadata.getColumnHandles(query);
        }
        catch (UndeclaredThrowableException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot get column definition", Throwables.getRootCause(e));
        }
        if (columnHandles.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot get column definition");
        }
        Descriptor returnedType = new Descriptor(columnHandles.stream()
                .map(LokiColumnHandle.class::cast)
                .map(column -> new Descriptor.Field(column.name(), Optional.of(column.type())))
                .collect(toImmutableList()));

        var tableHandle = new LokiTableHandle(
                query,
                // TODO: account for time zone
                Instant.ofEpochMilli(start.getEpochMillis()),
                Instant.ofEpochMilli(end.getEpochMillis())
        );

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(new QueryHandle(tableHandle))
                .build();
    }

    public static class QueryHandle
            implements ConnectorTableFunctionHandle
    {
        private final LokiTableHandle tableHandle;

        @JsonCreator
        public QueryHandle(@JsonProperty("tableHandle") LokiTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public LokiTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
