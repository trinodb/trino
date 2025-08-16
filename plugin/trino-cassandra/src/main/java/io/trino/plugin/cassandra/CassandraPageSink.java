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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.ID_COLUMN_NAME;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validColumnName;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validSchemaName;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CassandraPageSink
        implements ConnectorPageSink
{
    private final CassandraTypeManager cassandraTypeManager;
    private final CassandraSession cassandraSession;
    private final PreparedStatement insert;
    private final List<Type> columnTypes;
    private final boolean generateUuid;
    private final int batchSize;
    private final Function<Integer, Object> toCassandraDate;
    private final BatchStatementBuilder batchStatement = BatchStatement.builder(DefaultBatchType.LOGGED);

    public CassandraPageSink(
            CassandraTypeManager cassandraTypeManager,
            CassandraSession cassandraSession,
            ProtocolVersion protocolVersion,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            boolean generateUuid,
            int batchSize)
    {
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.generateUuid = generateUuid;
        this.batchSize = batchSize;

        if (protocolVersion.getCode() <= ProtocolVersion.V3.getCode()) {
            toCassandraDate = value -> DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(value));
        }
        else {
            toCassandraDate = LocalDate::ofEpochDay;
        }

        ImmutableMap.Builder<String, Term> parameters = ImmutableMap.builder();
        if (generateUuid) {
            parameters.put(ID_COLUMN_NAME, bindMarker());
        }
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            checkArgument(columnName != null, "columnName is null at position: %s", i);
            parameters.put(validColumnName(columnName), bindMarker());
        }
        SimpleStatement insertStatement = insertInto(validSchemaName(schemaName), validTableName(tableName))
                .values(parameters.buildOrThrow())
                .build();
        this.insert = cassandraSession.prepare(insertStatement);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(columnTypes.size() + 1);
            if (generateUuid) {
                values.add(UUID.randomUUID());
            }

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(values, page, position, channel);
            }

            batchStatement.addStatement(insert.bind(values.toArray()));

            if (batchStatement.getStatementsCount() >= batchSize) {
                cassandraSession.execute(batchStatement.build());
                batchStatement.clearStatements();
            }
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(List<Object> values, Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(channel);
        if (block.isNull(position)) {
            values.add(null);
        }
        else if (BOOLEAN.equals(type)) {
            values.add(BOOLEAN.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            values.add(BIGINT.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            values.add(INTEGER.getInt(block, position));
        }
        else if (SMALLINT.equals(type)) {
            values.add(SMALLINT.getShort(block, position));
        }
        else if (TINYINT.equals(type)) {
            values.add(TINYINT.getByte(block, position));
        }
        else if (DOUBLE.equals(type)) {
            values.add(DOUBLE.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            values.add(REAL.getFloat(block, position));
        }
        else if (DATE.equals(type)) {
            values.add(toCassandraDate.apply(DATE.getInt(block, position)));
        }
        else if (TIME_NANOS.equals(type)) {
            long value = type.getLong(block, position);
            values.add(LocalTime.ofNanoOfDay(roundDiv(value, PICOSECONDS_PER_NANOSECOND) % NANOSECONDS_PER_DAY));
        }
        else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            values.add(Instant.ofEpochMilli(unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position))));
        }
        else if (type instanceof VarcharType varcharType) {
            values.add(varcharType.getSlice(block, position).toStringUtf8());
        }
        else if (VARBINARY.equals(type)) {
            values.add(VARBINARY.getSlice(block, position).toByteBuffer());
        }
        else if (UuidType.UUID.equals(type)) {
            values.add(trinoUuidToJavaUuid(UuidType.UUID.getSlice(block, position)));
        }
        else if (cassandraTypeManager.isIpAddressType(type)) {
            values.add(InetAddresses.forString((String) type.getObjectValue(block, position)));
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (batchStatement.getStatementsCount() > 0) {
            cassandraSession.execute(batchStatement.build());
            batchStatement.clearStatements();
        }

        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
