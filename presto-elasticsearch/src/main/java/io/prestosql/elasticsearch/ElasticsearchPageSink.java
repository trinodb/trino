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
package io.prestosql.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.prestosql.elasticsearch.client.ElasticsearchClient;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ElasticsearchPageSink
        implements ConnectorPageSink
{
    private final ConnectorSession connectorSession;
    private final ElasticsearchClient clientSession;
    private final ElasticsearchOutputTableHandle handle;
    private final ZoneId zoneId;

    public ElasticsearchPageSink(
            ConnectorSession connectorSession,
            ElasticsearchClient clientSession,
            ElasticsearchOutputTableHandle tableHandle)
    {
        this.clientSession = clientSession;
        this.connectorSession = connectorSession;
        this.handle = tableHandle;
        this.zoneId = ZoneId.of(connectorSession.getTimeZoneKey().getId());
    }

    private Object columnValue(Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        Type type = handle.getColumnTypes().get(channel);

        Object value;
        if (block.isNull(position)) {
            value = null;
        }
        else if (BOOLEAN.equals(type)) {
            value = type.getBoolean(block, position);
        }
        else if (BIGINT.equals(type)) {
            value = type.getLong(block, position);
        }
        else if (INTEGER.equals(type)) {
            value = toIntExact(type.getLong(block, position));
        }
        else if (SMALLINT.equals(type)) {
            value = Shorts.checkedCast(type.getLong(block, position));
        }
        else if (TINYINT.equals(type)) {
            value = SignedBytes.checkedCast(type.getLong(block, position));
        }
        else if (DOUBLE.equals(type)) {
            value = type.getDouble(block, position);
        }
        else if (REAL.equals(type)) {
            value = intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        else if (DATE.equals(type)) {
            LocalDate dateTime = LocalDate.ofEpochDay(type.getLong(block, position));
            return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(dateTime);
        }
        else if (TIMESTAMP.equals(type)) {
            value = new Timestamp(Instant.ofEpochMilli(type.getLong(block, position)).atZone(zoneId).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
        }
        else if (isVarcharType(type)) {
            value = type.getSlice(block, position).toStringUtf8();
        }
        else if (VARBINARY.equals(type)) {
            value = type.getSlice(block, position).toByteBuffer();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        return value;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        ImmutableList.Builder<ImmutableMap<String, Object>> bulk = ImmutableList.builder();
        for (int position = 0; position < page.getPositionCount(); position++) {
            ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                String fieldName = handle.getColumnNames().get(channel);
                Object fieldValue = columnValue(page, position, channel);
                values.put(fieldName, fieldValue);
            }
            bulk.add(values.build());
        }
        try {
            clientSession.saveIndexBulk(handle.getIndex(), bulk.build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        // Ignore
    }
}
