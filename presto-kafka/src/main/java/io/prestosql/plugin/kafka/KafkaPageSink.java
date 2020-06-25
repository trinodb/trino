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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KafkaPageSink
        implements ConnectorPageSink
{
    private final String topicName;
    private final List<KafkaColumnHandle> columns;
    private final RowEncoder keyEncoder;
    private final RowEncoder messageEncoder;
    private final ConnectorSession session;
    private final KafkaProducer<byte[], byte[]> producer;

    public KafkaPageSink(
            String topicName,
            List<KafkaColumnHandle> columns,
            RowEncoder keyEncoder,
            RowEncoder messageEncoder,
            ConnectorSession session,
            PlainTextKafkaProducerFactory producerFactory)
    {
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.columns = (requireNonNull(ImmutableList.copyOf(columns), "columns is null"));
        this.keyEncoder = requireNonNull(keyEncoder, "keyEncoder is null");
        this.messageEncoder = requireNonNull(messageEncoder, "messageEncoder is null");
        this.session = requireNonNull(session, "session is null");
        this.producer = requireNonNull(producerFactory.create(new ByteArraySerializer(), new ByteArraySerializer()), "producerFactory is null");
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(messageEncoder, page, channel, position);
            }
            producer.send(new ProducerRecord<>(topicName, messageEncoder.toByteArray()));
            keyEncoder.clear();
            messageEncoder.clear();
        }
        producer.flush();
        return NOT_BLOCKED;
    }

    private void appendColumn(RowEncoder rowEncoder, Page page, int channel, int position)
    {
        Block block = page.getBlock(channel);
        EncoderColumnHandle columnHandle = columns.get(channel);
        Type type = columns.get(channel).getType();
        if (block.isNull(position)) {
            rowEncoder.putNullValue(columnHandle);
        }
        else if (type == BOOLEAN) {
            rowEncoder.put(columnHandle, type.getBoolean(block, position));
        }
        else if (type == BIGINT) {
            rowEncoder.put(columnHandle, type.getLong(block, position));
        }
        else if (type == INTEGER) {
            rowEncoder.put(columnHandle, toIntExact(type.getLong(block, position)));
        }
        else if (type == SMALLINT) {
            rowEncoder.put(columnHandle, Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (type == TINYINT) {
            rowEncoder.put(columnHandle, SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (type == DOUBLE) {
            rowEncoder.put(columnHandle, type.getDouble(block, position));
        }
        else if (type == REAL) {
            rowEncoder.put(columnHandle, intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (type instanceof VarcharType) {
            rowEncoder.put(columnHandle, type.getSlice(block, position).toStringUtf8());
        }
        else if (type == VARBINARY) {
            rowEncoder.put(columnHandle, type.getSlice(block, position).toByteBuffer());
        }
        else if (type == DATE) {
            rowEncoder.put(columnHandle, type.getObjectValue(session, block, position));
        }
        else if (type == TIME) {
            rowEncoder.put(columnHandle, type.getObjectValue(session, block, position));
        }
        else if (type == TIME_WITH_TIME_ZONE) {
            rowEncoder.put(columnHandle, type.getObjectValue(session, block, position));
        }
        else if (type instanceof TimestampType) {
            rowEncoder.put(columnHandle, type.getObjectValue(session, block, position));
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            rowEncoder.put(columnHandle, type.getObjectValue(session, block, position));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        producer.close();
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
