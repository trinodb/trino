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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.MapBlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.HEADERS_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.KEY_CORRUPT_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.KEY_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.KEY_LENGTH_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.MESSAGE_CORRUPT_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.MESSAGE_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.MESSAGE_LENGTH_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.PARTITION_ID_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.PARTITION_OFFSET_FIELD;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.max;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class KafkaRecordSet
        implements RecordSet
{
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int CONSUMER_POLL_TIMEOUT = 100;
    private static final FieldValueProvider EMPTY_HEADERS_FIELD_PROVIDER = createEmptyHeadersFieldProvider();

    private final KafkaSplit split;

    private final KafkaConsumerFactory consumerFactory;
    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(
            KafkaSplit split,
            KafkaConsumerFactory consumerFactory,
            List<KafkaColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder)
    {
        this.split = requireNonNull(split, "split is null");
        this.consumerFactory = requireNonNull(consumerFactory, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor();
    }

    private class KafkaRecordCursor
            implements RecordCursor
    {
        private final TopicPartition topicPartition;
        private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
        private Iterator<ConsumerRecord<byte[], byte[]>> records = emptyIterator();
        private long completedBytes;

        private final FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];

        private KafkaRecordCursor()
        {
            topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
            kafkaConsumer = consumerFactory.create();
            kafkaConsumer.assign(ImmutableList.of(topicPartition));
            kafkaConsumer.seek(topicPartition, split.getMessagesRange().getBegin());
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (!records.hasNext()) {
                if (kafkaConsumer.position(topicPartition) >= split.getMessagesRange().getEnd()) {
                    return false;
                }
                records = kafkaConsumer.poll(CONSUMER_POLL_TIMEOUT).iterator();
                return advanceNextPosition();
            }

            return nextRow(records.next());
        }

        private boolean nextRow(ConsumerRecord<byte[], byte[]> message)
        {
            requireNonNull(message, "message is null");

            if (message.offset() >= split.getMessagesRange().getEnd()) {
                return false;
            }

            completedBytes += max(message.serializedKeySize(), 0) + max(message.serializedValueSize(), 0);

            byte[] keyData = EMPTY_BYTE_ARRAY;
            if (message.key() != null) {
                keyData = message.key();
            }

            byte[] messageData = EMPTY_BYTE_ARRAY;
            if (message.value() != null) {
                messageData = message.value();
            }

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData);
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData);

            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    switch (columnHandle.getName()) {
                        case PARTITION_OFFSET_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(message.offset()));
                            break;
                        case MESSAGE_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(messageData));
                            break;
                        case MESSAGE_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageData.length));
                            break;
                        case KEY_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                            break;
                        case KEY_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                            break;
                        case KEY_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedKey.isEmpty()));
                            break;
                        case HEADERS_FIELD:
                            currentRowValuesMap.put(columnHandle, headerMapValueProvider((MapType) columnHandle.getType(), message.headers()));
                            break;
                        case MESSAGE_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedValue.isEmpty()));
                            break;
                        case PARTITION_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(message.partition()));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + columnHandle.getName());
                    }
                }
            }

            decodedKey.ifPresent(currentRowValuesMap::putAll);
            decodedValue.ifPresent(currentRowValuesMap::putAll);

            for (int i = 0; i < columnHandles.size(); i++) {
                ColumnHandle columnHandle = columnHandles.get(i);
                currentRowValues[i] = currentRowValuesMap.get(columnHandle);
            }

            return true; // Advanced successfully.
        }

        @Override
        public boolean getBoolean(int field)
        {
            return getFieldValueProvider(field, boolean.class).getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return getFieldValueProvider(field, long.class).getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return getFieldValueProvider(field, double.class).getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            return getFieldValueProvider(field, Slice.class).getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            return getFieldValueProvider(field, Block.class).getBlock();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return currentRowValues[field] == null || currentRowValues[field].isNull();
        }

        private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, expectedType);
            return currentRowValues[field];
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
            kafkaConsumer.close();
        }
    }

    private static FieldValueProvider createEmptyHeadersFieldProvider()
    {
        MapType mapType = new MapType(VarcharType.VARCHAR, new ArrayType(VarbinaryType.VARBINARY),
                MethodHandles.empty(methodType(Boolean.class, Block.class, int.class, long.class)),
                MethodHandles.empty(methodType(Boolean.class, Block.class, int.class, Block.class, int.class)),
                MethodHandles.empty(methodType(long.class, Object.class)),
                MethodHandles.empty(methodType(long.class, Object.class)));
        BlockBuilder mapBlockBuilder = new MapBlockBuilder(mapType, null, 0);
        mapBlockBuilder.beginBlockEntry();
        mapBlockBuilder.closeEntry();
        Block emptyMapBlock = mapType.getObject(mapBlockBuilder, 0);
        return new FieldValueProvider() {
            @Override
            public boolean isNull()
            {
                return false;
            }

            @Override
            public Block getBlock()
            {
                return emptyMapBlock;
            }
        };
    }

    public static FieldValueProvider headerMapValueProvider(MapType varcharMapType, Headers headers)
    {
        if (!headers.iterator().hasNext()) {
            return EMPTY_HEADERS_FIELD_PROVIDER;
        }

        Type keyType = varcharMapType.getTypeParameters().get(0);
        Type valueArrayType = varcharMapType.getTypeParameters().get(1);
        Type valueType = valueArrayType.getTypeParameters().get(0);

        BlockBuilder mapBlockBuilder = varcharMapType.createBlockBuilder(null, 1);
        BlockBuilder builder = mapBlockBuilder.beginBlockEntry();

        // Group by keys and collect values as array.
        Multimap<String, byte[]> headerMap = ArrayListMultimap.create();
        for (Header header : headers) {
            headerMap.put(header.key(), header.value());
        }

        for (String headerKey : headerMap.keySet()) {
            writeNativeValue(keyType, builder, headerKey);
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            for (byte[] value : headerMap.get(headerKey)) {
                writeNativeValue(valueType, arrayBuilder, value);
            }
            builder.closeEntry();
        }

        mapBlockBuilder.closeEntry();

        return new FieldValueProvider() {
            @Override
            public boolean isNull()
            {
                return false;
            }

            @Override
            public Block getBlock()
            {
                return varcharMapType.getObject(mapBlockBuilder, 0);
            }
        };
    }
}
