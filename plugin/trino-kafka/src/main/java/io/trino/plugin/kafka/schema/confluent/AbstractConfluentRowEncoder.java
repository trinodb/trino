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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.DUMMY_FIELD_NAME;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.ADD_DUMMY;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSessionProperties.getEmptyFieldStrategy;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.ENUM;

public abstract class AbstractConfluentRowEncoder
        implements RowEncoder
{
    protected final Schema schema;
    protected final ConnectorSession session;
    private int currentColumnIndex;
    protected final List<EncoderColumnHandle> columnHandles;
    private final KafkaAvroSerializer kafkaAvroSerializer;
    private final String topic;

    public AbstractConfluentRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema schema, KafkaAvroSerializer kafkaAvroSerializer, String topic)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.session = requireNonNull(session, "session is null");
        requireNonNull(columnHandles, "columnHandles is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.currentColumnIndex = 0;
        this.kafkaAvroSerializer = requireNonNull(kafkaAvroSerializer, "kafkaAvroSerializer is null");
        this.topic = requireNonNull(topic, "topic is null");
    }

    protected abstract Object buildRow();

    protected byte[] serialize(Object object)
    {
        return kafkaAvroSerializer.serialize(topic, object);
    }

    protected abstract void addValue(Block block, int position);

    @Override
    public void appendColumnValue(Block block, int position)
    {
        checkArgument(getCurrentColumnIndex() < columnHandles.size(), format("currentColumnIndex '%d' is greater than number of columns '%d'", getCurrentColumnIndex(), columnHandles.size()));
        addValue(block, position);
        currentColumnIndex++;
    }

    protected Object getValue(Type type, Block block, int position, Schema fieldSchema)
    {
        if (block.isNull(position)) {
            checkState(fieldSchema.isNullable(), "Unexpected null value for non-nullable schema '%s'", fieldSchema.toString(true));
            return null;
        }
        // Since the value is non-null, if this a union, extract complex type
        fieldSchema = extractBaseType(fieldSchema);

        if (type == BOOLEAN) {
            return type.getObjectValue(session, block, position);
        }
        else if (type == BIGINT) {
            return type.getLong(block, position);
        }
        else if (type == INTEGER) {
            return toIntExact(type.getLong(block, position));
        }
        else if (type == SMALLINT) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        else if (type == TINYINT) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        else if (type == DOUBLE) {
            return type.getDouble(block, position);
        }
        else if (type == REAL) {
            return intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        else if (type instanceof VarcharType) {
            if (fieldSchema.getType() == ENUM) {
                return new GenericData.EnumSymbol(fieldSchema, type.getSlice(block, position).toStringUtf8());
            }
            return type.getSlice(block, position).toStringUtf8();
        }
        else if (type instanceof VarbinaryType) {
            return type.getSlice(block, position).toByteBuffer();
        }

        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            Type elementType = arrayType.getElementType();
            Block arrayBlock = block.getObject(position, Block.class);
            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            Schema elementAvroType = fieldSchema.getElementType();
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getValue(elementType, arrayBlock, i, elementAvroType));
            }
            return Collections.unmodifiableList(list);
        }
        else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            Block mapBlock = block.getObject(position, Block.class);
            Schema valueSchema = fieldSchema.getValueType();
            Type valueType = mapType.getValueType();
            Map<String, Object> map = new HashMap<>();
            for (int index = 0; index < mapBlock.getPositionCount(); index += 2) {
                String key = VARCHAR.getSlice(mapBlock, index).toStringUtf8();
                Object value = getValue(valueType, mapBlock, index + 1, valueSchema);
                map.put(key, value);
            }
            return Collections.unmodifiableMap(map);
        }
        else if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(fieldSchema);
            // If the avro schema is a record with no fields and the empty field strategy is ADD_DUMMY, ignore the dummy value being inserted.
            if (!isEmptyStruct(fieldSchema, rowType)) {
                checkState(rowType.getFields().size() == fieldSchema.getFields().size(), "Mismatch in number of fields in row and schema");
                Block rowBlock = block.getObject(position, Block.class);
                for (int index = 0; index < rowType.getFields().size(); index++) {
                    RowType.Field field = rowType.getFields().get(index);
                    Schema.Field avroField = fieldSchema.getFields().get(index);
                    Schema avroFieldSchema = avroField.schema();
                    Object value = getValue(field.getType(), rowBlock, index, avroFieldSchema);
                    recordBuilder.set(avroField, value);
                }
            }
            return recordBuilder.build();
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported type '%s'", type));
        }
    }

    @VisibleForTesting
    public static Schema extractBaseType(Schema schema)
    {
        requireNonNull(schema, "schema is null");
        if (!schema.isUnion()) {
            return schema;
        }
        Optional<Schema> baseSchema = schema.getTypes().stream()
                .filter(fieldSchema -> fieldSchema.getType() != Schema.Type.NULL)
                .map(nonNullSchema -> {
                    if (nonNullSchema.isUnion()) {
                        return extractBaseType(nonNullSchema);
                    }
                    return nonNullSchema;
                }).findFirst();
        checkState(baseSchema.isPresent(), "baseSchema is empty");
        return baseSchema.get();
    }

    private boolean isEmptyStruct(Schema schema, Type type)
    {
        // If the avro schema is a record with no fields and the empty field strategy is ADD_DUMMY
        checkState(schema.getType() == Schema.Type.RECORD, "Unexpected type '%s' for record schema", schema.getType());
        checkState(type instanceof RowType, "Unexpected type '%s' for trino struct field", type.getTypeId());
        if (schema.getFields().isEmpty() && getEmptyFieldStrategy(session) == ADD_DUMMY) {
            RowType rowType = (RowType) type;
            if (rowType.getFields().size() == 1) {
                RowType.Field field = getOnlyElement(rowType.getFields());
                if (field.getName().isPresent() && field.getName().get().equals(DUMMY_FIELD_NAME)) {
                    return field.getType() instanceof BooleanType;
                }
            }
        }
        return false;
    }

    private void resetColumnIndex()
    {
        currentColumnIndex = 0;
    }

    protected int getCurrentColumnIndex()
    {
        return currentColumnIndex;
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] bytes = kafkaAvroSerializer.serialize(topic, buildRow());
        resetColumnIndex();
        return bytes;
    }

    @Override
    public void close()
    {
        // Clear thread local encoder
    }
}
