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
package io.trino.plugin.kafka.encoder.kafka;

import io.trino.plugin.kafka.encoder.AbstractRowEncoder;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// Used to serialize single value keys not registered with schema registry
public class KafkaSerializerRowEncoder
        extends AbstractRowEncoder
{
    private static final LongSerializer LONG_SERIALIZER = new LongSerializer();
    private static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();
    private static final DoubleSerializer DOUBLE_SERIALIZER = new DoubleSerializer();
    private static final FloatSerializer FLOAT_SERIALIZER = new FloatSerializer();
    // TODO: Support more encodings, currently only UTF8 is supported
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final ByteBufferSerializer BYTE_BUFFER_SERIALIZER = new ByteBufferSerializer();
    private static final int MAX_PRECISION = 12;
    private static final byte[] SERIALIZED_TRUE = new byte[]{1};
    private static final byte[] SERIALIZED_FALSE = new byte[]{0};

    private final String topic;
    private byte[] data;
    private boolean isNull;

    public KafkaSerializerRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, String topic)
    {
        super(session, columnHandles);
        checkState(columnHandles.size() == 1, "Multiple columns");
        Schema schema = fromTrinoType(getOnlyElement(columnHandles).getType());
        checkState(schema.getType() != Schema.Type.RECORD && schema.getType() != Schema.Type.MAP && schema.getType() != Schema.Type.ARRAY, "Unsupported error message '%s'", schema.getType());
        this.topic = requireNonNull(topic, "topic is null");
    }

    public static Schema fromTrinoType(Type trinoType)
    {
        if (trinoType instanceof BigintType) {
            return Schema.create(Schema.Type.LONG);
        }
        else if (trinoType instanceof IntegerType || trinoType instanceof SmallintType || trinoType instanceof TinyintType) {
            return Schema.create(Schema.Type.INT);
        }
        else if (trinoType instanceof VarcharType) {
            return Schema.create(Schema.Type.STRING);
        }
        else if (trinoType instanceof BooleanType) {
            return Schema.create(Schema.Type.BOOLEAN);
        }
        else if (trinoType instanceof DoubleType) {
            return Schema.create(Schema.Type.DOUBLE);
        }
        else if (trinoType instanceof RealType) {
            return Schema.create(Schema.Type.FLOAT);
        }
        else if (trinoType instanceof VarbinaryType) {
            return Schema.create(Schema.Type.BYTES);
        }
        else if (trinoType instanceof DateType) {
            return Schema.create(Schema.Type.INT);
        }
        else if (trinoType instanceof TimestampType && ((TimestampType) trinoType).getPrecision() <= MAX_SHORT_PRECISION) {
            return Schema.create(Schema.Type.LONG);
        }
        else if (trinoType instanceof TimeType) {
            return Schema.create(Schema.Type.LONG);
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported type for single value key column: %s", trinoType));
        }
    }

    private void checkIsNotSet()
    {
        checkState(data == null && !isNull, "Data is already set");
    }

    @Override
    public void appendColumnValue(Block block, int position)
    {
        checkIsNotSet();
        super.appendColumnValue(block, position);
    }

    @Override
    protected void appendNullValue()
    {
        isNull = true;
    }

    @Override
    protected void appendLong(long value)
    {
        data = LONG_SERIALIZER.serialize(topic, value);
    }

    @Override
    protected void appendInt(int value)
    {
        data = INTEGER_SERIALIZER.serialize(topic, value);
    }

    @Override
    protected void appendShort(short value)
    {
        data = INTEGER_SERIALIZER.serialize(topic, (int) value);
    }

    @Override
    protected void appendByte(byte value)
    {
        data = new byte[]{value};
    }

    @Override
    protected void appendDouble(double value)
    {
        data = DOUBLE_SERIALIZER.serialize(topic, value);
    }

    @Override
    protected void appendFloat(float value)
    {
        data = FLOAT_SERIALIZER.serialize(topic, value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        // This is how KafkaAvroSerializer serializes booleans
        data = value ? SERIALIZED_TRUE : SERIALIZED_FALSE;
    }

    @Override
    protected void appendString(String value)
    {
        data = STRING_SERIALIZER.serialize(topic, value);
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        data = BYTE_BUFFER_SERIALIZER.serialize(topic, value);
    }

    @Override
    protected void appendSqlDate(SqlDate value)
    {
        data = INTEGER_SERIALIZER.serialize(topic, value.getDays());
    }

    @Override
    protected void appendSqlTime(SqlTime value)
    {
        SqlTime maxPrecision = value.roundTo(MAX_PRECISION);
        data = LONG_SERIALIZER.serialize(topic, maxPrecision.getPicos());
    }

    @Override
    protected void appendSqlTimeWithTimeZone(SqlTimeWithTimeZone value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    @Override
    protected void appendSqlTimestamp(SqlTimestamp value)
    {
        checkState(value.getPrecision() <= MAX_SHORT_PRECISION, "Unsupported precision for timestamp type '%s' must be between 0 and %s", value.getPrecision(), MAX_SHORT_PRECISION);
        data = LONG_SERIALIZER.serialize(topic, value.getEpochMicros());
    }

    @Override
    protected void appendSqlTimestampWithTimeZone(SqlTimestampWithTimeZone value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    @Override
    protected void appendArray(List<Object> value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    @Override
    protected void appendMap(Map<Object, Object> value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    @Override
    protected void appendRow(List<Object> value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    @Override
    public byte[] toByteArray()
    {
        checkState(data != null || isNull, "Data is not set");
        byte[] result = data;
        data = null;
        isNull = false;
        resetColumnIndex();
        return result;
    }

    public static class Factory
            implements RowEncoderFactory
    {
        @Override
        public RowEncoder create(ConnectorSession session, Optional<String> dataSchema, List<EncoderColumnHandle> columnHandles, String topic, boolean isKey)
        {
            return new KafkaSerializerRowEncoder(session, columnHandles, topic);
        }
    }
}
