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
package io.prestosql.plugin.kafka.encoder.avro;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;

public class AvroRowEncoder
        implements RowEncoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL);

    public static final String NAME = "avro";

    private final Schema schema;
    private GenericData.Record record;

    public AvroRowEncoder(Schema schema, Set<EncoderColumnHandle> columnHandles)
    {
        parseColumns(columnHandles);
        this.schema = requireNonNull(schema, "schema is null");
        this.record = new GenericData.Record(schema);
    }

    // performs checks on column handles
    private void parseColumns(Set<EncoderColumnHandle> columnHandles)
    {
        for (EncoderColumnHandle columnHandle : columnHandles) {
            try {
                requireNonNull(columnHandle, "columnHandle is null");
                String columnName = columnHandle.getName();
                Type columnType = columnHandle.getType();

                checkArgument(!columnHandle.isInternal(), "unexpected internal column'%s'", columnName);
                checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
                checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);

                checkArgument(isSupportedType(columnType), "unsupported column type '%s' for column '%s'", columnType, columnName);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_USER_ERROR, e);
            }
        }
    }

    private boolean isSupportedType(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    public byte[] toByteArray()
    {
        try (ByteArrayOutputStream byteArrayOuts = new ByteArrayOutputStream();
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, byteArrayOuts);
            dataFileWriter.append(record);
            dataFileWriter.flush();
            return byteArrayOuts.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to convert to Avro.", e);
        }
    }

    @Override
    public void clear()
    {
        record = new GenericData.Record(schema);
    }

    @Override
    public RowEncoder putNullValue(EncoderColumnHandle columnHandle)
    {
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, Object value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, long value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, int value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, short value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, double value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, float value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, boolean value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, String value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, ByteBuffer value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte[] value)
    {
        record.put(columnHandle.getName(), value);
        return this;
    }
}
