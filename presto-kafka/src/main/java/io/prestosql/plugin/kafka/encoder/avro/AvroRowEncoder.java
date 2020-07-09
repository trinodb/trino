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
import io.prestosql.plugin.kafka.encoder.AbstractRowEncoder;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
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
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroRowEncoder
        extends AbstractRowEncoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL);

    public static final String NAME = "avro";

    private final Schema schema;
    private final GenericData.Record record;

    public AvroRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema parsedSchema)
    {
        super(session, columnHandles);
        this.schema = requireNonNull(parsedSchema, "parsedSchema is null");
        this.record = new GenericData.Record(parsedSchema);
    }

    @Override
    protected void validateColumns(List<EncoderColumnHandle> columnHandles)
    {
        for (EncoderColumnHandle columnHandle : columnHandles) {
            checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
            checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());

            checkArgument(isSupportedType(columnHandle.getType()), "unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
    }

    private boolean isSupportedType(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    protected void appendNullValue()
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), null);
    }

    @Override
    protected void appendLong(long value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendInt(int value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendShort(short value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendByte(byte value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendDouble(double value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendFloat(float value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendString(String value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        record.put(columnHandles.get(currentColumnIndex).getName(), value);
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        try (ByteArrayOutputStream byteArrayOuts = new ByteArrayOutputStream();
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, byteArrayOuts);
            dataFileWriter.append(record);
            dataFileWriter.flush();

            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return byteArrayOuts.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to convert to Avro.", e);
        }
    }
}
