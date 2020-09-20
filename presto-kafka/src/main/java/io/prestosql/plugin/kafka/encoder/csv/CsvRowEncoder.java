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
package io.prestosql.plugin.kafka.encoder.csv;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.AbstractRowEncoder;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public class CsvRowEncoder
        extends AbstractRowEncoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL);

    public static final String NAME = "csv";

    private final String[] row;

    public CsvRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        super(session, columnHandles);
        for (EncoderColumnHandle columnHandle : this.columnHandles) {
            checkArgument(columnHandle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
            checkArgument(columnHandle.getDataFormat() == null, "Unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());

            checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
        this.row = new String[this.columnHandles.size()];
    }

    private boolean isSupportedType(Type type)
    {
        return type instanceof VarcharType || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    protected void appendNullValue()
    {
        row[currentColumnIndex] = null;
    }

    @Override
    protected void appendLong(long value)
    {
        row[currentColumnIndex] = Long.toString(value);
    }

    @Override
    protected void appendInt(int value)
    {
        row[currentColumnIndex] = Integer.toString(value);
    }

    @Override
    protected void appendShort(short value)
    {
        row[currentColumnIndex] = Short.toString(value);
    }

    @Override
    protected void appendByte(byte value)
    {
        row[currentColumnIndex] = Byte.toString(value);
    }

    @Override
    protected void appendDouble(double value)
    {
        row[currentColumnIndex] = Double.toString(value);
    }

    @Override
    protected void appendFloat(float value)
    {
        row[currentColumnIndex] = Float.toString(value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        row[currentColumnIndex] = Boolean.toString(value);
    }

    @Override
    protected void appendString(String value)
    {
        row[currentColumnIndex] = value;
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        try (ByteArrayOutputStream byteArrayOuts = new ByteArrayOutputStream();
                OutputStreamWriter outsWriter = new OutputStreamWriter(byteArrayOuts, StandardCharsets.UTF_8);
                CSVWriter writer = new CSVWriter(outsWriter, ',', '"', "")) {
            writer.writeNext(row);
            writer.flush();

            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return byteArrayOuts.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
