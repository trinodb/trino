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
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;

public class CsvRowEncoder
        implements RowEncoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL);

    public static final String NAME = "csv";

    private String[] row;
    private final int size;

    public CsvRowEncoder(Set<EncoderColumnHandle> columnHandles)
    {
        this.parseColumns(columnHandles);
        this.size = columnHandles.size();
        this.row = new String[this.size];
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

    private int getPosition(EncoderColumnHandle columnHandle)
    {
        return columnHandle.getOrdinalPosition() - 1;
    }

    @Override
    public byte[] toByteArray()
    {
        try (ByteArrayOutputStream byteArrayOuts = new ByteArrayOutputStream();
                OutputStreamWriter outsWriter = new OutputStreamWriter(byteArrayOuts, StandardCharsets.UTF_8);
                CSVWriter writer = new CSVWriter(outsWriter, ',', '"', "")) {
            writer.writeNext(row);
            writer.flush();
            return byteArrayOuts.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear()
    {
        this.row = new String[size];
    }

    @Override
    public RowEncoder putNullValue(EncoderColumnHandle columnHandle)
    {
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, Object value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, long value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, int value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, short value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, double value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, float value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, boolean value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, String value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, ByteBuffer value)
    {
        row[getPosition(columnHandle)] = String.valueOf(value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte[] value)
    {
        row[getPosition(columnHandle)] = Arrays.toString(value);
        return this;
    }
}
