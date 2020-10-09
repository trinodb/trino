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
package io.prestosql.plugin.google.sheets;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsRecordCursor
        implements RecordCursor
{
    private final List<SheetsColumnHandle> columnHandles;
    private final long totalBytes;
    private final List<List<Object>> dataValues;

    private List<String> fields;
    private int currentIndex;

    private static final String DELIMITER_COMMA = ",";

    private static final DateTimeFormatter DATE_PARSER = ISODateTimeFormat.date().withZoneUTC();
    private static final DateTimeFormatter TIME_PARSER = DateTimeFormat.forPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_PARSER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public SheetsRecordCursor(List<SheetsColumnHandle> columnHandles, List<List<Object>> dataValues)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(dataValues, "dataValues is null");

        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.dataValues = ImmutableList.copyOf(dataValues);
        long inputLength = 0;
        for (List<Object> objList : dataValues) {
            for (Object obj : objList) {
                inputLength += String.valueOf(obj).length();
            }
        }
        totalBytes = inputLength;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
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
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        List<Object> currentVals = null;
        // Skip empty rows from sheet
        while (currentVals == null || currentVals.size() == 0) {
            if (currentIndex == dataValues.size()) {
                return false;
            }
            currentVals = dataValues.get(currentIndex++);
        }
        // Populate incomplete columns with null
        String[] allFields = new String[columnHandles.size()];

        for (int i = 0; i < allFields.length; i++) {
            int ordinalPos = columnHandles.get(i).getOrdinalPosition();
            if (currentVals.size() > ordinalPos) {
                allFields[i] = String.valueOf(currentVals.get(ordinalPos));
            }
        }
        fields = Arrays.asList(allFields);
        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT, INTEGER, TIME, TIMESTAMP, DATE, TINYINT, SMALLINT, REAL);
        Type type = getType(field);
        if (type.equals(TIME)) {
            return TIME_PARSER.parseDateTime(getFieldValue(field)).getMillis();
        }
        else if (type.equals(TIMESTAMP)) {
            return TIMESTAMP_PARSER.parseDateTime(getFieldValue(field)).getMillis();
        }
        else if (type.equals(DATE)) {
            return MILLISECONDS.toDays(DATE_PARSER.parseDateTime(getFieldValue(field)).getMillis());
        }
        else if (type.equals(REAL)) {
            return Float.floatToIntBits(Float.parseFloat(getFieldValue(field)));
        }
        else if (type.equals(TINYINT)) {
            return Byte.valueOf(getFieldValue(field));
        }
        else if (type.equals(SMALLINT)) {
            return Short.parseShort(getFieldValue(field));
        }
        else if (type.equals(BIGINT)) {
            return Long.parseLong(getFieldValue(field));
        }
        else if (type.equals(INTEGER)) {
            return Integer.parseInt(getFieldValue(field));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        if (type instanceof VarbinaryType) {
            return Slices.wrappedBuffer(getFieldValue(field).getBytes(UTF_8));
        }
        else if (type instanceof VarcharType) {
            return Slices.utf8Slice(getFieldValue(field));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + type);
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type... expected)
    {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s",
                field, Joiner.on(DELIMITER_COMMA).join(expected), actual));
    }

    @Override
    public void close()
    {
    }
}
