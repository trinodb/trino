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
package com.aliyun.odps.cupid.trino;

import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.cupid.table.v1.reader.SplitReader;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToRawIntBits;

public class OdpsRecordCursor
        implements RecordCursor {
    private final List<OdpsColumnHandle> columnHandles;

    private ArrayRecord record;
    private final SplitReader<ArrayRecord> recordReader;

    private final InputSplit odpsInputSplit;
    private final boolean isZeroColumn;

    public OdpsRecordCursor(InputSplit odpsInputSplit, List<OdpsColumnHandle> columnHandles, SplitReader<ArrayRecord> recordReader, boolean isZeroColumn) {
        this.odpsInputSplit = odpsInputSplit;
        this.columnHandles = columnHandles;
        this.recordReader = recordReader;
        this.isZeroColumn = isZeroColumn;
    }

    Object getValueInternal(int field) {
        int resField = getCurrentFiled(field);
        if (resField < record.getColumnCount() && !isZeroColumn) {
            return record.get(resField);
        }
        return odpsInputSplit.getPartitionSpec().get(columnHandles.get(resField).getName());
    }

    @Override
    public long getCompletedBytes() {
        return recordReader.getBytesRead();
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        boolean hasNext = recordReader.hasNext();
        if (hasNext) {
            record = recordReader.next();
        }
        return hasNext;
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return (boolean) getValueInternal(field);
    }

    private int getCurrentFiled(int field) {
        String fieldName = columnHandles.get(field).getName();
        Map<String, Integer> indexMap = new HashMap<>();
        for (int i = 0; i < odpsInputSplit.getDataColumns().size(); i++) {
            indexMap.put(odpsInputSplit.getDataColumns().get(i).getName(), i);
        }
        if (indexMap.containsKey(fieldName)) {
            return indexMap.get(fieldName);
        }
        return field;
    }

    @Override
    public long getLong(int field) {
        Type columnType = columnHandles.get(field).getType();
        Object value = getValueInternal(field);
        if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
        } else if (value instanceof Date) {
            long storageTime = ((Date) value).getTime();
            long utcMillis = storageTime + DateTimeZone.getDefault().getOffset(storageTime);
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
        } else if (value instanceof java.util.Date) {
            //for trino-364 timestamp type,odps datetime type must turn to microsecond.
            return ((java.util.Date) value).getTime() * 1000;
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Short) {
            return (Short) value;
        } else if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof BigDecimal) {
            DecimalType type = (DecimalType) columnType;
            return Decimals.encodeShortScaledValue((BigDecimal) value, type.getScale());
        } else if (value instanceof Float) {
            return floatToRawIntBits(((Float) value));
        } else if (value instanceof java.time.LocalDate) {
            LocalDate dateValue = (java.time.LocalDate) value;
            ZonedDateTime zonedDateTime = dateValue.atStartOfDay(ZoneId.systemDefault());
            java.util.Date date = java.util.Date.from(zonedDateTime.toInstant());
            long utcMillis = date.getTime() + DateTimeZone.getDefault().getOffset(date.getTime());
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
//            return date.getTime();
        }

        if (getValueInternal(field) == null) {
            return 0;
        }
        return (Long) getValueInternal(field);
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return (double) getValueInternal(field);
    }

    @Override
    public Slice getSlice(int field) {
        Type columnType = columnHandles.get(field).getType();
        Object value = getValueInternal(field);
        if (value instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) value);
        } else if (value instanceof Varchar) {
            return truncateToLength(Slices.utf8Slice(((Varchar) value).getValue()), columnType);
        } else if (value instanceof Char) {
            return truncateToLengthAndTrimSpaces(Slices.utf8Slice(((Char) value).getValue()), columnType);
        } else if (value instanceof BigDecimal) {
            DecimalType type = (DecimalType) columnType;
            return Slices.utf8Slice(Decimals.encodeScaledValue((BigDecimal) value, type.getScale()).toString());
        } else {
            return value == null ? Slices.utf8Slice("") : Slices.utf8Slice((String) value);
        }
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getValueInternal(field) == null;
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close() {
        try {
            recordReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
