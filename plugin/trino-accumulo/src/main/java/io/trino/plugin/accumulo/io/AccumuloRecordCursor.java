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
package io.trino.plugin.accumulo.io;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.accumulo.Types;
import io.trino.plugin.accumulo.model.AccumuloColumnHandle;
import io.trino.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.accumulo.AccumuloErrorCode.IO_ERROR;
import static io.trino.plugin.accumulo.io.AccumuloPageSink.ROW_ID_COLUMN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of Trino RecordCursor, responsible for iterating over a Trino split,
 * reading rows of data and then implementing various methods to retrieve columns within each row.
 *
 * @see AccumuloRecordSet
 * @see AccumuloRecordSetProvider
 */
public class AccumuloRecordCursor
        implements RecordCursor
{
    private static final int WHOLE_ROW_ITERATOR_PRIORITY = Integer.MAX_VALUE;

    private final List<AccumuloColumnHandle> columnHandles;
    private final String[] fieldToColumnName;
    private final BatchScanner scanner;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;

    private long bytesRead;

    public AccumuloRecordCursor(
            AccumuloRowSerializer serializer,
            BatchScanner scanner,
            String rowIdName,
            List<AccumuloColumnHandle> columnHandles)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.scanner = requireNonNull(scanner, "scanner is null");
        this.serializer = requireNonNull(serializer, "serializer is null");
        this.serializer.setRowIdName(requireNonNull(rowIdName, "rowIdName is null"));

        requireNonNull(columnHandles, "columnHandles is null");

        if (retrieveOnlyRowIds(rowIdName)) {
            this.scanner.addScanIterator(new IteratorSetting(1, "firstentryiter", FirstEntryInRowIterator.class));

            fieldToColumnName = new String[1];
            fieldToColumnName[0] = rowIdName;

            // Set a flag on the serializer saying we are only going to be retrieving the row ID
            this.serializer.setRowOnly(true);
        }
        else {
            // Else, we will be scanning some more columns here
            this.serializer.setRowOnly(false);

            // Fetch the reserved row ID column
            this.scanner.fetchColumn(ROW_ID_COLUMN, ROW_ID_COLUMN);

            Text family = new Text();
            Text qualifier = new Text();

            // Create an array which maps the column ordinal to the name of the column
            fieldToColumnName = new String[columnHandles.size()];
            for (int i = 0; i < columnHandles.size(); ++i) {
                AccumuloColumnHandle columnHandle = columnHandles.get(i);
                fieldToColumnName[i] = columnHandle.getName();

                // Make sure to skip the row ID!
                if (!columnHandle.getName().equals(rowIdName)) {
                    // Set the mapping of Trino column name to the family/qualifier
                    this.serializer.setMapping(columnHandle.getName(), columnHandle.getFamily().get(), columnHandle.getQualifier().get());

                    // Set our scanner to fetch this family/qualifier column
                    // This will help us prune which data we receive from Accumulo
                    family.set(columnHandle.getFamily().get());
                    qualifier.set(columnHandle.getQualifier().get());
                    this.scanner.fetchColumn(family, qualifier);
                }
            }
        }

        IteratorSetting setting = new IteratorSetting(WHOLE_ROW_ITERATOR_PRIORITY, WholeRowIterator.class);
        scanner.addScanIterator(setting);

        iterator = this.scanner.iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return bytesRead;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field >= 0 && field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (iterator.hasNext()) {
                serializer.reset();
                Entry<Key, Value> row = iterator.next();
                for (Entry<Key, Value> entry : WholeRowIterator.decodeRow(row.getKey(), row.getValue()).entrySet()) {
                    bytesRead += entry.getKey().getSize() + entry.getValue().getSize();
                    serializer.deserialize(entry);
                }
                return true;
            }
            return false;
        }
        catch (IOException e) {
            throw new TrinoException(IO_ERROR, "Caught IO error from serializer on read", e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return serializer.isNull(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return serializer.getBoolean(fieldToColumnName[field]);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return serializer.getDouble(fieldToColumnName[field]);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT, DATE, INTEGER, REAL, SMALLINT, TIME, TIMESTAMP_MILLIS, TINYINT);
        Type type = getType(field);
        if (type.equals(BIGINT)) {
            return serializer.getLong(fieldToColumnName[field]);
        }
        if (type.equals(DATE)) {
            return serializer.getDate(fieldToColumnName[field]);
        }
        if (type.equals(INTEGER)) {
            return serializer.getInt(fieldToColumnName[field]);
        }
        if (type.equals(REAL)) {
            return Float.floatToIntBits(serializer.getFloat(fieldToColumnName[field]));
        }
        if (type.equals(SMALLINT)) {
            return serializer.getShort(fieldToColumnName[field]);
        }
        if (type.equals(TIME)) {
            return serializer.getTime(fieldToColumnName[field]).getTime();
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return serializer.getTimestamp(fieldToColumnName[field]).getTime() * MICROSECONDS_PER_MILLISECOND;
        }
        if (type.equals(TINYINT)) {
            return serializer.getByte(fieldToColumnName[field]);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
    }

    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        checkArgument(Types.isArrayType(type) || Types.isMapType(type), "Expected field %s to be a type of array or map but is %s", field, type);

        if (Types.isArrayType(type)) {
            return serializer.getArray(fieldToColumnName[field], type);
        }

        return serializer.getMap(fieldToColumnName[field], type);
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        if (type instanceof VarbinaryType) {
            return Slices.wrappedBuffer(serializer.getVarbinary(fieldToColumnName[field]));
        }
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(serializer.getVarchar(fieldToColumnName[field]));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type " + type);
    }

    @Override
    public void close()
    {
        scanner.close();
    }

    /**
     * Gets a Boolean value indicating whether or not the scanner should only return row IDs.
     * <p>
     * This can occur in cases such as SELECT COUNT(*) or the table only has one column.
     * Trino doesn't need the entire contents of the row to count them,
     * so we can configure Accumulo to only give us the first key/value pair in the row
     *
     * @param rowIdName Row ID column name
     * @return True if scanner should retriev eonly row IDs, false otherwise
     */
    private boolean retrieveOnlyRowIds(String rowIdName)
    {
        return columnHandles.isEmpty() || (columnHandles.size() == 1 && columnHandles.get(0).getName().equals(rowIdName));
    }

    /**
     * Checks that the given field is one of the provided types.
     *
     * @param field Ordinal of the field
     * @param expected An array of expected types
     * @throws IllegalArgumentException If the given field does not match one of the types
     */
    private void checkFieldType(int field, Type... expected)
    {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }

        throw new IllegalArgumentException(format("Expected field %s to be a type of %s but is %s", field, StringUtils.join(expected, ","), actual));
    }
}
