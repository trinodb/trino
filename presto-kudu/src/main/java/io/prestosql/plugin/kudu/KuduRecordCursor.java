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
package io.prestosql.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.kudu.KuduColumnHandle.ROW_ID_POSITION;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class KuduRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(KuduRecordCursor.class);

    private static final Field ROW_DATA_FIELD;

    static {
        try {
            ROW_DATA_FIELD = RowResult.class.getDeclaredField("rowData");
            ROW_DATA_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private final KuduScanner scanner;
    private final List<Type> columnTypes;
    private final KuduTable table;
    private final Map<Integer, Integer> fieldMapping;
    private RowResultIterator nextRows;
    private RowResult currentRow;

    private long totalBytes;
    private boolean started;

    public KuduRecordCursor(KuduScanner scanner, KuduTable table, List<Type> columnTypes, Map<Integer, Integer> fieldMapping)
    {
        this.scanner = requireNonNull(scanner, "scanner is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.table = requireNonNull(table, "table is null");
        this.fieldMapping = ImmutableMap.copyOf(requireNonNull(fieldMapping, "fieldMapping is null"));
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
        return columnTypes.get(field);
    }

    private int mapping(int field)
    {
        return fieldMapping.get(field);
    }

    /**
     * get next Row/Page
     */
    @Override
    public boolean advanceNextPosition()
    {
        boolean needNextRows = !started || !nextRows.hasNext();

        if (!started) {
            started = true;
        }

        if (needNextRows) {
            currentRow = null;
            try {
                do {
                    if (!scanner.hasMoreRows()) {
                        return false;
                    }

                    nextRows = scanner.nextRows();
                }
                while (!nextRows.hasNext());
                log.debug("Fetched " + nextRows.getNumRows() + " rows");
            }
            catch (KuduException e) {
                throw new RuntimeException(e);
            }
        }

        currentRow = nextRows.next();
        totalBytes += getRowLength();
        return true;
    }

    private org.apache.kudu.util.Slice getCurrentRowRawData()
    {
        if (currentRow != null) {
            try {
                return (org.apache.kudu.util.Slice) ROW_DATA_FIELD.get(currentRow);
            }
            catch (IllegalAccessException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
        return null;
    }

    private int getRowLength()
    {
        org.apache.kudu.util.Slice rawData = getCurrentRowRawData();
        if (rawData != null) {
            return rawData.length();
        }
        return columnTypes.size();
    }

    @Override
    public boolean getBoolean(int field)
    {
        int index = mapping(field);
        return TypeHelper.getBoolean(columnTypes.get(field), currentRow, index);
    }

    @Override
    public long getLong(int field)
    {
        int index = mapping(field);
        return TypeHelper.getLong(columnTypes.get(field), currentRow, index);
    }

    @Override
    public double getDouble(int field)
    {
        int index = mapping(field);
        return TypeHelper.getDouble(columnTypes.get(field), currentRow, index);
    }

    @Override
    public Slice getSlice(int field)
    {
        int index = mapping(field);
        if (index == ROW_ID_POSITION) {
            PartialRow partialRow = buildPrimaryKey();
            return Slices.wrappedBuffer(KeyEncoderAccessor.encodePrimaryKey(partialRow));
        }
        return TypeHelper.getSlice(columnTypes.get(field), currentRow, index);
    }

    @Override
    public Object getObject(int field)
    {
        int index = mapping(field);
        return TypeHelper.getObject(columnTypes.get(field), currentRow, index);
    }

    @Override
    public boolean isNull(int field)
    {
        int mappedField = mapping(field);
        return mappedField >= 0 && currentRow.isNull(mappedField);
    }

    private PartialRow buildPrimaryKey()
    {
        PartialRow row = new PartialRow(table.getSchema());
        RowHelper.copyPrimaryKey(table.getSchema(), currentRow, row);
        return row;
    }

    @Override
    public void close()
    {
        currentRow = null;
        nextRows = null;
    }
}
