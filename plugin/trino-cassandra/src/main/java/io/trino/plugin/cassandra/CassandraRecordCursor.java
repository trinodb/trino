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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.cassandra.CassandraType.Kind;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validColumnName;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class CassandraRecordCursor
        implements RecordCursor
{
    private final List<String> columnNames;
    private final List<CassandraType> cassandraTypes;
    private final CassandraTypeManager cassandraTypeManager;
    private final ResultSet rs;
    private Row currentRow;

    public CassandraRecordCursor(CassandraSession cassandraSession, CassandraTypeManager cassandraTypeManager, List<String> columnNames, List<CassandraType> cassandraTypes, String cql)
    {
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.cassandraTypes = cassandraTypes;
        checkArgument(columnNames.size() == cassandraTypes.size(), "columnNames and cassandraTypes sizes don't match");
        this.cassandraTypeManager = cassandraTypeManager;
        rs = cassandraSession.execute(cql);
        currentRow = null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        Row row = rs.one();
        if (row != null) {
            currentRow = row;
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean getBoolean(int i)
    {
        return currentRow.getBool(validColumnName(columnNames.get(i)));
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public double getDouble(int i)
    {
        String columnName = validColumnName(columnNames.get(i));
        switch (getCassandraType(i).getKind()) {
            case DOUBLE:
                return currentRow.getDouble(columnName);
            case FLOAT:
                return currentRow.getFloat(columnName);
            case DECIMAL:
                return currentRow.getBigDecimal(columnName).doubleValue();
            default:
                throw new IllegalStateException("Cannot retrieve double for " + getCassandraType(i));
        }
    }

    @Override
    public long getLong(int i)
    {
        String columnName = validColumnName(columnNames.get(i));
        switch (getCassandraType(i).getKind()) {
            case INT:
                return currentRow.getInt(columnName);
            case SMALLINT:
                return currentRow.getShort(columnName);
            case TINYINT:
                return currentRow.getByte(columnName);
            case BIGINT:
            case COUNTER:
                return currentRow.getLong(columnName);
            case TIME:
                return currentRow.getLocalTime(columnName).toNanoOfDay() * PICOSECONDS_PER_NANOSECOND;
            case TIMESTAMP:
                return packDateTimeWithZone(currentRow.getInstant(columnName).toEpochMilli(), TimeZoneKey.UTC_KEY);
            case DATE:
                return currentRow.getLocalDate(columnName).toEpochDay();
            case FLOAT:
                return floatToRawIntBits(currentRow.getFloat(columnName));
            default:
                throw new IllegalStateException("Cannot retrieve long for " + getCassandraType(i));
        }
    }

    private CassandraType getCassandraType(int i)
    {
        return cassandraTypes.get(i);
    }

    @Override
    public Slice getSlice(int i)
    {
        if (getCassandraType(i).getKind() == Kind.TIMESTAMP) {
            throw new IllegalArgumentException("Timestamp column can not be accessed with getSlice");
        }
        NullableValue value = cassandraTypeManager.getColumnValue(cassandraTypes.get(i), currentRow, currentRow.firstIndexOf(validColumnName(columnNames.get(i))));
        if (value.getValue() instanceof Slice) {
            return (Slice) value.getValue();
        }
        return utf8Slice(value.getValue().toString());
    }

    @Override
    public Object getObject(int i)
    {
        CassandraType cassandraType = cassandraTypes.get(i);
        switch (cassandraType.getKind()) {
            case TUPLE:
            case UDT:
                return cassandraTypeManager.getColumnValue(cassandraType, currentRow, currentRow.firstIndexOf(validColumnName(columnNames.get(i)))).getValue();
            default:
                throw new IllegalArgumentException("getObject cannot be called for " + cassandraType);
        }
    }

    @Override
    public Type getType(int i)
    {
        return getCassandraType(i).getTrinoType();
    }

    @Override
    public boolean isNull(int i)
    {
        String columnName = validColumnName(columnNames.get(i));
        if (getCassandraType(i).getKind() == Kind.TIMESTAMP) {
            return currentRow.getInstant(columnName) == null;
        }
        return currentRow.isNull(columnName);
    }
}
