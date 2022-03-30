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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.airlift.slice.Slice;
import io.trino.plugin.cassandra.CassandraType.Kind;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static java.lang.Float.floatToRawIntBits;

public class CassandraRecordCursor
        implements RecordCursor
{
    private final List<CassandraType> cassandraTypes;
    private final ResultSet rs;
    private Row currentRow;

    public CassandraRecordCursor(CassandraSession cassandraSession, List<CassandraType> cassandraTypes, String cql)
    {
        this.cassandraTypes = cassandraTypes;
        rs = cassandraSession.execute(cql);
        currentRow = null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!rs.isExhausted()) {
            currentRow = rs.one();
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
        return currentRow.getBool(i);
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
        switch (getCassandraType(i).getKind()) {
            case DOUBLE:
                return currentRow.getDouble(i);
            case FLOAT:
                return currentRow.getFloat(i);
            case DECIMAL:
                return currentRow.getDecimal(i).doubleValue();
            default:
                throw new IllegalStateException("Cannot retrieve double for " + getCassandraType(i));
        }
    }

    @Override
    public long getLong(int i)
    {
        switch (getCassandraType(i).getKind()) {
            case INT:
                return currentRow.getInt(i);
            case SMALLINT:
                return currentRow.getShort(i);
            case TINYINT:
                return currentRow.getByte(i);
            case BIGINT:
            case COUNTER:
                return currentRow.getLong(i);
            case TIMESTAMP:
                return packDateTimeWithZone(currentRow.getTimestamp(i).getTime(), TimeZoneKey.UTC_KEY);
            case DATE:
                return currentRow.getDate(i).getDaysSinceEpoch();
            case FLOAT:
                return floatToRawIntBits(currentRow.getFloat(i));
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
        NullableValue value = cassandraTypes.get(i).getColumnValue(currentRow, i);
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
                return cassandraType.getColumnValue(currentRow, i).getValue();
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
        if (getCassandraType(i).getKind() == Kind.TIMESTAMP) {
            return currentRow.getTimestamp(i) == null;
        }
        return currentRow.isNull(i);
    }
}
