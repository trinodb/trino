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
package io.trino.plugin.tpcds;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.tpcds.Results;
import io.trino.tpcds.column.Column;
import io.trino.tpcds.column.ColumnType;
import org.joda.time.LocalTime;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.tpcds.TpcdsMetadata.getTrinoType;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.spi.type.Decimals.rescale;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public class TpcdsRecordSet
        implements RecordSet
{
    private final Results results;
    private final List<Column> columns;

    private final List<Type> columnTypes;

    public TpcdsRecordSet(Results results, List<Column> columns)
    {
        requireNonNull(results, "results is null");

        this.results = results;
        this.columns = ImmutableList.copyOf(columns);
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (Column column : columns) {
            columnTypes.add(getTrinoType(column.getType()));
        }
        this.columnTypes = columnTypes.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TpcdsRecordCursor(results.iterator(), columns);
    }

    public class TpcdsRecordCursor
            implements RecordCursor
    {
        private final Iterator<List<List<String>>> rows;
        private final List<Column> columns;
        private List<String> row;
        private boolean closed;

        public TpcdsRecordCursor(Iterator<List<List<String>>> rows, List<Column> columns)
        {
            this.rows = requireNonNull(rows);
            this.columns = requireNonNull(columns);
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
        public Type getType(int field)
        {
            return getTrinoType(columns.get(field).getType());
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (closed || !rows.hasNext()) {
                closed = true;
                row = null;
                return false;
            }

            row = rows.next().get(0);
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int field)
        {
            checkState(row != null, "No current row");
            Column column = columns.get(field);
            if (column.getType().getBase() == ColumnType.Base.DATE) {
                return LocalDate.parse(row.get(column.getPosition())).toEpochDay();
            }
            if (column.getType().getBase() == ColumnType.Base.TIME) {
                return (long) LocalTime.parse(row.get(column.getPosition())).getMillisOfDay() * PICOSECONDS_PER_MILLISECOND;
            }
            if (column.getType().getBase() == ColumnType.Base.INTEGER) {
                return parseInt(row.get(column.getPosition()));
            }
            if (column.getType().getBase() == ColumnType.Base.DECIMAL) {
                DecimalParseResult decimalParseResult = Decimals.parse(row.get(column.getPosition()));
                return rescale((Long) decimalParseResult.getObject(), decimalParseResult.getType().getScale(), ((DecimalType) columnTypes.get(field)).getScale());
            }
            return parseLong(row.get(column.getPosition()));
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return parseDouble(row.get(columns.get(field).getPosition()));
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            Column column = columns.get(field);
            if (column.getType().getBase() == ColumnType.Base.DECIMAL) {
                return (Slice) Decimals.parse(row.get(column.getPosition())).getObject();
            }

            Slice characters = Slices.utf8Slice(row.get(columns.get(field).getPosition()));
            if (column.getType().getBase() == ColumnType.Base.CHAR) {
                characters = trimTrailingSpaces(characters);
            }
            return characters;
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(row != null, "No current row");
            return row.get(columns.get(field).getPosition()) == null;
        }

        @Override
        public void close()
        {
            row = null;
            closed = true;
        }
    }
}
