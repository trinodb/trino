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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.tpch.TpchMetadata.getTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.tpch.TpchColumnTypes.IDENTIFIER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TpchRecordSet<E extends TpchEntity>
        implements RecordSet
{
    public static <E extends TpchEntity> RecordSet getRecordSet(
            TpchTable<E> table,
            List<? extends ColumnHandle> columns,
            double scaleFactor,
            int partNumber,
            int totalParts,
            TupleDomain<ColumnHandle> predicate,
            DecimalTypeMapping decimalTypeMapping)
    {
        ImmutableList.Builder<TpchColumn<E>> builder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            String columnName = ((TpchColumnHandle) column).columnName();
            if (columnName.equalsIgnoreCase(TpchMetadata.ROW_NUMBER_COLUMN_NAME)) {
                builder.add(new RowNumberTpchColumn<>());
            }
            else {
                builder.add(table.getColumn(columnName));
            }
        }

        return createTpchRecordSet(table, builder.build(), decimalTypeMapping, scaleFactor, partNumber + 1, totalParts, predicate);
    }

    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(TpchTable<E> table, double scaleFactor)
    {
        return createTpchRecordSet(table, table.getColumns(), DecimalTypeMapping.DOUBLE, scaleFactor, 1, 1, TupleDomain.all());
    }

    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(
            TpchTable<E> table,
            DecimalTypeMapping decimalTypeMapping,
            double scaleFactor,
            int part,
            int partCount,
            TupleDomain<ColumnHandle> predicate)
    {
        return createTpchRecordSet(table, table.getColumns(), decimalTypeMapping, scaleFactor, part, partCount, predicate);
    }

    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(
            TpchTable<E> table,
            List<TpchColumn<E>> columns,
            DecimalTypeMapping decimalTypeMapping,
            double scaleFactor,
            int part,
            int partCount,
            TupleDomain<ColumnHandle> predicate)
    {
        return new TpchRecordSet<>(table.createGenerator(scaleFactor, part, partCount), table, columns, decimalTypeMapping, predicate);
    }

    private final Iterable<E> rows;
    private final TpchTable<E> table;
    private final List<TpchColumn<E>> columns;
    private final List<Type> columnTypes;
    private final TupleDomain<ColumnHandle> predicate;

    public TpchRecordSet(Iterable<E> rows,
            TpchTable<E> table,
            List<TpchColumn<E>> columns,
            DecimalTypeMapping decimalTypeMapping,
            TupleDomain<ColumnHandle> predicate)
    {
        this.rows = requireNonNull(rows, "rows is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnTypes = columns.stream()
                .map(column -> getTrinoType(column, decimalTypeMapping))
                .collect(toImmutableList());
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TpchRecordCursor<>(rows.iterator(), table, columns, columnTypes, predicate);
    }

    public static final class TpchRecordCursor<E extends TpchEntity>
            implements RecordCursor
    {
        private final Iterator<E> rows;
        private final TpchTable<E> table;
        private final List<TpchColumn<E>> columns;
        private final List<Type> columnTypes;
        private final TupleDomain<ColumnHandle> predicate;
        private E row;
        private boolean closed;

        public TpchRecordCursor(Iterator<E> rows, TpchTable<E> table, List<TpchColumn<E>> columns, List<Type> columnTypes, TupleDomain<ColumnHandle> predicate)
        {
            this.rows = requireNonNull(rows, "rows is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
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
            return columnTypes.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (!closed && rows.hasNext()) {
                row = rows.next();
                if (rowMatchesPredicate()) {
                    return true;
                }
            }

            closed = true;
            row = null;
            return false;
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
            TpchColumn<E> tpchColumn = getTpchColumn(field);
            if (tpchColumn.getType().getBase() == TpchColumnType.Base.DATE) {
                return tpchColumn.getDate(row);
            }
            if (tpchColumn.getType().getBase() == TpchColumnType.Base.INTEGER) {
                return tpchColumn.getInteger(row);
            }
            return tpchColumn.getIdentifier(row);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return getTpchColumn(field).getDouble(row);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            return Slices.utf8Slice(getTpchColumn(field).getString(row));
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            return false;
        }

        @Override
        public void close()
        {
            row = null;
            closed = true;
        }

        private boolean rowMatchesPredicate()
        {
            if (predicate.isAll()) {
                return true;
            }
            if (predicate.isNone()) {
                return false;
            }

            Map<ColumnHandle, NullableValue> rowMap = predicate.getDomains().get().keySet().stream()
                    .collect(toImmutableMap(
                            column -> column,
                            column -> {
                                TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) column;
                                Type type = tpchColumnHandle.type();
                                TpchColumn<E> tpchColumn = table.getColumn(tpchColumnHandle.columnName());
                                return NullableValue.of(type, getTrinoObject(tpchColumn, type));
                            }));

            TupleDomain<ColumnHandle> rowTupleDomain = TupleDomain.fromFixedValues(rowMap);

            return predicate.contains(rowTupleDomain);
        }

        private Object getTrinoObject(TpchColumn<E> column, Type type)
        {
            if (type.getJavaType() == long.class) {
                return column.getInteger(row);
            }
            if (type.getJavaType() == double.class) {
                return column.getDouble(row);
            }
            if (type.getJavaType() == Slice.class) {
                return Slices.utf8Slice(column.getString(row));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported column type %s", type.getDisplayName()));
        }

        private TpchColumn<E> getTpchColumn(int field)
        {
            return columns.get(field);
        }
    }

    private static class RowNumberTpchColumn<E extends TpchEntity>
            implements TpchColumn<E>
    {
        @Override
        public String getColumnName()
        {
            return TpchMetadata.ROW_NUMBER_COLUMN_NAME;
        }

        @Override
        public TpchColumnType getType()
        {
            return IDENTIFIER;
        }

        @Override
        public double getDouble(E entity)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getIdentifier(E entity)
        {
            return entity.rowNumber();
        }

        @Override
        public int getInteger(E entity)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getString(E entity)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getDate(E entity)
        {
            throw new UnsupportedOperationException();
        }
    }
}
