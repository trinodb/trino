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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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

    public TpchRecordSet(
            Iterable<E> rows,
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
        private final List<Type> columnTypes;
        private final List<ColumnMatcher<E>> predicateMatchers;
        private final TpchColumn<E>[] columnArray;
        private final LongColumnKind[] longColumnKinds;
        private E row;
        private boolean closed;

        @SuppressWarnings("unchecked")
        public TpchRecordCursor(Iterator<E> rows, TpchTable<E> table, List<TpchColumn<E>> columns, List<Type> columnTypes, TupleDomain<ColumnHandle> predicate)
        {
            this.rows = requireNonNull(rows, "rows is null");
            requireNonNull(table, "table is null");
            requireNonNull(columns, "columns is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
            requireNonNull(predicate, "predicate is null");
            this.predicateMatchers = createMatchers(table, predicate);
            this.closed = predicate.isNone();
            this.columnArray = columns.toArray(new TpchColumn[0]);
            this.longColumnKinds = new LongColumnKind[columnArray.length];
            for (int field = 0; field < columnArray.length; field++) {
                longColumnKinds[field] = LongColumnKind.of(columnArray[field].getType().getBase());
            }
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
            return longColumnKinds[field].read(columnArray[field], row);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return columnArray[field].getDouble(row);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            return Slices.utf8Slice(columnArray[field].getString(row));
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
            for (ColumnMatcher<E> matcher : predicateMatchers) {
                if (!matcher.matches(row)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static <E extends TpchEntity> List<ColumnMatcher<E>> createMatchers(TpchTable<E> table, TupleDomain<ColumnHandle> predicate)
    {
        if (predicate.isAll() || predicate.isNone()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<ColumnMatcher<E>> matchers = ImmutableList.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
            TpchColumnHandle columnHandle = (TpchColumnHandle) entry.getKey();
            TpchColumn<E> column = table.getColumn(columnHandle.columnName());
            if (!TpchMetadata.includesAllValues(column, entry.getValue())) {
                matchers.add(createMatcher(column, columnHandle.type(), entry.getValue()));
            }
        }
        return matchers.build();
    }

    private static <E extends TpchEntity> ColumnMatcher<E> createMatcher(TpchColumn<E> column, Type type, Domain domain)
    {
        // Rows never contain nulls, so only the values of the domain matter
        if (type.getJavaType() == Slice.class && domain.getValues().isDiscreteSet()) {
            ImmutableSet.Builder<String> allowedValues = ImmutableSet.builder();
            for (Object value : domain.getValues().getDiscreteSet()) {
                allowedValues.add(((Slice) value).toStringUtf8());
            }
            return new AllowedStringsMatcher<>(column, allowedValues.build());
        }
        return new DomainMatcher<>(column, type, domain);
    }

    private enum LongColumnKind
    {
        DATE,
        INTEGER,
        IDENTIFIER;

        static LongColumnKind of(TpchColumnType.Base base)
        {
            return switch (base) {
                case DATE -> DATE;
                case INTEGER -> INTEGER;
                case IDENTIFIER, DOUBLE, VARCHAR -> IDENTIFIER;
            };
        }

        <E extends TpchEntity> long read(TpchColumn<E> column, E row)
        {
            return switch (this) {
                case DATE -> column.getDate(row);
                case INTEGER -> column.getInteger(row);
                case IDENTIFIER -> column.getIdentifier(row);
            };
        }
    }

    private interface ColumnMatcher<E extends TpchEntity>
    {
        boolean matches(E row);
    }

    private static class AllowedStringsMatcher<E extends TpchEntity>
            implements ColumnMatcher<E>
    {
        private final TpchColumn<E> column;
        private final Set<String> allowedValues;

        private AllowedStringsMatcher(TpchColumn<E> column, Set<String> allowedValues)
        {
            this.column = requireNonNull(column, "column is null");
            this.allowedValues = requireNonNull(allowedValues, "allowedValues is null");
        }

        @Override
        public boolean matches(E row)
        {
            return allowedValues.contains(column.getString(row));
        }
    }

    private static class DomainMatcher<E extends TpchEntity>
            implements ColumnMatcher<E>
    {
        private final TpchColumn<E> column;
        private final Type type;
        private final Domain domain;
        private final LongColumnKind longColumnKind;

        private DomainMatcher(TpchColumn<E> column, Type type, Domain domain)
        {
            this.column = requireNonNull(column, "column is null");
            this.type = requireNonNull(type, "type is null");
            this.domain = requireNonNull(domain, "domain is null");
            this.longColumnKind = LongColumnKind.of(column.getType().getBase());
        }

        @Override
        public boolean matches(E row)
        {
            return domain.includesNullableValue(trinoObject(row));
        }

        private Object trinoObject(E row)
        {
            if (type.getJavaType() == long.class) {
                return longColumnKind.read(column, row);
            }
            if (type.getJavaType() == double.class) {
                return column.getDouble(row);
            }
            if (type.getJavaType() == Slice.class) {
                return Slices.utf8Slice(column.getString(row));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported column type %s", type.getDisplayName()));
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
