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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.trino.plugin.hive.util.ForwardingRecordCursor;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;

import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.EMPTY;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.REGULAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveRecordCursor
        extends ForwardingRecordCursor
{
    private final RecordCursor delegate;

    private final List<ColumnMapping> columnMappings;
    private final Type[] types;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;

    public HiveRecordCursor(List<ColumnMapping> columnMappings, RecordCursor delegate)
    {
        requireNonNull(columnMappings, "columnMappings is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;

        int size = columnMappings.size();

        this.types = new Type[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);

            if (columnMapping.getKind() == EMPTY) {
                nulls[columnIndex] = true;
            }
            if (columnMapping.getKind() == PREFILLED) {
                Object prefilledValue = columnMapping.getPrefilledValue().getValue();
                String name = columnMapping.getHiveColumnHandle().getName();
                Type type = columnMapping.getHiveColumnHandle().getType();
                types[columnIndex] = type;

                if (prefilledValue == null) {
                    nulls[columnIndex] = true;
                }
                else if (BOOLEAN.equals(type)) {
                    booleans[columnIndex] = (boolean) prefilledValue;
                }
                else if (TINYINT.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (SMALLINT.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (INTEGER.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (BIGINT.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (REAL.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (DOUBLE.equals(type)) {
                    doubles[columnIndex] = (double) prefilledValue;
                }
                else if (type instanceof VarcharType) {
                    slices[columnIndex] = (Slice) prefilledValue;
                }
                else if (type instanceof CharType) {
                    slices[columnIndex] = (Slice) prefilledValue;
                }
                else if (DATE.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (TIMESTAMP_MILLIS.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (isShortDecimal(type)) {
                    longs[columnIndex] = (long) prefilledValue;
                }
                else if (isLongDecimal(type)) {
                    objects[columnIndex] = prefilledValue;
                }
                else {
                    throw new TrinoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
                }
            }
        }
    }

    @Override
    protected RecordCursor delegate()
    {
        return delegate;
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean getBoolean(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getBoolean(columnMapping.getIndex());
        }
        return booleans[field];
    }

    @Override
    public long getLong(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getLong(columnMapping.getIndex());
        }
        return longs[field];
    }

    @Override
    public double getDouble(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getDouble(columnMapping.getIndex());
        }
        return doubles[field];
    }

    @Override
    public Slice getSlice(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getSlice(columnMapping.getIndex());
        }
        return slices[field];
    }

    @Override
    public Object getObject(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getObject(columnMapping.getIndex());
        }
        return objects[field];
    }

    @Override
    public boolean isNull(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.isNull(columnMapping.getIndex());
        }
        return nulls[field];
    }

    @VisibleForTesting
    RecordCursor getRegularColumnRecordCursor()
    {
        return delegate;
    }
}
