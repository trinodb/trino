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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.client.StatementStats;
import io.trino.client.Warning;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.type.JsonType.JSON;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MaterializedResult
        implements Iterable<MaterializedRow>
{
    public static final int DEFAULT_PRECISION = 5;

    private final List<MaterializedRow> rows;
    private final List<Type> types;
    private final List<String> columnNames;
    private final Map<String, String> setSessionProperties;
    private final Set<String> resetSessionProperties;
    private final Optional<String> updateType;
    private final OptionalLong updateCount;
    private final List<Warning> warnings;
    private final Optional<StatementStats> statementStats;

    public MaterializedResult(List<MaterializedRow> rows, List<? extends Type> types)
    {
        this(rows, types, Optional.empty());
    }

    public MaterializedResult(List<MaterializedRow> rows, List<? extends Type> types, Optional<List<String>> columnNames)
    {
        this(rows, types, columnNames.orElse(ImmutableList.of()), ImmutableMap.of(), ImmutableSet.of(), Optional.empty(), OptionalLong.empty(), ImmutableList.of(), Optional.empty());
    }

    public MaterializedResult(
            List<MaterializedRow> rows,
            List<? extends Type> types,
            List<String> columnNames,
            Map<String, String> setSessionProperties,
            Set<String> resetSessionProperties,
            Optional<String> updateType,
            OptionalLong updateCount,
            List<Warning> warnings,
            Optional<StatementStats> statementStats)
    {
        this.rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.setSessionProperties = ImmutableMap.copyOf(requireNonNull(setSessionProperties, "setSessionProperties is null"));
        this.resetSessionProperties = ImmutableSet.copyOf(requireNonNull(resetSessionProperties, "resetSessionProperties is null"));
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.updateCount = requireNonNull(updateCount, "updateCount is null");
        this.warnings = requireNonNull(warnings, "warnings is null");
        this.statementStats = requireNonNull(statementStats, "statementStats is null");
    }

    public int getRowCount()
    {
        return rows.size();
    }

    @Override
    public Iterator<MaterializedRow> iterator()
    {
        return rows.iterator();
    }

    public List<MaterializedRow> getMaterializedRows()
    {
        return rows;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public List<String> getColumnNames()
    {
        checkState(!columnNames.isEmpty(), "Column names are unknown");
        return columnNames;
    }

    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    public OptionalLong getUpdateCount()
    {
        return updateCount;
    }

    public List<Warning> getWarnings()
    {
        return warnings;
    }

    public Optional<StatementStats> getStatementStats()
    {
        return statementStats;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedResult o = (MaterializedResult) obj;
        return Objects.equals(types, o.types) &&
                Objects.equals(rows, o.rows) &&
                Objects.equals(setSessionProperties, o.setSessionProperties) &&
                Objects.equals(resetSessionProperties, o.resetSessionProperties) &&
                Objects.equals(updateType, o.updateType) &&
                Objects.equals(updateCount, o.updateCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rows, types, setSessionProperties, resetSessionProperties, updateType, updateCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rows", rows)
                .add("types", types)
                .add("setSessionProperties", setSessionProperties)
                .add("resetSessionProperties", resetSessionProperties)
                .add("updateType", updateType.orElse(null))
                .add("updateCount", updateCount.isPresent() ? updateCount.getAsLong() : null)
                .omitNullValues()
                .toString();
    }

    public MaterializedResult exceptColumns(String... columnNamesToExclude)
    {
        validateIfColumnsPresent(columnNamesToExclude);
        checkArgument(columnNamesToExclude.length > 0, "At least one column must be excluded");
        checkArgument(columnNamesToExclude.length < getColumnNames().size(), "All columns cannot be excluded");
        return projected(((Predicate<String>) Set.of(columnNamesToExclude)::contains).negate());
    }

    public MaterializedResult project(String... columnNamesToInclude)
    {
        validateIfColumnsPresent(columnNamesToInclude);
        checkArgument(columnNamesToInclude.length > 0, "At least one column must be projected");
        return projected(Set.of(columnNamesToInclude)::contains);
    }

    private void validateIfColumnsPresent(String... columns)
    {
        Set<String> columnNames = ImmutableSet.copyOf(getColumnNames());
        for (String column : columns) {
            checkArgument(columnNames.contains(column), "[%s] column is not present in %s".formatted(column, columnNames));
        }
    }

    private MaterializedResult projected(Predicate<String> columnFilter)
    {
        List<String> columnNames = getColumnNames();
        Map<Integer, String> columnsIndexToNameMap = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            if (columnFilter.test(columnName)) {
                columnsIndexToNameMap.put(i, columnName);
            }
        }

        return new MaterializedResult(
                getMaterializedRows().stream()
                        .map(row -> new MaterializedRow(
                                row.getPrecision(),
                                columnsIndexToNameMap.keySet().stream()
                                        .map(row::getField)
                                        .collect(toList()))) // values are nullable
                        .collect(toImmutableList()),
                columnsIndexToNameMap.keySet().stream()
                        .map(getTypes()::get)
                        .collect(toImmutableList()));
    }

    public Stream<Object> getOnlyColumn()
    {
        checkState(types.size() == 1, "result set must have exactly one column");
        return rows.stream()
                .map(row -> row.getField(0));
    }

    public Set<Object> getOnlyColumnAsSet()
    {
        return getOnlyColumn()
                // values are nullable
                .collect(toSet());
    }

    public Object getOnlyValue()
    {
        checkState(rows.size() == 1, "result set must have exactly one row");
        checkState(types.size() == 1, "result set must have exactly one column");
        return rows.get(0).getField(0);
    }

    public Page toPage()
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        for (MaterializedRow row : rows) {
            appendToPage(pageBuilder, row);
        }
        return pageBuilder.build();
    }

    private static void appendToPage(PageBuilder pageBuilder, MaterializedRow row)
    {
        for (int field = 0; field < row.getFieldCount(); field++) {
            Type type = pageBuilder.getType(field);
            Object value = row.getField(field);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(field);
            writeValue(type, blockBuilder, value);
        }
        pageBuilder.declarePosition();
    }

    private static void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (BIGINT.equals(type)) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (INTEGER.equals(type)) {
            type.writeLong(blockBuilder, (Integer) value);
        }
        else if (SMALLINT.equals(type)) {
            type.writeLong(blockBuilder, (Short) value);
        }
        else if (TINYINT.equals(type)) {
            type.writeLong(blockBuilder, (Byte) value);
        }
        else if (REAL.equals(type)) {
            type.writeLong(blockBuilder, floatToRawIntBits(((Float) value)));
        }
        else if (DOUBLE.equals(type)) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (BOOLEAN.equals(type)) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (JSON.equals(type)) {
            type.writeSlice(blockBuilder, Slices.utf8Slice((String) value));
        }
        else if (type instanceof VarcharType) {
            type.writeSlice(blockBuilder, Slices.utf8Slice((String) value));
        }
        else if (type instanceof CharType) {
            type.writeSlice(blockBuilder, Slices.utf8Slice((String) value));
        }
        else if (VARBINARY.equals(type)) {
            type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) value));
        }
        else if (DATE.equals(type)) {
            int days = ((SqlDate) value).getDays();
            type.writeLong(blockBuilder, days);
        }
        else if (type instanceof TimeType) {
            SqlTime time = (SqlTime) value;
            type.writeLong(blockBuilder, time.getPicos());
        }
        else if (type instanceof TimeWithTimeZoneType) {
            long nanos = roundDiv(((SqlTimeWithTimeZone) value).getPicos(), PICOSECONDS_PER_NANOSECOND);
            int offsetMinutes = ((SqlTimeWithTimeZone) value).getOffsetMinutes();
            type.writeLong(blockBuilder, packTimeWithTimeZone(nanos, offsetMinutes));
        }
        else if (type instanceof TimestampType) {
            long micros = ((SqlTimestamp) value).getEpochMicros();
            if (((TimestampType) type).getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
                type.writeLong(blockBuilder, micros);
            }
            else {
                type.writeObject(blockBuilder, new LongTimestamp(micros, ((SqlTimestamp) value).getPicosOfMicros()));
            }
        }
        else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            long millisUtc = ((SqlTimestampWithTimeZone) value).getMillisUtc();
            TimeZoneKey timeZoneKey = ((SqlTimestampWithTimeZone) value).getTimeZoneKey();
            type.writeLong(blockBuilder, packDateTimeWithZone(millisUtc, timeZoneKey));
        }
        else if (type instanceof ArrayType) {
            List<?> list = (List<?>) value;
            Type elementType = ((ArrayType) type).getElementType();
            BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object element : list) {
                writeValue(elementType, arrayBlockBuilder, element);
            }
            blockBuilder.closeEntry();
        }
        else if (type instanceof MapType) {
            Map<?, ?> map = (Map<?, ?>) value;
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
            for (Entry<?, ?> entry : map.entrySet()) {
                writeValue(keyType, mapBlockBuilder, entry.getKey());
                writeValue(valueType, mapBlockBuilder, entry.getValue());
            }
            blockBuilder.closeEntry();
        }
        else if (type instanceof RowType) {
            List<?> row = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
            for (int field = 0; field < row.size(); field++) {
                writeValue(fieldTypes.get(field), rowBlockBuilder, row.get(field));
            }
            blockBuilder.closeEntry();
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    /**
     * Converts this {@link MaterializedResult} to a new one, representing the data using the same type domain as returned by {@code TestingTrinoClient}.
     */
    public MaterializedResult toTestTypes()
    {
        return new MaterializedResult(
                rows.stream()
                        .map(MaterializedResult::convertToTestTypes)
                        .collect(toImmutableList()),
                types,
                columnNames,
                setSessionProperties,
                resetSessionProperties,
                updateType,
                updateCount,
                warnings,
                statementStats);
    }

    private static MaterializedRow convertToTestTypes(MaterializedRow trinoRow)
    {
        List<Object> convertedValues = new ArrayList<>();
        for (int field = 0; field < trinoRow.getFieldCount(); field++) {
            Object trinoValue = trinoRow.getField(field);
            Object convertedValue;
            if (trinoValue instanceof SqlDate) {
                convertedValue = LocalDate.ofEpochDay(((SqlDate) trinoValue).getDays());
            }
            else if (trinoValue instanceof SqlTime) {
                convertedValue = DateTimeFormatter.ISO_LOCAL_TIME.parse(trinoValue.toString(), LocalTime::from);
            }
            else if (trinoValue instanceof SqlTimeWithTimeZone) {
                long nanos = roundDiv(((SqlTimeWithTimeZone) trinoValue).getPicos(), PICOSECONDS_PER_NANOSECOND);
                int offsetMinutes = ((SqlTimeWithTimeZone) trinoValue).getOffsetMinutes();
                convertedValue = OffsetTime.of(LocalTime.ofNanoOfDay(nanos), ZoneOffset.ofTotalSeconds(offsetMinutes * 60));
            }
            else if (trinoValue instanceof SqlTimestamp) {
                convertedValue = ((SqlTimestamp) trinoValue).toLocalDateTime();
            }
            else if (trinoValue instanceof SqlTimestampWithTimeZone) {
                convertedValue = ((SqlTimestampWithTimeZone) trinoValue).toZonedDateTime();
            }
            else if (trinoValue instanceof SqlDecimal) {
                convertedValue = ((SqlDecimal) trinoValue).toBigDecimal();
            }
            else {
                convertedValue = trinoValue;
            }
            convertedValues.add(convertedValue);
        }
        return new MaterializedRow(trinoRow.getPrecision(), convertedValues);
    }

    public static MaterializedResult materializeSourceDataStream(Session session, ConnectorPageSource pageSource, List<Type> types)
    {
        return materializeSourceDataStream(session.toConnectorSession(), pageSource, types);
    }

    public static MaterializedResult materializeSourceDataStream(ConnectorSession session, ConnectorPageSource pageSource, List<Type> types)
    {
        MaterializedResult.Builder builder = resultBuilder(session, types);
        while (!pageSource.isFinished()) {
            Page outputPage = pageSource.getNextPage();
            if (outputPage == null) {
                continue;
            }
            builder.page(outputPage);
        }
        return builder.build();
    }

    public static Builder resultBuilder(Session session, Type... types)
    {
        return resultBuilder(session.toConnectorSession(), types);
    }

    public static Builder resultBuilder(Session session, Iterable<? extends Type> types)
    {
        return resultBuilder(session.toConnectorSession(), types);
    }

    public static Builder resultBuilder(ConnectorSession session, Type... types)
    {
        return resultBuilder(session, ImmutableList.copyOf(types));
    }

    public static Builder resultBuilder(ConnectorSession session, Iterable<? extends Type> types)
    {
        return new Builder(session, ImmutableList.copyOf(types));
    }

    public static class Builder
    {
        private final ConnectorSession session;
        private final List<Type> types;
        private final ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
        private Optional<List<String>> columnNames = Optional.empty();

        Builder(ConnectorSession session, List<Type> types)
        {
            this.session = session;
            this.types = ImmutableList.copyOf(types);
        }

        public synchronized Builder rows(List<MaterializedRow> rows)
        {
            this.rows.addAll(rows);
            return this;
        }

        public synchronized Builder row(Object... values)
        {
            rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            return this;
        }

        public synchronized Builder rows(Object[][] rows)
        {
            for (Object[] row : rows) {
                row(row);
            }
            return this;
        }

        public synchronized Builder pages(Iterable<Page> pages)
        {
            for (Page page : pages) {
                this.page(page);
            }

            return this;
        }

        public synchronized Builder page(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(page.getChannelCount() == types.size(), "Expected a page with %s columns, but got %s columns", types.size(), page.getChannelCount());

            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(session, block, position));
                }
                values = Collections.unmodifiableList(values);

                rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            }
            return this;
        }

        public synchronized Builder columnNames(List<String> columnNames)
        {
            this.columnNames = Optional.of(ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null")));
            return this;
        }

        public synchronized MaterializedResult build()
        {
            return new MaterializedResult(rows.build(), types, columnNames);
        }
    }
}
