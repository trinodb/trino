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
import io.trino.Session;
import io.trino.client.StatementStats;
import io.trino.client.Warning;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;

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
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
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
    private final Optional<String> queryDataEncoding;
    private final Map<String, String> setSessionProperties;
    private final Set<String> resetSessionProperties;
    private final Optional<String> updateType;
    private final OptionalLong updateCount;
    private final List<Warning> warnings;
    private final Optional<StatementStats> statementStats;

    public MaterializedResult(List<MaterializedRow> rows, List<? extends Type> types)
    {
        this(rows, types, Optional.empty(), Optional.empty());
    }

    public MaterializedResult(List<MaterializedRow> rows, List<? extends Type> types, Optional<List<String>> columnNames, Optional<String> queryDataEncoding)
    {
        this(rows, types, columnNames.orElse(ImmutableList.of()), queryDataEncoding, ImmutableMap.of(), ImmutableSet.of(), Optional.empty(), OptionalLong.empty(), ImmutableList.of(), Optional.empty());
    }

    public MaterializedResult(
            List<MaterializedRow> rows,
            List<? extends Type> types,
            List<String> columnNames,
            Optional<String> queryDataEncoding,
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
        this.queryDataEncoding = requireNonNull(queryDataEncoding, "queryDataEncoding is null");
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

    public Optional<String> getQueryDataEncoding()
    {
        return queryDataEncoding;
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
                queryDataEncoding,
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
            Object convertedValue = switch (trinoValue) {
                case null -> null;
                case SqlDate sqlDate -> LocalDate.ofEpochDay(sqlDate.getDays());
                case SqlTime _ -> DateTimeFormatter.ISO_LOCAL_TIME.parse(trinoValue.toString(), LocalTime::from);
                case SqlTimeWithTimeZone sqlTimeWithTimeZone -> {
                    long nanos = roundDiv(sqlTimeWithTimeZone.getPicos(), PICOSECONDS_PER_NANOSECOND);
                    int offsetMinutes = sqlTimeWithTimeZone.getOffsetMinutes();
                    yield OffsetTime.of(LocalTime.ofNanoOfDay(nanos), ZoneOffset.ofTotalSeconds(offsetMinutes * 60));
                }
                case SqlTimestamp sqlTimestamp -> sqlTimestamp.toLocalDateTime();
                case SqlTimestampWithTimeZone sqlTimestampWithTimeZone -> sqlTimestampWithTimeZone.toZonedDateTime();
                case SqlDecimal sqlDecimal -> sqlDecimal.toBigDecimal();
                default -> trinoValue;
            };
            convertedValues.add(convertedValue);
        }
        return new MaterializedRow(trinoRow.getPrecision(), convertedValues);
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
        private Optional<String> queryDataEncoding = Optional.empty();
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

        public synchronized Builder queryDataEncoding(String encoding)
        {
            this.queryDataEncoding = Optional.of(requireNonNull(encoding, "encoding is null"));
            return this;
        }

        public synchronized MaterializedResult build()
        {
            return new MaterializedResult(rows.build(), types, columnNames, queryDataEncoding);
        }
    }
}
