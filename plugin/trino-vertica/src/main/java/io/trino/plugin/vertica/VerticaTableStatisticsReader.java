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
package io.trino.plugin.vertica;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class VerticaTableStatisticsReader
{
    private static final Logger log = Logger.get(VerticaTableStatisticsReader.class);

    private static final ObjectMapper OBJECT_MAPPER = new XmlMapper(new JacksonXmlModule())
            .disable(FAIL_ON_IGNORED_PROPERTIES, FAIL_ON_UNKNOWN_PROPERTIES);

    // We don't know null fraction in case of nan and null rows, but having no null fraction will make CBO useless. Assume some arbitrary value.
    private static final Estimate UNKNOWN_NULL_FRACTION_REPLACEMENT = Estimate.of(0.1);

    private VerticaTableStatisticsReader() {}

    public static TableStatistics readTableStatistics(Connection connection, JdbcTableHandle table, Supplier<List<JdbcColumnHandle>> columnSupplier)
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        log.debug("Reading statistics for %s", table);
        try (Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getRowCount(table);
            log.debug("Estimated row count of table %s is %s", table, rowCount);

            if (rowCount == null) {
                // Table not found, or is a view.
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            Schema schema = statisticsDao.getSchemaStatistics(table);
            if (schema == null || schema.tables().size() == 0) {
                return TableStatistics.empty();
            }
            Map<String, ColumnStatistics> columnsStatistics = getOnlyElement(schema.tables()).columns().stream()
                    .collect(toImmutableMap(Column::columnName, Column::stats));
            Map<String, Long> columnsDataSize = statisticsDao.getColumnDataSize(table);

            for (JdbcColumnHandle column : columnSupplier.get()) {
                io.trino.spi.statistics.ColumnStatistics.Builder columnStatisticsBuilder = io.trino.spi.statistics.ColumnStatistics.builder();

                String columnName = column.getColumnName();

                if (columnsDataSize.containsKey(columnName)) {
                    columnStatisticsBuilder.setDataSize(Estimate.of(columnsDataSize.get(columnName)));
                }

                ColumnStatistics columnStatistics = columnsStatistics.get(columnName);
                if (columnStatistics != null) {
                    log.debug("Reading column statistics for %s, %s from index statistics: %s", table, columnName, columnStatistics);

                    Optional<Long> nullsCount = columnStatistics.histogram().category().stream()
                            .filter(category -> category.bound().isNull())
                            .map(category -> category.count().value())
                            .findFirst();

                    long distinctValuesCount = columnStatistics.distinct().value;
                    // Vertica includes NULL values in the distinct values count
                    if (nullsCount.isPresent() || columnStatistics.minValue().isNull() || columnStatistics.maxValue().isNull()) {
                        distinctValuesCount = Math.max(columnStatistics.distinct().value - 1, 0);
                    }

                    columnStatisticsBuilder.setDistinctValuesCount(Estimate.of(distinctValuesCount));
                    if (isNumeric(column.getColumnType())) {
                        columnStatisticsBuilder.setRange(createNumericRange(
                                columnStatistics.minValue().getValue(),
                                columnStatistics.maxValue().getValue()));
                    }

                    columnStatisticsBuilder.setNullsFraction(getNullsFraction(rowCount, nullsCount, columnStatistics));
                }

                tableStatistics.setColumnStatistics(column, columnStatisticsBuilder.build());
            }

            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    private static Estimate getNullsFraction(Long rowCount, Optional<Long> nullsCount, ColumnStatistics columnStatistics)
    {
        if (nullsCount.isPresent()) {
            return Estimate.of(Math.min(1, (double) nullsCount.get() / rowCount));
        }
        else if (columnStatistics.distinct().value == 2 && (columnStatistics.minValue().isNan() || columnStatistics.maxValue().isNan())) {
            // We can't determine nulls fraction when the rows are only nan and null because the exported XML doesn't distinguish nan and null count
            return UNKNOWN_NULL_FRACTION_REPLACEMENT;
        }
        else if (columnStatistics.minValue().isNull() || columnStatistics.maxValue().isNull()) {
            long nonNullCount = columnStatistics.histogram().category().stream()
                    .mapToLong(category -> category.count().value())
                    .sum();
            return Estimate.of(((double) rowCount - nonNullCount) / rowCount);
        }
        return Estimate.zero();
    }

    private static boolean isNumeric(Type type)
    {
        // TINYINT, SMALLINT and INTEGER is mapped to BIGINT in SEP
        // REAL in Vertica is mapped to DOUBLE in SEP
        return type == BigintType.BIGINT || type == DOUBLE || type instanceof DecimalType;
    }

    private static Optional<DoubleRange> createNumericRange(Optional<String> minValue, Optional<String> maxValue)
    {
        if (minValue.isEmpty() && maxValue.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new DoubleRange(
                minValue
                        .filter(value -> !value.equals("-nan")) // Vertica returns -nan (=NaN) as the minimum value, but Trino doesn't support the value in statistics
                        .flatMap(VerticaTableStatisticsReader::tryParseDouble)
                        .orElse(Double.NEGATIVE_INFINITY),
                maxValue
                        .filter(value -> !value.equals("nan")) // Vertica returns nan (=NaN) as the maximum value, but Trino doesn't support the value in statistics
                        .flatMap(VerticaTableStatisticsReader::tryParseDouble)
                        .orElse(Double.POSITIVE_INFINITY)));
    }

    private static Optional<Double> tryParseDouble(String value)
    {
        try {
            return Optional.of(Double.valueOf(value));
        }
        catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getRowCount(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("" +
                            "SELECT row_count FROM v_monitor.projection_storage " +
                            "WHERE anchor_table_schema = :schema AND anchor_table_name = :table_name")
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        Schema getSchemaStatistics(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            // The empty '' returns XML to standard output
            return handle.createQuery("SELECT EXPORT_STATISTICS('', :schema_table_name)")
                    .bind("schema_table_name", format("%s.%s", remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()))
                    .map((rs, ctx) -> {
                        try {
                            String exportStatistics = rs.getString("EXPORT_STATISTICS");
                            return OBJECT_MAPPER.readValue(exportStatistics, Schema.class);
                        }
                        catch (JsonProcessingException e) {
                            log.warn(e, "Failed to read statistics");
                            return null;
                        }
                    })
                    .one();
        }

        Map<String, Long> getColumnDataSize(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("" +
                            "SELECT column_name, SUM(used_bytes) AS size FROM v_monitor.column_storage " +
                            "WHERE anchor_table_schema = :schema AND anchor_table_name = :table_name " +
                            "GROUP BY column_name")
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> new AbstractMap.SimpleEntry<>(rs.getString("column_name"), rs.getLong("size")))
                    .stream()
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        }
    }

    public record Schema(@JacksonXmlProperty(localName = "tables") List<Table> tables) {}

    public record Table(
            @JacksonXmlProperty(localName = "schema") String schemaName,
            @JacksonXmlProperty(localName = "name") String tableName,
            @JacksonXmlProperty(localName = "columns") List<Column> columns) {}

    public record Column(
            @JacksonXmlProperty(localName = "name") String columnName,
            @JacksonXmlProperty(localName = "dataType") String dataType,
            @JsonProperty("intStats") @JsonAlias({"stringStats", "floatStats", "numericStats"}) ColumnStatistics stats) {}

    public record ColumnStatistics(
            @JacksonXmlProperty(localName = "distinct") NumericValue distinct,
            @JacksonXmlProperty(localName = "minValue") NullableValue minValue,
            @JacksonXmlProperty(localName = "maxValue") NullableValue maxValue,
            @JacksonXmlProperty(localName = "histogram") Histogram histogram) {}

    public record Histogram(@JacksonXmlElementWrapper(useWrapping = false) @JacksonXmlProperty(localName = "category") List<Category> category)
    {
        public record Category(
                @JacksonXmlProperty(localName = "bound") NullableValue bound,
                @JacksonXmlProperty(localName = "count") NumericValue count) {}
    }

    public static class NullableValue
    {
        private final String value;
        private final String nullValue;

        @JsonCreator
        public NullableValue(String value)
        {
            this(null, value);
        }

        @JsonCreator
        public NullableValue(
                @JacksonXmlProperty(localName = "nullValue", isAttribute = true) String nullValue,
                @JacksonXmlProperty(localName = " ") String value)
        {
            this.value = value;
            this.nullValue = nullValue;
        }

        public Optional<String> getValue()
        {
            return Optional.ofNullable(value);
        }

        public boolean isNull()
        {
            if (nullValue == null) {
                return false;
            }
            return nullValue.equals("true");
        }

        public boolean isNan()
        {
            if (value == null) {
                return false;
            }
            return value.equals("-nan") || value.equals("nan");
        }
    }

    public record NumericValue(@JacksonXmlProperty(localName = "value", isAttribute = true) long value) {}
}
