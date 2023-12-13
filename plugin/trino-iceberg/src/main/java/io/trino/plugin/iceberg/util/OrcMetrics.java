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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInputFile;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.statistics.BooleanStatistics;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.DateStatistics;
import io.trino.orc.metadata.statistics.DecimalStatistics;
import io.trino.orc.metadata.statistics.DoubleStatistics;
import io.trino.orc.metadata.statistics.IntegerStatistics;
import io.trino.orc.metadata.statistics.StringStatistics;
import io.trino.orc.metadata.statistics.TimestampStatistics;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.iceberg.TrinoOrcDataSource;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.orc.OrcReader.createOrcReader;
import static io.trino.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.trino.plugin.iceberg.util.OrcIcebergIds.fileColumnsByIcebergId;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.ORC_ICEBERG_ID_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.toIntExact;
import static java.util.function.Function.identity;

public final class OrcMetrics
{
    private OrcMetrics() {}

    public static Metrics fileMetrics(TrinoInputFile file, MetricsConfig metricsConfig, Schema schema)
    {
        OrcReaderOptions options = new OrcReaderOptions();
        try (OrcDataSource dataSource = new TrinoOrcDataSource(file, options, new FileFormatDataSourceStats())) {
            Optional<OrcReader> reader = createOrcReader(dataSource, options);
            if (reader.isEmpty()) {
                return new Metrics(0L, null, null, null, null);
            }
            Footer footer = reader.get().getFooter();

            // use name mapping to compute missing Iceberg field IDs
            Optional<NameMapping> nameMapping = Optional.of(MappingUtil.create(schema));
            Map<OrcColumnId, OrcColumn> mappedColumns = fileColumnsByIcebergId(reader.get(), nameMapping)
                    .values().stream()
                    .collect(toImmutableMap(OrcColumn::getColumnId, identity()));

            // rebuild type list with mapped columns
            List<OrcType> mappedTypes = new ArrayList<>();
            ColumnMetadata<OrcType> types = footer.getTypes();
            for (int i = 0; i < types.size(); i++) {
                OrcColumnId id = new OrcColumnId(i);
                mappedTypes.add(Optional.ofNullable(mappedColumns.get(id))
                        .map(OrcMetrics::toBasicOrcType)
                        .orElseGet(() -> types.get(id)));
            }

            return computeMetrics(metricsConfig, schema, new ColumnMetadata<>(mappedTypes), footer.getNumberOfRows(), footer.getFileStats());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read file footer: " + file.location(), e);
        }
    }

    private static OrcType toBasicOrcType(OrcColumn column)
    {
        return new OrcType(
                column.getColumnType(),
                column.getNestedColumns().stream()
                        .map(OrcColumn::getColumnId)
                        .collect(toImmutableList()),
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                column.getAttributes());
    }

    public static Metrics computeMetrics(
            MetricsConfig metricsConfig,
            Schema icebergSchema,
            ColumnMetadata<OrcType> orcColumns,
            long fileRowCount,
            Optional<ColumnMetadata<ColumnStatistics>> columnStatistics)
    {
        if (columnStatistics.isEmpty()) {
            return new Metrics(fileRowCount, null, null, null, null, null, null);
        }
        // Columns that are descendants of LIST or MAP types are excluded because:
        // 1. Their stats are not used by Apache Iceberg to filter out data files
        // 2. Their record count can be larger than table-level row count. There's no good way to calculate nullCounts for them.
        // See https://github.com/apache/iceberg/pull/199#discussion_r429443627
        Set<OrcColumnId> excludedColumns = getExcludedColumns(orcColumns);

        ImmutableMap.Builder<Integer, Long> valueCountsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Long> nullCountsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Long> nanCountsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, ByteBuffer> lowerBoundsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, ByteBuffer> upperBoundsBuilder = ImmutableMap.builder();

        // OrcColumnId(0) is the root column that represents file-level schema
        for (int i = 1; i < orcColumns.size(); i++) {
            OrcColumnId orcColumnId = new OrcColumnId(i);
            if (excludedColumns.contains(orcColumnId)) {
                continue;
            }
            OrcType orcColumn = orcColumns.get(orcColumnId);
            ColumnStatistics orcColumnStats = columnStatistics.get().get(orcColumnId);
            int icebergId = getIcebergId(orcColumn);
            Types.NestedField icebergField = icebergSchema.findField(icebergId);
            MetricsModes.MetricsMode metricsMode = MetricsUtil.metricsMode(icebergSchema, metricsConfig, icebergId);
            if (metricsMode.equals(MetricsModes.None.get())) {
                continue;
            }
            verify(icebergField != null, "Cannot find Iceberg column with ID %s in schema %s", icebergId, icebergSchema);
            valueCountsBuilder.put(icebergId, fileRowCount);
            if (orcColumnStats.hasNumberOfValues()) {
                nullCountsBuilder.put(icebergId, fileRowCount - orcColumnStats.getNumberOfValues());
            }
            if (orcColumnStats.getNumberOfNanValues() > 0) {
                nanCountsBuilder.put(icebergId, orcColumnStats.getNumberOfNanValues());
            }

            if (!metricsMode.equals(MetricsModes.Counts.get())) {
                toIcebergMinMax(orcColumnStats, icebergField.type(), metricsMode).ifPresent(minMax -> {
                    lowerBoundsBuilder.put(icebergId, minMax.getMin());
                    upperBoundsBuilder.put(icebergId, minMax.getMax());
                });
            }
        }
        Map<Integer, Long> valueCounts = valueCountsBuilder.buildOrThrow();
        Map<Integer, Long> nullCounts = nullCountsBuilder.buildOrThrow();
        Map<Integer, Long> nanCounts = nanCountsBuilder.buildOrThrow();
        Map<Integer, ByteBuffer> lowerBounds = lowerBoundsBuilder.buildOrThrow();
        Map<Integer, ByteBuffer> upperBounds = upperBoundsBuilder.buildOrThrow();
        return new Metrics(
                fileRowCount,
                null, // TODO: Add column size accounting to ORC column writers
                valueCounts.isEmpty() ? null : valueCounts,
                nullCounts.isEmpty() ? null : nullCounts,
                nanCounts.isEmpty() ? null : nanCounts,
                lowerBounds.isEmpty() ? null : lowerBounds,
                upperBounds.isEmpty() ? null : upperBounds);
    }

    private static Set<OrcColumnId> getExcludedColumns(ColumnMetadata<OrcType> orcColumns)
    {
        ImmutableSet.Builder<OrcColumnId> excludedColumns = ImmutableSet.builder();
        populateExcludedColumns(orcColumns, ROOT_COLUMN, false, excludedColumns);
        return excludedColumns.build();
    }

    private static void populateExcludedColumns(ColumnMetadata<OrcType> orcColumns, OrcColumnId orcColumnId, boolean exclude, ImmutableSet.Builder<OrcColumnId> excludedColumns)
    {
        if (exclude) {
            excludedColumns.add(orcColumnId);
        }
        OrcType orcColumn = orcColumns.get(orcColumnId);
        switch (orcColumn.getOrcTypeKind()) {
            case LIST:
            case MAP:
                for (OrcColumnId child : orcColumn.getFieldTypeIndexes()) {
                    populateExcludedColumns(orcColumns, child, true, excludedColumns);
                }
                return;
            case STRUCT:
                for (OrcColumnId child : orcColumn.getFieldTypeIndexes()) {
                    populateExcludedColumns(orcColumns, child, exclude, excludedColumns);
                }
                return;
            default:
                // unexpected, TODO throw
        }
    }

    private static int getIcebergId(OrcType orcColumn)
    {
        String icebergId = orcColumn.getAttributes().get(ORC_ICEBERG_ID_KEY);
        verify(icebergId != null, "ORC column %s doesn't have an associated Iceberg ID", orcColumn);
        return Integer.parseInt(icebergId);
    }

    private static Optional<IcebergMinMax> toIcebergMinMax(ColumnStatistics orcColumnStats, Type icebergType, MetricsModes.MetricsMode metricsModes)
    {
        BooleanStatistics booleanStatistics = orcColumnStats.getBooleanStatistics();
        if (booleanStatistics != null) {
            boolean hasTrueValues = booleanStatistics.getTrueValueCount() != 0;
            boolean hasFalseValues = orcColumnStats.getNumberOfValues() != booleanStatistics.getTrueValueCount();
            return Optional.of(new IcebergMinMax(icebergType, !hasFalseValues, hasTrueValues, metricsModes));
        }

        IntegerStatistics integerStatistics = orcColumnStats.getIntegerStatistics();
        if (integerStatistics != null) {
            Object min = integerStatistics.getMin();
            Object max = integerStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            if (icebergType.typeId() == TypeID.INTEGER) {
                min = toIntExact((Long) min);
                max = toIntExact((Long) max);
            }
            return Optional.of(new IcebergMinMax(icebergType, min, max, metricsModes));
        }
        DoubleStatistics doubleStatistics = orcColumnStats.getDoubleStatistics();
        if (doubleStatistics != null) {
            Object min = doubleStatistics.getMin();
            Object max = doubleStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            if (icebergType.typeId() == TypeID.FLOAT) {
                min = ((Double) min).floatValue();
                max = ((Double) max).floatValue();
            }
            return Optional.of(new IcebergMinMax(icebergType, min, max, metricsModes));
        }
        StringStatistics stringStatistics = orcColumnStats.getStringStatistics();
        if (stringStatistics != null) {
            Slice min = stringStatistics.getMin();
            Slice max = stringStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            return Optional.of(new IcebergMinMax(icebergType, min.toStringUtf8(), max.toStringUtf8(), metricsModes));
        }
        DateStatistics dateStatistics = orcColumnStats.getDateStatistics();
        if (dateStatistics != null) {
            Integer min = dateStatistics.getMin();
            Integer max = dateStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            return Optional.of(new IcebergMinMax(icebergType, min, max, metricsModes));
        }
        DecimalStatistics decimalStatistics = orcColumnStats.getDecimalStatistics();
        if (decimalStatistics != null) {
            BigDecimal min = decimalStatistics.getMin();
            BigDecimal max = decimalStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            min = min.setScale(((Types.DecimalType) icebergType).scale());
            max = max.setScale(((Types.DecimalType) icebergType).scale());
            return Optional.of(new IcebergMinMax(icebergType, min, max, metricsModes));
        }
        TimestampStatistics timestampStatistics = orcColumnStats.getTimestampStatistics();
        if (timestampStatistics != null) {
            Long min = timestampStatistics.getMin();
            Long max = timestampStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            // Since ORC timestamp statistics are truncated to millisecond precision, this can cause some column values to fall outside the stats range.
            // We are appending 999 microseconds to account for the fact that Trino ORC writer truncates timestamps.
            return Optional.of(new IcebergMinMax(icebergType, min * MICROSECONDS_PER_MILLISECOND, (max * MICROSECONDS_PER_MILLISECOND) + (MICROSECONDS_PER_MILLISECOND - 1), metricsModes));
        }
        return Optional.empty();
    }

    private static class IcebergMinMax
    {
        private final ByteBuffer min;
        private final ByteBuffer max;

        private IcebergMinMax(Type type, Object min, Object max, MetricsModes.MetricsMode metricsMode)
        {
            if (metricsMode instanceof MetricsModes.Full) {
                this.min = Conversions.toByteBuffer(type, min);
                this.max = Conversions.toByteBuffer(type, max);
            }
            else if (metricsMode instanceof MetricsModes.Truncate truncateMode) {
                int truncateLength = truncateMode.length();
                switch (type.typeId()) {
                    case STRING:
                        this.min = UnicodeUtil.truncateStringMin(Literal.of((CharSequence) min), truncateLength).toByteBuffer();
                        this.max = UnicodeUtil.truncateStringMax(Literal.of((CharSequence) max), truncateLength).toByteBuffer();
                        break;
                    case FIXED:
                    case BINARY:
                        this.min = BinaryUtil.truncateBinaryMin(Literal.of((ByteBuffer) min), truncateLength).toByteBuffer();
                        this.max = BinaryUtil.truncateBinaryMax(Literal.of((ByteBuffer) max), truncateLength).toByteBuffer();
                        break;
                    default:
                        this.min = Conversions.toByteBuffer(type, min);
                        this.max = Conversions.toByteBuffer(type, max);
                }
            }
            else {
                throw new UnsupportedOperationException("Unsupported metrics mode for Iceberg Max/Min Bound: " + metricsMode);
            }
        }

        public ByteBuffer getMin()
        {
            return min;
        }

        public ByteBuffer getMax()
        {
            return max;
        }
    }
}
