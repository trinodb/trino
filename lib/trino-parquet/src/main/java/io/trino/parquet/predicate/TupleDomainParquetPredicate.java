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
package io.trino.parquet.predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.plugin.base.type.TrinoTimestampEncoder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt64Timestamp;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.trino.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public class TupleDomainParquetPredicate
        implements Predicate
{
    private final TupleDomain<ColumnDescriptor> effectivePredicate;
    private final List<RichColumnDescriptor> columns;
    private final DateTimeZone timeZone;

    public TupleDomainParquetPredicate(TupleDomain<ColumnDescriptor> effectivePredicate, List<RichColumnDescriptor> columns, DateTimeZone timeZone)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    @Override
    public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id)
            throws ParquetCorruptionException
    {
        if (numberOfRows == 0) {
            return false;
        }
        if (effectivePredicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        for (RichColumnDescriptor column : columns) {
            Domain effectivePredicateDomain = effectivePredicateDomains.get(column);
            if (effectivePredicateDomain == null) {
                continue;
            }

            Statistics<?> columnStatistics = statistics.get(column);
            if (columnStatistics == null || columnStatistics.isEmpty()) {
                // no stats for column
                continue;
            }

            Domain domain = getDomain(
                    column,
                    effectivePredicateDomain.getType(),
                    numberOfRows,
                    columnStatistics,
                    id,
                    timeZone);
            if (!effectivePredicateDomain.overlaps(domain)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean matches(DictionaryDescriptor dictionary)
    {
        requireNonNull(dictionary, "dictionary is null");
        if (effectivePredicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        Domain effectivePredicateDomain = effectivePredicateDomains.get(dictionary.getColumnDescriptor());

        return effectivePredicateDomain == null || effectivePredicateMatches(effectivePredicateDomain, dictionary);
    }

    @Override
    public boolean matches(long numberOfRows, ColumnIndexStore columnIndexStore, ParquetDataSourceId id)
            throws ParquetCorruptionException
    {
        requireNonNull(columnIndexStore, "columnIndexStore is null");

        if (numberOfRows == 0) {
            return false;
        }

        if (effectivePredicate.isNone()) {
            return false;
        }

        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        for (RichColumnDescriptor column : columns) {
            Domain effectivePredicateDomain = effectivePredicateDomains.get(column);
            if (effectivePredicateDomain == null) {
                continue;
            }

            ColumnIndex columnIndex = columnIndexStore.getColumnIndex(ColumnPath.get(column.getPath()));
            if (columnIndex == null) {
                continue;
            }

            Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnIndex, id, column, timeZone);
            if (!effectivePredicateDomain.overlaps(domain)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Optional<FilterPredicate> toParquetFilter(DateTimeZone timeZone)
    {
        return Optional.ofNullable(convertToParquetFilter(timeZone));
    }

    private boolean effectivePredicateMatches(Domain effectivePredicateDomain, DictionaryDescriptor dictionary)
    {
        return effectivePredicateDomain.overlaps(getDomain(effectivePredicateDomain.getType(), dictionary, timeZone));
    }

    @VisibleForTesting
    public static Domain getDomain(
            ColumnDescriptor column,
            Type type,
            long rowCount,
            Statistics<?> statistics,
            ParquetDataSourceId id,
            DateTimeZone timeZone)
            throws ParquetCorruptionException
    {
        if (statistics == null || statistics.isEmpty()) {
            return Domain.all(type);
        }

        if (statistics.isNumNullsSet() && statistics.getNumNulls() == rowCount) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = !statistics.isNumNullsSet() || statistics.getNumNulls() != 0L;

        if (!statistics.hasNonNullValue() || statistics.genericGetMin() == null || statistics.genericGetMax() == null) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }

        try {
            return getDomain(
                    column,
                    type,
                    ImmutableList.of(statistics.genericGetMin()),
                    ImmutableList.of(statistics.genericGetMax()),
                    hasNullValue,
                    timeZone);
        }
        catch (Exception e) {
            throw corruptionException(column.toString(), id, statistics, e);
        }
    }

    /**
     * Get a domain for the ranges defined by each pair of elements from {@code minimums} and {@code maximums}.
     * Both arrays must have the same length.
     */
    private static Domain getDomain(
            ColumnDescriptor column,
            Type type,
            List<Object> minimums,
            List<Object> maximums,
            boolean hasNullValue,
            DateTimeZone timeZone)
    {
        checkArgument(minimums.size() == maximums.size(), "Expected minimums and maximums to have the same size");

        if (type.equals(BOOLEAN)) {
            boolean hasTrueValues = minimums.stream().anyMatch(value -> (boolean) value) || maximums.stream().anyMatch(value -> (boolean) value);
            boolean hasFalseValues = minimums.stream().anyMatch(value -> !(boolean) value) || maximums.stream().anyMatch(value -> !(boolean) value);
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(type);
            }
            if (hasTrueValues) {
                return Domain.create(ValueSet.of(type, true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(ValueSet.of(type, false), hasNullValue);
            }
            // All nulls case is handled earlier
            throw new VerifyException("Impossible boolean statistics");
        }

        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(DATE) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < minimums.size(); i++) {
                long min = asLong(minimums.get(i));
                long max = asLong(maximums.get(i));
                if (isStatisticsOverflow(type, min, max)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }

                ranges.add(Range.range(type, min, true, max, true));
            }

            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            List<Range> ranges = new ArrayList<>();
            if (decimalType.isShort()) {
                for (int i = 0; i < minimums.size(); i++) {
                    Object min = minimums.get(i);
                    Object max = maximums.get(i);

                    long minValue = min instanceof Binary ? getShortDecimalValue(((Binary) min).getBytes()) : asLong(min);
                    long maxValue = min instanceof Binary ? getShortDecimalValue(((Binary) max).getBytes()) : asLong(max);

                    if (isStatisticsOverflow(type, minValue, maxValue)) {
                        return Domain.create(ValueSet.all(type), hasNullValue);
                    }

                    ranges.add(Range.range(type, minValue, true, maxValue, true));
                }
            }
            else {
                for (int i = 0; i < minimums.size(); i++) {
                    Int128 min = Int128.fromBigEndian(((Binary) minimums.get(i)).getBytes());
                    Int128 max = Int128.fromBigEndian(((Binary) maximums.get(i)).getBytes());

                    ranges.add(Range.range(type, min, true, max, true));
                }
            }

            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type.equals(REAL)) {
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < minimums.size(); i++) {
                Float min = (Float) minimums.get(i);
                Float max = (Float) maximums.get(i);

                if (min.isNaN() || max.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }

                ranges.add(Range.range(type, (long) floatToRawIntBits(min), true, (long) floatToRawIntBits(max), true));
            }
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type.equals(DOUBLE)) {
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < minimums.size(); i++) {
                Double min = (Double) minimums.get(i);
                Double max = (Double) maximums.get(i);

                if (min.isNaN() || max.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }

                ranges.add(Range.range(type, min, true, max, true));
            }
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type instanceof VarcharType) {
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < minimums.size(); i++) {
                Slice min = Slices.wrappedBuffer(((Binary) minimums.get(i)).toByteBuffer());
                Slice max = Slices.wrappedBuffer(((Binary) maximums.get(i)).toByteBuffer());
                ranges.add(Range.range(type, min, true, max, true));
            }
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type instanceof TimestampType) {
            if (column.getPrimitiveType().getPrimitiveTypeName().equals(INT96)) {
                TrinoTimestampEncoder<?> timestampEncoder = createTimestampEncoder((TimestampType) type, timeZone);
                List<Object> values = new ArrayList<>();
                for (int i = 0; i < minimums.size(); i++) {
                    Object min = minimums.get(i);
                    Object max = maximums.get(i);

                    // Parquet INT96 timestamp values were compared incorrectly for the purposes of producing statistics by older parquet writers, so
                    // PARQUET-1065 deprecated them. The result is that any writer that produced stats was producing unusable incorrect values, except
                    // the special case where min == max and an incorrect ordering would not be material to the result. PARQUET-1026 made binary stats
                    // available and valid in that special case
                    if (!(min instanceof Binary) || !(max instanceof Binary) || !min.equals(max)) {
                        return Domain.create(ValueSet.all(type), hasNullValue);
                    }

                    values.add(timestampEncoder.getTimestamp(decodeInt96Timestamp((Binary) min)));
                }
                return Domain.multipleValues(type, values, hasNullValue);
            }
            if (column.getPrimitiveType().getPrimitiveTypeName().equals(INT64)) {
                LogicalTypeAnnotation logicalTypeAnnotation = column.getPrimitiveType().getLogicalTypeAnnotation();
                if (!(logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation)) {
                    // Invalid statistics. Unit and UTC adjustment are not known
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }

                TimestampLogicalTypeAnnotation timestampTypeAnnotation = (TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
                // Bail out if the precision is not known
                if (timestampTypeAnnotation.getUnit() == null) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                TrinoTimestampEncoder<?> timestampEncoder = createTimestampEncoder((TimestampType) type, DateTimeZone.UTC);

                List<Range> ranges = new ArrayList<>();
                for (int i = 0; i < minimums.size(); i++) {
                    long min = (long) minimums.get(i);
                    long max = (long) maximums.get(i);

                    ranges.add(Range.range(
                            type,
                            timestampEncoder.getTimestamp(decodeInt64Timestamp(min, timestampTypeAnnotation.getUnit())),
                            true,
                            timestampEncoder.getTimestamp(decodeInt64Timestamp(max, timestampTypeAnnotation.getUnit())),
                            true));
                }
                return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
            }
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    @VisibleForTesting
    public static Domain getDomain(
            Type type,
            long rowCount,
            ColumnIndex columnIndex,
            ParquetDataSourceId id,
            RichColumnDescriptor descriptor,
            DateTimeZone timeZone)
            throws ParquetCorruptionException
    {
        if (columnIndex == null) {
            return Domain.all(type);
        }

        List<ByteBuffer> maxValues = columnIndex.getMaxValues();
        List<ByteBuffer> minValues = columnIndex.getMinValues();
        List<Long> nullCounts = columnIndex.getNullCounts();
        List<Boolean> nullPages = columnIndex.getNullPages();

        String columnName = descriptor.getPrimitiveType().getName();
        if (isCorruptedColumnIndex(minValues, maxValues, nullCounts, nullPages)) {
            throw corruptionException(columnName, id, columnIndex, null);
        }
        if (maxValues.isEmpty()) {
            return Domain.all(type);
        }

        long totalNullCount = nullCounts.stream()
                .mapToLong(value -> value)
                .sum();
        boolean hasNullValue = totalNullCount > 0;

        if (hasNullValue && totalNullCount == rowCount) {
            return Domain.onlyNull(type);
        }

        try {
            int pageCount = minValues.size();
            ColumnIndexValueConverter converter = new ColumnIndexValueConverter();
            Function<ByteBuffer, Object> converterFunction = converter.getConverter(descriptor.getPrimitiveType());
            List<Object> min = new ArrayList<>();
            List<Object> max = new ArrayList<>();
            for (int i = 0; i < pageCount; i++) {
                if (nullPages.get(i)) {
                    continue;
                }
                min.add(converterFunction.apply(minValues.get(i)));
                max.add(converterFunction.apply(maxValues.get(i)));
            }

            return getDomain(descriptor, type, min, max, hasNullValue, timeZone);
        }
        catch (Exception e) {
            throw corruptionException(columnName, id, columnIndex, e);
        }
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, DictionaryDescriptor dictionaryDescriptor)
    {
        return getDomain(type, dictionaryDescriptor, DateTimeZone.getDefault());
    }

    private static Domain getDomain(Type type, DictionaryDescriptor dictionaryDescriptor, DateTimeZone timeZone)
    {
        if (dictionaryDescriptor == null) {
            return Domain.all(type);
        }

        ColumnDescriptor columnDescriptor = dictionaryDescriptor.getColumnDescriptor();
        Optional<DictionaryPage> dictionaryPage = dictionaryDescriptor.getDictionaryPage();
        if (dictionaryPage.isEmpty()) {
            return Domain.all(type);
        }

        Dictionary dictionary;
        try {
            dictionary = dictionaryPage.get().getEncoding().initDictionary(columnDescriptor, dictionaryPage.get());
        }
        catch (Exception e) {
            // In case of exception, just continue reading the data, not using dictionary page at all
            // OK to ignore exception when reading dictionaries
            return Domain.all(type);
        }

        int dictionarySize = dictionaryPage.get().getDictionarySize();
        DictionaryValueConverter converter = new DictionaryValueConverter(dictionary);
        Function<Integer, Object> convertFunction = converter.getConverter(columnDescriptor.getPrimitiveType());
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < dictionarySize; i++) {
            values.add(convertFunction.apply(i));
        }

        // TODO: when min == max (i.e., singleton ranges, the construction of Domains can be done more efficiently
        return getDomain(columnDescriptor, type, values, values, true, timeZone);
    }

    private static ParquetCorruptionException corruptionException(String column, ParquetDataSourceId id, Statistics<?> statistics, Exception cause)
    {
        return new ParquetCorruptionException(cause, format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column, id, statistics));
    }

    private static ParquetCorruptionException corruptionException(String column, ParquetDataSourceId id, ColumnIndex columnIndex, Exception cause)
    {
        return new ParquetCorruptionException(cause, format("Corrupted statistics for column \"%s\" in Parquet file \"%s\". Corrupted column index: [%s]", column, id, columnIndex));
    }

    private static boolean isCorruptedColumnIndex(
            List<ByteBuffer> minValues,
            List<ByteBuffer> maxValues,
            List<Long> nullCounts,
            List<Boolean> nullPages)
    {
        if (maxValues == null || minValues == null || nullCounts == null || nullPages == null) {
            return true;
        }

        return maxValues.size() != minValues.size()
                || maxValues.size() != nullPages.size()
                || maxValues.size() != nullCounts.size();
    }

    public static long asLong(Object value)
    {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }

        throw new IllegalArgumentException("Can't convert value to long: " + value.getClass().getName());
    }

    private FilterPredicate convertToParquetFilter(DateTimeZone timeZone)
    {
        FilterPredicate filter = null;

        for (RichColumnDescriptor column : columns) {
            Domain domain = effectivePredicate.getDomains().get().get(column);
            if (domain == null || domain.isNone()) {
                continue;
            }

            if (domain.isAll()) {
                continue;
            }

            FilterPredicate columnFilter = FilterApi.userDefined(
                    new TrinoIntColumn(ColumnPath.get(column.getPath())),
                    new DomainUserDefinedPredicate<>(column, domain, timeZone));
            if (filter == null) {
                filter = columnFilter;
            }
            else {
                filter = FilterApi.and(filter, columnFilter);
            }
        }

        return filter;
    }

    /**
     * This class implements methods defined in UserDefinedPredicate based on the page statistic and tuple domain(for a column).
     */
    static class DomainUserDefinedPredicate<T extends Comparable<T>>
            extends UserDefinedPredicate<T>
            implements Serializable // Required by argument of FilterApi.userDefined call
    {
        private final ColumnDescriptor columnDescriptor;
        private final Domain columnDomain;
        private final DateTimeZone timeZone;

        public DomainUserDefinedPredicate(ColumnDescriptor columnDescriptor, Domain domain, DateTimeZone timeZone)
        {
            this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor is null");
            this.columnDomain = domain;
            this.timeZone = timeZone;
        }

        @Override
        public boolean keep(T value)
        {
            if (value == null && !columnDomain.isNullAllowed()) {
                return false;
            }

            return true;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistic)
        {
            if (statistic == null) {
                return false;
            }

            Domain domain = getDomain(
                    columnDescriptor,
                    columnDomain.getType(),
                    ImmutableList.of(statistic.getMin()),
                    ImmutableList.of(statistic.getMax()),
                    true,
                    timeZone);
            return !columnDomain.overlaps(domain);
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics)
        {
            // Since we don't use LogicalNotUserDefined, this method is not called.
            // To be safe, we just keep the record by returning false.
            return false;
        }
    }

    private static class ColumnIndexValueConverter
    {
        private ColumnIndexValueConverter()
        {
        }

        private Function<ByteBuffer, Object> getConverter(PrimitiveType primitiveType)
        {
            switch (primitiveType.getPrimitiveTypeName()) {
                case BOOLEAN:
                    return buffer -> buffer.get(0) != 0;
                case INT32:
                    return buffer -> buffer.order(LITTLE_ENDIAN).getInt(0);
                case INT64:
                    return buffer -> buffer.order(LITTLE_ENDIAN).getLong(0);
                case FLOAT:
                    return buffer -> buffer.order(LITTLE_ENDIAN).getFloat(0);
                case DOUBLE:
                    return buffer -> buffer.order(LITTLE_ENDIAN).getDouble(0);
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                case INT96:
                default:
                    return buffer -> Binary.fromReusedByteBuffer(buffer);
            }
        }
    }

    private static class DictionaryValueConverter
    {
        private final Dictionary dictionary;

        private DictionaryValueConverter(Dictionary dictionary)
        {
            this.dictionary = dictionary;
        }

        private Function<Integer, Object> getConverter(PrimitiveType primitiveType)
        {
            switch (primitiveType.getPrimitiveTypeName()) {
                case INT32:
                    return (i) -> dictionary.decodeToInt(i);
                case INT64:
                    return (i) -> dictionary.decodeToLong(i);
                case FLOAT:
                    return (i) -> dictionary.decodeToFloat(i);
                case DOUBLE:
                    return (i) -> dictionary.decodeToDouble(i);
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                case INT96:
                default:
                    return (i) -> dictionary.decodeToBinary(i);
            }
        }
    }

    // FilterApi#intColumn splits column name on ".". If column name contains a "." this leads to
    // ColumnIndexFilter#calculateRowRanges failing to detect that column as part of the projection
    // and treating it like a column with only NULL values.
    private static final class TrinoIntColumn
            extends Operators.Column<Integer>
            implements Operators.SupportsLtGt
    {
        TrinoIntColumn(ColumnPath columnPath)
        {
            super(columnPath, Integer.class);
        }
    }
}
