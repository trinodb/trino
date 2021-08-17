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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.parquet.ParquetTimestampUtils.decode;
import static io.trino.parquet.ParquetTypeUtils.getLongDecimalValue;
import static io.trino.parquet.predicate.ParquetLongStatistics.fromBinary;
import static io.trino.parquet.predicate.ParquetLongStatistics.fromNumber;
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

            Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnStatistics, id, column.toString(), timeZone);
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
            if (columnIndex == null || isEmptyColumnIndex(columnIndex)) {
                continue;
            }
            else {
                Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnIndex, id, column);
                if (effectivePredicateDomain.intersect(domain).isNone()) {
                    return false;
                }
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
            Type type,
            long rowCount,
            Statistics<?> statistics,
            ParquetDataSourceId id,
            String column,
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
            return getDomain(type, statistics.genericGetMin(), statistics.genericGetMax(), hasNullValue, timeZone);
        }
        catch (Exception e) {
            throw corruptionException(column, id, statistics, e);
        }
    }

    private static Domain getDomain(
            Type type,
            Object min,
            Object max,
            boolean hasNullValue,
            DateTimeZone timeZone)
    {
        requireNonNull(min);
        requireNonNull(max);

        if (type.equals(BOOLEAN)) {
            boolean minValue = (boolean) min;
            boolean maxValue = (boolean) max;
            boolean hasTrueValues = minValue || maxValue;
            boolean hasFalseValues = !minValue || !maxValue;
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
            ParquetLongStatistics longStatistics = fromNumber((Number) min, (Number) max);
            if (isStatisticsOverflow(type, longStatistics)) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            return createDomain(type, hasNullValue, longStatistics);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                ParquetLongStatistics longStatistics = min instanceof Binary && max instanceof Binary
                        ? fromBinary(decimalType, (Binary) min, (Binary) max)
                        : fromNumber((Number) min, (Number) max);
                if (isStatisticsOverflow(type, longStatistics)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return createDomain(type, hasNullValue, longStatistics);
            }
            else {
                Binary minValue = (Binary) min;
                Binary maxValue = (Binary) max;
                Slice minSlice = getLongDecimalValue(minValue.getBytes());
                Slice maxSlice = getLongDecimalValue(maxValue.getBytes());
                ParquetSliceStatistics sliceStatistics = new ParquetSliceStatistics(minSlice, maxSlice);
                return createDomain(type, hasNullValue, sliceStatistics);
            }
        }

        if (type.equals(REAL)) {
            Float minValue = (Float) min;
            Float maxValue = (Float) max;
            if (minValue.isNaN() || maxValue.isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetLongStatistics longStatistics = fromNumber(
                    floatToRawIntBits(minValue),
                    floatToRawIntBits(maxValue));
            return createDomain(type, hasNullValue, longStatistics);
        }

        if (type.equals(DOUBLE)) {
            Double minValue = (Double) min;
            Double maxValue = (Double) max;
            if (minValue.isNaN() || maxValue.isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetDoubleStatistics doubleStatistics = new ParquetDoubleStatistics(minValue, maxValue);
            return createDomain(type, hasNullValue, doubleStatistics);
        }

        if (type instanceof VarcharType) {
            Binary minValue = (Binary) min;
            Binary maxValue = (Binary) max;
            Slice minSlice = Slices.wrappedBuffer(minValue.toByteBuffer());
            Slice maxSlice = Slices.wrappedBuffer(maxValue.toByteBuffer());
            ParquetSliceStatistics sliceStatistics = new ParquetSliceStatistics(minSlice, maxSlice);
            return createDomain(type, hasNullValue, sliceStatistics);
        }

        if (type instanceof TimestampType && max instanceof Binary && min instanceof Binary) {
            // Parquet INT96 timestamp values were compared incorrectly for the purposes of producing statistics by older parquet writers, so
            // PARQUET-1065 deprecated them. The result is that any writer that produced stats was producing unusable incorrect values, except
            // the special case where min == max and an incorrect ordering would not be material to the result. PARQUET-1026 made binary stats
            // available and valid in that special case
            if (min.equals(max)) {
                return Domain.create(ValueSet.of(
                        type,
                        createTimestampEncoder((TimestampType) type, timeZone).getTimestamp(decode((Binary) max))),
                        hasNullValue);
            }
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    @VisibleForTesting
    public Domain getDomain(Type type, long rowCount, ColumnIndex columnIndex, ParquetDataSourceId id, RichColumnDescriptor descriptor)
            throws ParquetCorruptionException
    {
        if (columnIndex == null) {
            return Domain.all(type);
        }

        String columnName = descriptor.getPrimitiveType().getName();

        if (isCorruptedColumnIndex(columnIndex)) {
            throw corruptionException(columnName, id, columnIndex, null);
        }

        if (isEmptyColumnIndex(columnIndex)) {
            return Domain.all(type);
        }

        long totalNullCount = columnIndex.getNullCounts().stream()
                .mapToLong(value -> value)
                .sum();
        boolean hasNullValue = totalNullCount > 0;

        if (hasNullValue && totalNullCount == rowCount) {
            return Domain.onlyNull(type);
        }

        try {
            Domain domain = Domain.none(type);
            int pageCount = columnIndex.getMinValues().size();
            ColumnIndexValueConverter converter = new ColumnIndexValueConverter();
            Function<ByteBuffer, Object> converterFunction = converter.getConverter(descriptor.getPrimitiveType());
            for (int i = 0; i < pageCount; i++) {
                Object min = converterFunction.apply(columnIndex.getMinValues().get(i));
                Object max = converterFunction.apply(columnIndex.getMaxValues().get(i));
                domain = domain.union(getDomain(type, min, max, hasNullValue, timeZone));
            }

            return domain;
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

        Domain domain = Domain.none(type);
        int dictionarySize = dictionaryPage.get().getDictionarySize();
        DictionaryValueConverter converter = new DictionaryValueConverter(dictionary);
        Function<Integer, Object> convertFunction = converter.getConverter(columnDescriptor.getPrimitiveType());
        for (int i = 0; i < dictionarySize; i++) {
            Object value = convertFunction.apply(i);
            domain = domain.union(getDomain(type, value, value, true, timeZone));
        }

        return domain;
    }

    private static ParquetCorruptionException corruptionException(String column, ParquetDataSourceId id, Statistics<?> statistics, Exception cause)
    {
        return new ParquetCorruptionException(cause, format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column, id, statistics));
    }

    private static ParquetCorruptionException corruptionException(String column, ParquetDataSourceId id, ColumnIndex columnIndex, Exception cause)
    {
        return new ParquetCorruptionException(cause, format("Corrupted statistics for column \"%s\" in Parquet file \"%s\". Corrupted column index: [%s]", column, id, columnIndex));
    }

    public static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, ParquetRangeStatistics<T> rangeStatistics)
    {
        T min = rangeStatistics.getMin();
        T max = rangeStatistics.getMax();
        if (min != null && max != null) {
            return Domain.create(ValueSet.ofRanges(Range.range(type, min, true, max, true)), hasNullValue);
        }
        else if (max != null) {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, max)), hasNullValue);
        }
        else if (min != null) {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, min)), hasNullValue);
        }
        else {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
    }

    private boolean isCorruptedColumnIndex(ColumnIndex columnIndex)
    {
        if (columnIndex.getMaxValues() == null
                || columnIndex.getMinValues() == null
                || columnIndex.getNullCounts() == null
                || columnIndex.getNullPages() == null) {
            return true;
        }

        if (columnIndex.getMaxValues().size() != columnIndex.getMinValues().size()
                || columnIndex.getMaxValues().size() != columnIndex.getNullPages().size()
                || columnIndex.getMaxValues().size() != columnIndex.getNullCounts().size()) {
            return true;
        }

        return false;
    }

    // Caller should verify isCorruptedColumnIndex is false first
    private boolean isEmptyColumnIndex(ColumnIndex columnIndex)
    {
        return columnIndex.getMaxValues().size() == 0;
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

            FilterPredicate columnFilter = FilterApi.userDefined(FilterApi.intColumn(ColumnPath.get(column.getPath()).toDotString()), new DomainUserDefinedPredicate(domain, timeZone));
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
        private final Domain columnDomain;
        private final DateTimeZone timeZone;

        public DomainUserDefinedPredicate(Domain domain, DateTimeZone timeZone)
        {
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

            Domain domain = getDomain(columnDomain.getType(), statistic.getMin(), statistic.getMax(), true, timeZone);
            return columnDomain.intersect(domain).isNone();
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
                    return (buffer) -> buffer.get(0) != 0;
                case INT32:
                    return (buffer) -> buffer.order(LITTLE_ENDIAN).getInt(0);
                case INT64:
                    return (buffer) -> buffer.order(LITTLE_ENDIAN).getLong(0);
                case FLOAT:
                    return (buffer) -> buffer.order(LITTLE_ENDIAN).getFloat(0);
                case DOUBLE:
                    return (buffer) -> buffer.order(LITTLE_ENDIAN).getDouble(0);
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                case INT96:
                default:
                    return (buffer) -> Binary.fromReusedByteBuffer(buffer);
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
}
