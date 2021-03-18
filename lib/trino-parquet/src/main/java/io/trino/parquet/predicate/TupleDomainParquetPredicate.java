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
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.parquet.ParquetTimestampUtils.decode;
import static io.trino.parquet.ParquetTypeUtils.getLongDecimalValue;
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

public class TupleDomainParquetPredicate
        implements Predicate
{
    private final TupleDomain<ColumnDescriptor> effectivePredicate;
    private final List<RichColumnDescriptor> columns;
    private final DateTimeZone timeZone;
    private final ColumnIndexValueConverter converter;

    public TupleDomainParquetPredicate(TupleDomain<ColumnDescriptor> effectivePredicate, List<RichColumnDescriptor> columns, DateTimeZone timeZone)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.converter = new ColumnIndexValueConverter(columns);
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
                Domain domain = getDomain(effectivePredicateDomain.getType(), columnIndex, id, column);
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
        return Optional.of(convertToParquetFilter());
    }

    private static boolean effectivePredicateMatches(Domain effectivePredicateDomain, DictionaryDescriptor dictionary)
    {
        return effectivePredicateDomain.overlaps(getDomain(effectivePredicateDomain.getType(), dictionary));
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

        if (type.equals(BOOLEAN) && statistics instanceof BooleanStatistics) {
            BooleanStatistics booleanStatistics = (BooleanStatistics) statistics;

            boolean hasTrueValues = booleanStatistics.getMin() || booleanStatistics.getMax();
            boolean hasFalseValues = !booleanStatistics.getMin() || !booleanStatistics.getMax();
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

        if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) && (statistics instanceof LongStatistics || statistics instanceof IntStatistics)) {
            ParquetLongStatistics parquetIntegerStatistics = toParquetLongStatistics(statistics, id, column);
            if (isStatisticsOverflow(type, parquetIntegerStatistics)) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            return createDomain(type, id, column, hasNullValue, parquetIntegerStatistics, statistics);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                ParquetLongStatistics parquetLongStatistics = toParquetLongStatistics(statistics, id, column);
                if (isStatisticsOverflow(type, parquetLongStatistics)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return createDomain(type, id, column, hasNullValue, parquetLongStatistics, statistics);
            }
            else if (statistics instanceof BinaryStatistics) {
                BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
                Slice minSlice = getLongDecimalValue(binaryStatistics.genericGetMin().getBytes());
                Slice maxSlice = getLongDecimalValue(binaryStatistics.genericGetMax().getBytes());
                ParquetSliceStatistics parquetSliceStatistics = new ParquetSliceStatistics(minSlice, maxSlice);
                return createDomain(type, id, column, hasNullValue, parquetSliceStatistics, statistics);
            }
        }

        if (type.equals(REAL) && statistics instanceof FloatStatistics) {
            FloatStatistics floatStatistics = (FloatStatistics) statistics;
            if (floatStatistics.genericGetMin().isNaN() || floatStatistics.genericGetMax().isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetLongStatistics parquetStatistics = new ParquetLongStatistics(
                    (long) floatToRawIntBits(floatStatistics.getMin()),
                    (long) floatToRawIntBits(floatStatistics.getMax()));
            return createDomain(type, id, column, hasNullValue, parquetStatistics, statistics);
        }

        if (type.equals(DOUBLE) && statistics instanceof DoubleStatistics) {
            DoubleStatistics doubleStatistics = (DoubleStatistics) statistics;
            if (doubleStatistics.genericGetMin().isNaN() || doubleStatistics.genericGetMax().isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetDoubleStatistics parquetDoubleStatistics = new ParquetDoubleStatistics(doubleStatistics.genericGetMin(), doubleStatistics.genericGetMax());
            return createDomain(type, id, column, hasNullValue, parquetDoubleStatistics, statistics);
        }

        if (type instanceof VarcharType && statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            Slice minSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMin().getBytes());
            Slice maxSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMax().getBytes());
            ParquetSliceStatistics parquetSliceStatistics = new ParquetSliceStatistics(minSlice, maxSlice);
            return createDomain(type, id, column, hasNullValue, parquetSliceStatistics, statistics);
        }

        if (type.equals(DATE) && statistics instanceof IntStatistics) {
            IntStatistics intStatistics = (IntStatistics) statistics;
            ParquetLongStatistics parquetLongStatistics = new ParquetLongStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax());
            return createDomain(type, id, column, hasNullValue, parquetLongStatistics, statistics);
        }

        if (type instanceof TimestampType && statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            // Parquet INT96 timestamp values were compared incorrectly for the purposes of producing statistics by older parquet writers, so
            // PARQUET-1065 deprecated them. The result is that any writer that produced stats was producing unusable incorrect values, except
            // the special case where min == max and an incorrect ordering would not be material to the result. PARQUET-1026 made binary stats
            // available and valid in that special case
            if (binaryStatistics.genericGetMin().equals(binaryStatistics.genericGetMax())) {
                return Domain.create(ValueSet.of(
                        type,
                        createTimestampEncoder((TimestampType) type, timeZone).getTimestamp(decode(binaryStatistics.genericGetMax()))),
                        hasNullValue);
            }
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static ParquetLongStatistics toParquetLongStatistics(Object min, Object max)
    {
        if (min instanceof Number) {
            Number minValue = (Number) min;
            Number maxValue = (Number) max;
            return new ParquetLongStatistics(minValue.longValue(), maxValue.longValue());
        }
        else if (min instanceof Binary) {
            Binary minValue = (Binary) min;
            Binary maxValue = (Binary) max;
            return new ParquetLongStatistics(getShortDecimalValue(minValue.getBytes()), getShortDecimalValue(maxValue.getBytes()));
        }

        throw new IllegalArgumentException(format("Cannot convert statistics of type %s to ParquetLongStatistics", min.getClass().getName()));
    }

    @VisibleForTesting
    public Domain getDomain(Type type, ColumnIndex columnIndex, ParquetDataSourceId id, RichColumnDescriptor descriptor)
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

        if (descriptor.getType().equals(PrimitiveTypeName.INT32)
                || descriptor.getType().equals(PrimitiveTypeName.INT64)
                || descriptor.getType().equals(PrimitiveTypeName.FLOAT)
                || (descriptor.getType().equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    && type instanceof DecimalType
                    && ((DecimalType) type).isShort())) {
            List<Long> minimums = converter.getValuesAsLong(type, columnIndex.getMinValues(), columnName);
            if (!minimums.isEmpty()) {
                List<Long> maximums = converter.getValuesAsLong(type, columnIndex.getMaxValues(), columnName);
                if (!maximums.isEmpty()) {
                    return createDomain(type, columnIndex, id, columnName, hasNullValue, minimums, maximums, (BiFunction<Long, Long, ParquetLongStatistics>) (min, max) -> new ParquetLongStatistics(min, max));
                }
            }
        }
        else if (descriptor.getType().equals(PrimitiveTypeName.DOUBLE)) {
            List<Double> minimums = converter.getValuesAsDouble(type, columnIndex.getMinValues(), columnName);
            if (!minimums.isEmpty()) {
                List<Double> maximums = converter.getValuesAsDouble(type, columnIndex.getMaxValues(), columnName);
                if (!maximums.isEmpty()) {
                    return createDomain(type, columnIndex, id, columnName, hasNullValue, minimums, maximums, (BiFunction<Double, Double, ParquetDoubleStatistics>) (min, max) -> new ParquetDoubleStatistics(min, max));
                }
            }
        }
        else if (descriptor.getType().equals(PrimitiveTypeName.BINARY)
                || (descriptor.getType().equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    && type instanceof DecimalType
                    && !((DecimalType) type).isShort())) {
            List<Slice> minimums = converter.getValuesAsSlice(type, columnIndex.getMinValues());
            if (!minimums.isEmpty()) {
                List<Slice> maximums = converter.getValuesAsSlice(type, columnIndex.getMaxValues());
                if (!maximums.isEmpty()) {
                    return createDomain(type, columnIndex, id, columnName, hasNullValue, minimums, maximums, (BiFunction<Slice, Slice, ParquetSliceStatistics>) (min, max) -> new ParquetSliceStatistics(min, max));
                }
            }
        }

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static ParquetLongStatistics toParquetLongStatistics(Statistics<?> statistics, ParquetDataSourceId id, String column)
    {
        if (statistics instanceof LongStatistics) {
            LongStatistics longStatistics = (LongStatistics) statistics;
            return new ParquetLongStatistics(longStatistics.genericGetMin(), longStatistics.genericGetMax());
        }
        else if (statistics instanceof IntStatistics) {
            IntStatistics intStatistics = (IntStatistics) statistics;
            return new ParquetLongStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax());
        }
        else if (statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            Slice minSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMin().getBytes());
            Slice maxSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMax().getBytes());
            long minValue = getShortDecimalValue(minSlice.getBytes());
            long maxValue = getShortDecimalValue(maxSlice.getBytes());
            return new ParquetLongStatistics(minValue, maxValue);
        }

        throw new IllegalArgumentException(format("Cannot convert statistics of type %s for column \"%s\" in Parquet file \"%s\": [%s]", statistics.getClass().getName(), column, id, statistics));
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, DictionaryDescriptor dictionaryDescriptor)
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
        if (type.equals(BIGINT) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT64) {
            List<Long> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add(dictionary.decodeToLong(i));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if ((type.equals(BIGINT) || type.equals(DATE)) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT32) {
            List<Long> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add((long) dictionary.decodeToInt(i));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type.equals(DOUBLE) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE) {
            List<Double> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                double value = dictionary.decodeToDouble(i);
                if (Double.isNaN(value)) {
                    return Domain.all(type);
                }
                values.add(value);
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type.equals(DOUBLE) && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FLOAT) {
            List<Double> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                float value = dictionary.decodeToFloat(i);
                if (Float.isNaN(value)) {
                    return Domain.all(type);
                }
                values.add((double) value);
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type instanceof VarcharType && columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
            List<Slice> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add(Slices.wrappedBuffer(dictionary.decodeToBinary(i).getBytes()));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                List<Long> values = new ArrayList<>(dictionarySize);
                for (int i = 0; i < dictionarySize; i++) {
                    values.add(getShortDecimalValue(dictionary.decodeToBinary(i).getBytes()));
                }
                return Domain.create(ValueSet.copyOf(type, values), true);
            }
            else {
                List<Slice> values = new ArrayList<>(dictionarySize);
                for (int i = 0; i < dictionarySize; i++) {
                    values.add(getLongDecimalValue(dictionary.decodeToBinary(i).getBytes()));
                }
                return Domain.create(ValueSet.copyOf(type, values), true);
            }
        }

        return Domain.all(type);
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
        return createDomain(type, hasNullValue, Collections.singletonList(rangeStatistics));
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, ParquetDataSourceId id, String columnName, boolean hasNullValue, ParquetRangeStatistics<T> rangeStatistics, Statistics<?> statistics)
            throws ParquetCorruptionException
    {
        try {
            return createDomain(type, hasNullValue, rangeStatistics);
        }
        catch (Exception e) {
            throw corruptionException(columnName, id, statistics, e);
        }
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, ParquetDataSourceId id, String columnName, boolean hasNullValue, List<ParquetRangeStatistics<T>> rangeStatistics, ColumnIndex columnIndex)
            throws ParquetCorruptionException
    {
        try {
            return createDomain(type, hasNullValue, rangeStatistics);
        }
        catch (Exception e) {
            throw corruptionException(columnName, id, columnIndex, e);
        }
    }

    private static <F, T extends Comparable<T>> Domain createDomain(
            Type type,
            boolean hasNullValue,
            List<ParquetRangeStatistics<F>> rangeStatistics)
    {
        List<Range> ranges = new ArrayList<>(rangeStatistics.size());
        for (int i = 0; i < rangeStatistics.size(); i++) {
            F min = rangeStatistics.get(i).getMin();
            F max = rangeStatistics.get(i).getMax();
            Range range;
            if (min != null && max != null) {
                range = Range.range(type, min, true, max, true);
            }
            else if (max != null) {
                range = Range.lessThanOrEqual(type, max);
            }
            else if (min != null) {
                range = Range.greaterThanOrEqual(type, min);
            }
            else { // return early as range is unconstrained
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ranges.add(range);
        }

        return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(
            Type type,
            ColumnIndex columnIndex,
            ParquetDataSourceId id,
            String columnName,
            boolean hasNullValue,
            List<T> minimums,
            List<T> maximums,
            BiFunction<? super T, ? super T, ? extends ParquetRangeStatistics<T>> statisticsCreator)
            throws ParquetCorruptionException
    {
        int pageCount = minimums.size();
        List<ParquetRangeStatistics<T>> ranges = new ArrayList<>(pageCount);
        for (int i = 0; i < pageCount; i++) {
            ranges.add(statisticsCreator.apply(minimums.get(i), maximums.get(i)));
        }
        return createDomain(type, id, columnName, hasNullValue, ranges, columnIndex);
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

    private FilterPredicate convertToParquetFilter()
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

            FilterPredicate columnFilter = FilterApi.userDefined(FilterApi.intColumn(ColumnPath.get(column.getPath()).toDotString()), new DomainUserDefinedPredicate(domain));
            if (filter == null) {
                filter = columnFilter;
            }
            else {
                filter = FilterApi.or(filter, columnFilter);
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

        public DomainUserDefinedPredicate(Domain domain)
        {
            this.columnDomain = domain;
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

            if (statistic.getMin() instanceof Integer || statistic.getMin() instanceof Long) {
                Number min = (Number) statistic.getMin();
                Number max = (Number) statistic.getMax();
                return canDropCanWithRangeStats(new ParquetLongStatistics(min.longValue(), max.longValue()));
            }
            else if (statistic.getMin() instanceof Float) {
                Integer min = floatToRawIntBits((Float) statistic.getMin());
                Integer max = floatToRawIntBits((Float) statistic.getMax());
                return canDropCanWithRangeStats(new ParquetLongStatistics(min.longValue(), max.longValue()));
            }
            else if (statistic.getMin() instanceof Double) {
                Double min = (Double) statistic.getMin();
                Double max = (Double) statistic.getMax();
                return canDropCanWithRangeStats(new ParquetDoubleStatistics(min, max));
            }
            else if (statistic.getMin() instanceof Binary) {
                Binary min = (Binary) statistic.getMin();
                Binary max = (Binary) statistic.getMax();
                if (columnDomain.getType() instanceof VarcharType) {
                    return canDropCanWithRangeStats(new ParquetSliceStatistics((Slices.wrappedBuffer(min.getBytes())), Slices.wrappedBuffer(max.getBytes())));
                }
                else if (columnDomain.getType() instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) columnDomain.getType();
                    if (decimalType.isShort()) {
                        return canDropCanWithRangeStats(new ParquetLongStatistics(getShortDecimalValue(min.getBytes()), getShortDecimalValue(max.getBytes())));
                    }
                    else {
                        return canDropCanWithRangeStats(new ParquetSliceStatistics(getLongDecimalValue(min.getBytes()), getLongDecimalValue(max.getBytes())));
                    }
                }
            }

            return false;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics)
        {
            // Since we don't use LogicalNotUserDefined, this method is not called.
            // To be safe, we just keep the record by returning false.
            return false;
        }

        private boolean canDropCanWithRangeStats(ParquetRangeStatistics parquetStatistics)
        {
            Domain domain = createDomain(columnDomain.getType(), true, parquetStatistics);
            return columnDomain.intersect(domain).isNone();
        }
    }

    private class ColumnIndexValueConverter
    {
        private final Map<String, BiFunction<ByteBuffer, Type, Object>> columnIndexConversions;

        private ColumnIndexValueConverter(List<RichColumnDescriptor> columns)
        {
            this.columnIndexConversions = new HashMap<>();
            for (RichColumnDescriptor column : columns) {
                columnIndexConversions.put(column.getPrimitiveType().getName(), getColumnIndexConversions(column.getPrimitiveType()));
            }
        }

        public List<Long> getValuesAsLong(Type type, List<ByteBuffer> buffers, String column)
        {
            int pageCount = buffers.size();
            List<Long> values = new ArrayList<>(pageCount);
            for (int i = 0; i < pageCount; i++) {
                if (BIGINT.equals(type) || TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || DATE.equals(type)) {
                    values.add(((Number) converter.convert(buffers.get(i), type, column)).longValue());
                }
                else if (REAL.equals(type)) {
                    values.add((long) floatToRawIntBits(converter.convert(buffers.get(i), type, column)));
                }
                else if (type instanceof DecimalType) {
                    values.add(converter.convert(buffers.get(i), type, column));
                }
            }
            return values;
        }

        public List<Double> getValuesAsDouble(Type type, List<ByteBuffer> buffers, String column)
        {
            int pageCount = buffers.size();
            List<Double> values = new ArrayList<>(pageCount);
            if (DOUBLE.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    values.add(converter.convert(buffers.get(i), type, column));
                }
            }
            return values;
        }

        public List<Slice> getValuesAsSlice(Type type, List<ByteBuffer> buffers)
        {
            int pageCount = buffers.size();
            List<Slice> values = new ArrayList<>(pageCount);
            if (type instanceof VarcharType) {
                for (int i = 0; i < pageCount; i++) {
                    values.add(Slices.wrappedBuffer(buffers.get(i)));
                }
            }
            else if (type instanceof DecimalType) {
                for (int i = 0; i < pageCount; i++) {
                    values.add(getLongDecimalValue(buffers.get(i)));
                }
            }
            return values;
        }

        private <T> T convert(ByteBuffer buf, Type type, String name)
        {
            return (T) columnIndexConversions.get(name).apply(buf, type);
        }

        private BiFunction<ByteBuffer, Type, Object> getColumnIndexConversions(PrimitiveType primitiveType)
        {
            switch (primitiveType.getPrimitiveTypeName()) {
                case BOOLEAN:
                    return (buffer, type) -> buffer.get(0) != 0;
                case INT32:
                    return (buffer, type) -> buffer.order(LITTLE_ENDIAN).getInt(0);
                case INT64:
                    return (buffer, type) -> buffer.order(LITTLE_ENDIAN).getLong(0);
                case FLOAT:
                    return (buffer, type) -> buffer.order(LITTLE_ENDIAN).getFloat(0);
                case DOUBLE:
                    return (buffer, type) -> buffer.order(LITTLE_ENDIAN).getDouble(0);
                case FIXED_LEN_BYTE_ARRAY:
                    return (buffer, type) ->
                            ((DecimalType) type).isShort()
                                    ? getShortDecimalValue(buffer)
                                    : getLongDecimalValue(buffer);
                default:
                    return (buffer, type) -> buffer;
            }
        }
    }
}
