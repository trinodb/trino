/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import it.unimi.dsi.fastutil.HashCommon;

import java.lang.invoke.MethodHandle;
import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.AlwaysFalse;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.IsNotNullFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.IsNullFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongBitSetFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongCustomHashSetFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongHashSetFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongRangeFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.SliceBloomFilter;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Domain.DiscreteSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class TupleDomainFilterUtils
{
    private static final long FILTER_MAX_SIZE_BYTES = 4 * 1024 * 1024;

    private TupleDomainFilterUtils() {}

    static Map<ColumnHandle, TupleDomainFilter> createTupleDomainFilters(
            TupleDomain<ColumnHandle> tupleDomain,
            TypeOperators typeOperators)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");
        checkArgument(!tupleDomain.isNone(), "tupleDomain is none");
        // NONE case is handled in DynamicPageFilter#createPageFilter
        return tupleDomain.getDomains().orElseThrow()
                .entrySet().stream()
                .map(entry -> {
                    Type type = entry.getValue().getType();
                    Optional<TupleDomainFilter> filter = createTupleDomainFilter(entry.getValue(), type, typeOperators);
                    if (filter.isEmpty()) {
                        return Optional.empty();
                    }
                    return Optional.of(new SimpleEntry<>(entry.getKey(), filter.get()));
                })
                .filter(Optional::isPresent)
                .map(optionalEntry -> (SimpleEntry<ColumnHandle, TupleDomainFilter>) optionalEntry.get())
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Optional<TupleDomainFilter> createTupleDomainFilter(Domain domain, Type type, TypeOperators typeOperators)
    {
        if (!(type.isComparable() && type.isOrderable())) {
            return Optional.empty();
        }
        if (domain.isNullableDiscreteSet()) {
            return createTupleDomainFilter(domain.getNullableDiscreteSet(), type, typeOperators);
        }

        // When type is orderable, ValueSet is always a SortedRangeSet
        return createTupleDomainFilter(domain.isNullAllowed(), domain.getValues().getRanges().getSpan());
    }

    private static Optional<TupleDomainFilter> createTupleDomainFilter(DiscreteSet nullableDiscreteSet, Type type, TypeOperators typeOperators)
    {
        if (type.getJavaType() == long.class) {
            return createTupleDomainFilterForLongJavaType(nullableDiscreteSet, type, typeOperators);
        }
        if (type.getJavaType() == Slice.class) {
            return createTupleDomainFilterForSliceJavaType(nullableDiscreteSet, type);
        }
        // Skipping boolean type as joins on boolean data type are rare
        // Skipping double type as equality joins on double are rare
        return Optional.empty();
    }

    private static Optional<TupleDomainFilter> createTupleDomainFilterForLongJavaType(DiscreteSet nullableDiscreteSet, Type type, TypeOperators typeOperators)
    {
        checkArgument(
                type.getJavaType() == long.class,
                "Expected java type to be long");
        boolean nullAllowed = nullableDiscreteSet.containsNull();
        @SuppressWarnings("unchecked")
        List<Long> values = (List<Long>) (List<?>) nullableDiscreteSet.getNonNullValues();

        if (values.isEmpty()) {
            checkState(nullAllowed, "Unexpected always-false filter");
            return Optional.of(new IsNullFilter(type));
        }

        long min = values.get(0);
        long max = min;
        for (int i = 1; i < values.size(); i++) {
            min = Math.min(min, values.get(i));
            max = Math.max(max, values.get(i));
        }
        boolean isIntegerArithmeticValid = isIntegerArithmeticValidType(type);
        // If all values within a range are included, use a range filter
        if (isIntegerArithmeticValid && (max - min + 1) == values.size()) {
            return Optional.of(new LongRangeFilter(nullAllowed, type, min, max));
        }
        // Filter based on a fast utils hash set uses next power of two (size / load-factor)
        // slots in a hash table which is an array of longs.
        long fastUtilsSetEntries = (int) Math.min(
                1 << 30,
                HashCommon.nextPowerOfTwo((long) Math.ceil(values.size() / 0.25f)));
        // Filter based on a bitmap uses (max - min) / 64 longs
        // Choose the filter that uses fewer entries
        BigInteger range = BigInteger.valueOf(max)
                .subtract(BigInteger.valueOf(min))
                .add(BigInteger.valueOf(1));
        if (!isIntegerArithmeticValid
                || range.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0
                || (range.intValueExact() / 64) + 1 > fastUtilsSetEntries) {
            if (fastUtilsSetEntries * 8 > FILTER_MAX_SIZE_BYTES) {
                // Bail out on filter size above 4MB
                return Optional.empty();
            }
            if (isIntegerArithmeticValid) {
                return Optional.of(new LongHashSetFilter(nullAllowed, type, values, min, max));
            }
            MethodHandle hashCodeHandle = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            MethodHandle equalsHandle = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            return Optional.of(new LongCustomHashSetFilter(nullAllowed, type, hashCodeHandle, equalsHandle, values));
        }
        if ((range.intValueExact() / 64 + 1) * 8 > FILTER_MAX_SIZE_BYTES) {
            // Bail out on filter size above 4MB
            return Optional.empty();
        }
        return Optional.of(new LongBitSetFilter(nullAllowed, type, values, min, max));
    }

    private static Optional<TupleDomainFilter> createTupleDomainFilterForSliceJavaType(DiscreteSet nullableDiscreteSet, Type type)
    {
        checkArgument(
                type.getJavaType() == Slice.class,
                "Expected java type to be Slice");
        boolean nullAllowed = nullableDiscreteSet.containsNull();
        @SuppressWarnings("unchecked")
        List<Slice> values = (List<Slice>) (List<?>) nullableDiscreteSet.getNonNullValues();

        if (values.isEmpty()) {
            checkState(nullAllowed, "Unexpected always-false filter");
            return Optional.of(new IsNullFilter(type));
        }

        if (!(type instanceof VarcharType || type instanceof CharType)) {
            return Optional.empty();
        }
        if (SliceBloomFilter.getBloomFilterSize(values.size()) > FILTER_MAX_SIZE_BYTES / 8) {
            return Optional.empty();
        }
        return Optional.of(new SliceBloomFilter(nullAllowed, type, values));
    }

    private static Optional<TupleDomainFilter> createTupleDomainFilter(boolean nullAllowed, Range range)
    {
        Type type = range.getType();
        if (type.getJavaType() != long.class) {
            return Optional.empty();
        }
        if (!isIntegerArithmeticValidType(type)) {
            // TODO (https://starburstdata.atlassian.net/browse/SEP-6647) add support for double/real type here
            // as they may be used in in-equality joins. We receive range filter for in-equality join case
            return Optional.empty();
        }
        long lower = range.getLowValue().map(Long.class::cast).orElse(Long.MIN_VALUE);
        long upper = range.getHighValue().map(Long.class::cast).orElse(Long.MAX_VALUE);
        if (range.isLowUnbounded() && range.isHighUnbounded()) {
            checkState(!nullAllowed, "Unexpected range of ALL values");
            return Optional.of(new IsNotNullFilter(type));
        }
        if (!range.isLowUnbounded() && !range.isLowInclusive()) {
            if (lower == Long.MAX_VALUE) {
                return Optional.of(getNullOrFalseFilter(nullAllowed, type));
            }
            lower++;
        }
        if (!range.isHighUnbounded() && !range.isHighInclusive()) {
            if (upper == Long.MIN_VALUE) {
                return Optional.of(getNullOrFalseFilter(nullAllowed, type));
            }
            upper--;
        }
        if (upper < lower) {
            return Optional.of(getNullOrFalseFilter(nullAllowed, type));
        }
        return Optional.of(new LongRangeFilter(nullAllowed, type, lower, upper));
    }

    private static TupleDomainFilter getNullOrFalseFilter(boolean nullAllowed, Type type)
    {
        if (nullAllowed) {
            return new IsNullFilter(type);
        }
        return new AlwaysFalse(type);
    }

    private static boolean isIntegerArithmeticValidType(Type type)
    {
        // Types for which we can safely use equality and comparison on the stored long value
        // instead of going through type specific methods
        return type == TINYINT ||
                type == SMALLINT ||
                type == INTEGER ||
                type == BIGINT ||
                type == TIME_SECONDS ||
                type == TIME_MILLIS ||
                type == TIME_MICROS ||
                type == TIME_NANOS ||
                type == TIME_PICOS ||
                type == DATE ||
                type == TIMESTAMP_SECONDS ||
                type == TIMESTAMP_MILLIS ||
                type == TIMESTAMP_MICROS ||
                (type instanceof DecimalType && ((DecimalType) type).isShort());
    }
}
