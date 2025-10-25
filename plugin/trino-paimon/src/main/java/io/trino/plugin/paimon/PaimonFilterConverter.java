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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.paimon.PaimonTypeUtils.fieldNames;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.data.Decimal.fromBigDecimal;
import static org.apache.paimon.predicate.PredicateBuilder.and;

public class PaimonFilterConverter
{
    private static final Logger LOG = LoggerFactory.getLogger(PaimonFilterConverter.class);

    private final RowType rowType;
    private final PredicateBuilder builder;

    public PaimonFilterConverter(RowType rowType)
    {
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.builder = new PredicateBuilder(rowType);
    }

    public Optional<Predicate> convert(TupleDomain<PaimonColumnHandle> tupleDomain)
    {
        return convert(tupleDomain, new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    public Optional<Predicate> convert(
            TupleDomain<PaimonColumnHandle> tupleDomain,
            Map<PaimonColumnHandle, Domain> acceptedDomains,
            Map<PaimonColumnHandle, Domain> unsupportedDomains)
    {
        if (tupleDomain.isAll()) {
            // TODO alwaysTrue
            return Optional.empty();
        }

        if (tupleDomain.getDomains().isEmpty()) {
            // TODO alwaysFalse
            return Optional.empty();
        }

        Map<PaimonColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        List<String> fieldNames = fieldNames(rowType);
        for (Map.Entry<PaimonColumnHandle, Domain> entry : domainMap.entrySet()) {
            PaimonColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            String field = columnHandle.columnName();
            Optional<Integer> nestedColumn = FileIndexOptions.topLevelIndexOfNested(field);
            if (nestedColumn.isPresent()) {
                int position = nestedColumn.get();
                field = field.substring(0, position);
            }
            int index = fieldNames.indexOf(field);
            if (index != -1) {
                try {
                    toPredicate(
                            index,
                            columnHandle.columnName(),
                            columnHandle.trinoType(),
                            domain).ifPresent(conjuncts::add);
                    acceptedDomains.put(columnHandle, domain);
                    continue;
                }
                catch (UnsupportedOperationException exception) {
                    LOG.warn("Unsupported predicate, maybe the type of column is not supported yet", exception);
                }
            }
            unsupportedDomains.put(columnHandle, domain);
        }

        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(and(conjuncts));
    }

    private Optional<Predicate> toPredicate(int columnIndex, String field, Type type, Domain domain)
    {
        if (domain.isAll()) {
            // TODO alwaysTrue
            throw new UnsupportedOperationException();
        }
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return Optional.of(builder.isNull(columnIndex));
            }
            // TODO alwaysFalse
            throw new UnsupportedOperationException();
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                // TODO alwaysTrue
                throw new UnsupportedOperationException();
            }
            return Optional.of(builder.isNotNull(columnIndex));
        }

        // TODO support structural types
        switch (type) {
            case ArrayType _ -> throw new UnsupportedOperationException();
            case io.trino.spi.type.RowType _ -> throw new UnsupportedOperationException();
            case MapType maptype -> {
                List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
                List<Object> values = new ArrayList<>();
                Predicate predicate = null;
                for (Range range : orderedRanges) {
                    if (range.isSingleValue()) {
                        values.add(
                                getLiteralValue(maptype.getValueType(), range.getLowBoundedValue()));
                    }
                }
                if (!values.isEmpty()) {
                    predicate =
                            new LeafPredicate(
                                    In.INSTANCE,
                                    PaimonTypeUtils.toPaimonType(type),
                                    columnIndex,
                                    field,
                                    values);
                }
                return Optional.ofNullable(predicate);
            }
            default -> {}
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> values = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    values.add(getLiteralValue(type, range.getLowBoundedValue()));
                }
                else {
                    predicates.add(toPredicate(columnIndex, range));
                }
            }

            if (!values.isEmpty()) {
                predicates.add(builder.in(columnIndex, values));
            }

            if (domain.isNullAllowed()) {
                predicates.add(builder.isNull(columnIndex));
            }
            Predicate predicate = predicates.size() > 1
                    ? new CompoundPredicate(Or.INSTANCE, predicates)
                    : predicates.getFirst();
            return Optional.of(predicate);
        }

        throw new UnsupportedOperationException();
    }

    private Predicate toPredicate(int columnIndex, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object value = getLiteralValue(type, range.getSingleValue());
            return builder.equal(columnIndex, value);
        }

        List<Predicate> conjuncts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            Object low = getLiteralValue(type, range.getLowBoundedValue());
            Predicate lowBound;
            if (range.isLowInclusive()) {
                lowBound = builder.greaterOrEqual(columnIndex, low);
            }
            else {
                lowBound = builder.greaterThan(columnIndex, low);
            }
            conjuncts.add(lowBound);
        }

        if (!range.isHighUnbounded()) {
            Object high = getLiteralValue(type, range.getHighBoundedValue());
            Predicate highBound;
            if (range.isHighInclusive()) {
                highBound = builder.lessOrEqual(columnIndex, high);
            }
            else {
                highBound = builder.lessThan(columnIndex, high);
            }
            conjuncts.add(highBound);
        }

        return and(conjuncts);
    }

    private static Object getLiteralValue(Type type, Object trinoNativeValue)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");

        if (type instanceof BooleanType) {
            return trinoNativeValue;
        }

        if (type instanceof TinyintType) {
            return ((Long) trinoNativeValue).byteValue();
        }

        if (type instanceof SmallintType) {
            return ((Long) trinoNativeValue).shortValue();
        }

        if (type instanceof IntegerType) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type instanceof BigintType) {
            return trinoNativeValue;
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) trinoNativeValue));
        }

        if (type instanceof DoubleType) {
            return trinoNativeValue;
        }

        if (type instanceof DateType) {
            return toIntExact(((Long) trinoNativeValue));
        }

        if (type.equals(TIME_MILLIS)) {
            return (int) ((long) trinoNativeValue / PICOSECONDS_PER_MILLISECOND);
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            return Timestamp.fromEpochMillis((long) trinoNativeValue / 1000);
        }

        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            if (trinoNativeValue instanceof Long) {
                return trinoNativeValue;
            }
            return Timestamp.fromEpochMillis(((LongTimestampWithTimeZone) trinoNativeValue).getEpochMillis());
        }

        if (type instanceof VarcharType || type instanceof CharType) {
            return BinaryString.fromBytes(((Slice) trinoNativeValue).getBytes());
        }

        if (type instanceof VarbinaryType) {
            return ((Slice) trinoNativeValue).getBytes();
        }

        if (type instanceof DecimalType decimalType) {
            BigDecimal bigDecimal;
            if (trinoNativeValue instanceof Long) {
                bigDecimal = BigDecimal.valueOf((long) trinoNativeValue).movePointLeft(decimalType.getScale());
            }
            else {
                bigDecimal = new BigDecimal(((Int128) trinoNativeValue).toBigInteger(), decimalType.getScale());
            }
            return fromBigDecimal(
                    bigDecimal, decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
