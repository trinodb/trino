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

import io.airlift.log.Logger;
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
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimePicosToPaimonMillis;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimestampToPaimon;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimestampWithTimeZoneToPaimon;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;

public class PaimonFilterConverter
{
    private static final Logger LOG = Logger.get(PaimonFilterConverter.class);

    private final RowType rowType;
    private final PredicateBuilder builder;

    public PaimonFilterConverter(RowType rowType)
    {
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.builder = new PredicateBuilder(rowType);
    }

    public Optional<Predicate> convert(TupleDomain<PaimonColumnHandle> tupleDomain)
    {
        return convert(tupleDomain, new LinkedHashMap<>(), new LinkedHashMap<>(), false);
    }

    public Optional<Predicate> convertForFileIndex(TupleDomain<PaimonColumnHandle> tupleDomain)
    {
        return convert(tupleDomain, new LinkedHashMap<>(), new LinkedHashMap<>(), true);
    }

    public Optional<Predicate> convert(
            TupleDomain<PaimonColumnHandle> tupleDomain,
            HashMap<PaimonColumnHandle, Domain> acceptedDomains,
            HashMap<PaimonColumnHandle, Domain> unsupportedDomains)
    {
        return convert(tupleDomain, acceptedDomains, unsupportedDomains, false);
    }

    private Optional<Predicate> convert(
            TupleDomain<PaimonColumnHandle> tupleDomain,
            HashMap<PaimonColumnHandle, Domain> acceptedDomains,
            HashMap<PaimonColumnHandle, Domain> unsupportedDomains,
            boolean includeFileIndexColumns)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");
        requireNonNull(acceptedDomains, "acceptedDomains is null");
        requireNonNull(unsupportedDomains, "unsupportedDomains is null");
        if (tupleDomain.isAll()) {
            // alwaysTrue - no filtering needed, return empty to skip filter
            return Optional.empty();
        }

        if (tupleDomain.isNone()) {
            // alwaysFalse - filter out all rows
            return Optional.of(PredicateBuilder.alwaysFalse());
        }

        Map<PaimonColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        Map<String, Integer> fieldNameIndexes = FieldNameUtils.fieldNameIndexes(rowType);
        for (Map.Entry<PaimonColumnHandle, Domain> entry : domainMap.entrySet()) {
            PaimonColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            String field = columnHandle.getColumnName();
            Optional<Integer> nestedColumn = FileIndexOptions.topLevelIndexOfNested(field);
            if (nestedColumn.isPresent()) {
                if (!includeFileIndexColumns) {
                    unsupportedDomains.put(columnHandle, domain);
                    continue;
                }
                int position = nestedColumn.get();
                field = field.substring(0, position);
            }
            // Fix case-sensitivity issue: fieldNameIndexes keys are lowercase, so convert field to lowercase for lookup
            Integer index = fieldNameIndexes.get(FieldNameUtils.toLowerCase(field));
            if (index != null) {
                try {
                    conjuncts.add(toPredicate(
                            index,
                            columnHandle.getColumnName(),
                            columnHandle.logicalType(),
                            columnHandle.getTrinoType(),
                            domain,
                            nestedColumn.isPresent()));
                    acceptedDomains.put(columnHandle, domain);
                    continue;
                }
                catch (UnsupportedOperationException | ArithmeticException | IllegalArgumentException exception) {
                    LOG.debug(exception, "Predicate is not supported for pushdown");
                }
            }
            unsupportedDomains.put(columnHandle, domain);
        }

        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(and(conjuncts));
    }

    private Predicate toPredicate(
            int columnIndex,
            String field,
            DataType logicalType,
            Type type,
            Domain domain,
            boolean fileIndexNestedColumn)
    {
        if (fileIndexNestedColumn) {
            return toFileIndexMapElementPredicate(columnIndex, field, logicalType, type, domain);
        }

        if (domain.isAll()) {
            // alwaysTrue for this column - no predicate needed, throw to skip
            throw new UnsupportedOperationException("Domain is ALL, no predicate needed for column: " + field);
        }
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return builder.isNull(columnIndex);
            }
            // alwaysFalse - no values match and null not allowed
            return PredicateBuilder.alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                // alwaysTrue - all values including null are allowed, no predicate needed
                throw new UnsupportedOperationException("Domain allows all values including null for column: " + field);
            }
            return builder.isNotNull(columnIndex);
        }

        // Structural types support only NULL checks at the Paimon predicate layer.
        if (type instanceof ArrayType || type instanceof MapType || type instanceof io.trino.spi.type.RowType) {
            throw new UnsupportedOperationException(
                    "Value-based predicates on structural types are not supported: " + type);
        }
        if (logicalType.getTypeRoot() == DataTypeRoot.BLOB) {
            throw new UnsupportedOperationException(
                    "Value-based predicates on Paimon BLOB columns are not supported: " + field);
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
            return or(predicates);
        }

        throw new UnsupportedOperationException();
    }

    private Predicate toFileIndexMapElementPredicate(
            int columnIndex,
            String field,
            DataType logicalType,
            Type type,
            Domain domain)
    {
        if (!(type instanceof MapType mapType)) {
            throw new UnsupportedOperationException("File-index nested predicates require a map column: " + field);
        }
        if (!(logicalType instanceof org.apache.paimon.types.MapType paimonMapType)) {
            throw new UnsupportedOperationException("File-index nested predicates require a Paimon MAP column: " + field);
        }
        DataType valueLogicalType = paimonMapType.getValueType();

        if (domain.isAll()) {
            throw new UnsupportedOperationException("Domain is ALL, no predicate needed for file-index column: " + field);
        }
        if (domain.isNullAllowed()) {
            throw new UnsupportedOperationException("File-index map element predicates with NULL are not supported: " + field);
        }
        if (domain.getValues().isNone()) {
            return PredicateBuilder.alwaysFalse();
        }
        if (domain.getValues().isAll()) {
            throw new UnsupportedOperationException("Only equality/IN file-index map element predicates are supported: " + field);
        }

        List<Object> values = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (!range.isSingleValue()) {
                throw new UnsupportedOperationException("Only equality/IN file-index map element predicates are supported: " + field);
            }
            values.add(getLiteralValue(mapType.getValueType(), range.getSingleValue()));
        }
        if (values.isEmpty()) {
            return PredicateBuilder.alwaysFalse();
        }
        return new LeafPredicate(In.INSTANCE, valueLogicalType, columnIndex, field, values);
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

    static Object getLiteralValue(Type type, Object trinoNativeValue)
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

        if (type instanceof TimeType) {
            return trinoTimePicosToPaimonMillis((long) trinoNativeValue);
        }

        if (type instanceof TimestampType) {
            return trinoTimestampToPaimon(trinoNativeValue);
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return trinoTimestampWithTimeZoneToPaimon(trinoNativeValue);
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
                bigDecimal = new BigDecimal(DecimalUtils.toBigInteger(trinoNativeValue), decimalType.getScale());
            }
            return Decimal.fromBigDecimal(bigDecimal, decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
