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
package io.trino.plugin.varada.dispatcher.query.classifier;

import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.PredicateInfo;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public class PredicateUtil
{
    private static final Logger logger = Logger.get(PredicateUtil.class);

    public static final int PREDICATE_HEADER_SIZE = 5; // determined by the storage engine layer and verified to be correct at NativeStorageEngine init

    private PredicateUtil()
    {
    }

    static boolean canApplyPredicate(Optional<WarmUpElement> warmUpElement, Type type)
    {
        // No support for: arrays, non-orderable types (since we need the values sorted in the predicate)
        return warmUpElement.isPresent() && !(type instanceof ArrayType) &&
                (type.isOrderable() || warmUpElement.get().getVaradaColumn().isTransformedColumn());
    }

    static PredicateData calcPredicateData(NativeExpression nativeExpression, int recTypeLength, boolean transformAllowed, Type columnType)
    {
        int numMatchElements;
        int predicateSize = PREDICATE_HEADER_SIZE;
        Domain domain = nativeExpression.domain();
        ValueSet values = domain.getValues();
        checkArgument(values instanceof SortedRangeSet, "unsupported ValueSet " + values.getClass());
        SortedRangeSet sortedRangeSet = (SortedRangeSet) values;
        numMatchElements = sortedRangeSet.getRangeCount();
        Type type = domain.getType();
        PredicateType predicateType = nativeExpression.predicateType();

        FunctionType functionType = nativeExpression.functionType();
        if (functionType == FunctionType.FUNCTION_TYPE_TRANSFORMED) {
            functionType = FunctionType.FUNCTION_TYPE_NONE;
        }
        if (functionType != FunctionType.FUNCTION_TYPE_NONE) {
            predicateSize += Byte.BYTES;
            if (type instanceof TimestampType) {
                predicateSize += Byte.BYTES;
            }
        }
        if (nativeExpression.functionParams().size() == 1 && functionType == FunctionType.FUNCTION_TYPE_CAST) {
            predicateSize += Integer.BYTES;
        }
        else if (nativeExpression.functionParams().size() >= 1) {
            throw new UnsupportedOperationException(format("unfamiliar function params nativeExpression=%s", nativeExpression));
        }

        if (numMatchElements > 0) {
            if (TypeUtils.isStrType(type)) {
                if (nativeExpression.allSingleValue()) {
                    checkArgument(predicateType == PredicateType.PREDICATE_TYPE_STRING_VALUES, "predicateType is not string values as expected");
                    predicateSize += predicateSizeStringValues(numMatchElements);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_STRING;
                    numMatchElements--; // we ignore inifinity
                    predicateSize += predicateSizeStringInverseValues(numMatchElements);
                }
                else {
                    checkArgument(predicateType == PredicateType.PREDICATE_TYPE_STRING_RANGES, "predicateType is not string ranges as expected");
                    predicateSize += predicateSizeStringRanges(numMatchElements);
                }
            }
            else {
                // @TODO in testMapMultipleMapTypes we have a predicate that is all single but also ranges
                // this is why we check here if type is values and not force it until the issue is fixed
                if (nativeExpression.allSingleValue() && (predicateType == PredicateType.PREDICATE_TYPE_VALUES)) { // values and not ranges
                    predicateSize += predicateSizeValues(numMatchElements, recTypeLength);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_VALUES;
                    numMatchElements--; // (-inf, 5),(5, 10),(10, inf) - we ignore infinity and take 5, 10
                    predicateSize += predicateSizeInverseValues(numMatchElements, recTypeLength);
                }
                else {
                    logger.debug("nativeExpression all-single %b predicate-type %s", nativeExpression.allSingleValue(), predicateType);
                    checkArgument(predicateType == PredicateType.PREDICATE_TYPE_RANGES, "predicateType is not ranges as expected");
                    predicateSize += predicateSizeRanges(numMatchElements, recTypeLength);
                }
            }
        }

        PredicateInfo predicateInfo = new PredicateInfo(predicateType, functionType, numMatchElements, nativeExpression.functionParams(), recTypeLength);
        int hashCode = nativeExpression.domain().getValues().isNone() ?
                Objects.hash(domain.getValues().isNone()) :
                Objects.hash(domain.getValues().getRanges().getSpan().isLowUnbounded(), domain.getValues().getRanges().getSpan().isLowUnbounded());
        hashCode = Objects.hash(nativeExpression, columnType, hashCode, predicateType);
        return PredicateData.builder()
                            .predicateHashCode(hashCode)
                            .isCollectNulls(domain.isNullAllowed())
                            .predicateSize(predicateSize)
                            .predicateInfo(predicateInfo)
                            .columnType(columnType)
                            .build();
    }

    static PredicateData calcPredicateData(Domain domain, int recTypeLength, boolean transformAllowed, Type columnType)
    {
        ValueSet values = domain.getValues();
        checkArgument(values instanceof SortedRangeSet, "unsupported ValueSet " + values.getClass());
        SortedRangeSet sortedRangeSet = (SortedRangeSet) values;
        Block sortedRanges = sortedRangeSet.getSortedRanges();
        boolean[] inclusive = sortedRangeSet.getInclusive();
        int numMatchElements = sortedRangeSet.getRangeCount();
        Type type = domain.getType();
        int predicateSize = PREDICATE_HEADER_SIZE;
        PredicateType predicateType = PredicateType.PREDICATE_TYPE_NONE;

        if (numMatchElements > 0) {
            if (TypeUtils.isStrType(type)) {
                if (isAllSingleValue(inclusive, sortedRanges, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_STRING_VALUES;
                    predicateSize += predicateSizeStringValues(numMatchElements);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_STRING;
                    numMatchElements--; // we ignore inifinity
                    predicateSize += predicateSizeStringInverseValues(numMatchElements);
                }
                else {
                    predicateType = PredicateType.PREDICATE_TYPE_STRING_RANGES;
                    predicateSize += predicateSizeStringRanges(numMatchElements);
                }
            }
            else {
                if (isAllSingleValue(inclusive, sortedRanges, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_VALUES;
                    predicateSize += predicateSizeValues(numMatchElements, recTypeLength);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_VALUES;
                    numMatchElements--; // (-inf, 5),(5, 10),(10, inf) - we ignore infinity and take 5, 10
                    predicateSize += predicateSizeInverseValues(numMatchElements, recTypeLength);
                }
                else {
                    predicateType = PredicateType.PREDICATE_TYPE_RANGES;
                    predicateSize += predicateSizeRanges(numMatchElements, recTypeLength);
                }
            }
        }

        PredicateInfo predicateInfo = new PredicateInfo(predicateType, FunctionType.FUNCTION_TYPE_NONE, numMatchElements, Collections.emptyList(), recTypeLength);
        int hashCode = domain.getValues().isNone() ?
                Objects.hash(domain.getValues().isNone()) :
                Objects.hash(domain.getValues().getRanges().getSpan().isLowUnbounded(), domain.getValues().getRanges().getSpan().isLowUnbounded());
        hashCode = Objects.hash(domain, columnType, hashCode, predicateType);
        return PredicateData
                .builder()
                .predicateHashCode(hashCode)
                .isCollectNulls(domain.isNullAllowed())
                .predicateSize(predicateSize)
                .predicateInfo(predicateInfo)
                .columnType(columnType)
                .build();
    }

    static PredicateType calcPredicateType(Domain domain, Type columnType)
    {
        return calcPredicateData(domain, 0, false, columnType).getPredicateInfo().predicateType();
    }

    private static int predicateSizeStringValues(int numMatchElements)
    {
        return Math.multiplyExact(numMatchElements + 2, Long.BYTES); // +2 for the min/max
    }

    private static int predicateSizeStringInverseValues(int numMatchElements)
    {
        return Math.multiplyExact(numMatchElements * 2, Long.BYTES); // *2 since for inverse string we put crc and str2int
    }

    private static int predicateSizeStringRanges(int numMatchElements)
    {
        return Math.multiplyExact(numMatchElements * 2, Long.BYTES + 1); // *2 for range, +1 for inclusive/exclusive
    }

    private static int predicateSizeValues(int numMatchElements, int recTypeLength)
    {
        return Math.multiplyExact(numMatchElements, recTypeLength);
    }

    private static int predicateSizeInverseValues(int numMatchElements, int recTypeLength)
    {
        return Math.multiplyExact(numMatchElements, recTypeLength);
    }

    private static int predicateSizeRanges(int numMatchElements, int recTypeLength)
    {
        return Math.multiplyExact(numMatchElements * 2, recTypeLength + 1); // *2 for range, +1 for inclusive/exclusive
    }

    // check if we can inverse the predicate since its more efficient in native.
    // for example: x != 7 (PredicateType.PREDICATE_TYPE_RANGES) to col1 = 7 (predicateType = PredicateType.PREDICATE_TYPE_INVERSE_VALUES)
    // This can be done only if match collect isn't enabled - this condition is checked by the caller
    public static boolean isInversePredicate(SortedRangeSet sortedRangeSet, Type type)
    {
        int rangeCount = sortedRangeSet.getRangeCount();
        if (TypeUtils.isRealType(type) || TypeUtils.isBooleanType(type)) {
            return false;
        }

        if (rangeCount <= 1) {
            return false;
        }
        /* this supports the formats:
         * col != 'value'
         * col NOT IN ('value1', 'value2')
         *
         * the case of col <= 3 AND col >= 5 which implies col != 4 is not supported
         */
        for (boolean isInclusive : sortedRangeSet.getInclusive()) {
            if (isInclusive) {
                return false;
            }
        }
        List<Range> orderedRanges = sortedRangeSet.getOrderedRanges();

        if (!(orderedRanges.get(0).isLowUnbounded() && orderedRanges.get(rangeCount - 1).isHighUnbounded())) {
            return false;
        }

        for (int rangeIdx = 0; rangeIdx < rangeCount - 1; rangeIdx++) {
            Range currentRange = orderedRanges.get(rangeIdx);
            Range nextRange = orderedRanges.get(rangeIdx + 1);
            if (!currentRange.getHighBoundedValue().equals(nextRange.getLowBoundedValue())) {
                return false;
            }
        }

        return true;
    }

    //this method replaced sortedRangeSet.getOrderedRanges().stream().allMatch(range -> range.isSingleValue()) because of complexity of getOrderRanges
    public static boolean isAllSingleValue(boolean[] inclusive, Block sortedRangesBlock, Type type)
    {
        for (boolean isInclusive : inclusive) {
            if (!isInclusive) {
                return false;
            }
        }
        if (TypeUtils.isStrType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!type.getSlice(sortedRangesBlock, i).equals(type.getSlice(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isLongDecimalType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!type.getObject(sortedRangesBlock, i).equals(type.getObject(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isLongType(type) || TypeUtils.isShortDecimalType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(type.getLong(sortedRangesBlock, i) == type.getLong(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isIntegerType(type) || TypeUtils.isDateType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(INTEGER.getInt(sortedRangesBlock, i) == INTEGER.getInt(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isBooleanType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(type.getBoolean(sortedRangesBlock, i) == type.getBoolean(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isDoubleType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(type.getDouble(sortedRangesBlock, i) == type.getDouble(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isRealType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(Float.intBitsToFloat(REAL.getInt(sortedRangesBlock, i)) == Float.intBitsToFloat(REAL.getInt(sortedRangesBlock, i + 1)))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isSmallIntType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(SMALLINT.getShort(sortedRangesBlock, i) == SMALLINT.getShort(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else if (TypeUtils.isTinyIntType(type)) {
            for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                if (!(TINYINT.getByte(sortedRangesBlock, i) == TINYINT.getByte(sortedRangesBlock, i + 1))) {
                    return false;
                }
            }
        }
        else {
            throw new UnsupportedOperationException("not supported type=" + type + ", inclusive=" + Arrays.toString(inclusive));
        }
        return true;
    }

    public static boolean canMapMatchCollect(Type type, PredicateType predicateType, FunctionType functionType, int numValues)
    {
        return TypeUtils.isMappedMatchCollectSupportedTypes(type) &&
                (predicateType == PredicateType.PREDICATE_TYPE_VALUES) &&
                (functionType == FunctionType.FUNCTION_TYPE_NONE) &&
                (numValues <= GlobalConfiguration.MAX_NUMBER_OF_MAPPED_MATCH_COLLECT_ELEMENTS);
    }
}
