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

import io.airlift.slice.Slices;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_INVERSE_VALUES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_RANGES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_RANGES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_VALUES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_VALUES;
import static io.trino.spi.predicate.ValueSet.ofRanges;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class PredicateUtilTest
{
    @Test
    public void testCalcPredicateInfo_singleRange()
    {
        Range range = Range.range(IntegerType.INTEGER, 4L, true, 5L, true);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_RANGES, false);
    }

    @Test
    public void testCalcPredicateInfo_singleStringRange()
    {
        Range range = Range.range(VarcharType.createVarcharType(10), Slices.utf8Slice("AAA"), true, Slices.utf8Slice("BBB"), true);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_STRING_RANGES, true);
    }

    @Test
    public void testCalcPredicateInfo_stringRangeSingleValue()
    {
        Range range = Range.equal(VarcharType.createVarcharType(10), Slices.utf8Slice("AAA"));
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);

        validatePredicateType(domain, PREDICATE_TYPE_STRING_VALUES, true);
    }

    @Test
    public void testCalcPredicateInfo_multipleRange()
    {
        Range range1 = Range.range(IntegerType.INTEGER, 4L, true, 5L, true);
        Range range2 = Range.range(IntegerType.INTEGER, 9L, true, 10L, true);
        Domain domain = Domain.create(ValueSet.ofRanges(range1, range2), false);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_RANGES, false);
    }

    @Test
    public void singleRangeButNotSingleValue()
    {
        Range range1 = Range.range(IntegerType.INTEGER, 0L, true, 1L, false);
        Domain domain = Domain.create(ValueSet.ofRanges(range1), false);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_RANGES, false);
    }

    @Test
    public void testCalcPredicateInfo_StringType()
    {
        Range range = Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("blabla"));
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);

        validatePredicateType(domain, PREDICATE_TYPE_STRING_VALUES, false);
    }

    @Test
    public void testPredicateTypeString()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(VarcharType.VARCHAR, Slices.utf8Slice("bla"), true, Slices.utf8Slice("bla2"), true),
                        Range.range(VarcharType.VARCHAR, Slices.utf8Slice("kla"), true, Slices.utf8Slice("kla2"), true)),
                false);
        assertThat(TypeUtils.isStrType(domain.getType())).isTrue();
        validatePredicateType(domain, PREDICATE_TYPE_STRING_RANGES, false);
    }

    @Test
    public void testPredicateTypeStringRangeOfSingleValue()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(VarcharType.VARCHAR, Slices.utf8Slice("bla"), true, Slices.utf8Slice("bla"), true)),
                false);
        assertThat(TypeUtils.isStrType(domain.getType())).isTrue();
        validatePredicateType(domain, PREDICATE_TYPE_STRING_VALUES, false);
    }

    @Test
    public void testCalcPredicateInfo_SingleValue()
    {
        Range range = Range.equal(IntegerType.INTEGER, 5L);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);

        validatePredicateType(domain, PREDICATE_TYPE_VALUES, false);
    }

    @Test
    public void testCalcPredicateInfo_transformAllowedInverseValues()
    {
        Range range1 = Range.greaterThan(IntegerType.INTEGER, 5L);
        Range range2 = Range.lessThan(IntegerType.INTEGER, 5L);

        Domain domain = Domain.create(ValueSet.ofRanges(range1, range2), true);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_INVERSE_VALUES, true);
    }

    @Test
    public void testCalcPredicateInfo_avoidTransformdisallowedInverseValues()
    {
        Range range1 = Range.range(IntegerType.INTEGER, 5L, false, 10L, false);
        Range range2 = Range.lessThan(IntegerType.INTEGER, 5L);

        Domain domain = Domain.create(ValueSet.ofRanges(range1, range2), true);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_RANGES, true);
    }

    @Test
    public void testCalcPredicateInfo_notEquals()
    {
        Range range1 = Range.greaterThan(IntegerType.INTEGER, 4L);
        Range range2 = Range.lessThan(IntegerType.INTEGER, 4L);

        Domain domain = Domain.create(ValueSet.ofRanges(range1, range2), true);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_INVERSE_VALUES, true);
    }

    @Test
    public void testCalcPredicateInfo_multiNotEquals()
    {
        Range range2 = Range.lessThan(IntegerType.INTEGER, 4L);

        Range range3 = Range.range(IntegerType.INTEGER, 4L, false, 8L, false);
        Range range4 = Range.greaterThan(IntegerType.INTEGER, 8L);

        Domain domain = Domain.create(ValueSet.ofRanges(range2, range3, range4), true);

        validatePredicateType(domain, PredicateType.PREDICATE_TYPE_INVERSE_VALUES, true);
    }

    @Test
    public void testCalcPredicateInfo_multiRange()
    {
        Range range2 = Range.lessThan(IntegerType.INTEGER, 4L);

        Range range3 = Range.range(IntegerType.INTEGER, 5L, false, 8L, false);
        Range range4 = Range.greaterThan(IntegerType.INTEGER, 8L);

        Domain domain = Domain.create(ValueSet.ofRanges(range2, range3, range4), true);

        validatePredicateType(domain, PREDICATE_TYPE_RANGES, true);
    }

    @Test
    public void testCalcPredicateInfo_transformNotAllowedInverseValues()
    {
        Range range1 = Range.greaterThan(IntegerType.INTEGER, 5L);
        Range range2 = Range.lessThan(IntegerType.INTEGER, 5L);

        Domain domain = Domain.create(ValueSet.ofRanges(range1, range2), true);

        validatePredicateType(domain, PREDICATE_TYPE_RANGES, false);
    }

    @Test
    public void testAllRangesTheSame_ValuesType()
    {
        Int128 int128 = Decimals.encodeScaledValue(new BigDecimal(50), 0);
        Domain longDecimalDomain = Domain.create(ofRanges(Range.range(DecimalType.createDecimalType(20, 0), int128, true, int128, true)), false);
        assertThat(TypeUtils.isLongDecimalType(longDecimalDomain.getType())).isTrue();

        validatePredicateType(longDecimalDomain, PREDICATE_TYPE_VALUES, false);
    }

    @Test
    public void testNotAllRangesTheSame_RangesType()
    {
        Int128 low = Decimals.encodeScaledValue(new BigDecimal(50), 0);
        Int128 high = Decimals.encodeScaledValue(new BigDecimal(59), 0);
        Domain domain = Domain.create(ofRanges(Range.range(DecimalType.createDecimalType(20, 0), low, true, high, true)), false);
        assertThat(TypeUtils.isLongDecimalType(domain.getType())).isTrue();

        List<Type> typesToTest = List.of(IntegerType.INTEGER, DateType.DATE, TinyintType.TINYINT, DecimalType.createDecimalType(5), BIGINT, SmallintType.SMALLINT);
        for (Type type : typesToTest) {
            domain = Domain.create(ValueSet.ofRanges(Range.range(type, 5L, true, 9L, true)), false);
            validatePredicateType(domain, PredicateType.PREDICATE_TYPE_RANGES, false);
        }
    }

    @Test
    public void allSingleValue()
    {
        Int128 value = Decimals.encodeScaledValue(new BigDecimal(50), 0);
        Domain domain = Domain.create(ofRanges(Range.range(DecimalType.createDecimalType(20, 0), value, true, value, true)), false);
        assertThat(TypeUtils.isLongDecimalType(domain.getType())).isTrue();

        List<Type> typesToTest = List.of(IntegerType.INTEGER, DateType.DATE, TinyintType.TINYINT, DecimalType.createDecimalType(5), BIGINT, SmallintType.SMALLINT);
        for (Type type : typesToTest) {
            Range range1 = Range.range(type, 5L, true, 5L, true);
            Range range2 = Range.range(type, 8L, true, 8L, true);

            domain = Domain.create(ValueSet.ofRanges(range1, range2), true);
            validatePredicateType(domain, PREDICATE_TYPE_VALUES, false);
        }
    }

    private void validatePredicateType(Domain domain, PredicateType expectedPredicateType, boolean transformAllowed)
    {
        int recTypeLength = 1;
        PredicateData predicateData = PredicateUtil.calcPredicateData(domain, recTypeLength, transformAllowed, IntegerType.INTEGER);
        assertThat(predicateData.getPredicateInfo().predicateType()).isEqualTo(expectedPredicateType);
        int expectedNumMatchedRanges = expectedPredicateType == PREDICATE_TYPE_INVERSE_VALUES ? domain.getValues().getRanges().getRangeCount() - 1 : domain.getValues().getRanges().getRangeCount();
        assertThat(predicateData.getPredicateInfo().numValues()).isEqualTo(expectedNumMatchedRanges);
    }
}
