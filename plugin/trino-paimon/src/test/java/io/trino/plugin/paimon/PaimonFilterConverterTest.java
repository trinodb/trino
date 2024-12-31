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

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PaimonFilterConverter}.
 */
public class PaimonFilterConverterTest
{
    @Test
    public void testAll()
    {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", new IntType(), 0);
        TupleDomain<PaimonColumnHandle> isNull =
                TupleDomain.withColumnDomains(ImmutableMap.of(idColumn, Domain.onlyNull(INTEGER)));
        Predicate expectedIsNull = builder.isNull(0);
        Predicate actualIsNull = converter.convert(isNull).get();
        assertThat(actualIsNull).isEqualTo(expectedIsNull);

        TupleDomain<PaimonColumnHandle> isNotNull =
                TupleDomain.withColumnDomains(ImmutableMap.of(idColumn, Domain.notNull(INTEGER)));
        Predicate expectedIsNotNull = builder.isNotNull(0);
        Predicate actualIsNotNull = converter.convert(isNotNull).get();
        assertThat(actualIsNotNull).isEqualTo(expectedIsNotNull);

        TupleDomain<PaimonColumnHandle> lt =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.lessThan(INTEGER, 1L)), false)));
        Predicate expectedLt = builder.lessThan(0, 1);
        Predicate actualLt = converter.convert(lt).get();
        assertThat(actualLt).isEqualTo(expectedLt);

        TupleDomain<PaimonColumnHandle> ltEq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 1L)),
                                        false)));
        Predicate expectedLtEq = builder.lessOrEqual(0, 1);
        Predicate actualLtEq = converter.convert(ltEq).get();
        assertThat(actualLtEq).isEqualTo(expectedLtEq);

        TupleDomain<PaimonColumnHandle> gt =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(INTEGER, 1L)), false)));
        Predicate expectedGt = builder.greaterThan(0, 1);
        Predicate actualGt = converter.convert(gt).get();
        assertThat(actualGt).isEqualTo(expectedGt);

        TupleDomain<PaimonColumnHandle> gtEq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 1L)),
                                        false)));
        Predicate expectedGtEq = builder.greaterOrEqual(0, 1);
        Predicate actualGtEq = converter.convert(gtEq).get();
        assertThat(actualGtEq).isEqualTo(expectedGtEq);

        TupleDomain<PaimonColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(idColumn, Domain.singleValue(INTEGER, 1L)));
        Predicate expectedEq = builder.equal(0, 1);
        Predicate actualEq = converter.convert(eq).get();
        assertThat(actualEq).isEqualTo(expectedEq);

        TupleDomain<PaimonColumnHandle> in =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.multipleValues(INTEGER, Arrays.asList(1L, 2L, 3L))));
        Predicate expectedIn = builder.in(0, Arrays.asList(1, 2, 3));
        Predicate actualIn = converter.convert(in).get();
        assertThat(actualIn).isEqualTo(expectedIn);
    }

    @Test
    public void testCharType()
    {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        0, "date", new org.apache.paimon.types.CharType(10))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn =
                PaimonColumnHandle.of("date", new org.apache.paimon.types.CharType(10), 0);
        TupleDomain<PaimonColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn,
                                Domain.singleValue(
                                        CharType.createCharType(10),
                                        Slices.utf8Slice("2020-11-11"))));
        Predicate expectedEqq = builder.equal(0, BinaryString.fromString("2020-11-11"));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTimeStamp()
    {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        0, "ts", new org.apache.paimon.types.TimestampType(3))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn =
                PaimonColumnHandle.of("ts", new org.apache.paimon.types.TimestampType(3), 0);
        TupleDomain<PaimonColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                tsColumn,
                                Domain.singleValue(
                                        TimestampType.createTimestampType(3), 1695645403000L)));
        Predicate expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(1695645403000L / 1000));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTimeStampWithTimeZone()
    {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "ts",
                                        new org.apache.paimon.types.LocalZonedTimestampType(3))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn =
                PaimonColumnHandle.of(
                        "ts", new org.apache.paimon.types.LocalZonedTimestampType(3), 0);
        TupleDomain<PaimonColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                tsColumn,
                                Domain.singleValue(
                                        createTimestampWithTimeZoneType(6),
                                        fromEpochMillisAndFraction(
                                                1695645403000L, 0, TimeZoneKey.UTC_KEY))));
        Predicate expectedEqq =
                builder.equal(
                        0,
                        Timestamp.fromEpochMillis(
                                (fromEpochMillisAndFraction(1695645403000L, 0, TimeZoneKey.UTC_KEY))
                                        .getEpochMillis()));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);

        eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                tsColumn,
                                Domain.singleValue(
                                        createTimestampWithTimeZoneType(3), 1695645403000L)));
        expectedEqq = builder.equal(0, 1695645403000L);
        actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTinyint()
    {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        0, "tiny", new org.apache.paimon.types.TinyIntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn =
                PaimonColumnHandle.of("tiny", new org.apache.paimon.types.TinyIntType(), 0);
        TupleDomain<PaimonColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(idColumn, Domain.singleValue(TinyintType.TINYINT, 127L)));
        Predicate expectedEqq = builder.equal(0, Byte.MAX_VALUE);
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testSmallint()
    {
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        0, "small", new org.apache.paimon.types.SmallIntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn =
                PaimonColumnHandle.of("small", new org.apache.paimon.types.SmallIntType(), 0);
        TupleDomain<PaimonColumnHandle> eq =
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                idColumn, Domain.singleValue(SmallintType.SMALLINT, 32767L)));
        Predicate expectedEqq = builder.equal(0, Short.MAX_VALUE);
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }
}
