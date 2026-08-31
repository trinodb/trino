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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoFilterConverterTest
{
    private static final PaimonCatalog TESTING_CATALOG = new PaimonCatalog(
            new Options(),
            unsupportedFileSystemFactory());
    private static final ConnectorSession TESTING_SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
            .build();
    private static final Type JSON_TYPE = TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON));

    @Test
    public void testAll()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", new IntType());
        TupleDomain<PaimonColumnHandle> isNull = TupleDomain
                .withColumnDomains(Map.of(idColumn, Domain.onlyNull(INTEGER)));
        Predicate expectedIsNull = builder.isNull(0);
        Predicate actualIsNull = converter.convert(isNull).get();
        assertThat(actualIsNull).isEqualTo(expectedIsNull);

        TupleDomain<PaimonColumnHandle> isNotNull = TupleDomain
                .withColumnDomains(Map.of(idColumn, Domain.notNull(INTEGER)));
        Predicate expectedIsNotNull = builder.isNotNull(0);
        Predicate actualIsNotNull = converter.convert(isNotNull).get();
        assertThat(actualIsNotNull).isEqualTo(expectedIsNotNull);

        TupleDomain<PaimonColumnHandle> lt = TupleDomain.withColumnDomains(
                Map.of(idColumn, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 1L)), false)));
        Predicate expectedLt = builder.lessThan(0, 1);
        Predicate actualLt = converter.convert(lt).get();
        assertThat(actualLt).isEqualTo(expectedLt);

        TupleDomain<PaimonColumnHandle> ltEq = TupleDomain.withColumnDomains(
                Map.of(idColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 1L)), false)));
        Predicate expectedLtEq = builder.lessOrEqual(0, 1);
        Predicate actualLtEq = converter.convert(ltEq).get();
        assertThat(actualLtEq).isEqualTo(expectedLtEq);

        TupleDomain<PaimonColumnHandle> gt = TupleDomain.withColumnDomains(
                Map.of(idColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 1L)), false)));
        Predicate expectedGt = builder.greaterThan(0, 1);
        Predicate actualGt = converter.convert(gt).get();
        assertThat(actualGt).isEqualTo(expectedGt);

        TupleDomain<PaimonColumnHandle> gtEq = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 1L)), false)));
        Predicate expectedGtEq = builder.greaterOrEqual(0, 1);
        Predicate actualGtEq = converter.convert(gtEq).get();
        assertThat(actualGtEq).isEqualTo(expectedGtEq);

        TupleDomain<PaimonColumnHandle> eq = TupleDomain
                .withColumnDomains(Map.of(idColumn, Domain.singleValue(INTEGER, 1L)));
        Predicate expectedEq = builder.equal(0, 1);
        Predicate actualEq = converter.convert(eq).get();
        assertThat(actualEq).isEqualTo(expectedEq);

        TupleDomain<PaimonColumnHandle> in = TupleDomain.withColumnDomains(
                Map.of(idColumn, Domain.multipleValues(INTEGER, Arrays.asList(1L, 2L, 3L))));
        Predicate expectedIn = builder.in(0, Arrays.asList(1, 2, 3));
        Predicate actualIn = converter.convert(in).get();
        assertThat(actualIn).isEqualTo(expectedIn);
    }

    @Test
    public void testCharType()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "date", new org.apache.paimon.types.CharType(10))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("date", new org.apache.paimon.types.CharType(10));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.singleValue(CharType.createCharType(10), Slices.utf8Slice("2020-11-11"))));
        Predicate expectedEqq = builder.equal(0, BinaryString.fromString("2020-11-11"));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTimeStamp()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "ts", new TimestampType(3))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn = PaimonColumnHandle.of("ts", new TimestampType(3));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.singleValue(createTimestampType(3), 1695645403000L)));
        Predicate expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(1695645403000L / 1000));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testHighPrecisionTimeStamp()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "ts", new TimestampType(9))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn = PaimonColumnHandle.of("ts", new TimestampType(9));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.singleValue(
                        createTimestampType(9),
                        new LongTimestamp(1_695_645_403_123_456L, 789_000))));
        Predicate expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testNegativeHighPrecisionTimeStamp()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "ts", new TimestampType(9))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn = PaimonColumnHandle.of("ts", new TimestampType(9));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.singleValue(
                        createTimestampType(9),
                        new LongTimestamp(-1_234L, 567_000))));
        Predicate expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(-2L, 766_567));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTimeStampWithTimeZone()
    {
        RowType rowType = new RowType(Collections
                .singletonList(new DataField(0, "ts", new LocalZonedTimestampType(3))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn = PaimonColumnHandle.of("ts", new LocalZonedTimestampType(3));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain
                .withColumnDomains(Map.of(tsColumn, Domain.singleValue(
                        createTimestampWithTimeZoneType(6),
                        fromEpochMillisAndFraction(1695645403000L, 0, TimeZoneKey.UTC_KEY))));
        Predicate expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(
                fromEpochMillisAndFraction(1695645403000L, 0, TimeZoneKey.UTC_KEY).getEpochMillis()));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);

        eq = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.singleValue(
                        createTimestampWithTimeZoneType(3),
                        packDateTimeWithZone(1695645403000L, TimeZoneKey.UTC_KEY))));
        expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(1695645403000L));
        actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testNegativeTimeStampWithTimeZone()
    {
        RowType rowType = new RowType(Collections
                .singletonList(new DataField(0, "ts", new LocalZonedTimestampType(6))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle tsColumn = PaimonColumnHandle.of("ts", new LocalZonedTimestampType(6));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain
                .withColumnDomains(Map.of(tsColumn, Domain.singleValue(
                        createTimestampWithTimeZoneType(6),
                        fromEpochMillisAndFraction(-2L, 766_000_000, TimeZoneKey.UTC_KEY))));
        Predicate expectedEqq = builder.equal(0, Timestamp.fromEpochMillis(-2L, 766_000));
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTime()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "t", new TimeType(6))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("t", new TimeType(6));
        TupleDomain<PaimonColumnHandle> eq = TupleDomain
                .withColumnDomains(Map.of(
                        idColumn, Domain.singleValue(TIME_MILLIS, 12_345L * PICOSECONDS_PER_MILLISECOND)));
        Predicate expectedEqq = builder.equal(0, 12_345);
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testTinyint()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "tiny", new TinyIntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("tiny", new TinyIntType());
        TupleDomain<PaimonColumnHandle> eq = TupleDomain
                .withColumnDomains(Map.of(idColumn, Domain.singleValue(TinyintType.TINYINT, 127L)));
        Predicate expectedEqq = builder.equal(0, Byte.MAX_VALUE);
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testSmallint()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "small", new SmallIntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("small", new SmallIntType());
        TupleDomain<PaimonColumnHandle> eq = TupleDomain
                .withColumnDomains(Map.of(idColumn, Domain.singleValue(SmallintType.SMALLINT, 32767L)));
        Predicate expectedEqq = builder.equal(0, Short.MAX_VALUE);
        Predicate actualEqq = converter.convert(eq).get();
        assertThat(actualEqq).isEqualTo(expectedEqq);
    }

    @Test
    public void testUnsafeTimeLiteralConversionRemainsUnsupported()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "t", new TimeType(6))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle timeColumn = PaimonColumnHandle.of("t", new TimeType(6));
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                timeColumn, Domain.singleValue(TIME_MICROS, Long.MAX_VALUE)));

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();

        assertThat(converter.convert(domain, acceptedDomains, unsupportedDomains)).isEmpty();
        assertThat(acceptedDomains).isEmpty();
        assertThat(unsupportedDomains).containsEntry(timeColumn, domain.getDomains().orElseThrow().get(timeColumn));
    }

    @Test
    public void testSubMillisecondTimeLiteralConversionRemainsUnsupported()
    {
        RowType rowType = new RowType(
                Collections.singletonList(new DataField(0, "t", new TimeType(6))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle timeColumn = PaimonColumnHandle.of("t", new TimeType(6));
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                timeColumn, Domain.singleValue(TIME_MICROS, 12_345L * PICOSECONDS_PER_MILLISECOND + 1)));

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();

        assertThat(converter.convert(domain, acceptedDomains, unsupportedDomains)).isEmpty();
        assertThat(acceptedDomains).isEmpty();
        assertThat(unsupportedDomains).containsEntry(timeColumn, domain.getDomains().orElseThrow().get(timeColumn));
    }

    @Test
    public void testUnsafeMapElementTimeLiteralConversionRemainsUnsupportedForFileIndex()
    {
        MapType mapType = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new TimeType(6));
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "properties", mapType)));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle mapElement = PaimonColumnHandle.of(toMapKey("properties", "last_seen"), mapType);
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                mapElement, Domain.singleValue(TIME_MICROS, Long.MAX_VALUE)));

        assertThat(converter.convertForFileIndex(domain)).isEmpty();
    }

    @Test
    public void testTupleDomainNoneUsesPaimonPredicateVisitor()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);

        Predicate predicate = converter.convert(TupleDomain.none()).orElseThrow();

        assertThat(predicate.test(null)).isFalse();
        assertThat(predicate.visit(new PredicateVisitor<Boolean>()
        {
            @Override
            public Boolean visit(LeafPredicate predicate)
            {
                return true;
            }

            @Override
            public Boolean visit(CompoundPredicate predicate)
            {
                return false;
            }
        })).isTrue();
    }

    @Test
    public void testFilterConverterRejectsNullInputs()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);

        assertThatThrownBy(() -> new PaimonFilterConverter(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowType is null");
        assertThatThrownBy(() -> converter.convert(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tupleDomain is null");
        assertThatThrownBy(() -> converter.convert(TupleDomain.all(), null, new LinkedHashMap<>()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("acceptedDomains is null");
        assertThatThrownBy(() -> converter.convert(TupleDomain.all(), new LinkedHashMap<>(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("unsupportedDomains is null");
    }

    @Test
    public void testMapElementPredicateIsOnlyConvertedForFileIndex()
    {
        MapType mapType = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
        RowType rowType = new RowType(Collections.singletonList(new DataField(
                0,
                "properties",
                mapType)));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle mapElement = PaimonColumnHandle.of(toMapKey("properties", "region"), mapType);
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                mapElement, Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        Optional<Predicate> rowPredicate = converter.convert(domain, acceptedDomains, unsupportedDomains);

        assertThat(rowPredicate).isEmpty();
        assertThat(acceptedDomains).isEmpty();
        assertThat(unsupportedDomains).containsEntry(mapElement, domain.getDomains().orElseThrow().get(mapElement));

        Predicate fileIndexPredicate = converter.convertForFileIndex(domain).orElseThrow();
        assertThat(fileIndexPredicate).isInstanceOf(LeafPredicate.class);
        LeafPredicate leafPredicate = (LeafPredicate) fileIndexPredicate;
        assertThat(leafPredicate.fieldNames()).containsExactly(toMapKey("properties", "region"));
        DataType fieldRefType = leafPredicate.fieldRefOptional().orElseThrow().type();
        assertThat(fieldRefType).isEqualTo(mapType.getValueType());
        assertThat(leafPredicate.literals()).containsExactly(BinaryString.fromString("ap-south"));
        assertThat(BitmapFileIndex.getValueMapper(fieldRefType)
                .apply(leafPredicate.literals().get(0)))
                .isEqualTo(BinaryString.fromString("ap-south"));
    }

    @Test
    public void testPredicateConversionRejectsCaseInsensitiveDuplicateFieldNames()
    {
        RowType rowType = new RowType(List.of(
                new DataField(0, "ID", new IntType()),
                new DataField(1, "id", new VarCharType(VarCharType.MAX_LENGTH))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", new IntType());
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.singleValue(INTEGER, 1L)));

        assertThatThrownBy(() -> converter.convert(domain))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon row type contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testTopLevelMapValuePredicateIsNotConvertedAsMapElementPredicate()
    {
        MapType mapType = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "properties", mapType)));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle mapColumn = PaimonColumnHandle.of("properties", mapType);
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                mapColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        Optional<Predicate> rowPredicate = converter.convert(domain, acceptedDomains, unsupportedDomains);

        assertThat(rowPredicate).isEmpty();
        assertThat(acceptedDomains).isEmpty();
        assertThat(unsupportedDomains).containsEntry(mapColumn, domain.getDomains().orElseThrow().get(mapColumn));
        assertThat(converter.convertForFileIndex(domain)).isEmpty();
    }

    @Test
    public void testMapElementFileIndexPredicateOnlySupportsSingleValues()
    {
        MapType mapType = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "properties", mapType)));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle mapElement = PaimonColumnHandle.of(toMapKey("properties", "region"), mapType);
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                mapElement, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("ap-south"))), false)));

        assertThat(converter.convertForFileIndex(domain)).isEmpty();
    }

    @Test
    public void testMultisetElementPredicateIsNotConvertedAsMapElementPredicate()
    {
        MultisetType multisetType = new MultisetType(
                new VarCharType(VarCharType.MAX_LENGTH));
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "tags", multisetType)));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle multisetElement = PaimonColumnHandle.of(toMapKey("tags", "red"), multisetType);
        TupleDomain<PaimonColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                multisetElement, Domain.singleValue(INTEGER, 2L)));

        assertThat(converter.convertForFileIndex(domain)).isEmpty();
    }

    @Test
    public void testVariantNullPredicateIsConvertedButValuePredicateRemainsInTrino()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "payload", DataTypes.VARIANT())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle payload = PaimonColumnHandle.of("payload", DataTypes.VARIANT(), TESTING_TYPE_MANAGER);

        TupleDomain<PaimonColumnHandle> isNull = TupleDomain.withColumnDomains(Map.of(
                payload, Domain.onlyNull(JSON_TYPE)));
        assertThat(converter.convert(isNull).orElseThrow()).isEqualTo(builder.isNull(0));

        TupleDomain<PaimonColumnHandle> jsonValue = TupleDomain.withColumnDomains(Map.of(
                payload, Domain.singleValue(JSON_TYPE, Slices.utf8Slice("{\"a\":1}"))));
        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();

        assertThat(converter.convert(jsonValue, acceptedDomains, unsupportedDomains)).isEmpty();
        assertThat(acceptedDomains).isEmpty();
        assertThat(unsupportedDomains).containsEntry(payload, jsonValue.getDomains().orElseThrow().get(payload));
    }

    @Test
    public void testBlobNullPredicateIsConvertedButValuePredicateRemainsInTrino()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "payload", DataTypes.BLOB())));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        PaimonColumnHandle payload = PaimonColumnHandle.of("payload", DataTypes.BLOB());

        TupleDomain<PaimonColumnHandle> isNull = TupleDomain.withColumnDomains(Map.of(
                payload, Domain.onlyNull(VARBINARY)));
        assertThat(converter.convert(isNull).orElseThrow()).isEqualTo(builder.isNull(0));

        TupleDomain<PaimonColumnHandle> blobValue = TupleDomain.withColumnDomains(Map.of(
                payload, Domain.singleValue(VARBINARY, Slices.wrappedBuffer(new byte[] {1, 2, 3}))));
        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();

        assertThat(converter.convert(blobValue, acceptedDomains, unsupportedDomains)).isEmpty();
        assertThat(acceptedDomains).isEmpty();
        assertThat(unsupportedDomains).containsEntry(payload, blobValue.getDomains().orElseThrow().get(payload));
    }

    @Test
    public void testVarbinaryPredicatePushdownUsesByteArrayLiterals()
    {
        RowType rowType = new RowType(Collections.singletonList(
                new DataField(0, "payload", DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH))));
        PaimonFilterConverter converter = new PaimonFilterConverter(rowType);
        PaimonColumnHandle payload = PaimonColumnHandle.of(
                "payload",
                DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH));

        TupleDomain<PaimonColumnHandle> equal = TupleDomain.withColumnDomains(Map.of(
                payload, Domain.singleValue(VARBINARY, Slices.wrappedBuffer(new byte[] {0x01, (byte) 0xFF}))));
        Predicate equalPredicate = converter.convert(equal).orElseThrow();

        assertThat(equalPredicate).isInstanceOf(LeafPredicate.class);
        LeafPredicate equalLeaf = (LeafPredicate) equalPredicate;
        assertThat(equalLeaf.literals()).hasSize(1);
        assertThat((byte[]) equalLeaf.literals().get(0)).containsExactly(0x01, (byte) 0xFF);
        assertThat(equalLeaf.test(GenericRow.of((Object) new byte[] {0x01, (byte) 0xFF}))).isTrue();
        assertThat(equalLeaf.test(GenericRow.of((Object) new byte[] {0x01, (byte) 0xFE}))).isFalse();
        assertThat(equalLeaf.test(
                10,
                GenericRow.of((Object) new byte[] {0x01}),
                GenericRow.of((Object) new byte[] {(byte) 0xFF}),
                new GenericArray(new long[] {0}))).isTrue();
        assertThat(equalLeaf.test(
                10,
                GenericRow.of((Object) new byte[] {0x02}),
                GenericRow.of((Object) new byte[] {(byte) 0xFF}),
                new GenericArray(new long[] {0}))).isFalse();

        TupleDomain<PaimonColumnHandle> largeIn = TupleDomain.withColumnDomains(Map.of(
                payload, Domain.multipleValues(VARBINARY, IntStream.range(0, 21)
                        .mapToObj(value -> Slices.wrappedBuffer(new byte[] {(byte) value}))
                        .toList())));
        Predicate inPredicate = converter.convert(largeIn).orElseThrow();

        assertThat(inPredicate).isInstanceOf(LeafPredicate.class);
        LeafPredicate inLeaf = (LeafPredicate) inPredicate;
        assertThat(inLeaf.literals()).hasSize(21);
        assertThat(inLeaf.test(GenericRow.of((Object) new byte[] {20}))).isTrue();
        assertThat(inLeaf.test(GenericRow.of((Object) new byte[] {21}))).isFalse();
    }

    @Test
    public void testMapElementExpressionExtractionKeepsOriginalExpressionForEngineFiltering()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                new Call(VARCHAR, new FunctionName("element_at"), List.of(
                        new Variable("properties", properties.getTrinoType()),
                        new Constant(Slices.utf8Slice("region"), VARCHAR))),
                new Constant(Slices.utf8Slice("ap-south"), VARCHAR)));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south")));
        assertThat(new PaimonFilterConverter(new RowType(Collections.singletonList(new DataField(0, "properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)))))).convert(TupleDomain.withColumnDomains(extracted)))
                .isEmpty();
    }

    @Test
    public void testMapElementExpressionExtractionUsesAssignedColumnName()
    {
        MapType mapType = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "properties", mapType)));
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties", mapType);
        Call expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                mapElement("properties_symbol", properties.getTrinoType(), "region"),
                new Constant(Slices.utf8Slice("ap-south"), VARCHAR)));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties_symbol", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south")));

        Predicate fileIndexPredicate = new PaimonFilterConverter(rowType)
                .convertForFileIndex(TupleDomain.withColumnDomains(extracted))
                .orElseThrow();
        assertThat(fileIndexPredicate).isInstanceOf(LeafPredicate.class);
        assertThat(((LeafPredicate) fileIndexPredicate).fieldNames()).containsExactly(toMapKey("properties", "region"));
    }

    @Test
    public void testMapElementExpressionFilterIsAppliedWhenSummaryIsUnchanged()
    {
        MapType mapType = new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "properties", mapType)));
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties", mapType);
        Call expression = mapElementEquals("properties", "region");
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());

        PaimonFilterExtractor.TrinoFilter firstFilter = PaimonFilterExtractor
                .extract(table, constraint, rowType, List.of())
                .orElseThrow();

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(firstFilter.filter().getDomains().orElseThrow()).containsOnly(Map.entry(
                mapElement,
                Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));
        assertThat(firstFilter.remainFilter()).isEqualTo(TupleDomain.all());
        assertThat(firstFilter.remainingExpression()).isEqualTo(expression);

        PaimonTableHandle filteredTable = table.copy(firstFilter.filter());
        assertThat(PaimonFilterExtractor.extract(filteredTable, constraint, rowType, List.of())).isEmpty();
    }

    @Test
    public void testTupleDomainNoneIsAppliedAsFilter()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());
        @SuppressWarnings({"unchecked", "rawtypes"})
        Constraint constraint = new Constraint((TupleDomain<ColumnHandle>) (TupleDomain) TupleDomain.none());

        PaimonFilterExtractor.TrinoFilter filter = PaimonFilterExtractor
                .extract(table, constraint, rowType, List.of())
                .orElseThrow();

        assertThat(filter.filter()).isEqualTo(TupleDomain.none());
        assertThat(filter.remainFilter()).isEqualTo(TupleDomain.all());
        assertThat(filter.remainingExpression()).isEqualTo(Constant.TRUE);

        PaimonTableHandle filteredTable = table.copy(filter.filter());
        assertThat(PaimonFilterExtractor.extract(filteredTable, constraint, rowType, List.of())).isEmpty();
    }

    @Test
    public void testPartitionFilterExtractionMatchesPartitionKeysCaseInsensitively()
    {
        RowType rowType = new RowType(List.of(
                new DataField(0, "region", DataTypes.INT()),
                new DataField(1, "id", DataTypes.INT())));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle region = PaimonColumnHandle.of("REGION", DataTypes.INT());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                region, Domain.singleValue(INTEGER, 7L))));

        PaimonFilterExtractor.TrinoFilter filter = PaimonFilterExtractor
                .extract(table, constraint, rowType, List.of("region"))
                .orElseThrow();

        assertThat(filter.filter().getDomains().orElseThrow()).containsOnly(Map.entry(
                region,
                Domain.singleValue(INTEGER, 7L)));
        assertThat(filter.remainFilter()).isEqualTo(TupleDomain.all());
        assertThat(filter.remainingExpression()).isEqualTo(Constant.TRUE);
    }

    @Test
    public void testFilterExtractionSummaryRequiresPaimonColumnHandles()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());
        ColumnHandle wrongColumn = new ColumnHandle() {};
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                wrongColumn, Domain.singleValue(INTEGER, 1L))));

        assertThatThrownBy(() -> PaimonFilterExtractor.extract(table, constraint, rowType, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon filter extraction requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    @Test
    public void testFilterExtractorRejectsNullInputs()
    {
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());
        Constraint constraint = new Constraint(TupleDomain.all());

        assertThatThrownBy(() -> PaimonFilterExtractor.extract(null, table, TESTING_SESSION, constraint))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("catalog is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(TESTING_CATALOG, null, TESTING_SESSION, constraint))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonTableHandle is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(TESTING_CATALOG, table, null, constraint))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(TESTING_CATALOG, table, TESTING_SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("constraint is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(table, null, rowType, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("constraint is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(null, constraint, rowType, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonTableHandle is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(table, constraint, null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowType is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extract(table, constraint, rowType, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionKeys is null");
        assertThatThrownBy(() -> PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("constraint is null");
    }

    @Test
    public void testCatalogFilterExtractionRefreshesLatestFileStoreSchema()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType staleRowType = new RowType(Collections.singletonList(new DataField(0, "old_id", new IntType())));
        RowType latestRowType = new RowType(Collections.singletonList(new DataField(0, "new_id", new IntType())));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());
        setCachedTable(
                table,
                TESTING_CATALOG,
                staleFileStoreTable(copiedWithLatestSchema, staleRowType, latestRowType, List.of("new_id")));
        PaimonColumnHandle latestColumn = PaimonColumnHandle.of("new_id", DataTypes.INT());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                latestColumn, Domain.singleValue(INTEGER, 1L))));

        PaimonFilterExtractor.TrinoFilter filter = PaimonFilterExtractor.extract(
                        testingCatalog(),
                        table,
                        TESTING_SESSION,
                        constraint)
                .orElseThrow();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(filter.filter().getDomains().orElseThrow()).containsOnlyKeys(latestColumn);
    }

    @Test
    public void testCatalogFilterExtractionLeavesBaseTableRowTrackingHiddenColumnForEngine()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType baseRowType = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
        PaimonTableHandle table = new PaimonTableHandle("schema", "table", Map.of());
        setCachedTable(table, TESTING_CATALOG, rowTrackingFileStoreTable(copiedWithLatestSchema, baseRowType));
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", SpecialFields.ROW_ID.type());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.singleValue(BIGINT, 7L))));

        PaimonFilterExtractor.TrinoFilter filter = PaimonFilterExtractor.extract(
                        testingCatalog(),
                        table,
                        TESTING_SESSION,
                        constraint)
                .orElseThrow();

        assertThat(filter.filter().getDomains().orElseThrow()).containsOnlyKeys(rowIdColumn);
        assertThat(filter.remainFilter()).isEqualTo(constraint.getSummary());
    }

    @Test
    public void testMapElementInExpressionExtraction()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                mapElement("properties", properties.getTrinoType(), "region"),
                new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                        List.of(
                                new Constant(Slices.utf8Slice("ap-south"), VARCHAR),
                                new Constant(Slices.utf8Slice("eu-west"), VARCHAR)))));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.multipleValues(
                VARCHAR,
                List.of(Slices.utf8Slice("ap-south"), Slices.utf8Slice("eu-west"))));
    }

    @Test
    public void testMapElementReverseEqualExpressionExtraction()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                new Constant(Slices.utf8Slice("ap-south"), VARCHAR),
                mapElement("properties", properties.getTrinoType(), "region")));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south")));
    }

    @Test
    public void testAndExpressionIntersectsRepeatedMapElementPredicates()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(
                new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                        mapElement("properties", properties.getTrinoType(), "region"),
                        new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                                List.of(
                                        new Constant(Slices.utf8Slice("ap-south"), VARCHAR),
                                        new Constant(Slices.utf8Slice("eu-west"), VARCHAR))))),
                mapElementEquals("properties", "region")));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south")));
    }

    @Test
    public void testNestedAndExpressionExtractsMapElementPredicates()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(
                new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(
                        new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                                mapElement("properties", properties.getTrinoType(), "region"),
                                new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                                        List.of(
                                                new Constant(Slices.utf8Slice("ap-south"), VARCHAR),
                                                new Constant(Slices.utf8Slice("eu-west"), VARCHAR))))),
                        mapElementEquals("properties", "region"))),
                mapElementEquals("properties", "zone", "primary")));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle region = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        PaimonColumnHandle zone = PaimonColumnHandle.of(
                toMapKey("properties", "zone"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(region, zone);
        assertThat(extracted.get(region)).isEqualTo(Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south")));
        assertThat(extracted.get(zone)).isEqualTo(Domain.singleValue(VARCHAR, Slices.utf8Slice("primary")));
    }

    @Test
    public void testMultisetElementExpressionIsNotExtractedAsMapFileIndexPredicate()
    {
        PaimonColumnHandle tags = PaimonColumnHandle.of(
                "tags",
                new MultisetType(new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                mapElement("tags", tags.getTrinoType(), "red"),
                new Constant(2L, INTEGER)));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("tags", tags));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    @Test
    public void testMapElementExpressionExtractionRequiresMapValueType()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                mapElement("properties", properties.getTrinoType(), "region"),
                new Constant(1L, BIGINT)));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    @Test
    public void testMapElementInExpressionExtractionRequiresMapValueType()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                mapElement("properties", properties.getTrinoType(), "region"),
                new Call(new ArrayType(BIGINT), ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                        List.of(
                                new Constant(1L, BIGINT),
                                new Constant(2L, BIGINT)))));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    @Test
    public void testMapElementInExpressionDoesNotPartiallyExtractNonConstantValues()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                mapElement("properties", properties.getTrinoType(), "region"),
                new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                        List.of(
                                new Constant(Slices.utf8Slice("ap-south"), VARCHAR),
                                new Variable("fallback", VARCHAR)))));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties, "fallback", properties));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    @Test
    public void testMapElementInExpressionDoesNotPartiallyExtractNullValues()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                mapElement("properties", properties.getTrinoType(), "region"),
                new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                        List.of(
                                new Constant(Slices.utf8Slice("ap-south"), VARCHAR),
                                new Constant(null, VARCHAR)))));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    @Test
    public void testUnsupportedMapElementExpressionsAreNotExtracted()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Constraint validMapElement = new Constraint(
                TupleDomain.all(),
                mapElementEquals("properties", "region"),
                Map.of("properties", properties));
        Constraint nullMapKey = new Constraint(
                TupleDomain.all(),
                new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(
                                new Call(VARCHAR, new FunctionName("element_at"), List.of(
                                        new Variable("properties", properties.getTrinoType()),
                                        new Constant(null, VARCHAR))),
                                new Constant(Slices.utf8Slice("ap-south"), VARCHAR))),
                Map.of("properties", properties));
        Constraint nonMapAssignment = new Constraint(
                TupleDomain.all(),
                mapElementEquals("properties", "region"),
                Map.of("properties", PaimonColumnHandle.of("properties", new VarCharType(VarCharType.MAX_LENGTH))));

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(validMapElement))
                .containsOnlyKeys(mapElement);
        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(nullMapKey)).isEmpty();
        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(nonMapAssignment)).isEmpty();
    }

    @Test
    public void testOrExpressionDoesNotPartiallyExtractUnsupportedDisjunct()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(
                mapElementEquals("properties", "region"),
                new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                        new Variable("id", BIGINT),
                        new Constant(1L, BIGINT)))));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties, "id", properties));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    @Test
    public void testOrExpressionUnionsSameMapElementKey()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(
                mapElementEquals("properties", "region"),
                mapElementEquals("properties", "region", "eu-west")));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.multipleValues(
                VARCHAR,
                List.of(Slices.utf8Slice("ap-south"), Slices.utf8Slice("eu-west"))));
    }

    @Test
    public void testNestedOrExpressionUnionsSameMapElementKey()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(
                new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(
                        mapElementEquals("properties", "region"),
                        mapElementEquals("properties", "region", "eu-west"))),
                mapElementEquals("properties", "region", "us-east")));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor
                .extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle mapElement = PaimonColumnHandle.of(
                toMapKey("properties", "region"),
                properties.logicalType(),
                properties.getTrinoType());
        assertThat(extracted).containsOnlyKeys(mapElement);
        assertThat(extracted.get(mapElement)).isEqualTo(Domain.multipleValues(
                VARCHAR,
                List.of(Slices.utf8Slice("ap-south"), Slices.utf8Slice("eu-west"), Slices.utf8Slice("us-east"))));
    }

    @Test
    public void testOrExpressionDoesNotExtractDifferentMapElementKeys()
    {
        PaimonColumnHandle properties = PaimonColumnHandle.of("properties",
                new MapType(
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH)));
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(
                mapElementEquals("properties", "region"),
                mapElementEquals("properties", "zone")));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, Map.of("properties", properties));

        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint)).isEmpty();
    }

    private static Call mapElementEquals(String columnName, String key)
    {
        return mapElementEquals(columnName, key, "ap-south");
    }

    private static Call mapElementEquals(String columnName, String key, String value)
    {
        return new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(
                mapElement(columnName, VARCHAR, key),
                new Constant(Slices.utf8Slice(value), VARCHAR)));
    }

    private static Call mapElement(String columnName, io.trino.spi.type.Type mapType, String key)
    {
        return new Call(VARCHAR, new FunctionName("element_at"), List.of(
                new Variable(columnName, mapType),
                new Constant(Slices.utf8Slice(key), VARCHAR)));
    }

    private static FileStoreTable staleFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            RowType staleRowType,
            RowType latestRowType,
            List<String> latestPartitionKeys)
    {
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                TrinoFilterConverterTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "rowType" -> latestRowType;
                    case "partitionKeys" -> latestPartitionKeys;
                    case "toString" -> "latest-filter-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                TrinoFilterConverterTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "rowType" -> staleRowType;
                    case "partitionKeys" -> List.of("old_id");
                    case "toString" -> "stale-filter-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable rowTrackingFileStoreTable(AtomicBoolean copiedWithLatestSchema, RowType rowType)
    {
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                TrinoFilterConverterTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "coreOptions" -> new CoreOptions(
                            new Options(
                                    Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")));
                    case "toString" -> "latest-row-tracking-filter-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                TrinoFilterConverterTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "coreOptions" -> new CoreOptions(
                            new Options(
                                    Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")));
                    case "toString" -> "stale-row-tracking-filter-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static PaimonCatalog testingCatalog()
    {
        return TESTING_CATALOG;
    }

    private static TrinoFileSystemFactory unsupportedFileSystemFactory()
    {
        return _ -> {
            throw new UnsupportedOperationException("filesystem is not used by this test");
        };
    }

    private static void setCachedTable(PaimonTableHandle handle, Catalog catalog, Table table)
            throws Exception
    {
        Field tableField = PaimonTableHandle.class.getDeclaredField("tablesByCatalog");
        tableField.setAccessible(true);
        IdentityHashMap<Catalog, Table> tablesByCatalog = new IdentityHashMap<>();
        tablesByCatalog.put(catalog, table);
        tableField.set(handle, tablesByCatalog);
    }
}
