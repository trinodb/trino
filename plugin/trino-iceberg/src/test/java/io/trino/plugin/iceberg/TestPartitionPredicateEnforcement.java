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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.PartitionPredicateEnforcement.canEnforceColumnConstraintInSpecs;
import static io.trino.plugin.iceberg.PartitionPredicateEnforcement.implies;
import static io.trino.plugin.iceberg.PartitionPredicateEnforcement.selectPartitionSpecs;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionPredicateEnforcement
{
    private static final HadoopTables HADOOP_TABLES = new HadoopTables();
    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    @TempDir
    Path tempDir;

    @Test
    public void testIdentityTransform()
    {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "i", Types.IntegerType.get()),
                Types.NestedField.optional(2, "d", Types.DoubleType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("i")
                .identity("d")
                .build();
        Table table = createTable(schema, spec);
        IcebergColumnHandle intCol = column(1, "i", INTEGER);
        IcebergColumnHandle doubleCol = column(2, "d", DOUBLE);

        assertThat(canEnforce(table, intCol, Domain.onlyNull(INTEGER))).isTrue();
        assertThat(canEnforce(table, intCol, Domain.notNull(INTEGER))).isTrue();
        assertThat(canEnforce(table, intCol, Domain.singleValue(INTEGER, 10L))).isTrue();
        assertThat(canEnforce(table, intCol, Domain.multipleValues(INTEGER, List.of(10L, 20L, 30L)))).isTrue();
        assertThat(canEnforce(table, intCol, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false))).isTrue();
        assertThat(canEnforce(table, intCol, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 10L), Range.greaterThan(INTEGER, 10L)), false))).isTrue();

        assertThat(canEnforce(table, doubleCol, Domain.singleValue(DOUBLE, -0.0))).isTrue();
        assertThat(canEnforce(table, doubleCol, Domain.singleValue(DOUBLE, Double.NaN))).isTrue();
        assertThat(canEnforce(table, doubleCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true))).isTrue();
    }

    @Test
    public void testTruncateIntegerTransform()
    {
        Schema schema = new Schema(Types.NestedField.optional(1, "d", Types.IntegerType.get()));
        Table table = createTable(schema, PartitionSpec.builderFor(schema).truncate("d", 10).build());
        IcebergColumnHandle col = column(1, "d", INTEGER);

        assertThat(canEnforce(table, col, Domain.onlyNull(INTEGER))).isTrue();
        assertThat(canEnforce(table, col, Domain.notNull(INTEGER))).isTrue();

        // Partition boundary ranges
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 30L)), false))).isTrue();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 29L)), false))).isTrue();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 30L)), false))).isTrue();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 29L)), false))).isTrue();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 10L, true, 29L, true)), false))).isTrue();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, -10L)), false))).isTrue();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 10L), Range.greaterThanOrEqual(INTEGER, 30L)), true))).isTrue();

        // Non-boundary ranges and equality
        assertThat(canEnforce(table, col, Domain.singleValue(INTEGER, 10L))).isFalse();
        assertThat(canEnforce(table, col, Domain.multipleValues(INTEGER, List.of(10L, 20L)))).isFalse();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 30L)), false))).isFalse();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 31L)), false))).isFalse();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 30L)), false))).isFalse();
        assertThat(canEnforce(table, col, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 31L)), false))).isFalse();

        // truncate(1) is 1-to-1, but Iceberg does not project = and IN strictly for it
        Table widthOneTable = createTable(schema, PartitionSpec.builderFor(schema).truncate("d", 1).build());
        assertThat(canEnforce(widthOneTable, col, Domain.singleValue(INTEGER, 10L))).isFalse();
        assertThat(canEnforce(widthOneTable, col, Domain.multipleValues(INTEGER, List.of(10L, 20L)))).isFalse();
        assertThat(canEnforce(widthOneTable, col, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false))).isTrue();
    }

    @Test
    public void testTruncateBigintAndDecimalTransforms()
    {
        Schema longSchema = new Schema(Types.NestedField.optional(1, "d", Types.LongType.get()));
        Table longTable = createTable(longSchema, PartitionSpec.builderFor(longSchema).truncate("d", 10).build());
        IcebergColumnHandle longCol = column(1, "d", BIGINT);

        assertThat(canEnforce(longTable, longCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 30L)), false))).isTrue();
        assertThat(canEnforce(longTable, longCol, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 30L)), false))).isFalse();
        assertThat(canEnforce(longTable, longCol, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 30L)), false))).isTrue();
        assertThat(canEnforce(longTable, longCol, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 30L)), false))).isFalse();

        DecimalType shortDecimal = createDecimalType(9, 2);
        Schema decSchema = new Schema(Types.NestedField.optional(1, "d", Types.DecimalType.of(9, 2)));
        Table decTable = createTable(decSchema, PartitionSpec.builderFor(decSchema).truncate("d", 10).build());
        IcebergColumnHandle decCol = column(1, "d", shortDecimal);

        assertThat(canEnforce(decTable, decCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(shortDecimal, 1220L)), false))).isTrue();
        assertThat(canEnforce(decTable, decCol, Domain.create(ValueSet.ofRanges(Range.greaterThan(shortDecimal, 1219L)), false))).isTrue();
        assertThat(canEnforce(decTable, decCol, Domain.create(ValueSet.ofRanges(Range.greaterThan(shortDecimal, 1220L)), false))).isFalse();
        assertThat(canEnforce(decTable, decCol, Domain.create(ValueSet.ofRanges(Range.lessThan(shortDecimal, 1220L)), false))).isTrue();
        assertThat(canEnforce(decTable, decCol, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(shortDecimal, 1220L)), false))).isFalse();

        DecimalType longDecimal = createDecimalType(38, 2);
        Schema longDecSchema = new Schema(Types.NestedField.optional(1, "d", Types.DecimalType.of(38, 2)));
        Table longDecTable = createTable(longDecSchema, PartitionSpec.builderFor(longDecSchema).truncate("d", 10).build());
        IcebergColumnHandle longDecCol = column(1, "d", longDecimal);
        Object decBoundary = encodeScaledValue(new BigDecimal("12345678901234567890.20"), 2);
        Object decNonBoundary = encodeScaledValue(new BigDecimal("12345678901234567890.21"), 2);

        assertThat(canEnforce(longDecTable, longDecCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(longDecimal, decBoundary)), false))).isTrue();
        assertThat(canEnforce(longDecTable, longDecCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(longDecimal, decNonBoundary)), false))).isFalse();
    }

    @Test
    public void testTruncateVarcharAndVarbinaryTransforms()
    {
        Schema strSchema = new Schema(Types.NestedField.optional(1, "d", Types.StringType.get()));
        Table strTable = createTable(strSchema, PartitionSpec.builderFor(strSchema).truncate("d", 20).build());
        IcebergColumnHandle strCol = column(1, "d", VARCHAR);

        assertThat(canEnforce(strTable, strCol, Domain.onlyNull(VARCHAR))).isTrue();
        assertThat(canEnforce(strTable, strCol, Domain.notNull(VARCHAR))).isTrue();
        assertThat(canEnforce(strTable, strCol, Domain.singleValue(VARCHAR, utf8Slice("a")))).isFalse();
        assertThat(canEnforce(strTable, strCol, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("a"))), false))).isFalse();
        assertThat(canEnforce(strTable, strCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR, utf8Slice("a"))), false))).isFalse();
        assertThat(canEnforce(strTable, strCol, Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("a"))), false))).isFalse();
        assertThat(canEnforce(strTable, strCol, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("a"))), false))).isFalse();

        Schema binSchema = new Schema(Types.NestedField.optional(1, "d", Types.BinaryType.get()));
        Table binTable = createTable(binSchema, PartitionSpec.builderFor(binSchema).truncate("d", 20).build());
        IcebergColumnHandle binCol = column(1, "d", VARBINARY);

        assertThat(canEnforce(binTable, binCol, Domain.onlyNull(VARBINARY))).isTrue();
        assertThat(canEnforce(binTable, binCol, Domain.notNull(VARBINARY))).isTrue();
        assertThat(canEnforce(binTable, binCol, Domain.singleValue(VARBINARY, utf8Slice("a")))).isFalse();
        assertThat(canEnforce(binTable, binCol, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARBINARY, utf8Slice("a"))), false))).isFalse();
        assertThat(canEnforce(binTable, binCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARBINARY, utf8Slice("a"))), false))).isFalse();
        assertThat(canEnforce(binTable, binCol, Domain.create(ValueSet.ofRanges(Range.lessThan(VARBINARY, utf8Slice("a"))), false))).isFalse();
        assertThat(canEnforce(binTable, binCol, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARBINARY, utf8Slice("a"))), false))).isFalse();
    }

    @Test
    public void testTimeTransforms()
    {
        Schema dateSchema = new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()));
        IcebergColumnHandle dateCol = column(1, "d", DATE);

        // day(date)
        Table dayTable = createTable(dateSchema, PartitionSpec.builderFor(dateSchema).day("d").build());
        long day2015 = LocalDate.of(2015, 1, 13).toEpochDay();
        long day2015Later = LocalDate.of(2015, 1, 15).toEpochDay();
        long day1969 = LocalDate.of(1969, 12, 31).toEpochDay();
        assertThat(canEnforce(dayTable, dateCol, Domain.singleValue(DATE, day2015))).isTrue();
        assertThat(canEnforce(dayTable, dateCol, Domain.multipleValues(DATE, List.of(day2015, day2015Later)))).isTrue();
        assertThat(canEnforce(dayTable, dateCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, day2015)), false))).isTrue();
        assertThat(canEnforce(dayTable, dateCol, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DATE, day1969)), false))).isTrue();
        // The day of a date is the date itself, so single values are enforced although Iceberg does not project them strictly
        assertThat(canEnforce(dayTable, dateCol, Domain.singleValue(DATE, day1969))).isTrue();
        assertThat(canEnforce(dayTable, dateCol, Domain.create(ValueSet.ofRanges(Range.equal(DATE, day1969), Range.greaterThan(DATE, day2015)), true))).isTrue();
        // Iceberg steps past the bound with wrapping arithmetic and keeps every partition for `d < min`
        assertThat(canEnforce(dayTable, dateCol, Domain.create(ValueSet.ofRanges(Range.lessThan(DATE, (long) Integer.MIN_VALUE)), false))).isFalse();
        assertThat(canEnforce(dayTable, dateCol, Domain.singleValue(DATE, (long) Integer.MAX_VALUE))).isFalse();

        // year(date): Iceberg widens pre-1970 inclusive upper bounds and tightens t <= 1 strict lower bounds
        Table yearTable = createTable(dateSchema, PartitionSpec.builderFor(dateSchema).year("d").build());
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, LocalDate.of(2015, 1, 1).toEpochDay())), false))).isTrue();
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, LocalDate.of(2015, 1, 2).toEpochDay())), false))).isFalse();
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, LocalDate.of(1972, 1, 1).toEpochDay())), false))).isTrue();
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, LocalDate.of(1971, 1, 1).toEpochDay())), false))).isFalse();
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, LocalDate.of(1970, 1, 1).toEpochDay())), false))).isFalse();
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.lessThan(DATE, LocalDate.of(1970, 1, 1).toEpochDay())), false))).isFalse();
        assertThat(canEnforce(yearTable, dateCol, Domain.create(ValueSet.ofRanges(Range.lessThan(DATE, LocalDate.of(1971, 1, 1).toEpochDay())), false))).isTrue();

        // hour(timestamp)
        Schema tsSchema = new Schema(Types.NestedField.optional(1, "d", Types.TimestampType.withoutZone()));
        Table hourTable = createTable(tsSchema, PartitionSpec.builderFor(tsSchema).hour("d").build());
        IcebergColumnHandle tsCol = column(1, "d", TIMESTAMP_MICROS);
        long hourMicros = 3_600_000_000L;
        assertThat(canEnforce(hourTable, tsCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TIMESTAMP_MICROS, 1000 * hourMicros)), false))).isTrue();
        assertThat(canEnforce(hourTable, tsCol, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TIMESTAMP_MICROS, 1000 * hourMicros + 1)), false))).isFalse();
        assertThat(canEnforce(hourTable, tsCol, Domain.create(ValueSet.ofRanges(Range.lessThan(TIMESTAMP_MICROS, 0L)), false))).isFalse();
    }

    @Test
    public void testBucketAndVoidTransforms()
    {
        Schema schema = new Schema(Types.NestedField.optional(1, "d", Types.IntegerType.get()));
        IcebergColumnHandle col = column(1, "d", INTEGER);

        Table bucketTable = createTable(schema, PartitionSpec.builderFor(schema).bucket("d", 4).build());
        assertThat(canEnforce(bucketTable, col, Domain.onlyNull(INTEGER))).isTrue();
        assertThat(canEnforce(bucketTable, col, Domain.notNull(INTEGER))).isTrue();
        assertThat(canEnforce(bucketTable, col, Domain.singleValue(INTEGER, 10L))).isFalse();
        assertThat(canEnforce(bucketTable, col, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 10L)), false))).isFalse();

        Table voidTable = createTable(schema, PartitionSpec.builderFor(schema).alwaysNull("d").build());
        assertThat(canEnforce(voidTable, col, Domain.onlyNull(INTEGER))).isFalse();
        assertThat(canEnforce(voidTable, col, Domain.notNull(INTEGER))).isFalse();
        assertThat(canEnforce(voidTable, col, Domain.singleValue(INTEGER, 10L))).isFalse();
    }

    @Test
    public void testMultiFieldEvolutionAndRenamedColumn()
    {
        Schema schema = new Schema(Types.NestedField.optional(1, "d", Types.IntegerType.get()));
        // Multi-field spec: bucket + identity on the same column
        Table multiFieldTable = createTable(
                schema,
                PartitionSpec.builderFor(schema).bucket("d", 4).identity("d").build());
        IcebergColumnHandle col = column(1, "d", INTEGER);
        assertThat(canEnforce(multiFieldTable, col, Domain.singleValue(INTEGER, 10L))).isTrue();

        // Partition evolution: spec 0 = bucket(d, 4), spec 1 = truncate(d, 10)
        Table evolvedTable = createTable(schema, PartitionSpec.builderFor(schema).bucket("d", 4).build());
        evolvedTable.updateSpec().removeField("d_bucket").addField(Expressions.truncate("d", 10)).commit();
        int oldSpecId = 0;
        int newSpecId = evolvedTable.spec().specId();
        Domain boundaryDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 10L)), false);
        assertThat(canEnforceColumnConstraintInSpecs(selectPartitionSpecs(evolvedTable, evolvedTable.schema(), ImmutableSet.of(oldSpecId, newSpecId)), col, boundaryDomain)).isFalse();
        assertThat(canEnforceColumnConstraintInSpecs(selectPartitionSpecs(evolvedTable, evolvedTable.schema(), ImmutableSet.of(newSpecId)), col, boundaryDomain)).isTrue();

        // Time travel with renamed column: tableSchema has old name "d", current table schema has "d_renamed"
        Schema oldSchema = evolvedTable.schema();
        evolvedTable.updateSchema().renameColumn("d", "d_renamed").commit();
        assertThat(canEnforceColumnConstraintInSpecs(selectPartitionSpecs(evolvedTable, oldSchema, ImmutableSet.of(newSpecId)), col, boundaryDomain)).isTrue();
    }

    @Test
    public void testImpliesDistinguishesDifferentSetPredicateTerms()
    {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "p1", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p2", Types.IntegerType.get()));
        Expression left = Binder.bind(schema.asStruct(), in("p1", 1, 2), true);
        Expression right = Binder.bind(schema.asStruct(), in("p2", 1, 2), true);
        assertThat(implies(left, right)).isFalse();
    }

    @Test
    public void testImpliesDisjunctsInDifferentOrder()
    {
        Expression consequent = bound(or(
                and(greaterThanOrEqual("p", 0), lessThanOrEqual("p", 5)),
                greaterThan("p", 10)));

        assertThat(implies(bound(or(greaterThanOrEqual("p", 20), and(greaterThanOrEqual("p", 1), lessThanOrEqual("p", 3)))), consequent)).isTrue();
        assertThat(implies(bound(or(greaterThanOrEqual("p", 20), and(greaterThanOrEqual("p", 1), lessThanOrEqual("p", 7)))), consequent)).isFalse();
    }

    @Test
    public void testBoundsAtTypeLimitsAreNotEnforced()
    {
        Schema schema = new Schema(Types.NestedField.optional(1, "d", Types.IntegerType.get()));
        Table truncateTable = createTable(schema, PartitionSpec.builderFor(schema).truncate("d", 10).build());
        Table identityTable = createTable(schema, PartitionSpec.builderFor(schema).identity("d").build());
        IcebergColumnHandle col = column(1, "d", INTEGER);

        // Iceberg's truncate projections overflow at these bounds, which made `d < -2147483648` look enforced
        for (Range range : List.of(
                Range.lessThan(INTEGER, (long) Integer.MIN_VALUE),
                Range.greaterThanOrEqual(INTEGER, (long) Integer.MIN_VALUE),
                Range.lessThanOrEqual(INTEGER, (long) Integer.MAX_VALUE),
                Range.greaterThan(INTEGER, (long) Integer.MAX_VALUE))) {
            Domain domain = Domain.create(ValueSet.ofRanges(range), false);
            assertThat(canEnforce(truncateTable, col, domain)).isFalse();
            assertThat(canEnforce(truncateTable, col, domain.union(Domain.singleValue(INTEGER, 50L)))).isFalse();
            assertThat(canEnforce(identityTable, col, domain)).isTrue();
        }
    }

    private Table createTable(Schema schema, PartitionSpec spec)
    {
        Path location = tempDir.resolve("table_" + TABLE_COUNTER.incrementAndGet());
        return HADOOP_TABLES.create(schema, spec, location.toUri().toString());
    }

    private static boolean canEnforce(Table table, IcebergColumnHandle column, Domain domain)
    {
        return canEnforceColumnConstraintInSpecs(selectPartitionSpecs(table, table.schema(), table.specs().keySet()), column, domain);
    }

    private static Expression bound(Expression expression)
    {
        return Binder.bind(Types.StructType.of(Types.NestedField.optional(1, "p", Types.IntegerType.get())), expression, true);
    }

    private static IcebergColumnHandle column(int id, String name, Type type)
    {
        return IcebergColumnHandle.optional(primitiveColumnIdentity(id, name))
                .columnType(type)
                .build();
    }
}
