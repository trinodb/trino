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
package io.trino.plugin.hudi;

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.HudiPageSourceProvider.remapColumnIndicesToPhysical;
import static io.trino.plugin.hudi.HudiPageSourceProvider.remapPredicateColumnIndicesToPhysical;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.dynamicFilterOn;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.writeBaseFile;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.parseInt;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link HudiPageSourceProvider}'s column remapping from both sides: the index arithmetic of
 * {@code remapColumnIndicesToPhysical} and {@code remapPredicateColumnIndicesToPhysical} on their own, and reads of
 * a real base file through {@code createPageSource}, which are what prove a remapped predicate actually reaches the
 * parquet reader and prunes the column it was written for.
 * <p>
 * The base file the reading tests share has a physical column order the metastore does not: it carries the five
 * {@code _hoodie_*} meta columns, which hive sync with
 * {@code hoodie.datasource.hive_sync.omit_metadata_fields=true} leaves out, so every data column's metastore ordinal
 * is five below its physical position. With {@code hudi.parquet.use-column-names=false} the parquet page source
 * resolves columns positionally, so a predicate whose handle still carries the metastore ordinal lands on whichever
 * column physically sits there and row groups get pruned on that column's statistics. The fixture makes that
 * observable: {@link #PREDICATE_COLUMN} grows with the row index while every other data column stays in 0..9, so a
 * domain meant for it but applied to any other column excludes every row group and the read returns nothing.
 * <p>
 * Note that the shadowed column has to be part of the PROJECTION for the damage to appear: {@code
 * descriptorsByPath} is derived from the projection, so a domain resolving to a column the query does not read
 * finds no descriptor and is discarded instead. Do not "simplify" the projections below to the predicate column
 * alone - that turns those tests green against the unfixed code.
 */
class TestHudiPageSourceProviderTest
{
    private static final int DATA_COLUMN_COUNT = 10;
    /**
     * The column the predicate is on: physically at 12, but numbered 7 by a metastore without the meta columns.
     */
    private static final String PREDICATE_COLUMN = "c7";
    /**
     * The column physically sitting at {@code c7}'s stale ordinal, and therefore the one that shadows it.
     */
    private static final String SHADOWED_COLUMN = "c2";
    private static final int ROW_COUNT = 1000;
    private static final long THRESHOLD = 900;
    private static final int MATCHING_ROW_COUNT = (int) (ROW_COUNT - THRESHOLD - 1);

    @TempDir
    static Path tempDir;

    private static Path baseFile;

    @BeforeAll
    static void writeFixture()
            throws IOException
    {
        baseFile = tempDir.resolve("base_file.parquet");
        int rowGroups = writeBaseFile(baseFile, hudiFileSchema(DATA_COLUMN_COUNT), ROW_COUNT, (group, row) -> {
            for (String metaColumn : HOODIE_META_COLUMNS) {
                group.append(metaColumn, metaColumn + "_" + row);
            }
            for (int column = 0; column < DATA_COLUMN_COUNT; column++) {
                String columnName = "c" + column;
                group.append(columnName, columnName.equals(PREDICATE_COLUMN) ? row : row % 10);
            }
        });
        // Assert the outcome rather than the writer knobs: with a single row group there would be nothing to prune,
        // and every reading test below would pass without proving anything.
        assertThat(rowGroups).as("row groups written").isGreaterThan(1);
    }

    @Test
    public void testRemapSimpleMatchCaseInsensitive()
    {
        // Physical Schema: [col_a (int), col_b (string)]
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (same order, different case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("COL_A", 0, HiveType.HIVE_INT, INTEGER),
                createDummyHandle("COL_B", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-insensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(2);
        // First requested column "COL_A" should map to physical index 0
        assertHandle(remapped.get(0), "COL_A", 0, HiveType.HIVE_INT, INTEGER);
        // Second requested column "COL_B" should map to physical index 1
        assertHandle(remapped.get(1), "COL_B", 1, HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapSimpleMatchCaseSensitive()
    {
        // Physical Schema: [col_a (int), Col_B (string)] - Note the case difference
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("Col_B"));

        // Requested Columns (matching case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER),
                createDummyHandle("Col_B", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-sensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, true);

        assertThat(remapped).hasSize(2);
        assertHandle(remapped.get(0), "col_a", 0, HiveType.HIVE_INT, INTEGER);
        assertHandle(remapped.get(1), "Col_B", 1, HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapCaseSensitiveMismatch()
    {
        // Physical Schema: [col_a (int), col_b (string)]
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (different case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("COL_A", 0, HiveType.HIVE_INT, INTEGER), // This will mismatch
                createDummyHandle("col_b", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-sensitive) - "COL_A" won't be found
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, true);

        assertThat(remapped).hasSize(2);
        // An unmatched column maps one past the last physical field, so the parquet reader null-fills it
        assertHandle(remapped.get(0), "COL_A", fileSchema.getFieldCount(), HiveType.HIVE_INT, INTEGER);
        assertHandle(remapped.get(1), "col_b", 1, HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapDifferentOrder()
    {
        // Physical Schema: [id (int), name (string), timestamp (long)]
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("id"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("name"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL).named("timestamp"));

        // Requested Columns (different order)
        List<HiveColumnHandle> requestedColumns = List.of(
                // Original index irrelevant
                createDummyHandle("name", 99, HiveType.HIVE_STRING, VARCHAR),
                createDummyHandle("timestamp", 5, HiveType.HIVE_LONG, BigintType.BIGINT),
                createDummyHandle("id", 0, HiveType.HIVE_INT, INTEGER));

        // Perform remapping (case-insensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(3);
        // First requested "name" -> physical index 1
        assertHandle(remapped.get(0), "name", 1, HiveType.HIVE_STRING, VARCHAR);
        // Second requested "timestamp" -> physical index 2
        assertHandle(remapped.get(1), "timestamp", 2, HiveType.HIVE_LONG, BigintType.BIGINT);
        // Third requested "id" -> physical index 0
        assertHandle(remapped.get(2), "id", 0, HiveType.HIVE_INT, INTEGER);
    }

    @Test
    public void testRemapSubset()
    {
        // Physical Schema: [col_a, col_b, col_c, col_d]
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, OPTIONAL).named("col_c"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, OPTIONAL).named("col_d"));

        // Requested Columns (subset and different order)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_d", 1, HiveType.HIVE_DOUBLE, DOUBLE),
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER));

        // Perform remapping (case-insensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(2);
        // First requested "col_d" -> physical index 3
        assertHandle(remapped.get(0), "col_d", 3, HiveType.HIVE_DOUBLE, DOUBLE);
        // Second requested "col_a" -> physical index 0
        assertHandle(remapped.get(1), "col_a", 0, HiveType.HIVE_INT, INTEGER);
    }

    @Test
    public void testRemapEmptyRequested()
    {
        // Physical Schema: [col_a, col_b]
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (empty list)
        List<HiveColumnHandle> requestedColumns = List.of();

        // Perform remapping
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).isEmpty();
    }

    @Test
    public void testRemapColumnNotFound()
    {
        // Physical Schema: [col_a]
        MessageType fileSchema = new MessageType(
                "file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"));

        // Requested Columns (includes a non-existent column)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER),
                // Not in schema, e.g. a base file written before the column was added
                createDummyHandle("col_x", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-insensitive) - "col_x" won't be found
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(2);
        assertHandle(remapped.get(0), "col_a", 0, HiveType.HIVE_INT, INTEGER);
        // Out of range on purpose: ParquetPageSourceFactory reports such a column as absent and null-fills it
        assertHandle(remapped.get(1), "col_x", fileSchema.getFieldCount(), HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapPredicateStaleMetastoreOrdinals()
    {
        // Physical Schema: the five Hudi meta columns, then [c0, c1, c2]
        MessageType fileSchema = hudiFileSchema(3);

        // A metastore synced with omit_metadata_fields=true carries no meta columns, so "c2" is numbered 2
        // while it physically sits at 7, and "c0" is numbered 0 while it physically sits at 5.
        HiveColumnHandle staleC2 = createDummyHandle("c2", 2, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle staleC0 = createDummyHandle("c0", 0, HiveType.HIVE_INT, INTEGER);
        Domain c2Domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 900L)), false);
        Domain c0Domain = Domain.singleValue(INTEGER, 7L);

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(staleC2, c2Domain, staleC0, c0Domain)),
                false);

        Map<HiveColumnHandle, Domain> domains = remapped.getDomains().orElseThrow();
        assertThat(domains).hasSize(2);
        // Each domain now keys off the column's physical position, so it is matched against that column's statistics
        assertThat(handleOf(domains, "c2").getBaseHiveColumnIndex()).isEqualTo(7);
        assertThat(domains.get(handleOf(domains, "c2"))).isEqualTo(c2Domain);
        assertThat(handleOf(domains, "c0").getBaseHiveColumnIndex()).isEqualTo(5);
        assertThat(domains.get(handleOf(domains, "c0"))).isEqualTo(c0Domain);
    }

    @Test
    public void testRemapPredicateDropsColumnsAbsentFromFile()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle present = createDummyHandle("c0", 0, HiveType.HIVE_INT, INTEGER);
        // Added after this base file was written, so the file does not carry them. Two of them, because the
        // projection remap's out-of-range sentinel is one value that every absent column would share.
        HiveColumnHandle firstAbsent = createDummyHandle("c1", 1, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle secondAbsent = createDummyHandle("c2", 2, HiveType.HIVE_INT, INTEGER);
        Domain presentDomain = Domain.singleValue(INTEGER, 1L);

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(
                        present, presentDomain,
                        firstAbsent, Domain.singleValue(INTEGER, 2L),
                        secondAbsent, Domain.singleValue(INTEGER, 3L))),
                false);

        // Both absent columns are dropped rather than mapped to that shared sentinel, which would have collided.
        // Dropping them costs row group pruning only; the engine still applies the filter itself.
        Map<HiveColumnHandle, Domain> domains = remapped.getDomains().orElseThrow();
        assertThat(domains).hasSize(1);
        assertThat(handleOf(domains, "c0").getBaseHiveColumnIndex()).isEqualTo(5);
        assertThat(domains.get(handleOf(domains, "c0"))).isEqualTo(presentDomain);
    }

    @Test
    public void testRemapPredicateKeepsOneDomainPerPhysicalColumn()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        // Two handles whose names differ only by case resolve to the same file field, so both land on physical
        // index 5 while remaining unequal to each other. The connector cannot produce this - Hive normalises
        // column names to lower case - but pushing both down would hand getParquetTupleDomain one
        // ColumnDescriptor twice, which it rejects by failing the whole split.
        HiveColumnHandle upperCase = createDummyHandle("C0", 0, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle lowerCase = createDummyHandle("c0", 3, HiveType.HIVE_INT, INTEGER);
        Domain firstDomain = Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false);
        // Insertion-ordered so that "first wins" is a deterministic assertion
        Map<HiveColumnHandle, Domain> predicate = new LinkedHashMap<>();
        predicate.put(upperCase, firstDomain);
        predicate.put(lowerCase, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 20L)), false));

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema, TupleDomain.withColumnDomains(predicate), false);

        // Only the first is pushed down, and no IllegalArgumentException escapes
        Map<HiveColumnHandle, Domain> domains = remapped.getDomains().orElseThrow();
        assertThat(domains).hasSize(1);
        HiveColumnHandle survivor = handleOf(domains, "C0");
        assertThat(survivor.getBaseHiveColumnIndex()).isEqualTo(5);
        assertThat(domains.get(survivor)).isEqualTo(firstDomain);
    }

    @Test
    public void testRemapPredicateKeepsBothProjectionsOfOneBaseColumn()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        // Two dereference handles projecting DIFFERENT subfields of the same struct column. Both resolve to base
        // physical index 5, but getParquetTupleDomain builds a descriptor per subfield path, so it would accept
        // both; deduplicating on the base index alone would silently discard one of the two domains.
        HiveType structType = HiveType.valueOf("struct<f:int,g:int>");
        RowType baseType = RowType.rowType(RowType.field("f", INTEGER), RowType.field("g", INTEGER));
        HiveColumnHandle onF = dereferenceHandle(structType, baseType, 0, "f");
        HiveColumnHandle onG = dereferenceHandle(structType, baseType, 1, "g");
        Domain fDomain = Domain.singleValue(INTEGER, 5L);
        Domain gDomain = Domain.singleValue(INTEGER, 3L);

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(onF, fDomain, onG, gDomain)),
                false);

        assertThat(remapped.getDomains().orElseThrow())
                .as("both subfield domains survive, each on the base column's physical index")
                .isEqualTo(Map.of(
                        withBaseIndex(onF, 5), fDomain,
                        withBaseIndex(onG, 5), gDomain));
    }

    @Test
    public void testRemapPreservesTheBaseTypeOfADereferenceHandle()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        // A handle projecting one field out of a struct column. HudiMetadata does not implement applyProjection,
        // so the connector never builds one today, but the remap has to rebuild it without corrupting it: the
        // constructor's type argument is the BASE column's type, while getType() is the projected field's.
        RowType baseType = RowType.rowType(RowType.field("f", INTEGER));
        HiveColumnHandle dereference = dereferenceHandle(HiveType.valueOf("struct<f:int>"), baseType, 0, "f");

        HiveColumnHandle remapped = remapColumnIndicesToPhysical(fileSchema, List.of(dereference), false).get(0);

        assertThat(remapped.getBaseHiveColumnIndex())
                .as("physical index")
                .isEqualTo(5);
        assertThat(remapped.getBaseType())
                .as("base type, which is what the parquet page source reads")
                .isEqualTo(baseType);
        assertThat(remapped.getType())
                .as("projected field type")
                .isEqualTo(INTEGER);
        assertThat(remapped.getHiveColumnProjectionInfo())
                .as("projection info")
                .isEqualTo(dereference.getHiveColumnProjectionInfo());
    }

    @Test
    public void testRemapPredicateAllAndNonePassThrough()
    {
        MessageType fileSchema = hudiFileSchema(1);

        assertThat(remapPredicateColumnIndicesToPhysical(fileSchema, TupleDomain.<HiveColumnHandle>all(), false))
                .isEqualTo(TupleDomain.all());
        assertThat(remapPredicateColumnIndicesToPhysical(fileSchema, TupleDomain.<HiveColumnHandle>none(), false))
                .isEqualTo(TupleDomain.none());
    }

    @Test
    public void testRemapPredicateCaseSensitivity()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle upperCase = createDummyHandle("C0", 0, HiveType.HIVE_INT, INTEGER);
        TupleDomain<HiveColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(upperCase, Domain.singleValue(INTEGER, 1L)));

        // Case-insensitive: "C0" resolves to the file's "c0" at physical index 5
        Map<HiveColumnHandle, Domain> insensitive = remapPredicateColumnIndicesToPhysical(fileSchema, predicate, false)
                .getDomains().orElseThrow();
        assertThat(handleOf(insensitive, "C0").getBaseHiveColumnIndex()).isEqualTo(5);

        // Case-sensitive: no match, so the domain is dropped instead of being left on a stale ordinal
        assertThat(remapPredicateColumnIndicesToPhysical(fileSchema, predicate, true).isAll()).isTrue();
    }

    @Test
    public void testRemapPredicatePreservesEveryOtherHandleAttribute()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle original = new HiveColumnHandle(
                "c0",
                0,
                HiveType.HIVE_INT,
                INTEGER,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.of("a comment"));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 900L)), true);

        Map<HiveColumnHandle, Domain> domains = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(original, domain)),
                false)
                .getDomains().orElseThrow();

        HiveColumnHandle remapped = handleOf(domains, "c0");
        assertHandle(remapped, "c0", 5, HiveType.HIVE_INT, INTEGER);
        assertThat(remapped.getComment())
                .as("Comment mismatch for c0")
                .isEqualTo(Optional.of("a comment"));
        assertThat(domains.get(remapped))
                .as("Domain mismatch for c0")
                .isEqualTo(domain);
    }

    @Test
    public void testPredicateOnStaleOrdinalStillPrunesRowGroups()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));

        MaterializedResult result = read(projection, greaterThanThreshold(PREDICATE_COLUMN), false, DynamicFilter.EMPTY);

        // Correct results alone would also be produced by pushing nothing down; reading fewer rows than the file
        // holds is only possible if the domain reached the column it was written for, and the matching rows must
        // survive that pruning. The shadowed column never leaves 0..9, so a domain of "> 900" applied to it would
        // prune every row group instead.
        assertThat(result.getRowCount())
                .as("rows read out of %s", ROW_COUNT)
                .isLessThan(ROW_COUNT);
        assertThat(matchingRowCount(result, projection, PREDICATE_COLUMN))
                .as("rows matching %s > %s after pruning", PREDICATE_COLUMN, THRESHOLD)
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testStaleOrdinalArrivingThroughADynamicFilter()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));

        // A dynamic filter reaches getCombinedPredicate by its own route, and its handles carry the same stale
        // metastore ordinals the split's predicate does
        MaterializedResult result = read(
                projection,
                TupleDomain.all(),
                false,
                dynamicFilterOn(greaterThanThreshold(PREDICATE_COLUMN)));

        assertThat(matchingRowCount(result, projection, PREDICATE_COLUMN))
                .as("rows matching a dynamic filter of %s > %s", PREDICATE_COLUMN, THRESHOLD)
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testPredicateOnColumnAddedAfterBaseFileWasWritten()
            throws Exception
    {
        // The metastore carries one column more than this base file does, numbered 10 - an ordinal that is still
        // in range physically, where it picks out "c5"
        String addedColumn = "c" + DATA_COLUMN_COUNT;
        List<HiveColumnHandle> projection = List.of(dataColumn("c5"), dataColumn(PREDICATE_COLUMN), dataColumn(addedColumn));

        // IS NULL, not a range: the added column is null in every row of this base file, so this predicate is
        // satisfied by all of them. A range predicate would be unsatisfiable here and the buggy read's empty
        // result would be the right answer by accident.
        MaterializedResult result = read(
                projection,
                TupleDomain.withColumnDomains(Map.of(dataColumn(addedColumn), Domain.onlyNull(INTEGER))),
                false,
                DynamicFilter.EMPTY);

        // The added column must not stay on its stale metastore ordinal: pushed positionally it would land on
        // "c5", which has no nulls at all, and every row group would be pruned.
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(result.getMaterializedRows().getFirst().getField(2)).as("value of %s", addedColumn).isNull();
    }

    @Test
    public void testPositionalAndNameBasedResolutionAgree()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));
        TupleDomain<HiveColumnHandle> predicate = greaterThanThreshold(PREDICATE_COLUMN);

        MaterializedResult positional = read(projection, predicate, false, DynamicFilter.EMPTY);
        MaterializedResult byName = read(projection, predicate, true, DynamicFilter.EMPTY);

        // Anchor the comparison: both modes regressing to no pushdown at all would otherwise agree happily
        assertThat(byName.getRowCount()).as("rows read with use-column-names=true").isLessThan(ROW_COUNT);
        assertThat(positional.getMaterializedRows())
                .as("hudi.parquet.use-column-names=false must read what use-column-names=true reads")
                .isEqualTo(byName.getMaterializedRows());
    }

    private static MaterializedResult read(
            List<HiveColumnHandle> projection,
            TupleDomain<HiveColumnHandle> predicate,
            boolean useParquetColumnNames,
            DynamicFilter dynamicFilter)
            throws Exception
    {
        return TestingBaseFilePageSource.read(baseFile, projection, predicate, useParquetColumnNames, dynamicFilter);
    }

    /**
     * Builds a file schema laid out like a Hudi base file: the five {@code _hoodie_*} meta columns followed by
     * {@code dataColumnCount} int columns named {@code c0..cN}. A metastore synced with
     * {@code hoodie.datasource.hive_sync.omit_metadata_fields=true} omits the meta columns, so a data column's
     * metastore ordinal is its physical ordinal minus five.
     */
    private static MessageType hudiFileSchema(int dataColumnCount)
    {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (String metaColumn : HOODIE_META_COLUMNS) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(metaColumn));
        }
        for (int i = 0; i < dataColumnCount; i++) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("c" + i));
        }
        return new MessageType("hudi_base_file", fields);
    }

    /**
     * Builds the handle a metastore without the Hudi meta columns produces: numbered by its position among the
     * data columns alone, which is {@code HOODIE_META_COLUMNS.size()} short of its physical position. Only
     * {@code c0..cN} data column names are accepted - the numeric suffix IS the metastore ordinal - so a meta
     * column name passed here would fail to parse rather than produce a meaningful handle.
     */
    private static HiveColumnHandle dataColumn(String columnName)
    {
        return createBaseColumn(
                columnName,
                parseInt(columnName.substring(1)),
                HiveType.HIVE_INT,
                INTEGER,
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    /**
     * A handle projecting the {@code fieldIndex}-th field, named {@code fieldName}, out of the struct column {@code c0}.
     */
    private static HiveColumnHandle dereferenceHandle(HiveType structType, RowType baseType, int fieldIndex, String fieldName)
    {
        return new HiveColumnHandle(
                "c0",
                0,
                structType,
                baseType,
                Optional.of(new HiveColumnProjectionInfo(List.of(fieldIndex), List.of(fieldName), HiveType.HIVE_INT, INTEGER)),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    /**
     * The handle {@code remapColumnIndicesToPhysical} is expected to rebuild from {@code handle}.
     */
    private static HiveColumnHandle withBaseIndex(HiveColumnHandle handle, int baseHiveColumnIndex)
    {
        return new HiveColumnHandle(
                handle.getBaseColumnName(),
                baseHiveColumnIndex,
                handle.getBaseHiveType(),
                handle.getBaseType(),
                handle.getHiveColumnProjectionInfo(),
                handle.getColumnType(),
                handle.getComment());
    }

    private static TupleDomain<HiveColumnHandle> greaterThanThreshold(String columnName)
    {
        return TupleDomain.withColumnDomains(Map.of(
                dataColumn(columnName), Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, THRESHOLD)), false)));
    }

    private static long matchingRowCount(MaterializedResult result, List<HiveColumnHandle> projection, String columnName)
    {
        int fieldIndex = projection.indexOf(dataColumn(columnName));
        return result.getMaterializedRows().stream()
                .map(row -> row.getField(fieldIndex))
                .filter(value -> value != null && ((Number) value).longValue() > THRESHOLD)
                .count();
    }

    /**
     * Returns the single remapped handle carrying the given base column name.
     */
    private static HiveColumnHandle handleOf(Map<HiveColumnHandle, Domain> domains, String baseColumnName)
    {
        return domains.keySet().stream()
                .filter(handle -> handle.getBaseColumnName().equals(baseColumnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No domain was kept for column " + baseColumnName));
    }

    /**
     * Creates a basic HiveColumnHandle for testing.
     * Assumes REGULAR column type and no projection info or comments.
     * The initial hiveColumnIndex is often irrelevant for this specific test, as we are testing the remapping logic.
     *
     * @param name Name of the column handle
     * @param initialIndex The original index before remapping which might not be the physical one
     * @param hiveType Hive type of column handle
     * @param trinoType Trino type of column handle
     */
    private HiveColumnHandle createDummyHandle(
            String name,
            int initialIndex,
            HiveType hiveType,
            Type trinoType)
    {
        return new HiveColumnHandle(
                name,
                initialIndex,
                hiveType,
                trinoType,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    /**
     * Asserts that a HiveColumnHandle has the expected properties after remapping.
     */
    private void assertHandle(
            HiveColumnHandle handle,
            String expectedBaseName,
            int expectedPhysicalIndex,
            HiveType expectedHiveType,
            Type expectedTrinoType)
    {
        assertThat(handle.getBaseColumnName())
                .as("BaseColumnName mismatch for %s", expectedBaseName)
                .isEqualTo(expectedBaseName);
        assertThat(handle.getBaseHiveColumnIndex())
                .as("BaseHiveColumnIndex (physical) mismatch for %s", expectedBaseName)
                .isEqualTo(expectedPhysicalIndex);
        assertThat(handle.getBaseHiveType())
                .as("BaseHiveType mismatch for %s", expectedBaseName)
                .isEqualTo(expectedHiveType);
        assertThat(handle.getType())
                .as("Trino Type mismatch for %s", expectedBaseName)
                .isEqualTo(expectedTrinoType);
        // Assert that other fields if they are relevant
        assertThat(handle.getColumnType())
                .as("ColumnType mismatch for %s", expectedBaseName)
                .isEqualTo(HiveColumnHandle.ColumnType.REGULAR);
    }
}
