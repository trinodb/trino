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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.line.OpenXJsonPageSourceFactory;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.OPENX_JSON;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.projectedColumn;
import static io.trino.plugin.hive.HiveTestUtils.toHiveBaseColumnHandle;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.type.RowType.field;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

/**
 * This test proves that non-dereferenced fields are pruned from nested RowTypes.
 * <p>
 * It tests against the OpenX Serde because it turns out that the OpenX SerDe
 * supports "bare" field names, which are simpler to read/write in tests and
 * also valid Ion.
 * <p>
 * Testing against the OpenX Serde also proves how the technique can work for
 * deserializers which rely on position information from the schema.
 */
public class TestNestedPruning
{
    final RowType basic = RowType.rowType(
            field("basic_bool", BooleanType.BOOLEAN),
            field("basic_string", VarcharType.VARCHAR),
            field("basic_int", IntegerType.INTEGER));
    final RowType nestedRow = RowType.rowType(
            field("inner_int", IntegerType.INTEGER),
            field("inner_bool", BooleanType.BOOLEAN));
    final RowType twiceNested = RowType.rowType(
            field("nested_bool", BooleanType.BOOLEAN),
            field("nested_row", nestedRow));
    final List<HiveColumnHandle> tableColumns = List.of(
            toHiveBaseColumnHandle("basic", basic, 0),
            toHiveBaseColumnHandle("outer_string", VarcharType.VARCHAR, 1),
            toHiveBaseColumnHandle("twice_nested", twiceNested, 2));

    @Test
    public void testBasicProjection()
            throws IOException
    {
        // note projection order is different from table columns
        List<HiveColumnHandle> projectedColumns = List.of(
                tableColumns.get(1),
                projectedColumn(tableColumns.get(0), "basic_int"));

        assertValues(projectedColumns,
                "{ basic: { basic_bool: ignore, basic_int: 17 }, outer_string: foo }",
                List.of("foo", 17));
    }

    @Test
    public void testBasicProjectionOntoArray()
            throws IOException
    {
        // note projection order is different from table columns
        List<HiveColumnHandle> projectedColumns = List.of(
                tableColumns.get(1),
                projectedColumn(tableColumns.get(0), "basic_int"));

        assertValues(projectedColumns,
                "{ basic: [ignore, ignore, 17], outer_string: foo }",
                List.of("foo", 17));
    }

    @Test
    public void testNestedRowIsProjected()
            throws IOException
    {
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_row"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: true, inner_int: 51 } } }",
                List.of(List.of(51, true)));
    }

    @Test
    public void testDeeplyNestedProjection()
            throws IOException
    {
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_row", "inner_int"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: ignore, inner_int: 51 } } }",
                List.of(51));
    }

    @Test
    public void testCoveringDereference()
            throws IOException
    {
        // tests with "covering" projection before and after "covered" projection
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_row", "inner_int"),
                projectedColumn(tableColumns.get(2), "nested_row"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: true, inner_int: 51 } } }",
                List.of(51, List.of(51, true)));

        projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_row"),
                projectedColumn(tableColumns.get(2), "nested_row", "inner_int"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: true, inner_int: 51 } } }",
                List.of(List.of(51, true), 51));
    }

    @Test
    public void testTwoProjectionsFromSameRowType()
            throws IOException
    {
        // tests with projections both before and after each other in order
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_row", "inner_int"),
                projectedColumn(tableColumns.get(2), "nested_row", "inner_bool"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: true, inner_int: 51 } } }",
                List.of(51, true));

        projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_row", "inner_bool"),
                projectedColumn(tableColumns.get(2), "nested_row", "inner_int"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: true, inner_int: 51 } } }",
                List.of(true, 51));
    }

    // probably not strictly necessary, but a little belt and suspenders isn't bad.
    @Test
    public void testProjectionsFromDifferentPartsOfSameBase()
            throws IOException
    {
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(2), "nested_bool"),
                projectedColumn(tableColumns.get(2), "nested_row", "inner_int"));

        assertValues(projectedColumns,
                "{ twice_nested: { nested_row: { inner_bool: true, inner_int: 31 }, nested_bool: false } }",
                List.of(false, 31));
    }

    private void assertValues(List<HiveColumnHandle> projectedColumns, String text, List<Object> expected)
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///test.ion");

        final ConnectorSession session = getHiveSession(new HiveConfig());

        writeTextFile(text, location, fileSystemFactory.create(session));

        try (ConnectorPageSource pageSource = createPageSource(fileSystemFactory, location, tableColumns, projectedColumns, session)) {
            final MaterializedResult result = MaterializedResult.materializeSourceDataStream(session, pageSource, projectedColumns.stream().map(HiveColumnHandle::getType).toList());
            Assertions.assertEquals(1, result.getRowCount());
            Assertions.assertEquals(expected, result.getMaterializedRows().getFirst().getFields());
        }
    }

    private int writeTextFile(String text, Location location, TrinoFileSystem fileSystem)
            throws IOException
    {
        final TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        int written = 0;
        try (OutputStream outputStream = outputFile.create()) {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            outputStream.write(bytes);
            outputStream.flush();
            written = bytes.length;
        }
        return written;
    }

    /**
     * todo: this is very similar to what's in TestOrcPredicates, factor out.
     */
    private static ConnectorPageSource createPageSource(
            TrinoFileSystemFactory fileSystemFactory,
            Location location,
            List<HiveColumnHandle> tableColumns,
            List<HiveColumnHandle> projectedColumns,
            ConnectorSession session)
            throws IOException
    {
        OpenXJsonPageSourceFactory factory = new OpenXJsonPageSourceFactory(fileSystemFactory, new HiveConfig());

        long length = fileSystemFactory.create(session).newInputFile(location).length();

        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                "",
                ImmutableList.of(),
                projectedColumns,
                ImmutableList.of(),
                ImmutableMap.of(),
                location.toString(),
                OptionalInt.empty(),
                length,
                Instant.now().toEpochMilli());

        final Map<String, String> tableProperties = ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, OPENX_JSON.getInputFormat())
                .put(SERIALIZATION_LIB, OPENX_JSON.getSerde())
                .put(LIST_COLUMNS, tableColumns.stream().map(HiveColumnHandle::getName).collect(Collectors.joining(",")))
                .put(LIST_COLUMN_TYPES, tableColumns.stream().map(HiveColumnHandle::getHiveType).map(HiveType::toString).collect(Collectors.joining(",")))
                .buildOrThrow();

        return HivePageSourceProvider.createHivePageSource(
                        ImmutableSet.of(factory),
                        session,
                        location,
                        OptionalInt.empty(),
                        0,
                        length,
                        length,
                        tableProperties,
                        TupleDomain.all(),
                        TESTING_TYPE_MANAGER,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        NO_ACID_TRANSACTION,
                        columnMappings)
                .orElseThrow();
    }
}
