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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hudi.HudiPageSourceProvider.remapColumnIndicesToPhysical;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHudiPageSourceProviderTest
{
    @Test
    public void testRemapSimpleMatchCaseInsensitive()
    {
        // Physical Schema: [col_a (int), col_b (string)]
        MessageType fileSchema = new MessageType("file_schema",
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
        MessageType fileSchema = new MessageType("file_schema",
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
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (different case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("COL_A", 0, HiveType.HIVE_INT, INTEGER), // This will mismatch
                createDummyHandle("col_b", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-sensitive) - Expect NPE because "COL_A" won't be found
        assertThatThrownBy(() -> remapColumnIndicesToPhysical(fileSchema, requestedColumns, true))
                .isInstanceOf(NullPointerException.class); // Check the exception type
    }

    @Test
    public void testRemapDifferentOrder()
    {
        // Physical Schema: [id (int), name (string), timestamp (long)]
        MessageType fileSchema = new MessageType("file_schema",
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
        MessageType fileSchema = new MessageType("file_schema",
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
        MessageType fileSchema = new MessageType("file_schema",
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
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"));

        // Requested Columns (includes a non-existent column)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER),
                // Not in schema
                createDummyHandle("col_x", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-insensitive) - Expect NPE because "col_x" won't be found
        assertThatThrownBy(() -> remapColumnIndicesToPhysical(fileSchema, requestedColumns, false))
                .isInstanceOf(NullPointerException.class);
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
