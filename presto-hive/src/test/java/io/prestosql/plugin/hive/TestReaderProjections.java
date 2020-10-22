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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.ReaderProjections.projectBaseColumns;
import static io.prestosql.plugin.hive.ReaderProjections.projectSufficientColumns;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_PRIMITIVES;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.prestosql.plugin.hive.TestHiveReaderProjectionsUtil.createTestFullColumns;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestReaderProjections
{
    private static final List<String> TEST_COLUMN_NAMES = ImmutableList.of(
            "col_bigint",
            "col_struct_of_primitives",
            "col_struct_of_non_primitives",
            "col_partition_key_1",
            "col_partition_key_2");

    private static final Map<String, Type> TEST_COLUMN_TYPES = ImmutableMap.<String, Type>builder()
            .put("col_bigint", BIGINT)
            .put("col_struct_of_primitives", ROWTYPE_OF_PRIMITIVES)
            .put("col_struct_of_non_primitives", ROWTYPE_OF_ROW_AND_PRIMITIVES)
            .put("col_partition_key_1", BIGINT)
            .put("col_partition_key_2", BIGINT)
            .build();

    private static final Map<String, HiveColumnHandle> TEST_FULL_COLUMNS = createTestFullColumns(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES);

    @Test
    public void testNoProjections()
    {
        List<HiveColumnHandle> columns = new ArrayList<>(TEST_FULL_COLUMNS.values());
        Optional<ReaderProjections> mapping;

        mapping = projectBaseColumns(columns);
        assertTrue(mapping.isEmpty(), "Full columns should not require any adaptation");

        mapping = projectSufficientColumns(columns);
        assertTrue(mapping.isEmpty(), "Full columns should not require any adaptation");
    }

    @Test
    public void testBaseColumnsProjection()
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_primitives"), ImmutableList.of(0)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_primitives"), ImmutableList.of(1)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_bigint"), ImmutableList.of()),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0, 1)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0)));

        Optional<ReaderProjections> mapping = projectBaseColumns(columns);
        assertTrue(mapping.isPresent(), "Full columns should be created for corresponding projected columns");

        List<HiveColumnHandle> readerColumns = mapping.get().getReaderColumns();

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);
            int readerIndex = mapping.get().readerColumnPositionForHiveColumnAt(i);
            HiveColumnHandle readerColumn = mapping.get().readerColumnForHiveColumnAt(i);
            assertEquals(column.getBaseColumn(), readerColumn);
            assertEquals(readerColumns.get(readerIndex), readerColumn);
        }
    }

    @Test
    public void testProjectSufficientColumns()
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_primitives"), ImmutableList.of(0)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_primitives"), ImmutableList.of(1)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_bigint"), ImmutableList.of()),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0, 1)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0)));

        Optional<ReaderProjections> readerProjections = projectSufficientColumns(columns);
        assertTrue(readerProjections.isPresent(), "expected readerProjections to be present");

        assertEquals(readerProjections.get().readerColumnForHiveColumnAt(0), columns.get(0));
        assertEquals(readerProjections.get().readerColumnForHiveColumnAt(1), columns.get(1));
        assertEquals(readerProjections.get().readerColumnForHiveColumnAt(2), columns.get(2));
        assertEquals(readerProjections.get().readerColumnForHiveColumnAt(3), columns.get(4));
        assertEquals(readerProjections.get().readerColumnForHiveColumnAt(4), columns.get(4));

        assertEquals(readerProjections.get().readerColumnPositionForHiveColumnAt(0), 0);
        assertEquals(readerProjections.get().readerColumnPositionForHiveColumnAt(1), 1);
        assertEquals(readerProjections.get().readerColumnPositionForHiveColumnAt(2), 2);
        assertEquals(readerProjections.get().readerColumnPositionForHiveColumnAt(3), 3);
        assertEquals(readerProjections.get().readerColumnPositionForHiveColumnAt(4), 3);

        List<HiveColumnHandle> readerColumns = readerProjections.get().getReaderColumns();
        assertEquals(readerColumns.get(0), columns.get(0));
        assertEquals(readerColumns.get(1), columns.get(1));
        assertEquals(readerColumns.get(2), columns.get(2));
        assertEquals(readerColumns.get(3), columns.get(4));
    }
}
