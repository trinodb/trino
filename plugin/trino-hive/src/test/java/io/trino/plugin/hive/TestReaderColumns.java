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
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createTestFullColumns;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReaderColumns
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
            .buildOrThrow();

    private static final Map<String, HiveColumnHandle> TEST_FULL_COLUMNS = createTestFullColumns(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES);

    @Test
    public void testNoProjections()
    {
        List<HiveColumnHandle> columns = new ArrayList<>(TEST_FULL_COLUMNS.values());
        Optional<ReaderColumns> mapping = projectBaseColumns(columns);
        assertThat(mapping.isEmpty())
                .describedAs("Full columns should not require any adaptation")
                .isTrue();
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

        Optional<ReaderColumns> mapping = projectBaseColumns(columns);
        assertThat(mapping.isPresent())
                .describedAs("Full columns should be created for corresponding projected columns")
                .isTrue();

        List<HiveColumnHandle> readerColumns = mapping.get().get().stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toImmutableList());

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);
            int readerIndex = mapping.get().getPositionForColumnAt(i);
            HiveColumnHandle readerColumn = (HiveColumnHandle) mapping.get().getForColumnAt(i);
            assertThat(column.getBaseColumn()).isEqualTo(readerColumn);
            assertThat(readerColumns.get(readerIndex)).isEqualTo(readerColumn);
        }
    }
}
