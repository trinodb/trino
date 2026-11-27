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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.metastore.HiveType;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.util.DynamicFiltersTestUtil.TestingDynamicFilter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createTestFullColumns;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.projectSufficientColumns;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestParquetPageSourceFactory
{
    @Test
    public void testJoinFilterIsNotRefreshedBetweenRowGroups()
            throws Exception
    {
        File file = new File(Resources.getResource("issue-10873.parquet").toURI());
        HiveColumnHandle column = createBaseColumn("x", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());
        TestingDynamicFilter filter = new TestingDynamicFilter(2);
        filter.update(TupleDomain.withColumnDomains(Map.of(column, Domain.notNull(INTEGER))));
        try (ConnectorPageSource source = ParquetPageSourceFactory.createPageSource(
                new LocalInputFile(file),
                0,
                file.length(),
                List.of(column),
                List.of(TupleDomain.all()),
                true,
                UTC,
                new FileFormatDataSourceStats(),
                ParquetReaderOptions.defaultOptions(),
                Optional.empty(),
                Optional.empty(),
                100,
                OptionalLong.of(file.length()),
                MemoryContext.NO_LIMIT,
                filter)) {
            filter.update(TupleDomain.none());
            assertThat(source.getNextSourcePage()).isNotNull();
        }
    }

    @Test
    public void testGetNestedMixedRepetitionColumnType()
    {
        testGetNestedMixedRepetitionColumnType(true);
        testGetNestedMixedRepetitionColumnType(false);
    }

    private void testGetNestedMixedRepetitionColumnType(boolean useColumnNames)
    {
        RowType rowType = rowType(
                RowType.field(
                        "optional_level2",
                        rowType(RowType.field(
                                "required_level3",
                                IntegerType.INTEGER))));
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                "optional_level1",
                0,
                HiveType.valueOf("struct<optional_level2:struct<required_level3:int>>"),
                rowType,
                Optional.of(
                        new HiveColumnProjectionInfo(
                                ImmutableList.of(1, 1),
                                ImmutableList.of("optional_level2", "required_level3"),
                                toHiveType(IntegerType.INTEGER),
                                IntegerType.INTEGER)),
                REGULAR,
                Optional.empty());
        MessageType fileSchema = new MessageType(
                "hive_schema",
                new GroupType(OPTIONAL, "optional_level1",
                        new GroupType(
                                OPTIONAL,
                                "optional_level2",
                                new PrimitiveType(REQUIRED, INT32, "required_level3"))));
        assertThat(ParquetPageSourceFactory.getColumnType(columnHandle, fileSchema, useColumnNames).get()).isEqualTo(fileSchema.getType("optional_level1"));
    }

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
    void testNoProjections()
    {
        List<HiveColumnHandle> columns = new ArrayList<>(TEST_FULL_COLUMNS.values());
        List<HiveColumnHandle> sufficientColumns = projectSufficientColumns(columns);
        assertThat(sufficientColumns)
                .describedAs("Full columns should not require any adaptation")
                .isEqualTo(columns);
    }

    @Test
    void testProjectSufficientColumns()
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_primitives"), ImmutableList.of(0)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_primitives"), ImmutableList.of(1)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_bigint"), ImmutableList.of()),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0, 1)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col_struct_of_non_primitives"), ImmutableList.of(0)));

        List<HiveColumnHandle> sufficientColumns = projectSufficientColumns(columns);
        assertThat(sufficientColumns.get(0)).isEqualTo(columns.get(0));
        assertThat(sufficientColumns.get(1)).isEqualTo(columns.get(1));
        assertThat(sufficientColumns.get(2)).isEqualTo(columns.get(2));
        assertThat(sufficientColumns.get(3)).isEqualTo(columns.get(4));
    }
}
