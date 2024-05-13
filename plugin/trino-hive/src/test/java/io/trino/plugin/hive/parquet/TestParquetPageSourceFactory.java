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
import io.trino.metastore.HiveType;
import io.trino.plugin.base.subfield.Subfield;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.RowType.rowType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetPageSourceFactory
{
    private static final String OPTIONAL_LEVEL_1 = "optional_level1";
    private static final String OPTIONAL_LEVEL_2 = "optional_level2";
    private static final String REQUIRED_LEVEL_3_0 = "required_level3_0";
    private static final String REQUIRED_LEVEL_3_1 = "required_level3_1";

    @Test
    public void testGetNestedMixedRepetitionColumnType()
    {
        testGetNestedMixedRepetitionColumnType(true);
        testGetNestedMixedRepetitionColumnType(false);
    }

    @Test
    public void testSchemaPruningWithSubfields()
    {
        RowType rowType = rowType(
                RowType.field(
                        OPTIONAL_LEVEL_2,
                        rowType(RowType.field(
                                        REQUIRED_LEVEL_3_0,
                                        IntegerType.INTEGER),
                                RowType.field(
                                        REQUIRED_LEVEL_3_1,
                                        IntegerType.INTEGER))));
        // Parquet schema is pruned by Subfields, not the name/index lists
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                OPTIONAL_LEVEL_1,
                0,
                HiveType.valueOf("struct<optional_level2:struct<required_level3_0:int,required_level3_1:int>>"),
                rowType,
                Optional.of(
                        new HiveColumnProjectionInfo(
                                ImmutableList.of(1),
                                ImmutableList.of(OPTIONAL_LEVEL_2),
                                toHiveType(IntegerType.INTEGER),
                                IntegerType.INTEGER,
                                ImmutableList.of(new Subfield(OPTIONAL_LEVEL_1, ImmutableList.of(new Subfield.NestedField(OPTIONAL_LEVEL_2), new Subfield.NestedField("required_level3_0")))))),
                REGULAR,
                Optional.empty());
        MessageType fileSchema = new MessageType(
                "hive_schema",
                new GroupType(OPTIONAL, OPTIONAL_LEVEL_1,
                        new GroupType(OPTIONAL, OPTIONAL_LEVEL_2,
                                new PrimitiveType(REQUIRED, INT32, REQUIRED_LEVEL_3_0),
                                new PrimitiveType(REQUIRED, INT32, REQUIRED_LEVEL_3_1))));

        MessageType prunedFileSchema = new MessageType(
                "hive_schema",
                new GroupType(OPTIONAL, OPTIONAL_LEVEL_1,
                        new GroupType(OPTIONAL, OPTIONAL_LEVEL_2,
                                new PrimitiveType(REQUIRED, INT32, REQUIRED_LEVEL_3_0))));

        // Hive column name is based on subscript names, while schema pruning should still use original column name
        Type newType = ParquetPageSourceFactory.getColumnType(columnHandle, fileSchema, true).get();
        assertThat(OPTIONAL_LEVEL_1.equals(columnHandle.getBaseColumnName()));
        assertThat(columnHandle.getName().equals(OPTIONAL_LEVEL_1 + "#" + OPTIONAL_LEVEL_2));
        assertThat(newType.getName().equals(columnHandle.getBaseColumnName()));
        // Parquet schema is pruned and level 3_1 is removed
        assertThat(newType.equals(prunedFileSchema.getType(OPTIONAL_LEVEL_1)));
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
                        IntegerType.INTEGER,
                        ImmutableList.of())),
                REGULAR,
                Optional.empty());
        MessageType fileSchema = new MessageType(
                "hive_schema",
                new GroupType(OPTIONAL, "optional_level1",
                        new GroupType(OPTIONAL, "optional_level2",
                                new PrimitiveType(REQUIRED, INT32, "required_level3"))));
        assertThat(ParquetPageSourceFactory.getColumnType(columnHandle, fileSchema, useColumnNames).get()).isEqualTo(fileSchema.getType("optional_level1"));
    }
}
