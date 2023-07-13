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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.spi.type.RowType.rowType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;

public class TestParquetPageSourceFactory
{
    @Test(dataProvider = "useColumnNames")
    public void testGetNestedMixedRepetitionColumnType(boolean useColumnNames)
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
                        new GroupType(OPTIONAL, "optional_level2",
                                new PrimitiveType(REQUIRED, INT32, "required_level3"))));
        assertEquals(
                ParquetPageSourceFactory.getColumnType(columnHandle, fileSchema, useColumnNames).get(),
                fileSchema.getType("optional_level1"));
    }

    @DataProvider
    public Object[][] useColumnNames()
    {
        return new Object[][] {
                {true}, // use column name
                {false} // use column index
        };
    }
}
