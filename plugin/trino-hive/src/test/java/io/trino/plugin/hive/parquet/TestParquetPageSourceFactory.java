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

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.type.RowType;
import java.util.Optional;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

public class TestParquetPageSourceFactory
{
    @Test
    public void testGetNestedMixedRepetitionColumnType()
    {
        RowType rowType = rowType(
            RowType.field("optional_level2", rowType(
                RowType.field("required_level3", INTEGER))));
        HiveColumnHandle columnHandle = createBaseColumn("optional_level1", 0,
            HiveType.valueOf("struct<optional_level2:struct<required_level3:int>>"), rowType, REGULAR, Optional.empty());
        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "optional_level1",
                    new GroupType(OPTIONAL, "optionnal_level2",
                        new PrimitiveType(REQUIRED, INT32, "required_level3"))));
        assertEquals(ParquetPageSourceFactory.getColumnType(columnHandle, fileSchema, true).get(), fileSchema.getType("optional_level1"));
    }
}
