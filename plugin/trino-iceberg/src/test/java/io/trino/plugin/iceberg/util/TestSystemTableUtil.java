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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.system.IcebergPartitionColumn;
import io.trino.spi.type.RowType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSystemTableUtil
{
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "event_time", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(2, "value", Types.IntegerType.get()));

    @Test
    public void testPartitionColumnTypeIgnoresVoidPlaceholderOfDroppedField()
    {
        // a void placeholder reuses the field id of the dropped field and reports the source type as its result type
        PartitionSpec dayPartitioned = parseSpec(
                """
                {"spec-id": 0, "fields": [
                    {"name": "event_time_day", "transform": "day", "source-id": 1, "field-id": 1000}]}
                """);
        PartitionSpec hourPartitioned = parseSpec(
                """
                {"spec-id": 1, "fields": [
                    {"name": "event_time_day", "transform": "void", "source-id": 1, "field-id": 1000},
                    {"name": "event_time_hour", "transform": "hour", "source-id": 1, "field-id": 1001}]}
                """);

        List<PartitionField> partitionFields = getAllPartitionFields(SCHEMA, ImmutableMap.of(0, dayPartitioned, 1, hourPartitioned));
        IcebergPartitionColumn partitionColumn = getPartitionColumnType(TESTING_TYPE_MANAGER, partitionFields, SCHEMA).orElseThrow();

        assertThat(partitionColumn.rowType()).isEqualTo(RowType.from(ImmutableList.of(
                RowType.field("event_time_day", DATE),
                RowType.field("event_time_hour", INTEGER))));
        assertThat(partitionColumn.fieldIds()).containsExactly(1000, 1001);
    }

    private static PartitionSpec parseSpec(String json)
    {
        return PartitionSpecParser.fromJson(SCHEMA, json);
    }
}
