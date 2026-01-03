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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_TYPE_ID_PROPERTY;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGlueIcebergUtil
{
    @Test
    public void testLossyGlueMappingsPreserveTrinoTypeId()
    {
        Map<String, Type> expectedGlueTypes = new LinkedHashMap<>();
        expectedGlueTypes.put("time_value", Types.TimeType.get());
        expectedGlueTypes.put("uuid_value", Types.UUIDType.get());
        expectedGlueTypes.put("timestamp_value", Types.TimestampType.withoutZone());
        expectedGlueTypes.put("timestamptz_value", Types.TimestampType.withZone());
        expectedGlueTypes.put("ts_nano", Types.TimestampNanoType.withoutZone());
        expectedGlueTypes.put("ts_nano_tz", Types.TimestampNanoType.withZone());
        expectedGlueTypes.put("binary_value", Types.BinaryType.get());
        expectedGlueTypes.put("fixed_value", Types.FixedType.ofLength(16));

        Map<String, String> expectedLossyGlueType = new LinkedHashMap<>();
        expectedLossyGlueType.put("time_value", "string");
        expectedLossyGlueType.put("uuid_value", "string");
        expectedLossyGlueType.put("timestamp_value", "timestamp");
        expectedLossyGlueType.put("timestamptz_value", "timestamp");
        expectedLossyGlueType.put("ts_nano", "timestamp");
        expectedLossyGlueType.put("ts_nano_tz", "timestamp");
        expectedLossyGlueType.put("binary_value", "binary");
        expectedLossyGlueType.put("fixed_value", "binary");

        List<Types.NestedField> schemaColumns = new ArrayList<>(expectedGlueTypes.size());
        int fieldId = 1;
        for (Map.Entry<String, Type> entry : expectedGlueTypes.entrySet()) {
            schemaColumns.add(Types.NestedField.optional(fieldId++, entry.getKey(), entry.getValue()));
        }

        Schema schema = new Schema(schemaColumns);
        TableMetadata metadata = newTableMetadata(
                schema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "s3://test-bucket/test-table",
                ImmutableMap.of(FORMAT_VERSION, "3"));

        TableInput tableInput = getTableInput(
                TESTING_TYPE_MANAGER,
                "test_table",
                Optional.empty(),
                metadata,
                metadata.location(),
                "s3://test-bucket/test-table/metadata/00001.metadata.json",
                ImmutableMap.of(),
                true);

        List<Column> columns = tableInput.storageDescriptor().columns();
        assertThat(columns).hasSize(expectedGlueTypes.size());

        for (Column column : columns) {
            assertThat(column.type()).isEqualTo(expectedLossyGlueType.get(column.name()));
            assertThat(column.parameters())
                    .containsEntry(COLUMN_TRINO_TYPE_ID_PROPERTY, toTrinoType(expectedGlueTypes.get(column.name()), TESTING_TYPE_MANAGER).getTypeId().getId());
        }
    }
}
