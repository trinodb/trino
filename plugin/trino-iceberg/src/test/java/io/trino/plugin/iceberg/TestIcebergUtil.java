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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.IcebergUtil.parseVersion;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

public class TestIcebergUtil
{
    @Test
    public void testParseVersion()
    {
        assertThat(parseVersion("00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("99999-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(99999);
        assertThat(parseVersion("00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(10);
        assertThat(parseVersion("00011-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(11);
        assertThat(parseVersion("v0.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("v10.metadata.json")).isEqualTo(10);
        assertThat(parseVersion("v99999.metadata.json")).isEqualTo(99999);
        assertThat(parseVersion("v0.gz.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("v0.metadata.json.gz")).isEqualTo(0);

        assertThatThrownBy(() -> parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Not a file name: .*");
        assertThatThrownBy(() -> parseVersion("orders_5_581fad8517934af6be1857a903559d44"))
                .hasMessageMatching("Invalid metadata file name: .*");
        assertThatThrownBy(() -> parseVersion("metadata"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("00010_409702ba_4735_4645_8f14_09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v10_metadata_json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1..gz.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1.metadata.json.gz."))
                .hasMessageMatching("Invalid metadata file name:.*");

        assertThatThrownBy(() -> parseVersion("00003_409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("-00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v-10.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
    }

    @Test
    public void testGetProjectedColumns()
    {
        Schema schema = new Schema(
                NestedField.required(1, "id", Types.LongType.get()),
                NestedField.required(2, "nested", Types.StructType.of(
                        NestedField.required(3, "value", Types.StringType.get()),
                        NestedField.required(4, "list", Types.ListType.ofRequired(5, Types.StringType.get())),
                        NestedField.required(6, "nested", Types.StructType.of(
                                NestedField.required(7, "value", Types.StringType.get()))))));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(
                        tuple(1, "id", 1, ImmutableList.of()),
                        tuple(2, "nested", 2, ImmutableList.of()),
                        tuple(3, "value", 2, ImmutableList.of(3)),
                        tuple(4, "list", 2, ImmutableList.of(4)),
                        tuple(5, "element", 2, ImmutableList.of(4, 5)),
                        tuple(6, "nested", 2, ImmutableList.of(6)),
                        tuple(7, "value", 2, ImmutableList.of(6, 7)));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(1)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(1, "id", 1, ImmutableList.of()));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(2)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(2, "nested", 2, ImmutableList.of()));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(3)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(3, "value", 2, ImmutableList.of(3)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(4)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(4, "list", 2, ImmutableList.of(4)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(5)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(5, "element", 2, ImmutableList.of(4, 5)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(6)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(6, "nested", 2, ImmutableList.of(6)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(7)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(7, "value", 2, ImmutableList.of(6, 7)));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(3, 7)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(
                        tuple(3, "value", 2, ImmutableList.of(3)),
                        tuple(7, "value", 2, ImmutableList.of(6, 7)));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(1, 4, 5)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(
                        tuple(1, "id", 1, ImmutableList.of()),
                        tuple(4, "list", 2, ImmutableList.of(4)),
                        tuple(5, "element", 2, ImmutableList.of(4, 5)));
    }
}
