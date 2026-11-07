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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.SerdeInfo;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.cloud.biglake.hive.v1.TableName;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.MetastoreErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.CATALOG;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PROJECT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTableMapper
{
    @Test
    void mapsNamesColumnsPartitionKeysAndStorage()
    {
        HiveTable source = TestingLakehouseObjects.table(
                "sales",
                "orders",
                "",
                List.of(field("id", "int"), field("name", "string")),
                List.of(field("region", "string")));

        Table mapped = TableMapper.toTrino(source);

        assertThat(mapped.getDatabaseName()).isEqualTo("sales");
        assertThat(mapped.getTableName()).isEqualTo("orders");
        assertThat(mapped.getDataColumns()).extracting(Column::getName).containsExactly("id", "name");
        assertThat(mapped.getDataColumns()).extracting(Column::getType).containsExactly(HIVE_INT, HIVE_STRING);
        assertThat(mapped.getPartitionColumns()).extracting(Column::getName).containsExactly("region");
        assertThat(mapped.getStorage().getOptionalLocation()).contains("gs://warehouse/orders");
    }

    @Test
    void leavesOwnerEmptyBecauseLakehouseHasNone()
    {
        Table mapped = TableMapper.toTrino(simpleTable().build());

        assertThat(mapped.getOwner()).isEmpty();
    }

    @Test
    void keepsTheTableTypeThatLakehouseReports()
    {
        assertThat(TableMapper.toTrino(simpleTable().setTableType("VIRTUAL_VIEW").build()).getTableType())
                .isEqualTo("VIRTUAL_VIEW");
    }

    @Test
    void readsATableWithoutATypeAsExternal()
    {
        assertThat(TableMapper.toTrino(simpleTable().clearTableType().build()).getTableType())
                .isEqualTo("EXTERNAL_TABLE");
    }

    @Test
    void exposesTheDescriptionAsTheTableComment()
    {
        Table mapped = TableMapper.toTrino(simpleTable()
                .setDescription("customer orders")
                .putParameters("owner_team", "analytics")
                .build());

        assertThat(mapped.getParameters())
                .containsEntry(TABLE_COMMENT, "customer orders")
                .containsEntry("owner_team", "analytics");
    }

    @Test
    void passesParametersThroughWhenThereIsNoDescription()
    {
        Table mapped = TableMapper.toTrino(simpleTable().putParameters("owner_team", "analytics").build());

        assertThat(mapped.getParameters()).isEqualTo(Map.of("owner_team", "analytics"));
    }

    @Test
    void letsTheDescriptionReplaceACommentParameter()
    {
        Table mapped = TableMapper.toTrino(simpleTable()
                .setDescription("from the description")
                .putParameters(TABLE_COMMENT, "from the parameters")
                .build());

        assertThat(mapped.getParameters()).containsEntry(TABLE_COMMENT, "from the description");
    }

    @Test
    void carriesViewTextWhenPresentAndOtherwiseLeavesItEmpty()
    {
        Table plain = TableMapper.toTrino(simpleTable().build());
        Table view = TableMapper.toTrino(simpleTable()
                .setViewOriginalText("select 1")
                .setViewExpandedText("select 1 from dual")
                .build());

        assertThat(plain.getViewOriginalText()).isEmpty();
        assertThat(plain.getViewExpandedText()).isEmpty();
        assertThat(view.getViewOriginalText()).contains("select 1");
        assertThat(view.getViewExpandedText()).contains("select 1 from dual");
    }

    @Test
    void rejectsATableWithoutAStorageDescriptor()
    {
        HiveTable source = HiveTable.newBuilder()
                .setName(TableName.format(PROJECT, CATALOG, "sales", "orders"))
                .build();

        assertThatThrownBy(() -> TableMapper.toTrino(source))
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, HIVE_UNSUPPORTED_FORMAT))
                .hasMessageContaining("sales.orders");
    }

    @Test
    void readsTheTableNameFromTheResourceName()
    {
        assertThat(TableMapper.tableName(simpleTable().build())).isEqualTo("orders");
    }

    private static HiveTable.Builder simpleTable()
    {
        return HiveTable.newBuilder()
                .setName(TableName.format(PROJECT, CATALOG, "sales", "orders"))
                .setTableType("EXTERNAL_TABLE")
                .setStorageDescriptor(StorageDescriptor.newBuilder()
                        .setSerdeInfo(SerdeInfo.getDefaultInstance()));
    }

    private static void assertErrorCode(TrinoException thrown, ErrorCodeSupplier expected)
    {
        assertThat(thrown.getErrorCode()).isEqualTo(expected.toErrorCode());
    }
}
