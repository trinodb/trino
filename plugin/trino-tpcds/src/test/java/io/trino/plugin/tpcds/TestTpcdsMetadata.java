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
package io.trino.plugin.tpcds;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTpcdsMetadata
{
    private final TpcdsMetadata tpcdsMetadata = new TpcdsMetadata();
    private final ConnectorSession session = null;

    @Test
    public void testHiddenSchemas()
    {
        assertThat(tpcdsMetadata.schemaExists(session, "sf1")).isTrue();
        assertThat(tpcdsMetadata.schemaExists(session, "sf3000.0")).isTrue();
        assertThat(tpcdsMetadata.schemaExists(session, "sf0")).isFalse();
        assertThat(tpcdsMetadata.schemaExists(session, "hf1")).isFalse();
        assertThat(tpcdsMetadata.schemaExists(session, "sf")).isFalse();
        assertThat(tpcdsMetadata.schemaExists(session, "sfabc")).isFalse();
    }

    @Test
    public void testColumnNullability()
    {
        // Only primary keys are declared non-null; fact-table foreign keys and other columns are nullable.
        Map<String, Boolean> storeSales = columnNullability("store_sales");
        assertThat(storeSales.get("ss_item_sk")).isFalse();
        assertThat(storeSales.get("ss_ticket_number")).isFalse();
        assertThat(storeSales.get("ss_sold_date_sk")).isTrue();
        assertThat(storeSales.get("ss_customer_sk")).isTrue();

        // Dimension surrogate keys are non-null; non-key columns (including business ids) are nullable.
        assertThat(columnNullability("customer").get("c_customer_sk")).isFalse();
        assertThat(columnNullability("customer").get("c_customer_id")).isTrue();
        assertThat(columnNullability("customer").get("c_last_name")).isTrue();
        assertThat(columnNullability("date_dim").get("d_date_sk")).isFalse();
        assertThat(columnNullability("item").get("i_item_sk")).isFalse();

        // History (SCD) tables: the surrogate key is non-null, but rec_end_date is nullable for the
        // current record even though the generator's NOT NULL bitmap marks it.
        assertThat(columnNullability("web_site").get("web_site_sk")).isFalse();
        assertThat(columnNullability("web_site").get("web_rec_end_date")).isTrue();

        // Composite primary key of the inventory fact table is fully non-null.
        Map<String, Boolean> inventory = columnNullability("inventory");
        assertThat(inventory.get("inv_date_sk")).isFalse();
        assertThat(inventory.get("inv_item_sk")).isFalse();
        assertThat(inventory.get("inv_warehouse_sk")).isFalse();
        assertThat(inventory.get("inv_quantity_on_hand")).isTrue();
    }

    private Map<String, Boolean> columnNullability(String tableName)
    {
        ConnectorTableHandle tableHandle = tpcdsMetadata.getTableHandle(session, new SchemaTableName("sf1", tableName), Optional.empty(), Optional.empty());
        ConnectorTableMetadata tableMetadata = tpcdsMetadata.getTableMetadata(session, tableHandle);
        return tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, ColumnMetadata::isNullable, (a, _) -> a));
    }
}
