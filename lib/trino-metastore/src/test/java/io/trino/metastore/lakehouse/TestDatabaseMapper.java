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

import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import io.trino.metastore.Database;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.metastore.lakehouse.TestingLakehouseObjects.CATALOG;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PROJECT;
import static org.assertj.core.api.Assertions.assertThat;

class TestDatabaseMapper
{
    @Test
    void mapsAllFields()
    {
        HiveDatabase source = HiveDatabase.newBuilder()
                .setName(NamespaceName.format(PROJECT, CATALOG, "sales"))
                .setDescription("sales data")
                .setLocationUri("gs://warehouse/sales.db")
                .putAllParameters(Map.of("owner_team", "analytics"))
                .build();

        Database mapped = DatabaseMapper.toTrino(source);

        assertThat(mapped.getDatabaseName()).isEqualTo("sales");
        assertThat(mapped.getLocation()).contains("gs://warehouse/sales.db");
        assertThat(mapped.getComment()).contains("sales data");
        assertThat(mapped.getParameters()).containsEntry("owner_team", "analytics");
    }

    @Test
    void leavesOwnerEmptyBecauseLakehouseHasNone()
    {
        Database mapped = DatabaseMapper.toTrino(TestingLakehouseObjects.database("sales"));

        assertThat(mapped.getOwnerName()).isEmpty();
        assertThat(mapped.getOwnerType()).isEmpty();
    }

    @Test
    void reportsAnAbsentFieldAsEmptyRatherThanBlank()
    {
        Database mapped = DatabaseMapper.toTrino(HiveDatabase.newBuilder()
                .setName(NamespaceName.format(PROJECT, CATALOG, "sales"))
                .build());

        assertThat(mapped.getLocation()).isEmpty();
        assertThat(mapped.getComment()).isEmpty();
        assertThat(mapped.getParameters()).isEmpty();
    }
}
