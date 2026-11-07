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

import com.google.cloud.biglake.hive.v1.Partition;
import io.trino.metastore.Column;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.MetastoreErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.field;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.storage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPartitionMapper
{
    @Test
    void mapsNamesValuesStorageColumnsAndParameters()
    {
        Partition source = Partition.newBuilder()
                .addAllValues(List.of("emea", "2026"))
                .setStorageDescriptor(storage("gs://warehouse/orders/region=emea/year=2026", List.of(field("id", "int"))))
                .putAllParameters(Map.of("numFiles", "3"))
                .build();

        io.trino.metastore.Partition mapped = PartitionMapper.toTrino(source, "sales", "orders");

        assertThat(mapped.getDatabaseName()).isEqualTo("sales");
        assertThat(mapped.getTableName()).isEqualTo("orders");
        assertThat(mapped.getValues()).containsExactly("emea", "2026");
        assertThat(mapped.getStorage().getOptionalLocation()).contains("gs://warehouse/orders/region=emea/year=2026");
        assertThat(mapped.getColumns()).extracting(Column::getName).containsExactly("id");
        assertThat(mapped.getColumns()).extracting(Column::getType).containsExactly(HIVE_INT);
        assertThat(mapped.getParameters()).isEqualTo(Map.of("numFiles", "3"));
    }

    @Test
    void keepsTheOrderOfThePartitionValues()
    {
        Partition source = TestingLakehouseObjects.partition("orders", List.of("b", "a", "c"));

        assertThat(PartitionMapper.toTrino(source, "sales", "orders").getValues()).containsExactly("b", "a", "c");
    }

    @Test
    void rejectsAPartitionWithoutAStorageDescriptor()
    {
        Partition source = Partition.newBuilder().addAllValues(List.of("emea")).build();

        assertThatThrownBy(() -> PartitionMapper.toTrino(source, "sales", "orders"))
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, HIVE_UNSUPPORTED_FORMAT))
                .hasMessageContaining("sales.orders")
                .hasMessageContaining("emea");
    }

    private static void assertErrorCode(TrinoException thrown, ErrorCodeSupplier expected)
    {
        assertThat(thrown.getErrorCode()).isEqualTo(expected.toErrorCode());
    }
}
