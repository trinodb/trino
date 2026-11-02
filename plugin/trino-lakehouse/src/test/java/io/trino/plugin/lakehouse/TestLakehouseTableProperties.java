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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeTableProperties;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hudi.HudiTableProperties;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergTableProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.lakehouse.TableType.DELTA;
import static io.trino.plugin.lakehouse.TableType.HIVE;
import static io.trino.plugin.lakehouse.TableType.HUDI;
import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLakehouseTableProperties
{
    @Test
    void testIcebergObjectStoreLayoutDefault()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(false));

        assertThat(properties.unwrapProperties(ImmutableMap.of("type", ICEBERG)))
                .containsEntry("object_store_layout_enabled", true);
        assertThat(properties.unwrapProperties(ImmutableMap.of("type", DELTA)))
                .containsEntry("object_store_layout_enabled", false);
    }

    @Test
    void testDeltaObjectStoreLayoutDefault()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(false),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(true));

        assertThat(properties.unwrapProperties(ImmutableMap.of("type", ICEBERG)))
                .containsEntry("object_store_layout_enabled", false);
        assertThat(properties.unwrapProperties(ImmutableMap.of("type", DELTA)))
                .containsEntry("object_store_layout_enabled", true);
    }

    @Test
    void testObjectStoreLayoutDisabledExplicitly()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(true));

        assertThat(properties.unwrapProperties(ImmutableMap.of("type", ICEBERG, "object_store_layout_enabled", false)))
                .containsEntry("object_store_layout_enabled", false);
        assertThat(properties.unwrapProperties(ImmutableMap.of("type", DELTA, "object_store_layout_enabled", false)))
                .containsEntry("object_store_layout_enabled", false);
    }

    @Test
    void testObjectStoreLayoutEnabledExplicitly()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(false),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(false));

        assertThat(properties.unwrapProperties(ImmutableMap.of("type", ICEBERG, "object_store_layout_enabled", true)))
                .containsEntry("object_store_layout_enabled", true);
        assertThat(properties.unwrapProperties(ImmutableMap.of("type", DELTA, "object_store_layout_enabled", true)))
                .containsEntry("object_store_layout_enabled", true);
    }

    @Test
    void testObjectStoreLayoutDefaultForOtherTableTypes()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(true));

        assertThat(properties.unwrapProperties(ImmutableMap.of("type", HIVE)))
                .doesNotContainKey("object_store_layout_enabled");
        assertThat(properties.unwrapProperties(ImmutableMap.of("type", HUDI)))
                .doesNotContainKey("object_store_layout_enabled");
    }

    @Test
    void testResetObjectStoreLayoutToDefault()
    {
        Map<String, Optional<Object>> updates = ImmutableMap.of("object_store_layout_enabled", Optional.empty());
        LakehouseTableProperties icebergEnabled = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(false));
        assertThat(icebergEnabled.unwrapUpdatedProperties(ICEBERG, updates))
                .containsEntry("object_store_layout_enabled", Optional.of(true));
        assertThat(icebergEnabled.unwrapUpdatedProperties(DELTA, updates))
                .containsEntry("object_store_layout_enabled", Optional.of(false));

        LakehouseTableProperties deltaEnabled = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(false),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(true));
        assertThat(deltaEnabled.unwrapUpdatedProperties(ICEBERG, updates))
                .containsEntry("object_store_layout_enabled", Optional.of(false));
        assertThat(deltaEnabled.unwrapUpdatedProperties(DELTA, updates))
                .containsEntry("object_store_layout_enabled", Optional.of(true));
    }

    @Test
    void testExplicitObjectStoreLayoutUpdate()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(false));

        assertThat(properties.unwrapUpdatedProperties(ICEBERG, ImmutableMap.of("object_store_layout_enabled", Optional.of(false))))
                .containsEntry("object_store_layout_enabled", Optional.of(false));
        assertThat(properties.unwrapUpdatedProperties(DELTA, ImmutableMap.of("object_store_layout_enabled", Optional.of(true))))
                .containsEntry("object_store_layout_enabled", Optional.of(true));
    }

    @Test
    void testUnrelatedPropertyUpdate()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(true));
        Map<String, Optional<Object>> updates = ImmutableMap.of("format", Optional.of("ORC"));

        assertThat(properties.unwrapUpdatedProperties(ICEBERG, updates)).isEqualTo(updates);
        assertThat(properties.unwrapUpdatedProperties(DELTA, updates)).isEqualTo(updates);
    }

    @Test
    void testResetObjectStoreLayoutForOtherTableTypes()
    {
        LakehouseTableProperties properties = createTableProperties(
                new IcebergConfig().setObjectStoreLayoutEnabled(true),
                new DeltaLakeConfig().setObjectStoreLayoutEnabled(true));
        Map<String, Optional<Object>> updates = ImmutableMap.of("object_store_layout_enabled", Optional.empty());

        assertThat(properties.unwrapUpdatedProperties(HIVE, updates)).isEqualTo(updates);
        assertThat(properties.unwrapUpdatedProperties(HUDI, updates)).isEqualTo(updates);
    }

    private static LakehouseTableProperties createTableProperties(IcebergConfig icebergConfig, DeltaLakeConfig deltaConfig)
    {
        return new LakehouseTableProperties(
                new HiveTableProperties(new HiveConfig(), new OrcWriterConfig(), TESTING_TYPE_MANAGER),
                new IcebergTableProperties(icebergConfig, new OrcWriterConfig(), TESTING_TYPE_MANAGER),
                new DeltaLakeTableProperties(deltaConfig),
                new HudiTableProperties(),
                new LakehouseConfig());
    }
}
