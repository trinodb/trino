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
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class IcebergMaterializedViewProperties
{
    public static final String SEPARATE_STORAGE_TABLE = "separate_storage_table";

    private final List<PropertyMetadata<?>> materializedViewProperties;

    @Inject
    public IcebergMaterializedViewProperties(
            @EnableMaterializedViewSeparateStorageTable boolean enableMaterializedViewSeparateStorageTable, // for tests of legacy materialized views
            IcebergTableProperties tableProperties)
    {
        ImmutableList.Builder<PropertyMetadata<?>> materializedViewProperties = ImmutableList.builder();
        if (enableMaterializedViewSeparateStorageTable) {
            materializedViewProperties.add(booleanProperty(
                    SEPARATE_STORAGE_TABLE,
                    "Create separate storage table for materialized view",
                    false,
                    true));
        }
        // Materialized view should allow configuring all the supported iceberg table properties for the storage table
        materializedViewProperties.addAll(tableProperties.getTableProperties());
        this.materializedViewProperties = materializedViewProperties.build();
    }

    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties;
    }

    /**
     * @deprecated TODO creation of legacy materialized views with separate storage table to test code
     */
    @Deprecated
    public static boolean isSeparateStorageTable(Map<String, Object> materializedViewProperties)
    {
        return Optional.ofNullable(materializedViewProperties.get(SEPARATE_STORAGE_TABLE))
                .map(Boolean.class::cast)
                .orElse(false);
    }
}
