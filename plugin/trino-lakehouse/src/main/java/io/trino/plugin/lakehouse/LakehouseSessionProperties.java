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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hudi.HudiSessionProperties;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.spi.session.PropertyMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LakehouseSessionProperties
{
    private static final Set<String> IGNORED_DESCRIPTIONS = ImmutableSet.<String>builder()
            .add("compression_codec")
            .add("ignore_absent_partitions")
            .add("minimum_assigned_split_weight")
            .add("query_partition_filter_required")
            .add("size_based_split_weights_enabled")
            .add("sorted_writing_enabled")
            .add("timestamp_precision")
            .build();

    private static final Set<String> IGNORED_DEFAULT_VALUES = ImmutableSet.<String>builder()
            .add("compression_codec")
            .add("dynamic_filtering_wait_timeout")
            .add("max_split_size")
            .build();

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public LakehouseSessionProperties(
            HiveSessionProperties hiveSessionProperties,
            IcebergSessionProperties icebergSessionProperties,
            DeltaLakeSessionProperties deltaSessionProperties,
            HudiSessionProperties hudiSessionProperties)
    {
        List<PropertyMetadata<?>> allProperties = Streams.concat(
                hiveSessionProperties.getSessionProperties().stream(),
                icebergSessionProperties.getSessionProperties().stream(),
                deltaSessionProperties.getSessionProperties().stream(),
                hudiSessionProperties.getSessionProperties().stream()).toList();

        Map<String, PropertyMetadata<?>> properties = new HashMap<>();

        for (PropertyMetadata<?> property : allProperties) {
            PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
            if (existing == null) {
                continue;
            }
            if (!existing.getDescription().equals(property.getDescription()) && !IGNORED_DESCRIPTIONS.contains(property.getName())) {
                throw new VerifyException("Conflicting session property '%s' with different descriptions: %s <> %s"
                        .formatted(property.getName(), existing.getDescription(), property.getDescription()));
            }
            if (!existing.getJavaType().equals(property.getJavaType())) {
                throw new VerifyException("Conflicting session property '%s' with different Java types: %s <> %s"
                        .formatted(property.getName(), existing.getJavaType(), property.getJavaType()));
            }
            if (!existing.getSqlType().equals(property.getSqlType())) {
                throw new VerifyException("Conflicting session property '%s' with different SQL types: %s <> %s"
                        .formatted(property.getName(), existing.getSqlType(), property.getSqlType()));
            }
            if (!Objects.equals(existing.getDefaultValue(), property.getDefaultValue()) && !IGNORED_DEFAULT_VALUES.contains(property.getName())) {
                throw new VerifyException("Conflicting session property '%s' with different default values: %s <> %s"
                        .formatted(property.getName(), existing.getDefaultValue(), property.getDefaultValue()));
            }
            if (existing.isHidden() != property.isHidden() && !property.getName().equals("timestamp_precision")) {
                throw new VerifyException("Conflicting session property '%s' with different hidden flags: %s <> %s"
                        .formatted(property.getName(), existing.isHidden(), property.isHidden()));
            }
        }

        this.sessionProperties = ImmutableList.copyOf(properties.values());
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
