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
package io.trino.plugin.hidden.partitioning;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class HiddenPartitioningTableProperties
{
    public static final String PARTITION_SPEC_PROPERTY = "partition_spec";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HiddenPartitioningTableProperties(HiveTableProperties hiveTableProperties)
    {
        ImmutableList.Builder<PropertyMetadata<?>> builder = ImmutableList.builder();
        builder.addAll(hiveTableProperties.getTableProperties());
        builder.add(stringProperty(PARTITION_SPEC_PROPERTY, "Partition spec for hidden partitioning", null, false));
        tableProperties = builder.build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getPartitionSpecJson(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(PARTITION_SPEC_PROPERTY);
    }
}
