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
package io.trino.plugin.resourcegroups.db;

import io.airlift.units.Duration;
import org.jdbi.v3.core.result.RowReducer;
import org.jdbi.v3.core.result.RowView;

import java.util.Optional;
import java.util.stream.Stream;

public class ResourceGroupGlobalPropertiesReducer
        implements RowReducer<ResourceGroupGlobalProperties.Builder, ResourceGroupGlobalProperties>
{
    @Override
    public ResourceGroupGlobalProperties.Builder container()
    {
        return ResourceGroupGlobalProperties.builder();
    }

    @Override
    public void accumulate(ResourceGroupGlobalProperties.Builder container, RowView rowView)
    {
        String name = rowView.getColumn("name", String.class);
        Optional<Duration> value = Optional.ofNullable(rowView.getColumn("value", String.class)).map(Duration::valueOf);

        switch (name) {
            case "cpu_quota_period":
                container.setCpuQuotaPeriod(value);
                break;
            case "physical_data_scan_quota_period":
                container.setPhysicalDataScanQuotaPeriod(value);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized resource group global property: " + name);
        }
    }

    @Override
    public Stream<ResourceGroupGlobalProperties> stream(ResourceGroupGlobalProperties.Builder container)
    {
        return Stream.of(container.build());
    }
}
