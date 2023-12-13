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
package io.trino.plugin.iceberg.util;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;

import java.util.Map;

import static java.lang.String.format;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

// based on org.apache.iceberg.LocationProviders.DefaultLocationProvider
public class DefaultLocationProvider
        implements LocationProvider
{
    private final String dataLocation;

    public DefaultLocationProvider(String tableLocation, Map<String, String> properties)
    {
        this.dataLocation = stripTrailingSlash(dataLocation(properties, tableLocation));
    }

    @SuppressWarnings("deprecation")
    private static String dataLocation(Map<String, String> properties, String tableLocation)
    {
        String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
        if (dataLocation == null) {
            dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
            if (dataLocation == null) {
                dataLocation = format("%s/data", stripTrailingSlash(tableLocation));
            }
        }
        return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename)
    {
        return "%s/%s/%s".formatted(dataLocation, spec.partitionToPath(partitionData), filename);
    }

    @Override
    public String newDataLocation(String filename)
    {
        return "%s/%s".formatted(dataLocation, filename);
    }
}
