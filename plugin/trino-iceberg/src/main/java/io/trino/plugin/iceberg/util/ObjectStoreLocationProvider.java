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

import com.google.common.hash.HashFunction;
import io.trino.filesystem.Location;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;

import java.util.Base64;
import java.util.Map;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

// based on org.apache.iceberg.LocationProviders.ObjectStoreLocationProvider
public class ObjectStoreLocationProvider
        implements LocationProvider
{
    private static final HashFunction HASH_FUNC = murmur3_32_fixed();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private final String storageLocation;
    private final String context;

    public ObjectStoreLocationProvider(String tableLocation, Map<String, String> properties)
    {
        this.storageLocation = stripTrailingSlash(dataLocation(properties, tableLocation));
        // if the storage location is within the table prefix, don't add table and database name context
        this.context = storageLocation.startsWith(stripTrailingSlash(tableLocation)) ? null : pathContext(tableLocation);
    }

    @SuppressWarnings("deprecation")
    private static String dataLocation(Map<String, String> properties, String tableLocation)
    {
        String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
        if (dataLocation == null) {
            dataLocation = properties.get(TableProperties.OBJECT_STORE_PATH);
            if (dataLocation == null) {
                dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
                if (dataLocation == null) {
                    dataLocation = "%s/data".formatted(stripTrailingSlash(tableLocation));
                }
            }
        }
        return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename)
    {
        return newDataLocation("%s/%s".formatted(spec.partitionToPath(partitionData), filename));
    }

    @Override
    public String newDataLocation(String filename)
    {
        String hash = computeHash(filename);
        if (context != null) {
            return "%s/%s/%s/%s".formatted(storageLocation, hash, context, filename);
        }
        return "%s/%s/%s".formatted(storageLocation, hash, filename);
    }

    private static String pathContext(String tableLocation)
    {
        Location location;
        String name;
        try {
            location = Location.of(stripTrailingSlash(tableLocation));
            name = location.fileName();
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            return null;
        }

        try {
            String parent = stripTrailingSlash(location.parentDirectory().path());
            parent = parent.substring(parent.lastIndexOf('/') + 1);
            return "%s/%s".formatted(parent, name);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            return name;
        }
    }

    private static String computeHash(String fileName)
    {
        byte[] bytes = HASH_FUNC.hashString(fileName, UTF_8).asBytes();
        return BASE64_ENCODER.encodeToString(bytes);
    }
}
