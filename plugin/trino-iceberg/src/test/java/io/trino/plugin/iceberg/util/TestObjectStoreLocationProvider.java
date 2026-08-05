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

import io.trino.plugin.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.iceberg.TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

class TestObjectStoreLocationProvider
{
    @Test
    void testObjectStorageWithinTableLocation()
    {
        LocationProvider provider = new ObjectStoreLocationProvider("s3://table-location/xyz", Map.of());

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://table-location/xyz/data/E9Jrug/test");
    }

    @Test
    void testObjectStorageWithContextEmpty()
    {
        LocationProvider provider = new ObjectStoreLocationProvider(
                "s3://table-location/",
                Map.of(TableProperties.WRITE_DATA_LOCATION, "s3://data-location/write/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://data-location/write/E9Jrug/test");
    }

    @Test
    void testObjectStorageWithContextTable()
    {
        LocationProvider provider = new ObjectStoreLocationProvider(
                "s3://table-location/xyz",
                Map.of(TableProperties.WRITE_DATA_LOCATION, "s3://data-location/write/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://data-location/write/E9Jrug/xyz/test");
    }

    @Test
    void testObjectStorageWithContextDatabaseTable()
    {
        LocationProvider provider = new ObjectStoreLocationProvider(
                "s3://table-location/abc/xyz",
                Map.of(TableProperties.WRITE_DATA_LOCATION, "s3://data-location/write/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://data-location/write/E9Jrug/abc/xyz/test");
    }

    @Test
    void testObjectStorageWithContextPrefixDatabaseTable()
    {
        LocationProvider provider = new ObjectStoreLocationProvider(
                "s3://table-location/hello/world/abc/xyz",
                Map.of(TableProperties.WRITE_DATA_LOCATION, "s3://data-location/write/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://data-location/write/E9Jrug/abc/xyz/test");
    }

    @SuppressWarnings("deprecation")
    @Test
    void testObjectStoragePropertyResolution()
    {
        LocationProvider provider = new ObjectStoreLocationProvider("s3://table-location/", Map.of(
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://folder-location/xyz"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://folder-location/xyz/E9Jrug/test");

        provider = new ObjectStoreLocationProvider("s3://table-location/", Map.of(
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://folder-location/abc",
                TableProperties.OBJECT_STORE_PATH, "s3://object-location/xyz"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://object-location/xyz/E9Jrug/test");

        provider = new ObjectStoreLocationProvider("s3://table-location/", Map.of(
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://folder-location/abc",
                TableProperties.OBJECT_STORE_PATH, "s3://object-location/abc",
                TableProperties.WRITE_DATA_LOCATION, "s3://data-location/xyz"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://data-location/xyz/E9Jrug/test");
    }

    @Test
    void testObjectStoragePartitionedPathsEnabled()
    {
        Schema schema = new Schema(optional(1, "part", Types.IntegerType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("part").build();

        ObjectStoreLocationProvider provider = new ObjectStoreLocationProvider("s3://table-location/", Map.of(WRITE_OBJECT_STORE_PARTITIONED_PATHS, "true"));

        assertThat(provider.newDataLocation(partitionSpec, new PartitionData(new Integer[] {1}), "test"))
                .isEqualTo("s3://table-location/data/mJ158A/part=1/test");
        assertThat(provider.newDataLocation(partitionSpec, new PartitionData(new Integer[] {2}), "test"))
                .isEqualTo("s3://table-location/data/otWd0w/part=2/test");

        assertThat(provider.newDataLocation("test-a"))
                .isEqualTo("s3://table-location/data/rI2Q2Q/test-a");
        assertThat(provider.newDataLocation("test-b"))
                .isEqualTo("s3://table-location/data/F9UWCA/test-b");
    }

    @Test
    void testObjectStoragePartitionedPathsDisabled()
    {
        Schema schema = new Schema(optional(1, "part", Types.IntegerType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("part").build();

        LocationProvider provider = new ObjectStoreLocationProvider("s3://table-location/", Map.of(WRITE_OBJECT_STORE_PARTITIONED_PATHS, "false"));

        assertThat(provider.newDataLocation(partitionSpec, new PartitionData(new Integer[] {1}), "test"))
                .isEqualTo("s3://table-location/data/E9Jrug-test");
        assertThat(provider.newDataLocation(partitionSpec, new PartitionData(new Integer[] {2}), "test"))
                .isEqualTo("s3://table-location/data/E9Jrug-test");

        assertThat(provider.newDataLocation("test-a"))
                .isEqualTo("s3://table-location/data/rI2Q2Q-test-a");
        assertThat(provider.newDataLocation("test-b"))
                .isEqualTo("s3://table-location/data/F9UWCA-test-b");
    }
}
