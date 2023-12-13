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

import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
}
