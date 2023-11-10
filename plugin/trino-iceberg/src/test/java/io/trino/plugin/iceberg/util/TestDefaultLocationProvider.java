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

class TestDefaultLocationProvider
{
    @Test
    void testDefault()
    {
        LocationProvider provider = new DefaultLocationProvider("s3://table-location/", Map.of());

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://table-location/data/test");
    }

    @Test
    void testDefaultWithCustomDataLocation()
    {
        LocationProvider provider = new DefaultLocationProvider("s3://table-location/", Map.of(
                TableProperties.WRITE_DATA_LOCATION, "s3://write-location/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://write-location/test");
    }

    @SuppressWarnings("deprecation")
    @Test
    void testDefaultPropertyResolution()
    {
        LocationProvider provider = new DefaultLocationProvider("s3://table-location/", Map.of(
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://folder-location/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://folder-location/test");

        provider = new DefaultLocationProvider("s3://table-location/", Map.of(
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://folder-location/",
                TableProperties.WRITE_DATA_LOCATION, "s3://data-location/"));

        assertThat(provider.newDataLocation("test"))
                .isEqualTo("s3://data-location/test");
    }
}
