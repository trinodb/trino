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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_READ_TIMEOUT_MILLIS;

public class TestIcebergNessieCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergNessieCatalogConfig.class)
                .setDefaultWarehouseDir(null)
                .setServerUri(null)
                .setDefaultReferenceName("main")
                .setCompressionEnabled(true)
                .setConnectionTimeout(new Duration(DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS))
                .setReadTimeout(new Duration(DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS)));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.nessie-catalog.default-warehouse-dir", "/tmp")
                .put("iceberg.nessie-catalog.uri", "http://localhost:xxx/api/v1")
                .put("iceberg.nessie-catalog.ref", "someRef")
                .put("iceberg.nessie-catalog.enable-compression", "false")
                .put("iceberg.nessie-catalog.connection-timeout", "2s")
                .put("iceberg.nessie-catalog.read-timeout", "5m")
                .buildOrThrow();

        IcebergNessieCatalogConfig expected = new IcebergNessieCatalogConfig()
                .setDefaultWarehouseDir("/tmp")
                .setServerUri(URI.create("http://localhost:xxx/api/v1"))
                .setDefaultReferenceName("someRef")
                .setCompressionEnabled(false)
                .setConnectionTimeout(new Duration(2, TimeUnit.SECONDS))
                .setReadTimeout(new Duration(5, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
