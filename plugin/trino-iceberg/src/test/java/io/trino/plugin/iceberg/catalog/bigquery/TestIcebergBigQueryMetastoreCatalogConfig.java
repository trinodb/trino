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
package io.trino.plugin.iceberg.catalog.bigquery;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestIcebergBigQueryMetastoreCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergBigQueryMetastoreCatalogConfig.class)
                .setProjectID(null)
                .setLocation(null)
                .setListAllTables(null)
                .setWarehouse(null)
                .setJsonKeyFilePath(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.bqms-catalog.project-id", "test-project-id")
                .put("iceberg.bqms-catalog.location", "us-central1")
                .put("iceberg.bqms-catalog.list-all-tables", "true")
                .put("iceberg.bqms-catalog.warehouse", "gs://test-bucket/warehouse")
                .put("iceberg.bqms-catalog.json-key-file-path", "/path/to/service-account.json")
                .buildOrThrow();

        IcebergBigQueryMetastoreCatalogConfig expected = new IcebergBigQueryMetastoreCatalogConfig()
                .setProjectID("test-project-id")
                .setLocation("us-central1")
                .setListAllTables("true")
                .setWarehouse("gs://test-bucket/warehouse")
                .setJsonKeyFilePath("/path/to/service-account.json");

        assertFullMapping(properties, expected);
    }
}
